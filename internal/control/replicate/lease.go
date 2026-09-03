package replicate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact/s3"
)

const formatVersion = 1

type leaseRecord struct {
	Version   int       `json:"version"`
	Holder    string    `json:"holder"`
	Epoch     uint64    `json:"epoch"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Coordinator owns the conditional writer lease and the local self-fence
// clock. Holder must be unique per controller process, not merely per host.
type Coordinator struct {
	store  Store
	holder string
	opts   Options

	opMu        sync.Mutex
	mu          sync.RWMutex
	etag        string
	record      leaseRecord
	lastSuccess time.Time
	fenced      bool
}

// NewCoordinator validates bounds but does not contact object storage.
func NewCoordinator(store Store, holder string, options Options) (*Coordinator, error) {
	if store == nil || holder == "" {
		return nil, errors.New("control replication: store and unique holder are required")
	}
	opts, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	if _, err := cleanPrefix(opts.Prefix); err != nil {
		return nil, err
	}
	return &Coordinator{store: store, holder: holder, opts: opts}, nil
}

// Acquire obtains an absent or expired lease. It probes conditional semantics
// first; an endpoint that ignores If-Match is never admitted for failover.
func (c *Coordinator) Acquire(ctx context.Context) (epoch, previous uint64, err error) {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	if err := c.store.CheckConditionalWrites(ctx); err != nil {
		return 0, 0, fmt.Errorf("control replication: conditional-write probe: %w", err)
	}
	now := c.opts.Now()
	record, etag, err := c.readLease(ctx)
	if err != nil && !isNotFound(err) {
		return 0, 0, err
	}
	put := s3.PutOptions{ContentType: "application/json"}
	if err == nil {
		previous = record.Epoch
		if now.Before(record.ExpiresAt.Add(c.opts.MaxClockSkew)) {
			return 0, previous, fmt.Errorf("%w: holder=%s epoch=%d expires=%s", ErrLeaseHeld, record.Holder, record.Epoch, record.ExpiresAt.UTC().Format(time.RFC3339Nano))
		}
		if record.Epoch == math.MaxUint64 {
			return 0, previous, errors.New("control replication: controller epoch exhausted")
		}
		put.IfMatch = etag
		epoch = record.Epoch + 1
	} else {
		put.IfNoneMatch = "*"
		epoch = 1
	}
	next := leaseRecord{Version: formatVersion, Holder: c.holder, Epoch: epoch, IssuedAt: now.UTC(), ExpiresAt: now.Add(c.opts.LeaseTTL).UTC()}
	info, err := c.putJSON(ctx, c.leaseKey(), next, put)
	if err != nil {
		if errors.Is(err, s3.ErrPreconditionFailed) {
			return 0, previous, ErrLeaseHeld
		}
		return 0, previous, err
	}
	if info.ETag == "" {
		return 0, previous, errors.New("control replication: lease write returned no ETag")
	}
	c.mu.Lock()
	c.etag, c.record, c.lastSuccess, c.fenced = info.ETag, next, now, false
	c.mu.Unlock()
	if _, err := c.CanDecide(); err != nil {
		return 0, previous, err
	}
	return epoch, previous, nil
}

// Renew conditionally extends the lease. A precondition failure immediately
// fences this controller; a transport failure prevents publication and the
// local decision gate closes once LeaseTTL elapses.
func (c *Coordinator) Renew(ctx context.Context) error {
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.RLock()
	etag, record, fenced := c.etag, c.record, c.fenced
	c.mu.RUnlock()
	if fenced || etag == "" || record.Epoch == 0 {
		return ErrFenced
	}
	now := c.opts.Now()
	next := record
	next.IssuedAt = now.UTC()
	next.ExpiresAt = now.Add(c.opts.LeaseTTL).UTC()
	info, err := c.putJSON(ctx, c.leaseKey(), next, s3.PutOptions{IfMatch: etag, ContentType: "application/json"})
	if err != nil {
		if errors.Is(err, s3.ErrPreconditionFailed) || isNotFound(err) {
			c.markFenced()
			return ErrFenced
		}
		return err
	}
	if info.ETag == "" {
		return errors.New("control replication: lease renewal returned no ETag")
	}
	c.mu.Lock()
	c.etag, c.record, c.lastSuccess = info.ETag, next, now
	c.mu.Unlock()
	return nil
}

// Guard renews immediately before an authoritative publication.
func (c *Coordinator) Guard(ctx context.Context) (uint64, error) {
	if err := c.Renew(ctx); err != nil {
		return 0, err
	}
	return c.CanDecide()
}

// CanDecide returns the current epoch only while the last successful object-
// store lease write is younger than TTL. Callers must gate every decision.
func (c *Coordinator) CanDecide() (uint64, error) {
	c.mu.RLock()
	record, last, fenced := c.record, c.lastSuccess, c.fenced
	c.mu.RUnlock()
	if fenced || last.IsZero() || record.Epoch == 0 {
		return 0, ErrFenced
	}
	age := c.opts.Now().Sub(last)
	if age < 0 || age >= c.opts.LeaseTTL {
		c.markFenced()
		return 0, ErrFenced
	}
	return record.Epoch, nil
}

// LeaseAge reports time since the last successful conditional lease write.
// A negative clock step is surfaced as age zero; CanDecide still fences it.
func (c *Coordinator) LeaseAge() time.Duration {
	c.mu.RLock()
	last := c.lastSuccess
	c.mu.RUnlock()
	if last.IsZero() {
		return 0
	}
	age := c.opts.Now().Sub(last)
	if age < 0 {
		return 0
	}
	return age
}

// Fenced reports whether this process has irreversibly lost writer authority.
func (c *Coordinator) Fenced() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fenced
}

// KeepAlive renews until ctx ends or this controller is fenced. The caller
// owns the goroutine and must join it before reporting shutdown complete.
func (c *Coordinator) KeepAlive(ctx context.Context) error {
	ticker := time.NewTicker(c.opts.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Renew(ctx); err != nil {
				if _, gateErr := c.CanDecide(); gateErr != nil {
					return errors.Join(ErrFenced, err)
				}
			}
		}
	}
}

func (c *Coordinator) markFenced() {
	c.mu.Lock()
	c.fenced = true
	c.mu.Unlock()
}

func (c *Coordinator) readLease(ctx context.Context) (leaseRecord, string, error) {
	var record leaseRecord
	info, err := readJSON(ctx, c.store, c.leaseKey(), c.opts.MaxManifestBytes, &record)
	if err != nil {
		return record, "", err
	}
	if record.Version != formatVersion || record.Holder == "" || record.Epoch == 0 || record.IssuedAt.IsZero() || !record.ExpiresAt.After(record.IssuedAt) || info.ETag == "" {
		return record, "", fmt.Errorf("%w: invalid writer lease", ErrCorruptRecoveryPoint)
	}
	return record, info.ETag, nil
}

func (c *Coordinator) putJSON(ctx context.Context, key string, value any, options s3.PutOptions) (s3.ObjectInfo, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return s3.ObjectInfo{}, err
	}
	if int64(len(payload)) > c.opts.MaxManifestBytes {
		return s3.ObjectInfo{}, errors.New("control replication: metadata exceeds configured bound")
	}
	return c.store.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), options)
}

func readJSON(ctx context.Context, store Store, key string, max int64, target any) (s3.ObjectInfo, error) {
	reader, info, err := store.OpenObject(ctx, key)
	if err != nil {
		return s3.ObjectInfo{}, err
	}
	if info.Size < 1 || info.Size > max {
		reader.Close()
		return info, fmt.Errorf("%w: metadata object is %d bytes", ErrCorruptRecoveryPoint, info.Size)
	}
	payload, readErr := io.ReadAll(io.LimitReader(reader, max+1))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return info, err
	}
	if int64(len(payload)) != info.Size {
		return info, fmt.Errorf("%w: metadata object length mismatch", ErrCorruptRecoveryPoint)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return info, fmt.Errorf("%w: decode %s: %v", ErrCorruptRecoveryPoint, key, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return info, fmt.Errorf("%w: trailing metadata in %s", ErrCorruptRecoveryPoint, key)
	}
	return info, nil
}

func isNotFound(err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var serviceError *s3.Error
	return errors.As(err, &serviceError) && serviceError.StatusCode == http.StatusNotFound
}

func (c *Coordinator) leaseKey() string { return c.key("lease.json") }

func (c *Coordinator) key(suffix string) string { return c.opts.Prefix + "/" + suffix }

func cleanPrefix(prefix string) (string, error) {
	if prefix == "" || prefix[0] == '/' || prefix[len(prefix)-1] == '/' || strings.ContainsAny(prefix, "\\\r\n") {
		return "", errors.New("control replication: prefix must be a clean relative object path")
	}
	for _, segment := range bytes.Split([]byte(prefix), []byte{'/'}) {
		if len(segment) == 0 || string(segment) == "." || string(segment) == ".." {
			return "", errors.New("control replication: prefix must be a clean relative object path")
		}
	}
	return prefix, nil
}

func epochMetadata(epoch uint64) map[string]string {
	return map[string]string{"epoch": strconv.FormatUint(epoch, 10)}
}
