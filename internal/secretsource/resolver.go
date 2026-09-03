// Package secretsource resolves external binding-secret references without
// persisting or logging the resulting values.
package secretsource

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultCacheTTL     = 5 * time.Minute
	defaultMaxEntries   = 1024
	defaultMaxBytes     = int64(1 << 20)
	defaultTimeout      = 30 * time.Second
	maximumCacheTTL     = 24 * time.Hour
	maximumCacheEntries = 64 << 10
	maximumCacheBytes   = int64(64 << 20)
	maximumTimeout      = 5 * time.Minute
	maximumSourceBytes  = 4 << 10
)

// Resolver is the narrow seam used by binding-lease issuance and doctor.
// Probe verifies a source without returning its value.
type Resolver interface {
	Resolve(context.Context, string) (string, error)
	Probe(context.Context, string) error
}

// AWSCredentials are short-lived credentials used only to sign a Secrets
// Manager request.
type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// VaultCredentials select token authentication or AppRole authentication.
// Token takes precedence when both forms are populated.
type VaultCredentials struct {
	Token    string
	RoleID   string
	SecretID string
}

// Config supplies provider credentials and bounded cache/HTTP behavior.
type Config struct {
	CacheTTL   time.Duration
	MaxEntries int
	MaxBytes   int64
	Timeout    time.Duration
	Now        func() time.Time
	HTTPClient *http.Client
	// AllowInsecureHTTP exists for loopback test fixtures only. Production
	// callers must leave it false so credentials never cross plaintext HTTP.
	AllowInsecureHTTP bool

	VaultCredentials func(context.Context) (VaultCredentials, error)
	VaultScheme      string
	VaultKVVersion   int
	VaultAppRolePath string

	AWSCredentials func(context.Context) (AWSCredentials, error)
	AWSEndpoint    string

	GCPAccessToken func(context.Context) (string, error)
	GCPEndpoint    string

	GitHubPrivateKey func(context.Context, string) ([]byte, error)
	GitHubEndpoint   string
}

// CacheStats is a value-free view of the bounded resolver cache.
type CacheStats struct {
	Entries    int
	MaxEntries int
	Bytes      int64
	MaxBytes   int64
	Inflight   int
}

// CachedResolver resolves supported sources and caches values until their
// bounded TTL. Concurrent requests for one source share one provider call.
type CachedResolver struct {
	cfg Config

	mu       sync.Mutex
	entries  map[string]*cacheEntry
	inflight map[string]*resolveCall
	bytes    int64
	clock    uint64
}

var _ Resolver = (*CachedResolver)(nil)

type cacheEntry struct {
	value   []byte
	expires time.Time
	used    uint64
}

type resolveCall struct {
	done chan struct{}
	err  error
}

// New returns a bounded external-source resolver.
func New(cfg Config) (*CachedResolver, error) {
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = defaultCacheTTL
	}
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = defaultMaxEntries
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = defaultMaxBytes
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.VaultScheme == "" {
		cfg.VaultScheme = "https"
	}
	if cfg.VaultKVVersion == 0 {
		cfg.VaultKVVersion = 2
	}
	if cfg.VaultAppRolePath == "" {
		cfg.VaultAppRolePath = "auth/approle/login"
	}
	if cfg.GCPEndpoint == "" {
		cfg.GCPEndpoint = "https://secretmanager.googleapis.com"
	}
	if cfg.GitHubEndpoint == "" {
		cfg.GitHubEndpoint = "https://api.github.com"
	}
	if cfg.CacheTTL < time.Second || cfg.CacheTTL > maximumCacheTTL || cfg.MaxEntries < 1 || cfg.MaxEntries > maximumCacheEntries || cfg.MaxBytes < 1 || cfg.MaxBytes > maximumCacheBytes || cfg.Timeout < time.Second || cfg.Timeout > maximumTimeout {
		return nil, errors.New("secretsource: cache or request bounds are invalid")
	}
	if cfg.VaultScheme != "https" && cfg.VaultScheme != "http" {
		return nil, errors.New("secretsource: Vault scheme must be http or https")
	}
	if cfg.VaultScheme == "http" && !cfg.AllowInsecureHTTP {
		return nil, errors.New("secretsource: plaintext Vault transport is disabled")
	}
	if cfg.VaultKVVersion != 1 && cfg.VaultKVVersion != 2 {
		return nil, errors.New("secretsource: Vault KV version must be 1 or 2")
	}
	if !safeSegments(strings.Split(strings.Trim(cfg.VaultAppRolePath, "/"), "/")) {
		return nil, errors.New("secretsource: Vault AppRole path is invalid")
	}
	return &CachedResolver{cfg: cfg, entries: make(map[string]*cacheEntry), inflight: make(map[string]*resolveCall)}, nil
}

// ValidateSource rejects literal values and malformed or unsupported schemes.
func ValidateSource(source string) error {
	if len(source) == 0 || len(source) > maximumSourceBytes {
		return errors.New("secretsource: binding source length is invalid")
	}
	switch {
	case strings.HasPrefix(source, "env://"):
		_, err := parseEnvSource(source)
		return err
	case strings.HasPrefix(source, "file://"):
		_, err := parseFileSource(source)
		return err
	case strings.HasPrefix(source, "vault://"):
		_, err := parseVaultSource(source)
		return err
	case strings.HasPrefix(source, "awssm://"):
		_, err := parseAWSSource(source)
		return err
	case strings.HasPrefix(source, "gcpsm://"):
		_, err := parseGCPSource(source)
		return err
	case strings.HasPrefix(source, "github-app://"):
		_, err := parseGitHubSource(source)
		return err
	default:
		return errors.New("secretsource: literal or unsupported binding source")
	}
}

// Resolve returns a secret value. The caller must keep it out of persistence,
// logs, diagnostics, and error messages.
func (r *CachedResolver) Resolve(ctx context.Context, source string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := ValidateSource(source); err != nil {
		return "", err
	}
	for {
		now := r.cfg.Now()
		r.mu.Lock()
		if entry := r.entries[source]; entry != nil && now.Before(entry.expires) {
			r.clock++
			entry.used = r.clock
			value := string(entry.value)
			r.mu.Unlock()
			return value, nil
		}
		r.removeExpiredLocked(now)
		if call := r.inflight[source]; call != nil {
			done := call.done
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-done:
				if call.err != nil {
					return "", call.err
				}
				continue
			}
		}

		call := &resolveCall{done: make(chan struct{})}
		if len(r.inflight) >= r.cfg.MaxEntries {
			r.mu.Unlock()
			return "", errors.New("secretsource: too many source resolutions in progress")
		}
		r.inflight[source] = call
		r.mu.Unlock()

		value, providerExpiry, err := r.resolveUncached(ctx, source)
		r.mu.Lock()
		delete(r.inflight, source)
		if err == nil {
			expires := r.cfg.Now().Add(r.cfg.CacheTTL)
			if !providerExpiry.IsZero() && providerExpiry.Before(expires) {
				expires = providerExpiry
			}
			if int64(len(value)) > r.cfg.MaxBytes {
				err = errors.New("secretsource: resolved value exceeds cache byte limit")
			} else {
				r.insertLocked(source, value, expires)
			}
		}
		call.err = err
		close(call.done)
		r.mu.Unlock()
		zero(value)
		if err != nil {
			return "", err
		}
		// Read back a private string from the cache so the temporary byte slice
		// can be zeroed on every path.
	}
}

// Probe performs an uncached authenticated read and discards the value. It
// returns unavailable/provider failures as errors rather than healthy.
func (r *CachedResolver) Probe(ctx context.Context, source string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateSource(source); err != nil {
		return err
	}
	r.mu.Lock()
	if call := r.inflight[source]; call != nil {
		done := call.done
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return call.err
		}
	}

	call := &resolveCall{done: make(chan struct{})}
	if len(r.inflight) >= r.cfg.MaxEntries {
		r.mu.Unlock()
		return errors.New("secretsource: too many source probes in progress")
	}
	r.inflight[source] = call
	r.mu.Unlock()

	value, _, err := r.resolveUncached(ctx, source)
	zero(value)
	r.mu.Lock()
	delete(r.inflight, source)
	call.err = err
	close(call.done)
	r.mu.Unlock()
	return err
}

// Stats reports cache capacity without exposing source names or values.
func (r *CachedResolver) Stats() CacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeExpiredLocked(r.cfg.Now())
	return CacheStats{Entries: len(r.entries), MaxEntries: r.cfg.MaxEntries, Bytes: r.bytes, MaxBytes: r.cfg.MaxBytes, Inflight: len(r.inflight)}
}

// Purge removes and zeroes every cached secret.
func (r *CachedResolver) Purge() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, entry := range r.entries {
		zero(entry.value)
		delete(r.entries, key)
	}
	r.bytes = 0
}

func (r *CachedResolver) resolveUncached(ctx context.Context, source string) ([]byte, time.Time, error) {
	requestCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	switch {
	case strings.HasPrefix(source, "env://"):
		name, err := parseEnvSource(source)
		if err != nil {
			return nil, time.Time{}, err
		}
		value, ok := os.LookupEnv(name)
		if !ok || value == "" {
			return nil, time.Time{}, errors.New("secretsource: environment source is unavailable")
		}
		return []byte(value), time.Time{}, nil
	case strings.HasPrefix(source, "file://"):
		path, err := parseFileSource(source)
		if err != nil {
			return nil, time.Time{}, err
		}
		value, err := readSecretFile(path, r.cfg.MaxBytes)
		return value, time.Time{}, err
	case strings.HasPrefix(source, "vault://"):
		return r.resolveVault(requestCtx, source)
	case strings.HasPrefix(source, "awssm://"):
		return r.resolveAWS(requestCtx, source)
	case strings.HasPrefix(source, "gcpsm://"):
		return r.resolveGCP(requestCtx, source)
	case strings.HasPrefix(source, "github-app://"):
		return r.resolveGitHub(requestCtx, source)
	default:
		return nil, time.Time{}, errors.New("secretsource: unsupported source")
	}
}

func (r *CachedResolver) insertLocked(source string, value []byte, expires time.Time) {
	if old := r.entries[source]; old != nil {
		r.bytes -= int64(len(old.value))
		zero(old.value)
	}
	for len(r.entries) >= r.cfg.MaxEntries || r.bytes+int64(len(value)) > r.cfg.MaxBytes {
		var oldestKey string
		var oldestUsed uint64 = ^uint64(0)
		for key, entry := range r.entries {
			if entry.used < oldestUsed {
				oldestKey, oldestUsed = key, entry.used
			}
		}
		if oldestKey == "" {
			break
		}
		entry := r.entries[oldestKey]
		r.bytes -= int64(len(entry.value))
		zero(entry.value)
		delete(r.entries, oldestKey)
	}
	r.clock++
	copyValue := append([]byte(nil), value...)
	r.entries[source] = &cacheEntry{value: copyValue, expires: expires, used: r.clock}
	r.bytes += int64(len(copyValue))
}

func (r *CachedResolver) removeExpiredLocked(now time.Time) {
	for source, entry := range r.entries {
		if !now.Before(entry.expires) {
			r.bytes -= int64(len(entry.value))
			zero(entry.value)
			delete(r.entries, source)
		}
	}
}

func parseEnvSource(source string) (string, error) {
	name := strings.TrimPrefix(source, "env://")
	if name == "" || !envNameStart(name[0]) {
		return "", errors.New("secretsource: malformed environment source")
	}
	for i := 1; i < len(name); i++ {
		if !envNameStart(name[i]) && (name[i] < '0' || name[i] > '9') {
			return "", errors.New("secretsource: malformed environment source")
		}
	}
	return name, nil
}

func envNameStart(value byte) bool {
	return value == '_' || (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
}

func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

func sourceError(provider, operation string) error {
	return fmt.Errorf("secretsource: %s %s failed", provider, operation)
}
