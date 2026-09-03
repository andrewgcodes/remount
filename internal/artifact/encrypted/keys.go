package encrypted

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// WrappedKey is an opaque tenant key protected by a MasterKey.
type WrappedKey struct {
	KeyID      string
	Ciphertext []byte
}

// MasterKey protects tenant keys. Implementations may use a local AES key or
// a remote KMS. Associated data binds the ciphertext to its tenant and version.
type MasterKey interface {
	Wrap(ctx context.Context, plaintext, aad []byte) (WrappedKey, error)
	Unwrap(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error)
}

// AESMasterKey is a local AES-256-GCM master key suitable for env:// and
// file:// configuration. The key remains only in trusted process memory.
type AESMasterKey struct {
	id   string
	aead cipher.AEAD
}

// NewAESMasterKey creates a local master key with a non-secret version id.
func NewAESMasterKey(id string, key []byte) (*AESMasterKey, error) {
	if err := validateSegment("master key id", id); err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("encrypted artifact: master key must be 32 bytes")
	}
	owned := append([]byte(nil), key...)
	block, err := aes.NewCipher(owned)
	clear(owned)
	if err != nil {
		return nil, errors.New("encrypted artifact: invalid master key")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("encrypted artifact: invalid master key")
	}
	return &AESMasterKey{id: id, aead: aead}, nil
}

// NewEnvMasterKey loads a base64- or hex-encoded 32-byte key from envName.
// Decode errors never include the environment value.
func NewEnvMasterKey(envName, keyID string) (*AESMasterKey, error) {
	if envName == "" {
		envName = "REMOUNT_MASTER_KEY"
	}
	value, ok := os.LookupEnv(envName)
	if !ok || strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("encrypted artifact: required environment variable %s is unavailable", envName)
	}
	key, err := decodeMasterKey(value)
	if err != nil {
		return nil, fmt.Errorf("encrypted artifact: %s does not contain a 32-byte base64 or hex key", envName)
	}
	defer clear(key)
	return NewAESMasterKey(keyID, key)
}

// NewFileMasterKey loads a base64- or hex-encoded 32-byte key from a trusted
// control-plane file. Symlinks and group/world-readable files are refused.
func NewFileMasterKey(path, keyID string) (*AESMasterKey, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("encrypted artifact: master key path must be a regular file")
	}
	if st.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("encrypted artifact: master key file must not be group or world accessible")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !opened.Mode().IsRegular() || opened.Mode().Perm()&0o077 != 0 || !os.SameFile(st, opened) {
		_ = f.Close()
		return nil, errors.New("encrypted artifact: master key file changed during open")
	}
	body, readErr := io.ReadAll(io.LimitReader(f, 4097))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if len(body) > 4096 {
		clear(body)
		return nil, errors.New("encrypted artifact: master key file is too large")
	}
	key, err := decodeMasterKey(string(body))
	clear(body)
	if err != nil {
		return nil, errors.New("encrypted artifact: master key file does not contain a 32-byte base64 or hex key")
	}
	defer clear(key)
	return NewAESMasterKey(keyID, key)
}

// Wrap encrypts one tenant key.
func (k *AESMasterKey) Wrap(_ context.Context, plaintext, aad []byte) (WrappedKey, error) {
	var nonce [12]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return WrappedKey{}, err
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+k.aead.Overhead())
	out = append(out, nonce[:]...)
	out = k.aead.Seal(out, nonce[:], plaintext, aad)
	return WrappedKey{KeyID: k.id, Ciphertext: out}, nil
}

// Unwrap decrypts one tenant key and refuses a different master-key version.
func (k *AESMasterKey) Unwrap(_ context.Context, wrapped WrappedKey, aad []byte) ([]byte, error) {
	if wrapped.KeyID != k.id || len(wrapped.Ciphertext) < 12+k.aead.Overhead() {
		return nil, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	plain, err := k.aead.Open(nil, wrapped.Ciphertext[:12], wrapped.Ciphertext[12:], aad)
	if err != nil {
		return nil, fmt.Errorf("%w", ErrIntegrity)
	}
	return plain, nil
}

func decodeMasterKey(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "hex:") {
		value = strings.TrimPrefix(value, "hex:")
		decoded, err := hex.DecodeString(value)
		if err == nil && len(decoded) == 32 {
			return decoded, nil
		}
		clear(decoded)
		return nil, errors.New("invalid key")
	}
	value = strings.TrimPrefix(value, "base64:")
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		decoded, err := encoding.DecodeString(value)
		if err == nil && len(decoded) == 32 {
			return decoded, nil
		}
		clear(decoded)
	}
	decoded, err := hex.DecodeString(value)
	if err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	clear(decoded)
	return nil, errors.New("invalid key")
}

// DirectoryKeyOptions bounds retained tenant-key versions. Old versions stay
// readable; Rotate refuses at the bound until an operator proves no objects
// use a retired version and removes it through the provider's maintenance API.
type DirectoryKeyOptions struct {
	MaxVersionsPerTenant int
	MaxTenants           int
}

// DirectoryKeyProvider stores only master-wrapped tenant keys on disk. It is
// intended for the trusted control plane and supports one process writer.
type DirectoryKeyProvider struct {
	dir        string
	master     MasterKey
	max        int
	maxTenants int
	mu         sync.Mutex
}

type keyRecord struct {
	Format     int       `json:"format"`
	Tenant     string    `json:"tenant"`
	Version    string    `json:"version"`
	MasterKey  string    `json:"master_key"`
	Ciphertext string    `json:"ciphertext"`
	CreatedAt  time.Time `json:"created_at"`
}

// NewDirectoryKeyProvider opens a wrapped tenant-key repository.
func NewDirectoryKeyProvider(dir string, master MasterKey, opts DirectoryKeyOptions) (*DirectoryKeyProvider, error) {
	if master == nil {
		return nil, errors.New("encrypted artifact: master key is required")
	}
	if opts.MaxVersionsPerTenant < 0 {
		return nil, errors.New("encrypted artifact: key version limit must not be negative")
	}
	if opts.MaxVersionsPerTenant == 0 {
		opts.MaxVersionsPerTenant = 16
	}
	if opts.MaxTenants < 0 {
		return nil, errors.New("encrypted artifact: tenant key limit must not be negative")
	}
	if opts.MaxTenants == 0 {
		opts.MaxTenants = 10_000
	}
	if err := os.MkdirAll(filepath.Join(dir, "tenants"), 0o700); err != nil {
		return nil, err
	}
	p := &DirectoryKeyProvider{dir: dir, master: master, max: opts.MaxVersionsPerTenant, maxTenants: opts.MaxTenants}
	if err := p.inventory(); err != nil {
		return nil, err
	}
	return p, nil
}

// Current returns the active tenant key, initializing a new tenant on first
// use. Existing records without a current pointer are treated as an ambiguous
// failed commit and are never guessed.
func (p *DirectoryKeyProvider) Current(ctx context.Context, tenant string) (string, [32]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateSegment("tenant", tenant); err != nil {
		return "", [32]byte{}, err
	}
	version, err := p.readCurrent(tenant)
	if errors.Is(err, fs.ErrNotExist) {
		versions, listErr := p.listVersionNames(tenant)
		if listErr != nil {
			return "", [32]byte{}, listErr
		}
		if len(versions) != 0 {
			return "", [32]byte{}, fmt.Errorf("%w: current version is missing", ErrMalformed)
		}
		tenants, listErr := p.listTenants()
		if listErr != nil {
			return "", [32]byte{}, listErr
		}
		present := false
		for _, existing := range tenants {
			present = present || existing == tenant
		}
		if !present && len(tenants) >= p.maxTenants {
			return "", [32]byte{}, fmt.Errorf("encrypted artifact: maximum is %d tenants with keys", p.maxTenants)
		}
		version, err = p.createVersion(ctx, tenant)
	}
	if err != nil {
		return "", [32]byte{}, err
	}
	key, err := p.get(ctx, tenant, version)
	return version, key, err
}

// Get returns one readable tenant-key version.
func (p *DirectoryKeyProvider) Get(ctx context.Context, tenant, version string) ([32]byte, error) {
	if err := validateSegment("tenant", tenant); err != nil {
		return [32]byte{}, err
	}
	if err := validateSegment("key version", version); err != nil {
		return [32]byte{}, err
	}
	return p.get(ctx, tenant, version)
}

func (p *DirectoryKeyProvider) get(ctx context.Context, tenant, version string) ([32]byte, error) {
	var out [32]byte
	record, err := p.readRecord(tenant, version)
	if errors.Is(err, fs.ErrNotExist) {
		return out, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	if err != nil {
		return out, err
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(record.Ciphertext)
	if err != nil || len(ciphertext) > 1<<20 {
		clear(ciphertext)
		return out, fmt.Errorf("%w", ErrMalformed)
	}
	plain, err := p.master.Unwrap(ctx, WrappedKey{KeyID: record.MasterKey, Ciphertext: ciphertext}, tenantKeyAAD(tenant, version))
	clear(ciphertext)
	if err != nil {
		return out, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	defer clear(plain)
	if len(plain) != len(out) {
		return out, fmt.Errorf("%w", ErrIntegrity)
	}
	copy(out[:], plain)
	return out, nil
}

// Versions returns the current version first, then readable retired versions.
func (p *DirectoryKeyProvider) Versions(_ context.Context, tenant string) ([]KeyVersion, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateSegment("tenant", tenant); err != nil {
		return nil, err
	}
	current, err := p.readCurrent(tenant)
	if err != nil {
		return nil, err
	}
	versions, err := p.listVersionNames(tenant)
	if err != nil {
		return nil, err
	}
	out := make([]KeyVersion, 0, len(versions))
	found := false
	for _, version := range versions {
		record, err := p.readRecord(tenant, version)
		if err != nil {
			return nil, err
		}
		if version == current {
			found = true
			out = append([]KeyVersion{{Version: version, MasterKeyID: record.MasterKey}}, out...)
		} else {
			out = append(out, KeyVersion{Version: version, Retired: true, MasterKeyID: record.MasterKey})
		}
	}
	if !found {
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	return out, nil
}

// Rotate creates, durably records, and activates a fresh tenant key. Prior
// versions remain readable for migration.
func (p *DirectoryKeyProvider) Rotate(ctx context.Context, tenant string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateSegment("tenant", tenant); err != nil {
		return "", err
	}
	if _, err := p.readCurrent(tenant); err != nil {
		return "", err
	}
	versions, err := p.listVersionNames(tenant)
	if err != nil {
		return "", err
	}
	if len(versions) >= p.max {
		return "", fmt.Errorf("encrypted artifact: maximum is %d tenant key versions", p.max)
	}
	return p.createVersion(ctx, tenant)
}

// RewrapMaster decrypts a tenant key with its recorded master key and writes
// the same tenant-key version under the currently configured master key. The
// tenant key itself and all artifact ciphertext remain unchanged.
func (p *DirectoryKeyProvider) RewrapMaster(ctx context.Context, tenant, version string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateSegment("tenant", tenant); err != nil {
		return err
	}
	if err := validateSegment("key version", version); err != nil {
		return err
	}
	key, err := p.get(ctx, tenant, version)
	if err != nil {
		return err
	}
	defer erase32(&key)
	prior, err := p.readRecord(tenant, version)
	if err != nil {
		return err
	}
	wrapped, err := p.master.Wrap(ctx, key[:], tenantKeyAAD(tenant, version))
	if err != nil {
		return errors.New("encrypted artifact: master key wrap failed")
	}
	defer clear(wrapped.Ciphertext)
	if wrapped.KeyID == "" || len(wrapped.KeyID) > 1024 || len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > 1<<20 {
		return errors.New("encrypted artifact: master key returned invalid metadata")
	}
	record := keyRecord{
		Format: 1, Tenant: tenant, Version: version, MasterKey: wrapped.KeyID,
		Ciphertext: base64.RawStdEncoding.EncodeToString(wrapped.Ciphertext), CreatedAt: prior.CreatedAt,
	}
	return p.replaceRecord(record)
}

func (p *DirectoryKeyProvider) deleteRetired(_ context.Context, tenant, version string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, err := p.readCurrent(tenant)
	if err != nil {
		return err
	}
	if current == version {
		return errors.New("encrypted artifact: current key version cannot be pruned")
	}
	path := filepath.Join(p.tenantDir(tenant), "keys", version+".json")
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (p *DirectoryKeyProvider) createVersion(ctx context.Context, tenant string) (string, error) {
	version, err := newKeyVersion()
	if err != nil {
		return "", err
	}
	key, err := random32()
	if err != nil {
		return "", err
	}
	defer erase32(&key)
	wrapped, err := p.master.Wrap(ctx, key[:], tenantKeyAAD(tenant, version))
	if err != nil {
		return "", errors.New("encrypted artifact: master key wrap failed")
	}
	defer clear(wrapped.Ciphertext)
	if wrapped.KeyID == "" || len(wrapped.KeyID) > 1024 || len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > 1<<20 {
		return "", errors.New("encrypted artifact: master key returned invalid metadata")
	}
	record := keyRecord{
		Format: 1, Tenant: tenant, Version: version, MasterKey: wrapped.KeyID,
		Ciphertext: base64.RawStdEncoding.EncodeToString(wrapped.Ciphertext), CreatedAt: time.Now().UTC(),
	}
	if err := p.writeRecord(record); err != nil {
		return "", err
	}
	if err := p.writeCurrent(tenant, version); err != nil {
		return "", err
	}
	return version, nil
}

func (p *DirectoryKeyProvider) tenantDir(tenant string) string {
	return filepath.Join(p.dir, "tenants", tenant)
}

func (p *DirectoryKeyProvider) inventory() error {
	tenants, err := p.listTenants()
	if err != nil {
		return err
	}
	if len(tenants) > p.maxTenants {
		return errors.New("encrypted artifact: retained tenant keys exceed configured capacity")
	}
	removed := false
	err = filepath.WalkDir(filepath.Join(p.dir, "tenants"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".key-") {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w", ErrMalformed)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w", ErrMalformed)
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		removed = true
		return nil
	})
	if err != nil {
		return err
	}
	if removed {
		if err := syncDirectory(filepath.Join(p.dir, "tenants")); err != nil {
			return err
		}
	}
	for _, tenant := range tenants {
		versions, err := p.listVersionNames(tenant)
		if err != nil {
			return err
		}
		if len(versions) > p.max {
			return errors.New("encrypted artifact: retained tenant key versions exceed configured capacity")
		}
	}
	return nil
}

func (p *DirectoryKeyProvider) listTenants() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(p.dir, "tenants"))
	if err != nil {
		return nil, err
	}
	var tenants []string
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || validateSegment("tenant", entry.Name()) != nil {
			return nil, fmt.Errorf("%w", ErrMalformed)
		}
		tenants = append(tenants, entry.Name())
	}
	sort.Strings(tenants)
	return tenants, nil
}

func (p *DirectoryKeyProvider) readCurrent(tenant string) (string, error) {
	body, err := readBoundedFile(filepath.Join(p.tenantDir(tenant), "current"), 256)
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(string(body))
	if err := validateSegment("key version", version); err != nil {
		return "", fmt.Errorf("%w", ErrMalformed)
	}
	return version, nil
}

func (p *DirectoryKeyProvider) writeCurrent(tenant, version string) error {
	dir := p.tenantDir(tenant)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, "current"), []byte(version+"\n"), 0o600)
}

func (p *DirectoryKeyProvider) readRecord(tenant, version string) (keyRecord, error) {
	var record keyRecord
	body, err := readBoundedFile(filepath.Join(p.tenantDir(tenant), "keys", version+".json"), 1<<20)
	if err != nil {
		return record, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&record)
	var extra any
	if decodeErr == nil {
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			decodeErr = fmt.Errorf("trailing key metadata")
		}
	}
	if decodeErr != nil {
		return keyRecord{}, fmt.Errorf("%w", ErrMalformed)
	}
	if record.Format != 1 || record.Tenant != tenant || record.Version != version || record.MasterKey == "" || len(record.MasterKey) > 1024 || record.Ciphertext == "" || record.CreatedAt.IsZero() {
		return keyRecord{}, fmt.Errorf("%w", ErrMalformed)
	}
	return record, nil
}

func (p *DirectoryKeyProvider) writeRecord(record keyRecord) error {
	dir := filepath.Join(p.tenantDir(record.Tenant), "keys")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	path := filepath.Join(dir, record.Version+".json")
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%w", ErrMalformed)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return atomicWrite(path, body, 0o600)
}

func (p *DirectoryKeyProvider) replaceRecord(record keyRecord) error {
	dir := filepath.Join(p.tenantDir(record.Tenant), "keys")
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return atomicWrite(filepath.Join(dir, record.Version+".json"), body, 0o600)
}

func (p *DirectoryKeyProvider) listVersionNames(tenant string) ([]string, error) {
	dir := filepath.Join(p.tenantDir(tenant), "keys")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	versions := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(entry.Name(), ".json") {
			return nil, fmt.Errorf("%w", ErrMalformed)
		}
		version := strings.TrimSuffix(entry.Name(), ".json")
		if err := validateSegment("key version", version); err != nil {
			return nil, fmt.Errorf("%w", ErrMalformed)
		}
		versions = append(versions, version)
	}
	sort.Strings(versions)
	return versions, nil
}

func newKeyVersion() (string, error) {
	var suffix [8]byte
	if _, err := io.ReadFull(rand.Reader, suffix[:]); err != nil {
		return "", err
	}
	return "v" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + hex.EncodeToString(suffix[:]), nil
}

func tenantKeyAAD(tenant, version string) []byte {
	var buf bytes.Buffer
	buf.WriteString("remount/tenant-key/v1\x00")
	writeAADString(&buf, tenant)
	writeAADString(&buf, version)
	return buf.Bytes()
}

func atomicWrite(path string, body []byte, mode fs.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".key-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func readBoundedFile(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(f, max+1))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	return body, nil
}
