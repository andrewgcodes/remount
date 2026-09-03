// Package s3 implements artifact storage over the S3 REST API without an SDK.
package s3

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	defaultRegion             = "us-east-1"
	defaultMultipartThreshold = int64(64 << 20)
	defaultPartSize           = int64(16 << 20)
	defaultMaxObjectBytes     = int64(8 << 30)
	defaultRequestTimeout     = 2 * time.Minute
	defaultStagingTTL         = 24 * time.Hour
	minimumPartSize           = int64(5 << 20)
	maximumPartSize           = int64(5 << 30)
	maximumObjectSize         = int64(5 << 40)
	maximumSingleCopySize     = int64(5 << 30)
	maximumParts              = 10_000
)

var (
	// ErrPreconditionFailed means an If-Match or If-None-Match condition was
	// rejected by the object store.
	ErrPreconditionFailed = errors.New("s3: precondition failed")
	// ErrConditionalUnsupported means a capability probe observed an endpoint
	// accepting a write whose precondition was false.
	ErrConditionalUnsupported = errors.New("s3: conditional writes are not enforced")
)

// Config describes one bucket and an optional namespace within it.
type Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	PathStyle       bool
	HTTPClient      *http.Client

	MultipartThreshold int64
	PartSize           int64
	MaxObjectBytes     int64
	RequestTimeout     time.Duration
	StagingTTL         time.Duration
}

// Store is an S3-backed immutable blob store and a lower-level named-object
// client. Named keys are relative to Config.Prefix.
type Store struct {
	endpoint   *url.URL
	region     string
	bucket     string
	prefix     string
	accessKey  string
	secretKey  string
	session    string
	pathStyle  bool
	httpClient *http.Client
	threshold  int64
	partSize   int64
	maxBytes   int64
	timeout    time.Duration
	stagingTTL time.Duration
	copyLimit  int64
	now        func() time.Time

	mu      sync.Mutex
	readers map[string]int
}

// New validates cfg and returns an S3 store. It does not contact the endpoint;
// callers that gate readiness should call Inventory or CleanupStaging.
func New(cfg Config) (*Store, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3: parse endpoint: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("s3: endpoint scheme must be http or https")
	}
	if endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("s3: endpoint must contain only scheme, host, and optional path")
	}
	if cfg.Bucket == "" || strings.ContainsAny(cfg.Bucket, "/\\") {
		return nil, errors.New("s3: bucket must be non-empty and must not contain a slash")
	}
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return nil, errors.New("s3: access key id and secret access key must be supplied together")
	}
	prefix, err := cleanPrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}
	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}
	maxBytes := cfg.MaxObjectBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxObjectBytes
	}
	threshold := cfg.MultipartThreshold
	if threshold == 0 {
		threshold = min(defaultMultipartThreshold, maxBytes)
	}
	partSize := cfg.PartSize
	if partSize == 0 {
		partSize = defaultPartSize
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	stagingTTL := cfg.StagingTTL
	if stagingTTL == 0 {
		stagingTTL = defaultStagingTTL
	}
	if threshold < 1 || threshold > maxBytes || partSize < minimumPartSize || partSize > maximumPartSize || maxBytes < 1 || maxBytes > maximumObjectSize || timeout < 1 || stagingTTL < 1 {
		return nil, errors.New("s3: multipart, object, request, and staging bounds must be positive and valid")
	}
	if (maxBytes+partSize-1)/partSize > maximumParts {
		return nil, fmt.Errorf("s3: MaxObjectBytes requires more than %d multipart parts", maximumParts)
	}
	client := &http.Client{}
	if cfg.HTTPClient != nil {
		*client = *cfg.HTTPClient
	}
	// A redirect can copy the session-token header to a host outside the
	// configured trust boundary. Region and addressing mistakes fail closed.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/")
	endpoint.RawPath = ""
	return &Store{
		endpoint: endpoint, region: region, bucket: cfg.Bucket, prefix: prefix,
		accessKey: cfg.AccessKeyID, secretKey: cfg.SecretAccessKey, session: cfg.SessionToken,
		pathStyle: cfg.PathStyle, httpClient: client, threshold: threshold,
		partSize: partSize, maxBytes: maxBytes, timeout: timeout,
		stagingTTL: stagingTTL, copyLimit: maximumSingleCopySize,
		now: time.Now, readers: make(map[string]int),
	}, nil
}

func cleanPrefix(prefix string) (string, error) {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "", nil
	}
	clean := path.Clean(prefix)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != prefix {
		return "", errors.New("s3: prefix must be a clean relative object-key prefix")
	}
	return clean, nil
}

func (s *Store) physicalKey(key string) (string, error) {
	key = strings.TrimPrefix(key, "/")
	if key == "" || strings.ContainsRune(key, '\x00') {
		return "", errors.New("s3: object key must be non-empty")
	}
	if s.prefix == "" {
		return key, nil
	}
	return s.prefix + "/" + key, nil
}

func (s *Store) relativeKey(key string) (string, bool) {
	if s.prefix == "" {
		return key, true
	}
	prefix := s.prefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	return strings.TrimPrefix(key, prefix), true
}

func (s *Store) pin(key string) {
	s.mu.Lock()
	s.readers[key]++
	s.mu.Unlock()
}

func (s *Store) unpin(key string) {
	s.mu.Lock()
	if s.readers[key] <= 1 {
		delete(s.readers, key)
	} else {
		s.readers[key]--
	}
	s.mu.Unlock()
}

func (s *Store) pinned(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readers[key] > 0
}
