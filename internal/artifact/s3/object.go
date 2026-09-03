package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"remount.dev/remount/internal/artifact"
)

const maxErrorBody = 64 << 10

// PutOptions controls one named-object publication.
type PutOptions struct {
	IfMatch       string
	IfNoneMatch   string
	ContentType   string
	ContentSHA256 string
	Metadata      map[string]string
}

// ObjectInfo is the durable metadata returned for an S3 object.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	Metadata     map[string]string
}

// Inventory describes both durable objects and private upload staging. A
// failure to enumerate either class is returned as an error.
type Inventory struct {
	Objects          int
	Bytes            int64
	StagingObjects   int
	StagingBytes     int64
	MultipartUploads int
}

// Error is a sanitized S3 service error. It never contains credentials or
// request headers.
type Error struct {
	StatusCode int
	Code       string
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("s3: service returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("s3: %s (HTTP %d)", e.Code, e.StatusCode)
}

// PutObject atomically publishes a named object. Size must be exact. Objects
// larger than the configured threshold use bounded-memory multipart upload.
func (s *Store) PutObject(ctx context.Context, key string, r io.Reader, size int64, opts PutOptions) (ObjectInfo, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if size < 0 {
		return ObjectInfo{}, errors.New("s3: object size must be known")
	}
	if size > s.maxBytes {
		return ObjectInfo{}, fmt.Errorf("%w: maximum is %d bytes", artifact.ErrTooLarge, s.maxBytes)
	}
	if err := validatePutOptions(opts); err != nil {
		return ObjectInfo{}, err
	}
	if size > s.threshold {
		return s.multipartPut(ctx, physical, key, r, size, opts)
	}
	payload, err := readExact(r, size)
	if err != nil {
		return ObjectInfo{}, err
	}
	payloadHash := opts.ContentSHA256
	actualSum := sha256.Sum256(payload)
	if payloadHash == "" {
		payloadHash = hex.EncodeToString(actualSum[:])
	} else if payloadHash != hex.EncodeToString(actualSum[:]) {
		return ObjectInfo{}, artifact.ErrDigestMismatch
	}
	resp, err := s.do(ctx, http.MethodPut, physical, nil, putHeaders(opts), bytes.NewReader(payload), size, payloadHash)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK, http.StatusCreated); err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Key: key, Size: size, ETag: trimETag(resp.Header.Get("ETag")), Metadata: cloneMap(opts.Metadata)}, nil
}

// OpenObject returns a streaming reader and the object's metadata. DeleteObject
// on this Store is refused until the reader is closed.
func (s *Store) OpenObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	resp, cancel, err := s.doStream(ctx, http.MethodGet, physical, nil, nil, nil, 0, emptySHA256)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		resp.Body.Close()
		cancel()
		return nil, ObjectInfo{}, err
	}
	if resp.ContentLength < 0 {
		resp.Body.Close()
		cancel()
		return nil, ObjectInfo{}, errors.New("s3: GET response omitted Content-Length")
	}
	info := infoFromHeaders(key, resp.ContentLength, resp.Header)
	s.pin(physical)
	return &ownedReadCloser{ReadCloser: resp.Body, close: func() {
		s.unpin(physical)
		cancel()
	}}, info, nil
}

// HeadObject returns metadata without opening the object.
func (s *Store) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	resp, err := s.do(ctx, http.MethodHead, physical, nil, nil, nil, 0, emptySHA256)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return ObjectInfo{}, err
	}
	if resp.ContentLength < 0 {
		return ObjectInfo{}, errors.New("s3: HEAD response omitted Content-Length")
	}
	return infoFromHeaders(key, resp.ContentLength, resp.Header), nil
}

// DeleteObject removes a named object. A reader opened through this Store must
// be closed first.
func (s *Store) DeleteObject(ctx context.Context, key string) error {
	physical, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	if s.pinned(physical) {
		return errors.New("s3: object is in use")
	}
	resp, err := s.do(ctx, http.MethodDelete, physical, nil, nil, nil, 0, emptySHA256)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return expectStatus(resp, http.StatusNoContent, http.StatusOK)
}

// ListObjects lists every named object under prefix, following all continuation
// pages. Returned keys are relative to Config.Prefix.
func (s *Store) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	physicalPrefix := prefix
	if s.prefix != "" {
		physicalPrefix = s.prefix + "/" + prefix
	}
	var out []ObjectInfo
	seenTokens := make(map[string]struct{})
	continuation := ""
	for {
		query := url.Values{"list-type": {"2"}, "prefix": {physicalPrefix}}
		if continuation != "" {
			query.Set("continuation-token", continuation)
		}
		resp, err := s.do(ctx, http.MethodGet, "", query, nil, nil, 0, emptySHA256)
		if err != nil {
			return nil, err
		}
		if err := expectStatus(resp, http.StatusOK); err != nil {
			resp.Body.Close()
			return nil, err
		}
		var page listObjectsResult
		err = xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&page)
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("s3: decode object listing: %w", err)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for _, object := range page.Contents {
			relative, ok := s.relativeKey(object.Key)
			if !ok {
				return nil, errors.New("s3: object listing escaped configured prefix")
			}
			out = append(out, ObjectInfo{Key: relative, Size: object.Size, ETag: trimETag(object.ETag), LastModified: object.LastModified})
		}
		if !page.IsTruncated {
			return out, nil
		}
		if page.NextContinuationToken == "" {
			return nil, errors.New("s3: truncated object listing omitted continuation token")
		}
		if _, exists := seenTokens[page.NextContinuationToken]; exists {
			return nil, errors.New("s3: object listing repeated a continuation token")
		}
		seenTokens[page.NextContinuationToken] = struct{}{}
		continuation = page.NextContinuationToken
	}
}

// Inventory enumerates durable and private staging state. It returns no
// partial healthy-looking result when either listing fails.
func (s *Store) Inventory(ctx context.Context) (Inventory, error) {
	objects, err := s.ListObjects(ctx, "")
	if err != nil {
		return Inventory{}, err
	}
	uploads, err := s.listMultipartUploads(ctx, "")
	if err != nil {
		return Inventory{}, err
	}
	var inventory Inventory
	for _, object := range objects {
		if strings.HasPrefix(object.Key, ".staging/") {
			inventory.StagingObjects++
			inventory.StagingBytes += object.Size
			continue
		}
		inventory.Objects++
		inventory.Bytes += object.Size
	}
	inventory.MultipartUploads = len(uploads)
	return inventory, nil
}

// CleanupStaging aborts multipart uploads and deletes completed private staging
// objects older than the configured TTL. Every attempted cleanup error is
// returned; stale state is never silently reported as collected.
func (s *Store) CleanupStaging(ctx context.Context) error {
	cutoff := s.now().Add(-s.stagingTTL)
	objects, err := s.ListObjects(ctx, ".staging/")
	if err != nil {
		return err
	}
	uploads, err := s.listMultipartUploads(ctx, ".staging/")
	if err != nil {
		return err
	}
	var errs []error
	for _, object := range objects {
		if !object.LastModified.IsZero() && object.LastModified.Before(cutoff) {
			if err := s.DeleteObject(ctx, object.Key); err != nil {
				errs = append(errs, err)
			}
		}
	}
	for _, upload := range uploads {
		if !upload.Initiated.IsZero() && upload.Initiated.Before(cutoff) {
			physical, keyErr := s.physicalKey(upload.Key)
			if keyErr != nil {
				errs = append(errs, keyErr)
				continue
			}
			if err := s.abortMultipart(ctx, physical, upload.UploadID); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// CheckConditionalWrites verifies If-None-Match and If-Match against a private
// probe object and removes it before returning. Failover must not start when
// this check returns unavailable or ErrConditionalUnsupported.
func (s *Store) CheckConditionalWrites(ctx context.Context) (err error) {
	key, err := s.newStagingKey("conditional")
	if err != nil {
		return err
	}
	payload := []byte("remount conditional-write probe")
	info, err := s.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), PutOptions{IfNoneMatch: "*"})
	if err != nil {
		return conditionalCapabilityError(err)
	}
	defer func() {
		if cleanupErr := s.DeleteObject(context.WithoutCancel(ctx), key); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("s3: remove conditional probe: %w", cleanupErr))
		}
	}()
	if _, putErr := s.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), PutOptions{IfNoneMatch: "*"}); !errors.Is(putErr, ErrPreconditionFailed) {
		if putErr != nil {
			return conditionalCapabilityError(putErr)
		}
		return ErrConditionalUnsupported
	}
	if _, putErr := s.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), PutOptions{IfMatch: "definitely-not-the-current-etag"}); !errors.Is(putErr, ErrPreconditionFailed) {
		if putErr != nil {
			return conditionalCapabilityError(putErr)
		}
		return ErrConditionalUnsupported
	}
	if _, err := s.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), PutOptions{IfMatch: info.ETag}); err != nil {
		return conditionalCapabilityError(err)
	}
	return nil
}

func conditionalCapabilityError(err error) error {
	var serviceErr *Error
	if errors.As(err, &serviceErr) {
		switch serviceErr.Code {
		case "NotImplemented", "UnsupportedHeader", "InvalidRequest":
			return errors.Join(ErrConditionalUnsupported, err)
		}
		if serviceErr.StatusCode == http.StatusNotImplemented {
			return errors.Join(ErrConditionalUnsupported, err)
		}
	}
	return err
}

func (s *Store) multipartPut(ctx context.Context, physical, relative string, r io.Reader, size int64, opts PutOptions) (info ObjectInfo, err error) {
	uploadID, err := s.initiateMultipart(ctx, physical, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	completed := false
	defer func() {
		if !completed {
			if abortErr := s.abortMultipart(context.WithoutCancel(ctx), physical, uploadID); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("s3: abort multipart upload: %w", abortErr))
			}
		}
	}()
	remaining := size
	parts := make([]completedPart, 0, int((size+s.partSize-1)/s.partSize))
	buf := make([]byte, s.partSize)
	hasher := sha256.New()
	for partNumber := 1; remaining > 0; partNumber++ {
		partLen := min(remaining, s.partSize)
		if _, err := io.ReadFull(r, buf[:partLen]); err != nil {
			return ObjectInfo{}, fmt.Errorf("s3: object body shorter than declared size: %w", err)
		}
		_, _ = hasher.Write(buf[:partLen])
		etag, err := s.uploadPart(ctx, physical, uploadID, partNumber, buf[:partLen])
		if err != nil {
			return ObjectInfo{}, err
		}
		parts = append(parts, completedPart{PartNumber: partNumber, ETag: etag})
		remaining -= partLen
	}
	if opts.ContentSHA256 != "" && opts.ContentSHA256 != hex.EncodeToString(hasher.Sum(nil)) {
		return ObjectInfo{}, artifact.ErrDigestMismatch
	}
	var extra [1]byte
	if n, readErr := io.ReadFull(r, extra[:]); n != 0 || (readErr != nil && !errors.Is(readErr, io.EOF)) {
		if n != 0 {
			readErr = errors.New("body has trailing data")
		}
		return ObjectInfo{}, fmt.Errorf("s3: object body longer than declared size: %w", readErr)
	}
	etag, err := s.completeMultipart(ctx, physical, uploadID, parts, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	completed = true
	return ObjectInfo{Key: relative, Size: size, ETag: etag, Metadata: cloneMap(opts.Metadata)}, nil
}

func (s *Store) initiateMultipart(ctx context.Context, physical string, opts PutOptions) (string, error) {
	headers := putHeaders(opts)
	delete(headers, "If-Match")
	delete(headers, "If-None-Match")
	resp, err := s.do(ctx, http.MethodPost, physical, url.Values{"uploads": {""}}, headers, nil, 0, emptySHA256)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return "", err
	}
	var result initiateMultipartResult
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return "", fmt.Errorf("s3: decode multipart initiation: %w", err)
	}
	if result.UploadID == "" {
		return "", errors.New("s3: multipart initiation omitted upload id")
	}
	return result.UploadID, nil
}

func (s *Store) uploadPart(ctx context.Context, physical, uploadID string, partNumber int, payload []byte) (string, error) {
	sum := sha256.Sum256(payload)
	query := url.Values{"partNumber": {strconv.Itoa(partNumber)}, "uploadId": {uploadID}}
	resp, err := s.do(ctx, http.MethodPut, physical, query, nil, bytes.NewReader(payload), int64(len(payload)), hex.EncodeToString(sum[:]))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return "", err
	}
	etag := trimETag(resp.Header.Get("ETag"))
	if etag == "" {
		return "", errors.New("s3: uploaded part omitted ETag")
	}
	return etag, nil
}

func (s *Store) completeMultipart(ctx context.Context, physical, uploadID string, parts []completedPart, opts PutOptions) (string, error) {
	payload, err := xml.Marshal(completeMultipartUpload{Parts: parts})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	headers := make(http.Header)
	if opts.IfMatch != "" {
		headers.Set("If-Match", quoteETag(opts.IfMatch))
	}
	if opts.IfNoneMatch != "" {
		headers.Set("If-None-Match", opts.IfNoneMatch)
	}
	resp, err := s.do(ctx, http.MethodPost, physical, url.Values{"uploadId": {uploadID}}, headers, bytes.NewReader(payload), int64(len(payload)), hex.EncodeToString(sum[:]))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return "", err
	}
	var result completeMultipartResult
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return "", fmt.Errorf("s3: decode multipart completion: %w", err)
	}
	return trimETag(result.ETag), nil
}

func (s *Store) abortMultipart(ctx context.Context, physical, uploadID string) error {
	resp, err := s.do(ctx, http.MethodDelete, physical, url.Values{"uploadId": {uploadID}}, nil, nil, 0, emptySHA256)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return expectStatus(resp, http.StatusNoContent, http.StatusOK, http.StatusNotFound)
}

type multipartUpload struct {
	Key       string    `xml:"Key"`
	UploadID  string    `xml:"UploadId"`
	Initiated time.Time `xml:"Initiated"`
}

func (s *Store) listMultipartUploads(ctx context.Context, prefix string) ([]multipartUpload, error) {
	physicalPrefix := prefix
	if s.prefix != "" {
		physicalPrefix = s.prefix + "/" + prefix
	}
	var uploads []multipartUpload
	keyMarker, uploadMarker := "", ""
	seenMarkers := make(map[string]struct{})
	for {
		query := url.Values{"uploads": {""}, "prefix": {physicalPrefix}}
		if keyMarker != "" {
			query.Set("key-marker", keyMarker)
		}
		if uploadMarker != "" {
			query.Set("upload-id-marker", uploadMarker)
		}
		resp, err := s.do(ctx, http.MethodGet, "", query, nil, nil, 0, emptySHA256)
		if err != nil {
			return nil, err
		}
		if err := expectStatus(resp, http.StatusOK); err != nil {
			resp.Body.Close()
			return nil, err
		}
		var result listMultipartUploadsResult
		err = xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&result)
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("s3: decode multipart listing: %w", err)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for i := range result.Uploads {
			relative, ok := s.relativeKey(result.Uploads[i].Key)
			if !ok {
				return nil, errors.New("s3: multipart listing escaped configured prefix")
			}
			result.Uploads[i].Key = relative
			uploads = append(uploads, result.Uploads[i])
		}
		if !result.IsTruncated {
			return uploads, nil
		}
		if result.NextKeyMarker == "" || result.NextUploadIDMarker == "" {
			return nil, errors.New("s3: truncated multipart listing omitted continuation markers")
		}
		marker := result.NextKeyMarker + "\x00" + result.NextUploadIDMarker
		if _, exists := seenMarkers[marker]; exists {
			return nil, errors.New("s3: multipart listing repeated continuation markers")
		}
		seenMarkers[marker] = struct{}{}
		keyMarker, uploadMarker = result.NextKeyMarker, result.NextUploadIDMarker
	}
}

func (s *Store) newStagingKey(kind string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("s3: create staging id: %w", err)
	}
	return ".staging/" + s.now().UTC().Format("20060102T150405.000000000Z") + "-" + kind + "-" + hex.EncodeToString(random[:]), nil
}

func (s *Store) do(ctx context.Context, method, physical string, query url.Values, headers http.Header, body io.Reader, size int64, payloadHash string) (*http.Response, error) {
	resp, cancel, err := s.doStream(ctx, method, physical, query, headers, body, size, payloadHash)
	if err != nil {
		return nil, err
	}
	resp.Body = &ownedReadCloser{ReadCloser: resp.Body, close: cancel}
	return resp, nil
}

func (s *Store) doStream(ctx context.Context, method, physical string, query url.Values, headers http.Header, body io.Reader, size int64, payloadHash string) (*http.Response, context.CancelFunc, error) {
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	u := s.requestURL(physical, query)
	req, err := http.NewRequestWithContext(requestCtx, method, u.String(), body)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if headers != nil {
		req.Header = headers.Clone()
	}
	if body != nil {
		req.ContentLength = size
	}
	s.sign(req, payloadHash, s.now())
	resp, err := s.httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("s3: request failed: %w", err)
	}
	return resp, cancel, nil
}

func (s *Store) requestURL(physical string, query url.Values) *url.URL {
	u := *s.endpoint
	keyPath := escapedKeyPath(physical)
	basePath := strings.TrimSuffix(s.endpoint.EscapedPath(), "/")
	if s.pathStyle {
		u.RawPath = basePath + "/" + awsPathEscape(s.bucket) + keyPath
		u.Path, _ = url.PathUnescape(u.RawPath)
	} else {
		u.Host = s.bucket + "." + s.endpoint.Host
		u.RawPath = basePath + keyPath
		u.Path, _ = url.PathUnescape(u.RawPath)
	}
	if u.RawPath == "" {
		u.RawPath = "/"
		u.Path = "/"
	}
	u.RawQuery = canonicalQuery(query)
	return &u
}

func escapedKeyPath(key string) string {
	if key == "" {
		return "/"
	}
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = awsPathEscape(parts[i])
	}
	return "/" + strings.Join(parts, "/")
}

func awsPathEscape(value string) string {
	return awsPercentEncode(value)
}

func expectStatus(resp *http.Response, allowed ...int) error {
	for _, status := range allowed {
		if resp.StatusCode == status {
			return nil
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: s3 object", os.ErrNotExist)
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		return ErrPreconditionFailed
	}
	var body struct {
		Code string `xml:"Code"`
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	_ = xml.Unmarshal(data, &body)
	return &Error{StatusCode: resp.StatusCode, Code: body.Code}
}

func putHeaders(opts PutOptions) http.Header {
	headers := make(http.Header)
	if opts.IfMatch != "" {
		headers.Set("If-Match", quoteETag(opts.IfMatch))
	}
	if opts.IfNoneMatch != "" {
		headers.Set("If-None-Match", opts.IfNoneMatch)
	}
	if opts.ContentType != "" {
		headers.Set("Content-Type", opts.ContentType)
	}
	for key, value := range opts.Metadata {
		headers.Set("X-Amz-Meta-"+key, value)
	}
	return headers
}

func validatePutOptions(opts PutOptions) error {
	if opts.IfMatch != "" && opts.IfNoneMatch != "" {
		return errors.New("s3: If-Match and If-None-Match are mutually exclusive")
	}
	if opts.IfNoneMatch != "" && opts.IfNoneMatch != "*" {
		return errors.New("s3: If-None-Match supports only *")
	}
	if strings.ContainsAny(opts.IfMatch+opts.ContentType, "\r\n") {
		return errors.New("s3: conditional and content-type headers must not contain newlines")
	}
	if opts.ContentSHA256 != "" {
		decoded, err := hex.DecodeString(opts.ContentSHA256)
		if err != nil || len(decoded) != sha256.Size || strings.ToLower(opts.ContentSHA256) != opts.ContentSHA256 {
			return errors.New("s3: ContentSHA256 must be a lowercase SHA-256 hex digest")
		}
	}
	for key, value := range opts.Metadata {
		if !validMetadataName(key) || strings.ContainsAny(value, "\r\n") {
			return errors.New("s3: metadata names must be non-empty HTTP token suffixes")
		}
	}
	return nil
}

func validMetadataName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c)) {
			continue
		}
		return false
	}
	return true
}

func readExact(r io.Reader, size int64) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.Grow(int(size))
	n, err := io.CopyN(&buffer, r, size)
	if err != nil {
		return nil, fmt.Errorf("s3: object body shorter than declared size after %d bytes: %w", n, err)
	}
	var extra [1]byte
	extraN, extraErr := io.ReadFull(r, extra[:])
	if extraN != 0 || (extraErr != nil && !errors.Is(extraErr, io.EOF)) {
		return nil, errors.New("s3: object body longer than declared size")
	}
	return buffer.Bytes(), nil
}

func infoFromHeaders(key string, size int64, headers http.Header) ObjectInfo {
	info := ObjectInfo{Key: key, Size: size, ETag: trimETag(headers.Get("ETag")), Metadata: make(map[string]string)}
	if value := headers.Get("Last-Modified"); value != "" {
		info.LastModified, _ = http.ParseTime(value)
	}
	for name, values := range headers {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-meta-") && len(values) > 0 {
			info.Metadata[strings.TrimPrefix(strings.ToLower(name), "x-amz-meta-")] = values[0]
		}
	}
	return info
}

func trimETag(value string) string { return strings.Trim(value, "\"") }

func quoteETag(value string) string {
	if value == "*" || strings.HasPrefix(value, "\"") {
		return value
	}
	return "\"" + value + "\""
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type ownedReadCloser struct {
	io.ReadCloser
	once  sync.Once
	close func()
}

func (r *ownedReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.close)
	return err
}

type listObjectsResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string    `xml:"Key"`
		Size         int64     `xml:"Size"`
		ETag         string    `xml:"ETag"`
		LastModified time.Time `xml:"LastModified"`
	} `xml:"Contents"`
}

type initiateMultipartResult struct {
	UploadID string `xml:"UploadId"`
}

type completedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUpload struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []completedPart `xml:"Part"`
}

type completeMultipartResult struct {
	ETag string `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	IsTruncated        bool              `xml:"IsTruncated"`
	NextKeyMarker      string            `xml:"NextKeyMarker"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker"`
	Uploads            []multipartUpload `xml:"Upload"`
}
