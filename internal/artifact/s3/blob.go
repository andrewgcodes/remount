package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"remount.dev/remount/internal/artifact"
)

const blobPrefix = "blobs/"

var _ artifact.BlobStore = (*Store)(nil)

// Put hashes and stores r under its artifact id. Small bodies are published
// with a conditional PUT. Large bodies stream through a private multipart
// object and become visible only after an atomic server-side copy.
func (s *Store) Put(r io.Reader) (id string, size int64, err error) {
	readLimit := s.threshold + 1
	if s.maxBytes+1 < readLimit {
		readLimit = s.maxBytes + 1
	}
	prefix, err := io.ReadAll(io.LimitReader(r, readLimit))
	if err != nil {
		return "", 0, err
	}
	if int64(len(prefix)) > s.maxBytes {
		return "", int64(len(prefix)), fmt.Errorf("%w: maximum is %d bytes", artifact.ErrTooLarge, s.maxBytes)
	}
	if int64(len(prefix)) <= s.threshold {
		return s.putSmallBlob(context.Background(), prefix)
	}
	return s.putLargeBlob(context.Background(), io.MultiReader(bytes.NewReader(prefix), r))
}

func (s *Store) putSmallBlob(ctx context.Context, payload []byte) (string, int64, error) {
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	id := artifact.Prefix + digest
	key := blobPrefix + digest
	_, err := s.PutObject(ctx, key, bytes.NewReader(payload), int64(len(payload)), PutOptions{
		IfNoneMatch: "*", ContentSHA256: digest, Metadata: map[string]string{"remount-sha256": digest},
	})
	if errors.Is(err, ErrPreconditionFailed) {
		if verifyErr := s.Verify(id); verifyErr != nil {
			return "", 0, fmt.Errorf("s3: existing immutable blob failed verification: %w", verifyErr)
		}
		return id, int64(len(payload)), nil
	}
	if err != nil {
		return "", 0, err
	}
	return id, int64(len(payload)), nil
}

func (s *Store) putLargeBlob(ctx context.Context, source io.Reader) (id string, size int64, err error) {
	stagingKey, err := s.newStagingKey("blob")
	if err != nil {
		return "", 0, err
	}
	physical, err := s.physicalKey(stagingKey)
	if err != nil {
		return "", 0, err
	}
	uploadID, err := s.initiateMultipart(ctx, physical, PutOptions{Metadata: map[string]string{"remount-staging": "blob"}})
	if err != nil {
		return "", 0, err
	}
	completed := false
	defer func() {
		if !completed {
			if abortErr := s.abortMultipart(context.WithoutCancel(ctx), physical, uploadID); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("s3: abort multipart upload: %w", abortErr))
			}
		}
	}()

	hasher := sha256.New()
	parts := make([]completedPart, 0, int((s.maxBytes+s.partSize-1)/s.partSize))
	buf := make([]byte, s.partSize)
	for partNumber := 1; ; partNumber++ {
		remaining := s.maxBytes - size
		if remaining < 0 {
			return "", size, fmt.Errorf("%w: maximum is %d bytes", artifact.ErrTooLarge, s.maxBytes)
		}
		want := s.partSize
		if remaining+1 < want {
			want = remaining + 1
		}
		n, readErr := io.ReadFull(source, buf[:want])
		if errors.Is(readErr, io.EOF) && n == 0 {
			break
		}
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return "", size, readErr
		}
		if size+int64(n) > s.maxBytes {
			return "", size + int64(n), fmt.Errorf("%w: maximum is %d bytes", artifact.ErrTooLarge, s.maxBytes)
		}
		if partNumber > maximumParts {
			return "", size, errors.New("s3: multipart upload exceeded the part-count limit")
		}
		_, _ = hasher.Write(buf[:n])
		etag, uploadErr := s.uploadPart(ctx, physical, uploadID, partNumber, buf[:n])
		if uploadErr != nil {
			return "", size, uploadErr
		}
		parts = append(parts, completedPart{PartNumber: partNumber, ETag: etag})
		size += int64(n)
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}
	if len(parts) == 0 {
		return "", 0, errors.New("s3: multipart blob unexpectedly contained no parts")
	}
	if _, err := s.completeMultipart(ctx, physical, uploadID, parts, PutOptions{}); err != nil {
		return "", size, err
	}
	completed = true

	digest := hex.EncodeToString(hasher.Sum(nil))
	id = artifact.Prefix + digest
	destination, err := s.physicalKey(blobPrefix + digest)
	if err != nil {
		return "", size, err
	}
	if err := s.copyObject(ctx, physical, destination, digest, size); errors.Is(err, ErrPreconditionFailed) {
		if verifyErr := s.Verify(id); verifyErr != nil {
			return "", size, fmt.Errorf("s3: existing immutable blob failed verification: %w", verifyErr)
		}
	} else if err != nil {
		return "", size, err
	}
	if cleanupErr := s.DeleteObject(context.WithoutCancel(ctx), stagingKey); cleanupErr != nil {
		return "", size, fmt.Errorf("s3: published blob but failed to remove private staging object: %w", cleanupErr)
	}
	return id, size, nil
}

func (s *Store) copyObject(ctx context.Context, source, destination, digest string, size int64) error {
	if size > s.copyLimit {
		return s.multipartCopyObject(ctx, source, destination, digest, size)
	}
	headers := make(http.Header)
	headers.Set("X-Amz-Copy-Source", escapedCopySource(s.bucket, source))
	headers.Set("X-Amz-Metadata-Directive", "REPLACE")
	headers.Set("X-Amz-Meta-Remount-Sha256", digest)
	headers.Set("If-None-Match", "*")
	resp, err := s.do(ctx, http.MethodPut, destination, nil, headers, nil, 0, emptySHA256)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return err
	}
	var result struct {
		ETag string `xml:"ETag"`
		Code string `xml:"Code"`
	}
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return fmt.Errorf("s3: decode copy result: %w", err)
	}
	if result.Code != "" {
		return &Error{StatusCode: resp.StatusCode, Code: result.Code}
	}
	if trimETag(result.ETag) == "" {
		return errors.New("s3: copy result omitted ETag")
	}
	return nil
}

func (s *Store) multipartCopyObject(ctx context.Context, source, destination, digest string, size int64) (err error) {
	uploadID, err := s.initiateMultipart(ctx, destination, PutOptions{Metadata: map[string]string{"remount-sha256": digest}})
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			if abortErr := s.abortMultipart(context.WithoutCancel(ctx), destination, uploadID); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("s3: abort multipart copy: %w", abortErr))
			}
		}
	}()
	parts := make([]completedPart, 0, int((size+s.partSize-1)/s.partSize))
	for partNumber, offset := 1, int64(0); offset < size; partNumber++ {
		partLen := min(s.partSize, size-offset)
		etag, err := s.uploadPartCopy(ctx, source, destination, uploadID, partNumber, offset, offset+partLen-1)
		if err != nil {
			return err
		}
		parts = append(parts, completedPart{PartNumber: partNumber, ETag: etag})
		offset += partLen
	}
	if _, err := s.completeMultipart(ctx, destination, uploadID, parts, PutOptions{IfNoneMatch: "*"}); err != nil {
		return err
	}
	completed = true
	return nil
}

func (s *Store) uploadPartCopy(ctx context.Context, source, destination, uploadID string, partNumber int, first, last int64) (string, error) {
	headers := make(http.Header)
	headers.Set("X-Amz-Copy-Source", escapedCopySource(s.bucket, source))
	headers.Set("X-Amz-Copy-Source-Range", fmt.Sprintf("bytes=%d-%d", first, last))
	query := url.Values{"partNumber": {fmt.Sprint(partNumber)}, "uploadId": {uploadID}}
	resp, err := s.do(ctx, http.MethodPut, destination, query, headers, nil, 0, emptySHA256)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return "", err
	}
	var result completeMultipartResult
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return "", fmt.Errorf("s3: decode multipart copy part: %w", err)
	}
	etag := trimETag(result.ETag)
	if etag == "" {
		return "", errors.New("s3: multipart copy part omitted ETag")
	}
	return etag, nil
}

func escapedCopySource(bucket, key string) string {
	parts := strings.Split(bucket+"/"+key, "/")
	for i := range parts {
		parts[i] = awsPathEscape(parts[i])
	}
	return "/" + strings.Join(parts, "/")
}

// Open returns a reader that verifies the content digest when read to EOF.
func (s *Store) Open(id string) (io.ReadCloser, int64, error) {
	digest, err := artifact.Digest(id)
	if err != nil {
		return nil, 0, err
	}
	reader, info, err := s.OpenObject(context.Background(), blobPrefix+digest)
	if err != nil {
		return nil, 0, err
	}
	if metadataDigest := info.Metadata["remount-sha256"]; metadataDigest != "" && metadataDigest != digest {
		reader.Close()
		return nil, 0, artifact.ErrDigestMismatch
	}
	return &verifyingReadCloser{reader: reader, expected: digest, expectedSize: info.Size, hasher: sha256.New()}, info.Size, nil
}

// Head returns a blob's size without opening it and validates digest metadata
// when the endpoint supplies it.
func (s *Store) Head(id string) (int64, error) {
	digest, err := artifact.Digest(id)
	if err != nil {
		return 0, err
	}
	info, err := s.HeadObject(context.Background(), blobPrefix+digest)
	if err != nil {
		return 0, err
	}
	if metadataDigest := info.Metadata["remount-sha256"]; metadataDigest != "" && metadataDigest != digest {
		return 0, artifact.ErrDigestMismatch
	}
	return info.Size, nil
}

// Delete removes a blob after validating its id.
func (s *Store) Delete(id string) error {
	digest, err := artifact.Digest(id)
	if err != nil {
		return err
	}
	return s.DeleteObject(context.Background(), blobPrefix+digest)
}

// List returns every well-formed immutable blob id in lexical order.
func (s *Store) List() ([]string, error) {
	objects, err := s.ListObjects(context.Background(), blobPrefix)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(objects))
	for _, object := range objects {
		digest := strings.TrimPrefix(object.Key, blobPrefix)
		id := artifact.Prefix + digest
		parsed, digestErr := artifact.Digest(id)
		if digestErr != nil || parsed != digest || object.Key != blobPrefix+digest {
			return nil, fmt.Errorf("s3: unexpected object in immutable blob namespace %q", object.Key)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// Verify reads and re-hashes a blob in full.
func (s *Store) Verify(id string) error {
	reader, _, err := s.Open(id)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(io.Discard, reader)
	closeErr := reader.Close()
	return errors.Join(copyErr, closeErr)
}

type verifyingReadCloser struct {
	reader       io.ReadCloser
	expected     string
	expectedSize int64
	read         int64
	hasher       hash.Hash
	terminal     error
}

func (r *verifyingReadCloser) Read(p []byte) (int, error) {
	if r.terminal != nil {
		return 0, r.terminal
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		_, _ = r.hasher.Write(p[:n])
		r.read += int64(n)
	}
	if errors.Is(err, io.EOF) {
		if r.read != r.expectedSize || hex.EncodeToString(r.hasher.Sum(nil)) != r.expected {
			r.terminal = artifact.ErrDigestMismatch
			return n, r.terminal
		}
	}
	return n, err
}

func (r *verifyingReadCloser) Close() error { return r.reader.Close() }
