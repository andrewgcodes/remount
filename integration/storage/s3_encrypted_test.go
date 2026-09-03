package storage_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact/encrypted"
	"remount.dev/remount/internal/artifact/s3"
)

func TestS3EncryptedTenantRuntime(t *testing.T) {
	endpoint := os.Getenv("REMOUNT_S3_INTEGRATION_ENDPOINT")
	bucket := os.Getenv("REMOUNT_S3_INTEGRATION_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("REMOUNT_S3_INTEGRATION_ENDPOINT and REMOUNT_S3_INTEGRATION_BUCKET are unset; S3 tenant-storage lane is unavailable")
	}
	pathStyle := true
	if value := os.Getenv("REMOUNT_S3_INTEGRATION_PATH_STYLE"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			t.Fatalf("REMOUNT_S3_INTEGRATION_PATH_STYLE: %v", err)
		}
		pathStyle = parsed
	}
	prefix := fmt.Sprintf("tenant-artifacts/%d", time.Now().UnixNano())
	remote, err := s3.New(s3.Config{
		Endpoint: endpoint, Region: os.Getenv("REMOUNT_S3_INTEGRATION_REGION"), Bucket: bucket, Prefix: prefix,
		AccessKeyID: os.Getenv("REMOUNT_S3_INTEGRATION_ACCESS_KEY"), SecretAccessKey: os.Getenv("REMOUNT_S3_INTEGRATION_SECRET_KEY"),
		SessionToken: os.Getenv("REMOUNT_S3_INTEGRATION_SESSION_TOKEN"), PathStyle: pathStyle,
		MaxObjectBytes: 80 << 20, MultipartThreshold: 64 << 20, PartSize: 16 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := s3.NewKeyedStore(remote)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := objects.Ready(ctx); err != nil {
		t.Fatalf("S3 readiness: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			keys, listErr := objects.List(cleanupCtx, "tenants/"+tenant+"/")
			if listErr != nil {
				t.Errorf("cleanup list %s: %v", tenant, listErr)
				continue
			}
			for _, key := range keys {
				if deleteErr := objects.Delete(cleanupCtx, key); deleteErr != nil {
					t.Errorf("cleanup %s: %v", key, deleteErr)
				}
			}
		}
	}()

	masterBytes := sha256.Sum256([]byte("ephemeral S3 integration master"))
	master, err := encrypted.NewAESMasterKey("integration-v1", masterBytes[:])
	if err != nil {
		t.Fatal(err)
	}
	keys, err := encrypted.NewDirectoryKeyProvider(t.TempDir()+"/keys", master, encrypted.DirectoryKeyOptions{MaxTenants: 4, MaxVersionsPerTenant: 4})
	if err != nil {
		t.Fatal(err)
	}
	store, err := encrypted.NewStore(objects, keys, t.TempDir()+"/stage", encrypted.Options{MaxPlaintextBytes: 70 << 20, MaxStagingBytes: 140 << 20, MaxConcurrentWrites: 2})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := encrypted.NewResolver(store, keys, encrypted.ResolverOptions{Ready: objects.Ready, RewrapBatch: 10})
	if err != nil {
		t.Fatal(err)
	}
	a, err := resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolver.ResolveTenant("tenant-b")
	if err != nil {
		t.Fatal(err)
	}

	// 65 MiB plaintext plus the encrypted envelope crosses the 64 MiB S3
	// multipart boundary without retaining a second in-memory copy.
	const size = int64(65 << 20)
	pattern := strings.NewReader(strings.Repeat("remount!", 1024))
	payload := io.LimitReader(&repeatingReader{source: pattern, seed: strings.Repeat("remount!", 1024)}, size)
	id, written, err := a.Put(payload)
	if err != nil || written != size {
		t.Fatalf("tenant-a multipart Put = %s, %d, %v", id, written, err)
	}
	secondID, secondSize, err := b.Put(io.LimitReader(&repeatingReader{source: strings.NewReader(strings.Repeat("remount!", 1024)), seed: strings.Repeat("remount!", 1024)}, size))
	if err != nil || secondSize != size || secondID != id {
		t.Fatalf("tenant-b multipart Put = %s, %d, %v; want %s", secondID, secondSize, err, id)
	}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		physical, err := objects.List(ctx, "tenants/"+tenant+"/")
		if err != nil || len(physical) != 1 || !strings.Contains(physical[0], "/"+id) {
			t.Fatalf("%s physical inventory = %v, %v", tenant, physical, err)
		}
		if err := store.Verify(ctx, tenant, id); err != nil {
			t.Fatalf("verify %s: %v", tenant, err)
		}
	}
}

type repeatingReader struct {
	source *strings.Reader
	seed   string
}

func (r *repeatingReader) Read(buffer []byte) (int, error) {
	n, err := r.source.Read(buffer)
	if err == io.EOF {
		r.source.Reset(r.seed)
		if n == 0 {
			return r.source.Read(buffer)
		}
		err = nil
	}
	return n, err
}
