package main

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"remount.dev/remount/internal/server"
)

func TestBuildProductionTenantArtifactRuntimeFailsClosedWithoutMasterKey(t *testing.T) {
	t.Setenv("REMOUNT_MASTER_KEY", "")
	t.Setenv("REMOUNT_MASTER_KEY_FILE", "")
	if _, err := buildTenantArtifactRuntime(t.TempDir(), server.ModeProductionMultiTenant, "", 1<<20, 4<<20, 10); err == nil {
		t.Fatal("production artifact runtime started without a master key")
	}
}

func TestBuildLocalProductionTenantArtifactRuntimeEncryptsPhysicalBytes(t *testing.T) {
	key := sha256.Sum256([]byte("runtime test master"))
	t.Setenv("REMOUNT_MASTER_KEY", base64.StdEncoding.EncodeToString(key[:]))
	t.Setenv("REMOUNT_MASTER_KEY_FILE", "")
	root := t.TempDir()
	resolver, err := buildTenantArtifactRuntime(root, server.ModeProductionMultiTenant, "", 1<<20, 4<<20, 10)
	if err != nil {
		t.Fatal(err)
	}
	store, err := resolver.ResolveTenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	plaintext := "plaintext must not land in the physical store"
	id, _, err := store.Put(strings.NewReader(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	var physical []byte
	err = filepath.WalkDir(filepath.Join(root, "artifact-objects"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		physical, walkErr = os.ReadFile(path)
		return walkErr
	})
	if err != nil || len(physical) == 0 || strings.Contains(string(physical), plaintext) {
		t.Fatalf("physical ciphertext for %s = %d bytes, err=%v", id, len(physical), err)
	}
}

func TestS3ArtifactConfigUsesURIAndBoundedEnvironment(t *testing.T) {
	t.Setenv("REMOUNT_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("REMOUNT_S3_PATH_STYLE", "true")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_ACCESS_KEY_ID", "access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	cfg, err := s3ArtifactConfig("s3://bucket/tenant-artifacts", 12345)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bucket != "bucket" || cfg.Prefix != "tenant-artifacts" || cfg.Endpoint != "http://127.0.0.1:9000" ||
		cfg.Region != "us-west-2" || !cfg.PathStyle || cfg.MaxObjectBytes != 12345 {
		t.Fatalf("S3 config = %+v", cfg)
	}
	if _, err := s3ArtifactConfig("https://bucket/prefix", 12345); err == nil {
		t.Fatal("non-s3 blob URI accepted")
	}
}
