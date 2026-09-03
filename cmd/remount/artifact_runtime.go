package main

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/encrypted"
	artifacts3 "remount.dev/remount/internal/artifact/s3"
	"remount.dev/remount/internal/server"
)

func buildTenantArtifactRuntime(dataDir, mode, blob string, maxPlaintext, maxStoreBytes int64, maxObjects int) (artifact.TenantResolver, error) {
	if mode == server.ModeStandalone && blob == "" {
		return nil, nil
	}
	masterID := envOr("REMOUNT_MASTER_KEY_ID", "master-v1")
	var master encrypted.MasterKey
	var err error
	if path := strings.TrimSpace(os.Getenv("REMOUNT_MASTER_KEY_FILE")); path != "" {
		master, err = encrypted.NewFileMasterKey(path, masterID)
	} else {
		master, err = encrypted.NewEnvMasterKey("REMOUNT_MASTER_KEY", masterID)
	}
	if err != nil {
		return nil, fmt.Errorf("configure encrypted tenant artifacts: %w", err)
	}
	keys, err := encrypted.NewDirectoryKeyProvider(filepath.Join(dataDir, "artifact-keys"), master, encrypted.DirectoryKeyOptions{})
	if err != nil {
		return nil, err
	}
	var objects encrypted.KeyedStore
	var ready encrypted.ReadinessCheck
	if blob == "" {
		objects, err = encrypted.NewFileStore(filepath.Join(dataDir, "artifact-objects"), encrypted.FileStoreOptions{
			MaxBytes: maxStoreBytes, MaxObjects: maxObjects,
		})
	} else {
		cfg, configErr := s3ArtifactConfig(blob, encryptedObjectLimit(maxPlaintext))
		if configErr != nil {
			return nil, configErr
		}
		physical, createErr := artifacts3.New(cfg)
		if createErr != nil {
			return nil, createErr
		}
		var keyed *artifacts3.KeyedStore
		keyed, err = artifacts3.NewKeyedStore(physical)
		if err == nil {
			objects = keyed
			ready = keyed.Ready
		}
	}
	if err != nil {
		return nil, err
	}
	store, err := encrypted.NewStore(objects, keys, filepath.Join(dataDir, "artifact-stage"), encrypted.Options{
		MaxPlaintextBytes: maxPlaintext, MaxStagingBytes: maxStoreBytes,
	})
	if err != nil {
		return nil, err
	}
	return encrypted.NewResolver(store, keys, encrypted.ResolverOptions{Ready: ready})
}

func s3ArtifactConfig(raw string, maxObjectBytes int64) (artifacts3.Config, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return artifacts3.Config{}, errors.New("--blob must be s3://bucket[/clean-prefix]")
	}
	prefix := strings.Trim(u.EscapedPath(), "/")
	if decoded, decodeErr := url.PathUnescape(prefix); decodeErr != nil {
		return artifacts3.Config{}, errors.New("--blob contains an invalid escaped prefix")
	} else {
		prefix = decoded
	}
	region := envOr("AWS_REGION", envOr("AWS_DEFAULT_REGION", "us-east-1"))
	endpoint := strings.TrimSpace(os.Getenv("REMOUNT_S3_ENDPOINT"))
	if endpoint == "" {
		endpoint = "https://s3." + region + ".amazonaws.com"
	}
	pathStyle := false
	if value := strings.TrimSpace(os.Getenv("REMOUNT_S3_PATH_STYLE")); value != "" {
		pathStyle, err = strconv.ParseBool(value)
		if err != nil {
			return artifacts3.Config{}, errors.New("REMOUNT_S3_PATH_STYLE must be a boolean")
		}
	}
	return artifacts3.Config{
		Endpoint: endpoint, Region: region, Bucket: u.Host, Prefix: prefix,
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"), PathStyle: pathStyle, MaxObjectBytes: maxObjectBytes,
	}, nil
}

func encryptedObjectLimit(plaintext int64) int64 {
	if plaintext <= 0 {
		plaintext = 8 << 30
	}
	const chunk = int64(1 << 20)
	overhead := ((plaintext + chunk - 1) / chunk * 16) + 1024
	if plaintext > math.MaxInt64-overhead {
		return math.MaxInt64
	}
	return plaintext + overhead
}
