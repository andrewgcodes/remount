# Tenant artifact storage integration

The default test proves local encrypted storage, restart recovery, physical
tenant isolation, and bounded key rotation:

```sh
go test -race ./integration/storage
```

`TestS3EncryptedTenantRuntime` composes the hand-written S3 client, conditional
named-object adapter, envelope encryption, tenant resolver, and multipart path.
It skips as unavailable unless these variables select a disposable bucket or
prefix-capable S3-compatible service:

```text
REMOUNT_S3_INTEGRATION_ENDPOINT
REMOUNT_S3_INTEGRATION_BUCKET
REMOUNT_S3_INTEGRATION_REGION
REMOUNT_S3_INTEGRATION_ACCESS_KEY
REMOUNT_S3_INTEGRATION_SECRET_KEY
REMOUNT_S3_INTEGRATION_SESSION_TOKEN       (optional)
REMOUNT_S3_INTEGRATION_PATH_STYLE          (optional, default true)
```

The test uses a unique prefix, verifies conditional writes before becoming
ready, sends a 65 MiB encrypted object through multipart upload for two tenants,
decrypts and hashes both, and deletes every object it created. A skip is not a
pass and does not provide live-provider evidence.
