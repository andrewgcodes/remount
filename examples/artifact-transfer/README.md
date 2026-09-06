# artifact-transfer

Moves a directory of files between two workspaces through Remount's
content-addressed artifact store, and proves the round trip byte for byte.

1. Create workspace **origin** and wait for a node to claim it.
2. Upload a local directory into it with `Mkdir` and `WriteFile`.
3. `Snapshot(upload=true)` — the node publishes the object to the control
   plane's artifact store, which is what makes it reachable from another node.
4. Download that object over `GET /v1/artifacts/{id}` and check that its
   SHA-256 equals the id. An artifact id *is* the digest of its plaintext
   bytes, so a corrupted transfer is detectable without trusting the server.
5. Create workspace **restored** with `RestoreFrom` set to the same artifact.
   The control plane resolves the artifact's format and its transitive object
   closure; a caller supplies only the id.
6. Read every file back out of **restored** and compare it to the original.
7. Destroy both workspaces.

It imports only the two supported public packages,
`remount.dev/remount/api` and `remount.dev/remount/client`, plus `net/http` for
the artifact download. It needs no credentials and no network access beyond
the Remount server.

## Run it

```sh
go run ./cmd/remount standalone --listen 127.0.0.1:7443 --data ./remount-data
```

In a second shell, with no argument (a small tree is generated in a temporary
directory) or with a directory of your own:

```sh
REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/artifact-transfer
REMOUNT_SERVER=http://127.0.0.1:7443 go run ./examples/artifact-transfer ./docs/adr
```

Expected output, with ids, the format and byte counts differing by run:

```
source /tmp/artifact-transfer-…: 3 files
uploaded 3 files into ws_…
snapshot art_sha256:… format=chunked-v1 bytes=78
downloaded 911 bytes to /tmp/….bin, digest verified
verified 3 files byte for byte in ws_…
workspace ws_… destroyed
workspace ws_… destroyed
```

`REMOUNT_TOKEN` is read when set, so the same program runs unchanged against a
server that requires one; the bearer is sent on the artifact request too.

## Snapshot formats

`snapshot.Format` is `tar` for a deterministic archive of the whole tree and
`chunked-v1` when the node deduplicated against the artifact store, in which
case the downloaded object is the manifest that names the tree's chunks rather
than the tree itself. The digest check and the restore both hold either way,
which is why the example prints the format instead of assuming one.

## Proof

`integration/examples` boots the system in-process on a free loopback port,
builds and runs this program against it, and asserts on the output:

```sh
go test -count=1 -timeout 600s ./integration/examples/ -run TestArtifactTransferExample
```
