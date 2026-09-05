# Releases and installation

As of 2026-09-05, the repository is private and its authenticated GitHub release
inventory is empty. Build from an accessible checkout with `make build`; see
[Using Remount](using-remount.md#build-and-select-a-server). The commands below
describe the release workflow and its verification contract, not currently
available public artifacts. `v0.1.0` is illustrative, not a published release.

Remount releases are six static binaries: Linux, macOS and Windows on amd64
and arm64. A tag is not considered complete merely because files appeared on
a GitHub release. E18 requires the binary version, checksum signature, SBOM,
provenance, installer, Homebrew formula, public Go install path and container
digest to be verified against the same tag.

## Install a release binary

The installer supports Linux and macOS on amd64 and arm64. It downloads only
over HTTPS, verifies the selected binary against `checksums.txt`, optionally
verifies the Sigstore bundle, executes the binary only after verification, and
checks its reported version before an atomic rename into the destination.

The public install endpoint is intended to be:

```sh
bash -o pipefail -c \
  'curl --proto "=https" --tlsv1.2 -fsSL https://get.remount.dev/install.sh |
   REMOUNT_REQUIRE_SIGNATURE=1 sh'
```

Do not advertise this command as working until its TLS endpoint and the chosen
release pass the maintainer proof below. Once a release exists, inspect
`install.sh` in your accessible checkout and execute it locally:

```sh
REMOUNT_REQUIRE_SIGNATURE=1 ./install.sh
```

Signature-required installation expects a verified `cosign` on `PATH`. To pin
a version or install without administrator privileges:

```sh
REMOUNT_VERSION=v0.1.0 \
REMOUNT_INSTALL_DIR="$HOME/.local/bin" \
REMOUNT_REQUIRE_SIGNATURE=1 \
./install.sh
```

If `cosign` is absent and signature verification was not required, the script
prints that signer identity was not verified. It never describes a checksum
downloaded from the same release as a signature.

## Verify manually

```sh
tag=v0.1.0
gh release download "$tag" --repo andrewgcodes/remount --dir release
(cd release && sha256sum --check checksums.txt)

cosign verify-blob \
  --bundle release/checksums.txt.sigstore.json \
  --certificate-identity "https://github.com/andrewgcodes/remount/.github/workflows/release.yml@refs/tags/$tag" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  release/checksums.txt

gh attestation verify release/remount-linux-amd64 \
  --repo andrewgcodes/remount
```

An attestation ties bytes to a build workflow; it does not establish that the
program is safe. Consumers still choose the workflow identity and source tag
they trust.

## Homebrew, Go and container

The release follow-up generates `remount.rb` directly from signed checksums.
Once the external tap is populated, installation is:

```sh
brew install andrewgcodes/tap/remount
```

The public Go command is intended to be:

```sh
go install remount.dev/remount/cmd/remount@v0.1.0
```

The module's declared path is `remount.dev/remount`, so substituting its GitHub
URL is not a supported workaround. The vanity domain must publish Go import
metadata before this path can pass.

The runtime image contains only the static binary:

```sh
docker run --rm ghcr.io/andrewgcodes/remount:v0.1.0 version
cosign verify \
  --certificate-identity "https://github.com/andrewgcodes/remount/.github/workflows/release-e18.yml@refs/heads/main" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/andrewgcodes/remount@sha256:DIGEST
```

Deploy by digest rather than by a mutable tag. The container is the Remount
server/node binary, not the workspace image documented in `images.md`.

## Maintainer release proof

1. Create a signed `vMAJOR.MINOR.PATCH` tag on a clean commit.
2. Let `.github/workflows/release.yml` build, attest and publish the six
   binaries, SPDX JSON, checksums and checksum Sigstore bundle.
3. Let `.github/workflows/release-e18.yml`, chained from the completed release
   workflow, download those public assets, verify them, exercise `install.sh`,
   exercise public `go install`, generate the formula, publish and sign the
   two-platform runtime image, verify its digest, and run its `version` command.
4. Commit the generated formula to the external Homebrew tap and run
   `brew install --build-from-source` on both macOS architectures or CI runners.
5. Record workflow URLs, immutable image digest, tap commit and cleanup in the
   monthly verification ledger.

`scripts/release/reproducible.sh TAG` builds all six targets twice in separate
directories and requires byte identity. Run it before the tag. It proves local
reproducibility for that source/toolchain; the release's provenance attestation
proves which workflow produced the published bytes.

## Current E18 disposition (2026-09-03)

E18 has **not** been exercised. No `v0.1.0` release was created in this work,
and no package, formula or tag was published. Two external publication
prerequisites are still open:

- the 2026-09-03 check found `remount.dev` and `get.remount.dev` did not resolve;
  public Go import metadata and the short installer endpoint must be verified
  again as part of publication;
- the Homebrew tap repository/formula publication has not been created or
  verified.

The binary now falls back to Go module build information when no release
linker flag was supplied, so a versioned `go install ...@TAG` reports that tag.
Local source builds remain `dev`, and an explicit release linker value still
wins. The release workflow continues to test this against the public module
path rather than treating the unit contract as E18 evidence.

The verification workflow is intentionally red at the Go-install step until
the vanity metadata exists. Skipping that step would not make E18 pass.
Container publish, keyless signing and release uploads happen only in the
automatic `workflow_run` path after the tag workflow succeeds; a manual
dispatch is verification-only. None were invoked while preparing these files.

## Primary references

- [Go versioned installs](https://go.dev/cmd/go/#hdr-Compile_and_install_packages_and_dependencies)
- [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
- [Sigstore verification](https://docs.sigstore.dev/cosign/verifying/verify/)
- [Homebrew Formula Cookbook](https://docs.brew.sh/Formula-Cookbook)
- [GitHub workflow chaining and `GITHUB_TOKEN`](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow#triggering-a-workflow-from-a-workflow)
- [Docker multi-platform builds](https://docs.docker.com/build/building/multi-platform/)
- [GitHub tag signature verification](https://docs.github.com/en/rest/git/tags)
