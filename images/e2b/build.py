#!/usr/bin/env python3
"""Build the Remount node template on E2B.

    pip install e2b            # SDK 2.x
    export E2B_API_KEY=...     # from the E2B dashboard
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o remount-linux-amd64 ./cmd/remount
    python3 images/e2b/build.py remount-linux-amd64 [template-name]

The template carries the node binary and a start command that waits for the
bootstrap file the control plane's E2B driver delivers after each sandbox is
created (see remount-node-start.sh). Point the `e2b` provisioner entry at the
resulting template name and the reconciler does the rest.
"""
import os
import shutil
import sys

from e2b import Template, default_build_logger

HERE = os.path.dirname(os.path.abspath(__file__))

# Pinned gVisor release and Alpine minirootfs; update the digests together.
RUNSC_URL = "https://storage.googleapis.com/gvisor/releases/release/20260817.0/x86_64/runsc"
RUNSC_SHA = "84936438d583ec976800f464e75a83e1515f0890b451b9b4db219c4472b54ca9b106a6772ee683f1e64cce2128871d7637b14d800591f8451b8137f6c39fb2ef"
ROOTFS_URL = "https://dl-cdn.alpinelinux.org/alpine/v3.22/releases/x86_64/alpine-minirootfs-3.22.1-x86_64.tar.gz"
ROOTFS_SHA = "0e5cc5702ad72a4e151f219976ba946d50161c3acce210ef3b122a529aba1270"


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    binary = sys.argv[1]
    name = sys.argv[2] if len(sys.argv) > 2 else "remount-node"
    if not os.path.isfile(binary):
        sys.exit(f"{binary}: not a file")
    # The builder only copies files under the working directory, so stage the
    # binary next to the start script for the duration of the build.
    staged = os.path.join(HERE, "remount-linux-amd64")
    if os.path.abspath(binary) != staged:
        shutil.copyfile(binary, staged)
    os.chdir(HERE)
    binary = os.path.basename(staged)
    template = (
        Template()
        .from_debian_image("bookworm")
        .apt_install(["ca-certificates", "curl", "procps"])
        .copy(binary, "/usr/local/bin/remount", mode=0o755, user="root")
        .copy("remount-node-start.sh", "/usr/local/bin/remount-node-start", mode=0o755, user="root")
        .run_cmd("mkdir -p /var/lib/remount && chown user:user /var/lib/remount", user="root")
        # gVisor and an immutable rootfs: production control planes admit only
        # nodes whose backend satisfies the isolated floor, and inside an E2B
        # sandbox runsc runs on its systrap platform without KVM.
        .run_cmd(
            f"curl -fsSL -o /usr/local/bin/runsc {RUNSC_URL} && echo '{RUNSC_SHA}  /usr/local/bin/runsc' | sha512sum -c - "
            "&& chmod 0755 /usr/local/bin/runsc",
            user="root",
        )
        .run_cmd(
            f"mkdir -p /opt/remount/rootfs && curl -fsSL -o /tmp/rootfs.tgz {ROOTFS_URL} && echo '{ROOTFS_SHA}  /tmp/rootfs.tgz' | sha256sum -c - "
            "&& tar -xzf /tmp/rootfs.tgz -C /opt/remount/rootfs && rm /tmp/rootfs.tgz",
            user="root",
        )
        # The start command idles until the bootstrap file arrives, so a fixed
        # short wait is the honest ready check at build time.
        # The node runs as root: runsc and the enforced network need it.
        .set_start_cmd("sudo -n /usr/local/bin/remount-node-start", "sleep 3")
    )
    try:
        info = Template.build(template, name, cpu_count=2, memory_mb=2048, on_build_logs=default_build_logger())
    finally:
        if os.path.abspath(sys.argv[1]) != staged:
            os.unlink(staged)
    print(f"built {info.name} template_id={info.template_id} build_id={info.build_id}")


if __name__ == "__main__":
    main()
