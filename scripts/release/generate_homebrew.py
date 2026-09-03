#!/usr/bin/env python3
"""Generate a Homebrew formula from one signed release's checksum manifest."""

from __future__ import annotations

import argparse
import pathlib
import re


PLATFORMS = (
    ("darwin", "on_macos", (("arm64", "on_arm"), ("amd64", "on_intel"))),
    ("linux", "on_linux", (("arm64", "on_arm"), ("amd64", "on_intel"))),
)


def checksums(path: pathlib.Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) != 2 or not re.fullmatch(r"[0-9a-fA-F]{64}", fields[0]):
            raise ValueError(f"malformed checksum line: {line!r}")
        name = fields[1].removeprefix("./")
        if name in result:
            raise ValueError(f"duplicate checksum for {name}")
        result[name] = fields[0].lower()
    return result


def render(tag: str, repository: str, sums: dict[str, str]) -> str:
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?", tag):
        raise ValueError("tag must be semantic version prefixed by v")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("repository must be owner/name")
    blocks: list[str] = []
    for os_name, os_block, architectures in PLATFORMS:
        architecture_blocks: list[str] = []
        for arch, arch_block in architectures:
            asset = f"remount-{os_name}-{arch}"
            if asset not in sums:
                raise ValueError(f"checksums omit {asset}")
            architecture_blocks.append(
                f"    {arch_block} do\n"
                f'      url "https://github.com/{repository}/releases/download/{tag}/{asset}", using: :nounzip\n'
                f'      sha256 "{sums[asset]}"\n'
                "    end"
            )
        blocks.append(f"  {os_block} do\n{chr(10).join(architecture_blocks)}\n  end")
    return f'''# typed: strict
# frozen_string_literal: true

# Installs the verified Remount release binary for this host.
class Remount < Formula
  desc "Durable computers for AI agents"
  homepage "https://github.com/{repository}"
  version "{tag.removeprefix('v')}"
  license "Apache-2.0"

{chr(10).join(blocks)}

  def install
    bin.install Dir["remount-*"][0] => "remount"
  end

  test do
    assert_equal "remount v#{{version}}", shell_output("#{{bin}}/remount version").strip
  end
end
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--repository", default="andrewgcodes/remount")
    parser.add_argument("--checksums", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    output = render(args.tag, args.repository, checksums(args.checksums))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(output, encoding="utf-8")


if __name__ == "__main__":
    main()
