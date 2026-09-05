"""Offline contract checks for the generated documentation index."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCES = (
    "README.md",
    "docs/using-remount.md",
    "docs/tutorial.md",
    "docs/harness-integration.md",
    "docs/api.md",
)
BASE = "https://github.com/andrewgcodes/remount/blob/main"


class GeneratedDocsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="remount-docs-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in (*SOURCES, "scripts/gen-llms.sh"):
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)

    def generate(self, base=None):
        env = os.environ.copy()
        env.pop("LLMS_BASE_URL", None)
        if base is not None:
            env["LLMS_BASE_URL"] = base
        subprocess.run(
            ["sh", str(self.root / "scripts/gen-llms.sh")],
            env=env, check=True, capture_output=True, text=True,
        )
        return tuple((self.root / name).read_text() for name in ("llms.txt", "llms-full.txt"))

    def test_default_index_targets_current_repository_and_existing_sections(self):
        index, full = self.generate()
        links = re.findall(r"^- \[[^\]]+\]\(([^)]+)\)", index, re.MULTILINE)
        self.assertGreater(len(links), 50)
        for link in links:
            self.assertTrue(link.startswith(BASE + "/"), link)
            relative, _, fragment = link[len(BASE) + 1:].partition("#")
            target = ROOT / relative
            self.assertTrue(target.exists(), relative)
            if fragment:
                headings = re.findall(r"^## (.+)$", target.read_text(), re.MULTILINE)
                anchors = [re.sub(r"[^a-z0-9 _-]", "", h.lower()).replace(" ", "-") for h in headings]
                self.assertIn(fragment, anchors, link)
        self.assertIn(BASE + "/docs/engineering/current-status.md", index)
        self.assertNotIn("remount-dev/remount", index)
        for name in SOURCES:
            self.assertIn((ROOT / name).read_text(), full)

    def test_base_override_and_determinism(self):
        base = "https://docs.example.invalid/source"
        first = self.generate(base)
        self.assertEqual(first, self.generate(base))
        self.assertIn(base + "/README.md#", first[0])
        self.assertNotIn(BASE, first[0])

    def test_empty_override_uses_default(self):
        self.assertEqual(self.generate(), self.generate(""))

    def test_checked_in_outputs_match_sources(self):
        for name, generated in zip(("llms.txt", "llms-full.txt"), self.generate()):
            self.assertEqual((ROOT / name).read_text(), generated, "run make docs: " + name)


if __name__ == "__main__":
    unittest.main()
