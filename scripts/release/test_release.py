from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("generate_homebrew.py")
SPEC = importlib.util.spec_from_file_location("generate_homebrew", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FormulaTest(unittest.TestCase):
    def test_formula_is_complete_and_deterministic(self) -> None:
        sums = {
            f"remount-{os_name}-{arch}": str(index) * 64
            for index, (os_name, arch) in enumerate(
                (("darwin", "arm64"), ("darwin", "amd64"), ("linux", "arm64"), ("linux", "amd64")), 1
            )
        }
        first = MODULE.render("v1.2.3", "owner/repo", sums)
        self.assertEqual(first, MODULE.render("v1.2.3", "owner/repo", sums))
        self.assertIn('version "1.2.3"', first)
        self.assertEqual(first.count("sha256"), 4)
        self.assertEqual(first.count("on_macos do"), 1)
        self.assertEqual(first.count("on_linux do"), 1)
        self.assertEqual(first.count("https://github.com/owner/repo/releases/download/v1.2.3/"), 4)

    def test_checksum_parser_rejects_ambiguity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "checksums.txt")
            path.write_text(f"{'a' * 64}  ./remount-linux-amd64\n{'b' * 64}  remount-linux-amd64\n")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                MODULE.checksums(path)

    def test_refuses_untrusted_tag_and_repository(self) -> None:
        with self.assertRaises(ValueError):
            MODULE.render("../../latest", "owner/repo", {})
        with self.assertRaises(ValueError):
            MODULE.render("v1.2.3", "https://evil.example/x", {})


if __name__ == "__main__":
    unittest.main()
