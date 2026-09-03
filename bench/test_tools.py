from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).parent


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MOVE = load("move_matrix")
RUN = load("run")


class ToolTest(unittest.TestCase):
    def test_sizes_are_explicit_and_bounded_by_integer_parsing(self) -> None:
        self.assertEqual(MOVE.size("1KiB"), 1024)
        self.assertEqual(MOVE.size("500MiB"), 500 << 20)
        for invalid in ("0", "-1MiB", "1MB", "1.5GiB", "11GiB"):
            with self.assertRaises(Exception):
                MOVE.size(invalid)

    def test_server_url_cannot_put_a_secret_in_evidence_or_errors(self) -> None:
        self.assertEqual(MOVE.server_url("wss://control.example/v1/link"), "wss://control.example/v1/link")
        for invalid in ("control.example", "https://token@control.example", "https://control.example/?token=x"):
            with self.assertRaises(Exception):
                MOVE.server_url(invalid)

    def test_nearest_rank_quantile(self) -> None:
        self.assertEqual(MOVE.quantile([4, 1, 3, 2], 0.99), 4)
        self.assertEqual(RUN.quantile([1.0, 2.0, 3.0], 0.5), 2.0)

    def test_scale_evidence_rejects_a_smaller_synthetic_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "scale.json")
            path.write_text(json.dumps({"schema": 1, "backend": "process", "nodes": 20, "workspaces": 200}), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "200 nodes and 2,000 workspaces"):
                RUN.scale_evidence(path)

    def test_fixture_is_deterministic_and_exact_size(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = pathlib.Path(directory, "first")
            second = pathlib.Path(directory, "second")
            self.assertEqual(MOVE.fixture(first, 65539), MOVE.fixture(second, 65539))
            self.assertEqual(first.stat().st_size, 65539)

    def test_checked_in_report_matches_machine_evidence(self) -> None:
        evidence = json.loads((ROOT / "results/latest.json").read_text(encoding="utf-8"))
        report = (ROOT.parent / "docs/benchmarks.md").read_text(encoding="utf-8")
        self.assertEqual(RUN.render(evidence), report)


if __name__ == "__main__":
    unittest.main()
