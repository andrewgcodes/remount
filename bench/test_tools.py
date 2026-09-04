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
COMPARE = load("compare")


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


    # ---- bench/compare.py -------------------------------------------------
    #
    # These exist because compare.py's whole purpose is to notice a number
    # getting worse, and the 2026-09 sweep is the proof that "someone will read
    # the output" is not a control. Each test drives the tool with the shape of
    # a real regression from that sweep.

    @staticmethod
    def _scale(**latency) -> dict:
        return {
            "schema": 1, "backend": "process", "host": "Darwin arm64",
            "nodes": 200, "workspaces": 2000, "clients": 200,
            "latency_seconds": {k: v for k, v in latency.items()},
        }

    def test_compare_reports_the_claim_regression_that_only_moved_the_tail(self) -> None:
        # The real one: claim p50 did not move at all, p99 went 0.79s -> 7.82s.
        # A comparison that looked only at p50 would have called this clean.
        baseline = self._scale(claim={"p50": 0.44, "p99": 0.79})
        candidate = self._scale(claim={"p50": 0.44, "p99": 7.82})
        rows, regressed = COMPARE.compare(baseline, candidate, 3.0)
        self.assertTrue(regressed)
        by_pct = {r["percentile"]: r for r in rows}
        self.assertEqual(by_pct["p50"]["status"], "unchanged")
        self.assertEqual(by_pct["p99"]["status"], "regressed")
        self.assertAlmostEqual(by_pct["p99"]["ratio"], 7.82 / 0.79, places=6)

    def test_compare_calls_a_real_improvement_improved(self) -> None:
        baseline = self._scale(reattach={"p50": 6.75, "p99": 6.9})
        candidate = self._scale(reattach={"p50": 0.72, "p99": 0.8})
        rows, regressed = COMPARE.compare(baseline, candidate, 3.0)
        self.assertFalse(regressed)
        self.assertEqual({r["status"] for r in rows}, {"improved"})

    def test_a_measurement_that_disappeared_is_unavailable_and_not_a_pass(self) -> None:
        # A renamed or dropped operation is exactly how a regression becomes
        # invisible, so it must not read as success.
        baseline = self._scale(exec_round_trip={"p50": 0.645, "p99": 0.7})
        candidate = self._scale()
        rows, regressed = COMPARE.compare(baseline, candidate, 3.0)
        self.assertFalse(regressed)
        self.assertEqual({r["status"] for r in rows}, {"unavailable"})
        self.assertIn("unavailable", COMPARE.render(rows, 3.0))

    def test_runs_of_different_shapes_are_refused_rather_than_compared(self) -> None:
        baseline = self._scale(claim={"p50": 1.0, "p99": 1.0})
        candidate = dict(baseline, nodes=20, workspaces=200)
        reasons = COMPARE.check_comparable(baseline, candidate, force=False)
        self.assertTrue(any("nodes" in r for r in reasons))
        self.assertEqual(COMPARE.check_comparable(baseline, candidate, force=True), [])

    def test_a_zero_baseline_cannot_produce_a_ratio(self) -> None:
        baseline = self._scale(claim={"p50": 0.0, "p99": 0.0})
        candidate = self._scale(claim={"p50": 1.0, "p99": 1.0})
        rows, regressed = COMPARE.compare(baseline, candidate, 3.0)
        self.assertFalse(regressed)
        self.assertEqual({r["status"] for r in rows}, {"unavailable"})

    def test_a_file_that_is_not_scale_evidence_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "not-evidence.json")
            path.write_text(json.dumps({"schema": 1}), encoding="utf-8")
            with self.assertRaisesRegex(COMPARE.Incomparable, "latency_seconds"):
                COMPARE.load(path)



if __name__ == "__main__":
    unittest.main()
