"""Offline safety tests for the explicitly opt-in live handoff driver."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("live_handoff", Path(__file__).with_name("live-handoff.py"))
driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(driver)


class SafetyTests(unittest.TestCase):
    def test_nested_profile_only_changes_matching_labeled_handoff_run(self):
        image = "remount-handoff:test"
        argv = ["run", "--name", "remount-ws-test", "--label", "remount.workspace=ws_test", image, "sleep", "infinity"]
        changed = driver.docker_profile_argv(argv, image, ["seccomp=unconfined"])
        self.assertEqual(changed[:3], ["run", "--security-opt", "seccomp=unconfined"])
        self.assertNotIn("--privileged", changed)
        for untouched in (["build", image], ["run", image],
                          ["run", "--name", "unrelated", "--label", "remount.workspace=ws_test", image],
                          ["run", "--name", "remount-ws-test", "--label", "remount.workspace=ws_test", "other:image"]):
            self.assertEqual(driver.docker_profile_argv(untouched, image, ["seccomp=unconfined"]), untouched)
        with self.assertRaises(driver.Failure):
            driver.docker_profile_argv(argv, image, ["privileged"])
        source = driver.docker_wrapper_source(image, ["seccomp=unconfined"])
        self.assertIn("/usr/bin/docker", source)
        self.assertNotIn("--privileged", source)
        compile(source, "docker-wrapper", "exec")

    def test_nested_probe_creates_codex_home_before_executing_pinned_sandbox(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = {"image": "remount-handoff:test"}
            args = SimpleNamespace(run_dir=Path(tmp))
            calls = []
            def remote(sb, argv, timeout=180):
                calls.append(argv)
                if argv[0] == "/opt/remount-handoff/bin/docker":
                    self.assertIn('mkdir -p "$CODEX_HOME"; exec "$@"', argv)
                    return 0, "inside-write-ok\noutside-write-denied\n", ""
                return 0, "", ""
            with mock.patch.object(driver, "remote", side_effect=remote), mock.patch("builtins.print"):
                driver.prepare_nested_profile(args, {}, mock.Mock(), state)
            self.assertTrue(state["nested_sandbox_test_profile"]["probe_passed"])
            self.assertEqual(state["nested_sandbox_test_profile"]["security_options"], ["seccomp=unconfined"])
            self.assertEqual(sum(c[0] == "/opt/remount-handoff/bin/docker" for c in calls), 1)

    def test_nested_probe_setup_failure_does_not_claim_permission_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = {"image": "remount-handoff:test"}
            args = SimpleNamespace(run_dir=Path(tmp))
            def remote(sb, argv, timeout=180):
                return (1, "", "Error: CODEX_HOME does not exist") if argv[0].endswith("/docker") else (0, "", "")
            with mock.patch.object(driver, "remote", side_effect=remote), mock.patch("builtins.print"):
                with self.assertRaises(driver.Failure):
                    driver.prepare_nested_profile(args, {}, mock.Mock(), state)
            self.assertEqual(state["nested_sandbox_test_profile"]["failure_kind"], "probe_setup")

    def test_node_inventory_uses_public_nodes_command(self):
        self.assertEqual(driver.node_inventory_command(Path("/candidate/remount")),
                         ["/candidate/remount", "nodes", "--json"])

    def test_no_execute_does_not_read_credentials_or_provision(self):
        result = subprocess.run([sys.executable, str(Path(driver.__file__)), "prepare",
                                 "--run-dir", "/does/not/exist"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("no provider or model calls were made", result.stderr)

    def test_scanner_detects_plaintext_and_base64_and_refuses_symlinks(self):
        keys = {"OPENAI_API_KEY": "synthetic-test-key-0123456789"}
        needles = driver.patterns(keys)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver.prove_scanner(root)
            (root / "plain").write_bytes(needles[0])
            (root / "encoded").write_bytes(needles[1])
            self.assertEqual(driver.scan(root, needles)["leaking_files"], 2)
            (root / "link").symlink_to(root / "plain")
            with self.assertRaises(driver.Failure):
                driver.scan(root, needles)

    def test_clean_environment_does_not_inherit_auth_or_provider_helpers(self):
        with mock.patch.dict(driver.os.environ, {"OPENAI_API_KEY": "secret", "GIT_ASKPASS": "/helper"}):
            env = driver.clean_env(Path("/synthetic/home"))
        self.assertNotIn("OPENAI_API_KEY", env)
        self.assertNotIn("GIT_ASKPASS", env)
        self.assertEqual(env["CODEX_HOME"], "/synthetic/home/.codex")
        self.assertEqual(env["CLAUDE_CONFIG_DIR"], "/synthetic/home/.claude")
        self.assertEqual(env["GIT_CONFIG_GLOBAL"], "/dev/null")

    def test_evidence_redacts_and_fails_on_credential_output(self):
        keys = {"OPENAI_API_KEY": "synthetic-test-key-0123456789"}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(driver.Failure):
                driver.save_command(root, "probe", (0, keys["OPENAI_API_KEY"], ""), keys)
            text = (root / "evidence/probe.json").read_text()
            self.assertNotIn(keys["OPENAI_API_KEY"], text)
            self.assertIn("REDACTED", text)
            self.assertEqual((root / "evidence/probe.json").stat().st_mode & 0o777, 0o600)

    def test_candidate_must_be_explicit_and_hashes_frozen_bytes(self):
        with self.assertRaises(driver.Failure):
            driver.candidate_manifest(SimpleNamespace(binary=None, linux_binary=None, candidate=None))
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "binary"
            p.write_bytes(b"candidate")
            args = SimpleNamespace(binary=p, linux_binary=p, candidate="test-revision")
            first = driver.candidate_manifest(args)
            p.write_bytes(b"different")
            self.assertNotEqual(first, driver.candidate_manifest(args))

    def test_prepare_refuses_second_vm_before_app_create(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver.private_json(root / "provider.json", {"cleaned": True})
            modal = mock.Mock()
            with mock.patch.object(driver, "modal_client", return_value=modal):
                with self.assertRaises(driver.Failure):
                    driver.prepare(SimpleNamespace(run_dir=root), {})
            modal.App.lookup.assert_not_called()
            modal.Sandbox.create.assert_not_called()

    def test_seed_reuse_rejects_metadata_outside_original_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            seed_run = root / "remount-data/seed-run"
            fixture = seed_run / "fixture-claude"
            (fixture / "home").mkdir(parents=True)
            (fixture / "checkout").mkdir()
            data = {"home": str(fixture / "home"), "checkout": str(fixture / "checkout"), "exit": 0}
            driver.private_json(seed_run / "seed-claude.json", data)
            args = SimpleNamespace(seed_run_dir=seed_run, run_dir=root / "other")
            with mock.patch.object(driver, "ROOT", root):
                self.assertEqual(driver.load_seed(args, "claude", {}), data)
                data["home"] = "/personal/home"
                driver.private_json(seed_run / "seed-claude.json", data)
                with self.assertRaises(driver.Failure):
                    driver.load_seed(args, "claude", {})

    def test_cleanup_only_terminates_recorded_run_and_checks_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver.private_json(root / "provider.json", {
                "app_id": "ap-test", "run_id": "run-test", "sandbox_id": "sb-owned", "cleaned": False})
            driver.private_json(root / "runtime.json", {"REMOUNT_TOKEN": "synthetic"})
            owned, other = mock.Mock(), mock.Mock()
            owned.object_id, other.object_id = "sb-owned", "sb-other"
            other.get_tags.return_value = {"remount.handoff": "another-run"}
            modal = mock.Mock()
            modal.Sandbox.list.side_effect = [[owned, other], [other]]
            with mock.patch.object(driver, "modal_client", return_value=modal), \
                 mock.patch.object(driver, "command", return_value=(0, "", "")), \
                 mock.patch("builtins.print"):
                driver.cleanup(SimpleNamespace(run_dir=root), {})
            owned.terminate.assert_called_once()
            owned.wait.assert_called_once_with(raise_on_termination=False)
            other.terminate.assert_not_called()
            self.assertTrue(driver.state_load(root)["cleaned"])
            self.assertFalse((root / "runtime.json").exists())


if __name__ == "__main__":
    unittest.main()
