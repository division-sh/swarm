import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("acceptance", Path(__file__).with_name("first-run-acceptance.py"))
D = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(D)
FIXTURE = json.loads(Path(__file__).with_name("first-run-transcript.json").read_text())


class PeerTerminal:
    """Driver-only protocol double; never Swarm acceptance evidence."""
    def __init__(self, output=b"", exit_code=0, respond=None):
        self.chunks, self.sent = [b"/dev/pts/7\r\n" + output], []
        self.status, self.exit_code, self.respond = None, exit_code, respond

    def receive(self):
        return self.chunks.pop(0) if self.chunks else b""

    def poll(self):
        if not self.chunks:
            self.status = self.exit_code
        return self.status

    def send(self, value):
        self.sent.append(value)
        if self.respond:
            self.chunks.append(self.respond(value, len(self.sent)))


class FirstRunControls(unittest.TestCase):
    def drive(self, terminal, kind="ordinary", hidden=lambda _: True, ready=lambda: "ready\n", timeout=1):
        self.capture = []
        return D.drive(terminal, kind, FIXTURE, hidden, ready, self.capture.append, timeout)

    def test_next_refuses_missing_extra_wrong_cwd_and_old_dot(self):
        lines = FIXTURE["next"]
        D.assert_next("Next:\n  " + "\n  ".join(lines), lines)
        for bad in [lines[:-1], lines + ["echo unknown"], ["cd elsewhere", *lines[1:]],
                    [*lines[:2], "swarm test . tests/smoke.yaml", *lines[3:]]]:
            with self.subTest(commands=bad), self.assertRaisesRegex(RuntimeError, "whitelist"):
                D.assert_next("Next:\n" + "\n".join(bad), lines)
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            D.assert_next("not printed", lines)

    def test_exact_identity_not_generic_version_success(self):
        identity = dict(binary_version="v1.2.3", commit="a" * 40, goos="linux", goarch="amd64")
        D.assert_identity(identity, "v1.2.3", "a" * 40)
        for key in identity:
            with self.subTest(field=key), self.assertRaisesRegex(RuntimeError, "identity"):
                D.assert_identity({**identity, key: "wrong"}, "v1.2.3", "a" * 40)

    def test_hidden_entry_is_observed_twice_without_storing_value(self):
        terminal = PeerTerminal(b"Secret value: ", respond=lambda _, n: b"Confirm secret value: " if n == 1 else b"done")
        _, evidence = self.drive(terminal, "secret")
        self.assertEqual(evidence["hidden_prompts"], [True, True])
        self.assertEqual(terminal.sent[0], terminal.sent[1])
        self.assertNotIn(terminal.sent[0].strip(), self.capture[0].encode())

    def test_enabled_echo_refuses_before_any_input(self):
        terminal = PeerTerminal(b"Secret value: ")
        with self.assertRaisesRegex(RuntimeError, "echo is enabled"):
            self.drive(terminal, "secret", hidden=lambda _: False)
        self.assertEqual(terminal.sent, [])

    def test_echoed_and_incomplete_secrets_are_failures_with_safe_capture(self):
        terminal = PeerTerminal(b"Secret value: ", respond=lambda value, n: value + (b"Confirm secret value: " if n == 1 else b"done"))
        with self.assertRaisesRegex(RuntimeError, "input echoed"):
            self.drive(terminal, "secret")
        self.assertNotIn(terminal.sent[0].strip(), self.capture[0].encode())
        self.assertIn("[SYNTHETIC REDACTED]", self.capture[0])
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            self.drive(PeerTerminal(b"Secret value: "), "secret")

    def test_failed_exit_readiness_and_timeout_never_pass(self):
        with self.assertRaisesRegex(RuntimeError, "exit"):
            self.drive(PeerTerminal(b"bad command", exit_code=3))
        for response in ["not ready\n", "ready"]:
            with self.subTest(response=response), self.assertRaisesRegex(RuntimeError, "readiness"):
                self.drive(PeerTerminal(FIXTURE["serve"][-2].encode()), "serve", ready=lambda: response)
        with self.assertRaisesRegex(RuntimeError, "deadline"):
            self.drive(PeerTerminal(exit_code=None), timeout=0)
        with self.assertRaisesRegex(RuntimeError, "oversized"):
            self.drive(PeerTerminal(b"x" * D.LIMIT))

    def test_readiness_cancels_once_and_requires_terminal_identity(self):
        terminal = PeerTerminal(FIXTURE["serve"][-2].encode())
        _, evidence = self.drive(terminal, "serve")
        self.assertEqual(terminal.sent, [b"\x03"])
        self.assertTrue(evidence["sigint"])
        terminal = PeerTerminal()
        terminal.chunks = [b""]
        with self.assertRaisesRegex(RuntimeError, "PTY identity"):
            self.drive(terminal)

    def test_artifact_failure_has_no_container_and_first_failure_is_retained(self):
        for corrupt, cleanup_failure in [(True, False), (False, False), (False, True)]:
            with self.subTest(corrupt=corrupt, cleanup_failure=cleanup_failure), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                archive = root / "swarm_v1.2.3_linux_amd64.tar.gz"
                archive.write_bytes(b"driver-only artifact")
                (root / "SHA256SUMS").write_text(("0" * 64 if corrupt else D.digest(archive.read_bytes())) + "  " + archive.name + "\n")
                args = SimpleNamespace(assets=root, output=root / "output", source="a" * 40, version="v1.2.3", kind="candidate")
                def docker(*argv, **_kwargs):
                    if argv[0] == "inspect":
                        return json.dumps([dict(Mounts=[], Image="driver-only-image", State=dict(Pid=17), HostConfig=dict(NetworkMode="none"), Config=dict(User="1000:1000"))])
                    if argv[0] == "rm" and cleanup_failure:
                        raise RuntimeError("cleanup refused")
                    return ""
                with patch.object(D, "docker", side_effect=docker) as calls, patch.object(D.Acceptance, "step", side_effect=RuntimeError("first command failed")) as steps:
                    run = D.Acceptance(args)
                    self.assertFalse(run.run())
                receipt = json.loads((args.output / "receipt.json").read_text())
                self.assertFalse(receipt["complete"])
                self.assertIn("checksum" if corrupt else "first command", receipt["failure"])
                self.assertEqual(steps.call_count, 0 if corrupt else 1)
                if corrupt:
                    calls.assert_not_called()
                else:
                    self.assertEqual(sum(call.args[0] == "rm" for call in calls.call_args_list), 1)
                    self.assertEqual("cleanup_failure" in receipt, cleanup_failure)

    def test_missing_artifact_never_creates_or_deletes_a_container(self):
        with tempfile.TemporaryDirectory() as root, patch.object(D, "docker") as calls:
            root = Path(root)
            args = SimpleNamespace(assets=root, output=root / "output", source="a" * 40, version="v1.2.3", kind="candidate")
            self.assertFalse(D.Acceptance(args).run())
            receipt = json.loads((args.output / "receipt.json").read_text())
            self.assertIn("nonregular artifact", receipt["failure"])
            self.assertEqual(receipt["unstarted"], D.STEPS)
            calls.assert_not_called()

    def test_real_driver_terminal_cancels_and_joins_without_docker(self):
        code = "import os,signal,sys; signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); print(os.ttyname(0),flush=True); print('ready — waiting for events',flush=True); signal.pause()"
        terminal = D.Terminal([sys.executable, "-c", code])
        try:
            _, evidence = self.drive(terminal, "serve")
            self.assertEqual(evidence["exit"], 0)
        finally:
            self.assertFalse(terminal.close())

    def test_real_terminal_forced_cleanup_is_not_normal_completion(self):
        terminal = D.Terminal([sys.executable, "-c", "import signal; signal.signal(signal.SIGINT,signal.SIG_IGN); print('armed',flush=True); signal.pause()"])
        deadline, output = D.time.monotonic() + 5, b""
        try:
            while b"armed" not in output and D.time.monotonic() < deadline:
                output += terminal.receive()
            self.assertIn(b"armed", output)
        finally:
            self.assertTrue(terminal.close())
        self.assertEqual(terminal.status, -D.signal.SIGKILL)

    def test_idle_process_read_requires_exact_header_pid_and_single_supervisor(self):
        D.assert_idle("PID COMMAND\n17 sleep\n", 17)
        for output in ["COMMAND\nsleep\n", "PID COMMAND\n", "PID COMMAND\n17 swarm\n",
                       "PID COMMAND\n18 sleep\n", "PID COMMAND\n17 sleep extra\n",
                       "PID COMMAND\n17 sleep\n18 sleep\n", "PID COMMAND\nnot-a-pid sleep\n"]:
            with self.subTest(output=output), self.assertRaisesRegex(RuntimeError, "idle supervisor"):
                D.assert_idle(output, 17)

    def test_docker_nonzero_is_retained_without_arbitrary_stderr(self):
        for stderr, expected in [(b"Couldn't find PID field in ps output", "PID field"),
                                 (b"private-location secret-MUST-NOT-PRINT", "redacted")]:
            with patch.object(D.subprocess, "run", return_value=SimpleNamespace(returncode=17, stderr=stderr)):
                with self.assertRaises(RuntimeError) as error:
                    D.docker("top", "driver-only-container", "-eo", "pid,comm")
            self.assertIn("exit=17", str(error.exception))
            self.assertIn(expected, str(error.exception))
            self.assertNotIn("secret-MUST-NOT-PRINT", str(error.exception))

    def test_parent_signals_join_child_cleanup_container_and_write_failure_receipt(self):
        for sig in [signal.SIGINT, signal.SIGTERM]:
            with self.subTest(signal=sig), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                (root / "swarm_v1.2.3_linux_amd64.tar.gz").write_bytes(b"driver-only")
                (root / "SHA256SUMS").write_text(D.digest(b"driver-only") + "  swarm_v1.2.3_linux_amd64.tar.gz\n")
                child = "import os,signal,sys; from pathlib import Path; signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); Path(" + repr(str(root / "armed")) + ").write_text(str(os.getpid())); print(os.ttyname(0),flush=True); signal.pause()"
                code = f'''
import importlib.util,json,sys,time
from pathlib import Path
from types import SimpleNamespace
spec=importlib.util.spec_from_file_location("driver",{str(Path(D.__file__).resolve())!r})
d=importlib.util.module_from_spec(spec); spec.loader.exec_module(d)
root=Path({str(root)!r}); original=d.Terminal
d.Terminal=lambda _argv:original([sys.executable,"-c",{child!r}])
def docker(*argv,**kwargs):
    if argv[0]=="inspect": return json.dumps([dict(Mounts=[],Image="test",State=dict(Pid=17),HostConfig=dict(NetworkMode="none"),Config=dict(User="1000:1000"))])
    if argv[0]=="rm":
        time.sleep(.15); (root/"removed").write_text("true")
    return ""
d.docker=docker
args=SimpleNamespace(assets=root,output=root/"output",source="a"*40,version="v1.2.3",kind="candidate")
sys.exit(0 if d.run_owned(d.Acceptance(args)) else 1)
'''
                parent = subprocess.Popen([sys.executable, "-B", "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 5
                    while not (root / "armed").exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue((root / "armed").exists())
                    parent.send_signal(sig)
                    time.sleep(.02)
                    parent.send_signal(sig)
                    output, error = parent.communicate(timeout=5)
                    self.assertEqual(parent.returncode, 1, (output, error))
                    receipt = json.loads((root / "output/receipt.json").read_text())
                    self.assertFalse(receipt["complete"])
                    self.assertEqual(receipt["cancelled"], sig.name)
                    self.assertTrue(receipt["container_removed"])
                    self.assertEqual(receipt["steps"][0]["joined_exit"], 0)
                    self.assertFalse(receipt["steps"][0]["forced_child_cleanup"])
                    self.assertTrue((root / "removed").exists())
                finally:
                    if parent.poll() is None:
                        if (root / "armed").exists():
                            try:
                                os.killpg(int((root / "armed").read_text()), signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                        parent.kill()
                        parent.communicate()


if __name__ == "__main__":
    unittest.main()
