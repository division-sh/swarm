#!/usr/bin/env python3
"""Install one identified archive in a fresh offline environment, not a live demo."""

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import pty
import re
import secrets
import select
import signal
import subprocess
import termios
import time
import uuid


IMAGE = "buildpack-deps@sha256:10ffecf0416e47f8ed9a621ce234b209b69c3d1e7e79ba273394ef6ff74b4f5b"
HOME = "/home/newcomer"
LIMIT = 2 * 1024 * 1024
STEPS = "directories checksum extract install path version help scaffold cd verify test secret serve".split()
CANCELLED = None


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def check_cancelled():
    require(CANCELLED is None, "driver cancelled by " + str(CANCELLED))


def run_owned(run):
    global CANCELLED
    def cancel(signum, _frame):
        global CANCELLED
        CANCELLED = signal.Signals(signum).name
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, signal.SIG_IGN)
    previous = {sig: signal.signal(sig, cancel) for sig in [signal.SIGINT, signal.SIGTERM]}
    try:
        return run.run()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        CANCELLED = None


def docker(*args, cleanup=False):
    if not cleanup:
        check_cancelled()
    result = subprocess.run(["docker", *args], capture_output=True, timeout=30, check=False)
    if result.returncode:
        # Retain only closed safe fragments and a digest, not arbitrary engine/plugin text.
        known = ["Couldn't find PID field in ps output", "No such container", "permission denied", "Cannot connect to the Docker daemon"]
        detail = next((text for text in known if text.encode() in result.stderr), "redacted")
        raise RuntimeError(f"docker {args[0]} exit={result.returncode} stderr={detail} stderr_sha256={digest(result.stderr)}")
    require(len(result.stdout) + len(result.stderr) <= LIMIT, "oversized docker observation")
    return result.stdout.decode()


class Terminal:
    def __init__(self, argv):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            try:
                os.execvp(argv[0], argv)
            except OSError:
                os._exit(127)
        flags = termios.tcgetattr(self.fd)
        flags[3] &= ~termios.ECHO  # Only the outer relay; inspect the product PTY separately.
        termios.tcsetattr(self.fd, termios.TCSANOW, flags)
        self.status = None

    def poll(self):
        if self.status is None:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.status = os.waitstatus_to_exitcode(status)
        return self.status

    def receive(self):
        if not select.select([self.fd], [], [], 0.05)[0]:
            return b""
        try:
            return os.read(self.fd, 65536)
        except OSError as error:
            if error.errno != errno.EIO:
                raise
            return b""

    def send(self, value):
        os.write(self.fd, value)

    def close(self):
        try:
            if self.poll() is None:
                try:
                    self.send(b"\x03")
                except OSError:
                    pass  # A closed input does not remove the obligation to join.
                deadline = time.monotonic() + 2
                while self.poll() is None and time.monotonic() < deadline:
                    self.receive()
            forced = self.poll() is None
            if forced:
                try:
                    os.killpg(self.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # It may exit between poll and kill; wait still owns the join.
                _, status = os.waitpid(self.pid, 0)
                self.status = os.waitstatus_to_exitcode(status)
            return forced
        finally:
            os.close(self.fd)


def drive(terminal, kind, fixture, echo_hidden, readiness, capture, timeout=60):
    raw, prompts, echo, interrupted = b"", 0, [], False
    synthetic = ("synthetic-only-" + secrets.token_hex(20)).encode()
    deadline, exit_seen, terminal_name = time.monotonic() + timeout, False, None
    try:
        while True:
            check_cancelled()
            chunk = terminal.receive()
            raw += chunk
            require(len(raw) <= LIMIT, "oversized terminal output")
            text = raw.decode(errors="replace").replace("\r", "")
            if terminal_name is None and "\n" in text:
                terminal_name, _ = text.split("\n", 1)
                require(re.fullmatch(r"/dev/pts/[0-9]+", terminal_name), "missing inner PTY identity")
            output = text.split("\n", 1)[1] if terminal_name else ""
            if kind == "secret" and prompts < 2 and fixture["secret"][prompts] in output:
                hidden = echo_hidden(terminal_name)
                echo.append(hidden)
                require(hidden, "product terminal echo is enabled")
                prompts += 1
                terminal.send(synthetic + b"\n")
            if kind == "serve" and not interrupted and fixture["serve"][-2] in output:
                require(readiness() == "ready\n", "unexpected readiness response")
                terminal.send(b"\x03")
                interrupted = True
            if terminal.poll() is not None and not chunk:
                if exit_seen:
                    break
                exit_seen = True
            require(time.monotonic() < deadline, "terminal safety deadline exceeded")
        require(terminal_name is not None, "missing inner PTY identity")
        require(synthetic not in raw, "synthetic input echoed")
        require(terminal.status == 0, "command exit was not zero")
        if kind == "secret":
            require(prompts == 2, "incomplete hidden-entry prompts")
        if kind == "serve":
            require(interrupted, "serve exited without observed readiness and cancellation")
        return output, {"exit": terminal.status, "hidden_prompts": echo, "sigint": interrupted}
    finally:
        # Sanitize even failed echo controls; never retain the synthetic input.
        capture(raw.replace(synthetic, b"[SYNTHETIC REDACTED]").decode(errors="replace"))


def assert_next(output, expected):
    require(output.count("Next:\n") == 1, "missing or ambiguous Next instructions")
    actual = [line.strip() for line in output.split("Next:\n", 1)[1].strip().splitlines()]
    require(actual == expected, "printed Next differs from the approved command whitelist")


def assert_identity(version, expected_version, source):
    require(all(version.get(key) == value for key, value in
                {"binary_version": expected_version, "commit": source, "goos": "linux", "goarch": "amd64"}.items()), "wrong binary identity")


def assert_idle(output, pid):
    rows = [line.split() for line in output.splitlines()]
    require(type(pid) is int and pid > 0 and rows == [["PID", "COMMAND"], [str(pid), "sleep"]],
            "expected only the identified idle supervisor in process observation")


class Acceptance:
    def __init__(self, args):
        self.args, self.container = args, "swarm-first-run-" + uuid.uuid4().hex
        self.cwd, self.path = HOME, "/usr/local/bin:/usr/bin:/bin"
        self.fixture = json.loads(Path(__file__).with_name("first-run-transcript.json").read_text())
        require(self.fixture["format"] == 1, "unknown transcript format")
        args.output.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.record = {"label": args.kind + " archive; synthetic/offline startup, NOT provider or guided acceptance",
                       "source": args.source, "version": args.version, "image": IMAGE,
                       "driver_sha256": file_digest(Path(__file__)),
                       "started": time.time(), "steps": [], "complete": False}
        self.created = False

    def persist(self):
        (self.args.output / "receipt.json").write_text(json.dumps(self.record, indent=2) + "\n")

    def exec(self, command):
        env = ["env", "-i", "HOME=" + HOME, "PATH=" + self.path, "LANG=C.UTF-8", "NO_COLOR=1"]
        env += ["XDG_" + key + "_HOME=" + HOME + suffix for key, suffix in
                [("CONFIG", "/.config"), ("CACHE", "/.cache"), ("DATA", "/.local/share"), ("STATE", "/.local/state")]]
        return ["exec", "--user", "1000:1000", "--workdir", self.cwd,
                self.container, *env, "/bin/bash", "--noprofile", "--norc", "-o", "pipefail", "-c", command]

    def observe(self, command):
        return docker(*self.exec(command))

    def step(self, name, command, kind="ordinary"):
        check_cancelled()
        start, entry, terminal, captured = time.monotonic(), {"step": name, "command": command, "cwd": self.cwd, "started": time.time()}, None, []
        try:
            args = self.exec(command)
            shell = args.index("/bin/bash")
            terminal = Terminal(["docker", args[0], "--interactive", "--tty", *args[1:shell],
                                 "/bin/sh", "-c", 'tty; exec "$@"', "relay", *args[shell:]])
            output, details = drive(terminal, kind, self.fixture,
                lambda tty: bool(re.search(r"(?:^|\s)-echo(?:\s|;|$)", self.observe("stty --file=" + tty + " -a"))),
                lambda: self.observe("curl --fail --silent --show-error --max-time 2 http://127.0.0.1:8081/readyz"), captured.append)
            entry.update(details)
            return output
        except Exception as error:
            entry["failure"] = str(error)
            self.record.setdefault("failure", str(error))
            raise
        finally:
            cleanup_error = None
            if terminal:
                try:
                    entry["forced_child_cleanup"] = terminal.close()
                    entry["joined_exit"] = terminal.status
                except Exception as error:
                    entry["cleanup_failure"], cleanup_error = str(error), error
            output = captured[0] if captured else ""
            (self.args.output / (name + ".tty")).write_text(output)
            entry.update(seconds=round(time.monotonic() - start, 3), output_sha256=digest(output.encode()))
            self.record["steps"].append(entry)
            self.persist()
            if cleanup_error:
                raise cleanup_error

    def run(self):
        archive = "swarm_" + self.args.version + "_linux_amd64.tar.gz"
        start = time.monotonic()
        try:
            for name in [archive, "SHA256SUMS"]:
                path = self.args.assets / name
                require(path.is_file() and not path.is_symlink(), "missing or nonregular artifact")
                self.record[name + "_sha256"] = file_digest(path)
            checksums = (self.args.assets / "SHA256SUMS").read_text(encoding="ascii")
            require(re.findall(r"^([0-9a-f]{64})  " + re.escape(archive) + "$", checksums, re.MULTILINE)
                    == [self.record[archive + "_sha256"]], "selected archive checksum mismatch")
            docker("create", "--pull=never", "--network=none", "--user=1000:1000", "--name", self.container,
                   IMAGE, "sleep", "infinity")
            self.created = True
            docker("start", self.container)
            state = json.loads(docker("inspect", self.container))[0]
            require(not state["Mounts"] and state["HostConfig"]["NetworkMode"] == "none"
                    and state["Config"]["User"] == "1000:1000", "wrong isolation")
            self.record["isolation"] = {"network": "none", "mounts": [], "user": "1000:1000", "image_id": state["Image"]}
            docker("exec", "--user=0", self.container, "install", "-d", "-m", "700", "-o", "1000", "-g", "1000", HOME)
            self.observe('test -z "$(ls -A "$HOME")" && test ! -e /var/run/docker.sock && ! command -v go && ! command -v docker')
            self.step("directories", 'mkdir -p "$HOME/.local/bin" "$HOME/release" "$HOME/work"')
            self.cwd = HOME + "/release"
            for name in [archive, "SHA256SUMS"]:
                docker("cp", str(self.args.assets / name), self.container + ":" + self.cwd + "/" + name)
            require(self.step("checksum", "sha256sum --check --ignore-missing SHA256SUMS").strip()
                    == archive + ": OK", "selected archive was not checked")
            self.step("extract", "tar -xzf " + archive)
            self.step("install", 'install -m 755 swarm "$HOME/.local/bin/swarm"')
            self.step("path", 'export PATH="$HOME/.local/bin:$PATH"')
            self.path = HOME + "/.local/bin:" + self.path
            product_start = time.monotonic()
            self.record["setup_seconds"] = round(product_start - start, 3)
            version = json.loads(self.step("version", "swarm version --json"))
            assert_identity(version, self.args.version, self.args.source)
            self.record["binary_sha256"] = self.observe('sha256sum "$HOME/.local/bin/swarm"').split()[0]
            help_output = self.step("help", "swarm --help")
            require(all(word in help_output for word in ["new", "verify", "test", "serve"]), "missing first-contact commands")
            self.cwd = HOME + "/work"
            assert_next(self.step("scaffold", "swarm new " + self.fixture["project"]), self.fixture["next"])
            self.step("cd", self.fixture["next"][0])
            self.cwd += "/" + self.fixture["project"]
            config = self.observe("cat .swarm/swarm.yaml")
            require("store:\n  backend: sqlite\nserve:" in config, "unexpected explicit store configuration")
            authored = "find . -path './.swarm' -prune -o -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum"
            before = self.observe(authored)
            require(self.step("verify", self.fixture["next"][1]).startswith(self.fixture["verify"]), "unexpected portable verification")
            require(self.step("test", self.fixture["next"][2]).strip() == self.fixture["test"], "unexpected mock scenario result")
            secret = self.step("secret", self.fixture["next"][3], "secret")
            require(all(text in secret for text in self.fixture["secret"]), "unexpected credential presence output")
            serve = self.step("serve", self.fixture["next"][4], "serve")
            require(all(text in serve for text in self.fixture["serve"]), "unexpected serve scope or shutdown")
            self.record["product_seconds"] = round(time.monotonic() - product_start, 3)
            boot = re.findall(r"^  ready in (.+)$", serve, re.MULTILINE)
            require(len(boot) == 1, "missing or ambiguous measured boot time")
            self.record["reported_boot_duration"] = boot[0]
            require(before == self.observe(authored), "authored source changed")
            require(config == self.observe("cat .swarm/swarm.yaml"), "deployment configuration changed")
            self.record["authored_sha256"] = digest(before.encode())
            require(self.record["binary_sha256"] == self.observe('sha256sum "$HOME/.local/bin/swarm"').split()[0], "installed binary changed")
            processes = docker("top", self.container, "-eo", "pid,comm")
            assert_idle(processes, state["State"]["Pid"])
            self.record["idle_processes_sha256"] = digest(processes.encode())
            check_cancelled()
            self.record["complete"] = True
        except Exception as error:
            self.record.setdefault("failure", str(error))
        finally:
            if self.created:
                try:
                    docker("rm", "--force", self.container, cleanup=True)
                    self.record["container_removed"] = True
                except Exception as error:
                    self.record.update(complete=False, cleanup_failure=str(error))
            self.record["seconds"] = round(time.time() - self.record["started"], 3)
            self.record["unstarted"] = [name for name in STEPS if name not in [s["step"] for s in self.record["steps"]]]
            if CANCELLED:
                self.record.update(complete=False, cancelled=CANCELLED)
                self.record.setdefault("failure", "driver cancelled by " + CANCELLED)
            self.persist()
        return self.record["complete"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--kind", choices=["candidate", "published"], required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", args.version), "invalid version")
    require(re.fullmatch(r"[0-9a-f]{40}", args.source), "invalid source identity")
    raise SystemExit(0 if run_owned(Acceptance(args)) else 1)
