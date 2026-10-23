#!/usr/bin/env python3
"""Bound transport retries while preserving Go's module and sumdb verification."""

import os
import re
import signal
import subprocess
import sys
import time


TOTAL_SECONDS = 180
ATTEMPT_SECONDS = 60
BACKOFF_SECONDS = (0, 5, 15)
TRANSPORT = re.compile(
    r"https?://[^\n]*:\s*(?:408|429|5\d\d)\b"
    r"|stream error: stream ID \d+; (?:INTERNAL_ERROR|REFUSED_STREAM)\b"
    r"|https?://[^\n]*(?:unexpected EOF|connection reset by peer|TLS handshake timeout|i/o timeout)",
    re.IGNORECASE,
)
PERMANENT = re.compile(
    r"checksum mismatch|SECURITY ERROR|invalid signature|authentication|unauthorized"
    r"|forbidden|permission denied|\b(?:401|403)\b|fatal:|panic:", re.IGNORECASE
)


def transient(output):
    errors = [line for line in output.splitlines()
              if line.startswith("go: ") and not line.startswith("go: downloading ")]
    return (not PERMANENT.search(output) and bool(errors)
            and all(TRANSPORT.search(line) for line in errors))


def main():
    deadline = time.monotonic() + TOTAL_SECONDS
    for attempt, delay in enumerate(BACKOFF_SECONDS, 1):
        if deadline - time.monotonic() <= delay:
            return 124
        time.sleep(delay)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return 124
        print(f"Go module bootstrap attempt {attempt}/3", flush=True)
        try:
            with subprocess.Popen(["go", "mod", "download"], stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True,
                                  start_new_session=True) as process:
                try:
                    output, _ = process.communicate(
                        timeout=max(0, min(ATTEMPT_SECONDS, deadline - time.monotonic())))
                except BaseException:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                    raise
        except subprocess.TimeoutExpired as error:
            output = error.output or ""
            output = output.decode(errors="replace") if isinstance(output, bytes) else output
            print(output, end="", flush=True)
            print("Go module bootstrap command deadline exceeded", file=sys.stderr)
            if (attempt < len(BACKOFF_SECONDS) and time.monotonic() < deadline
                    and not PERMANENT.search(output)):
                continue
            return 124
        except OSError as error:
            print(f"Go module bootstrap failed: {error}", file=sys.stderr)
            return 1
        print(output, end="", flush=True)
        if process.returncode == 0:
            return 0
        if process.returncode < 0 or attempt == 3 or not transient(output):
            return max(process.returncode, 1)
    return 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
