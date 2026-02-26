#!/usr/bin/env python3
"""
Spec/runtime wiring gate wrapper.

Runs the canonical Go wiring verifier test and prints a concise report with:
- PASS/FAIL/WARN counts
- grouped FAIL reasons
- first N failing lines for quick triage
"""

from __future__ import annotations

import argparse
import collections
import os
import re
import subprocess
import sys
from typing import List


LINE_RE = re.compile(r"wiring_verification_test\.go:\d+:\s+(PASS|FAIL|WARN):\s+(.*)$")
SUMMARY_RE = re.compile(r"summary:\s+pass=(\d+)\s+fail=(\d+)\s+warn=(\d+)")


def classify(msg: str) -> str:
    if "has no explicit entry" in msg or "no EventSchemaRegistry entry" in msg:
        return "NO_SCHEMA"
    if "not allowed to emit" in msg:
        return "EMIT_NOT_ALLOWED"
    if "no producer" in msg or "nobody emits it" in msg:
        return "DEAD_SUB"
    if "prompt has no handling instructions" in msg:
        return "PROMPT_GAP"
    if "missing required fields" in msg or "lacks [" in msg:
        return "PAYLOAD_GAP"
    if "has no handleEvent case" in msg:
        return "INTERCEPTOR_GAP"
    if "chain broken" in msg:
        return "PATH_BROKEN"
    return "OTHER"


def run_verifier(verbose: bool) -> int:
    env = os.environ.copy()
    env["EMPIRE_WIRING_STRICT"] = "1"
    cmd = [
        "go",
        "test",
        "./internal/runtime",
        "-run",
        "TestSpecRuntimeWiringVerification",
        "-v",
        "-count=1",
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        check=False,
    )
    lines = proc.stdout.splitlines()
    passes: List[str] = []
    fails: List[str] = []
    warns: List[str] = []
    summary = None
    for line in lines:
        m = LINE_RE.search(line)
        if m:
            sev, msg = m.group(1), m.group(2)
            if sev == "PASS":
                passes.append(msg)
            elif sev == "FAIL":
                fails.append(msg)
            else:
                warns.append(msg)
            continue
        sm = SUMMARY_RE.search(line)
        if sm:
            summary = (int(sm.group(1)), int(sm.group(2)), int(sm.group(3)))

    if summary is None:
        summary = (len(passes), len(fails), len(warns))

    by_check = collections.Counter(classify(msg) for msg in fails)
    print(f"Event Wiring Verifier: PASS={summary[0]} FAIL={summary[1]} WARN={summary[2]}")
    if fails:
        print("\nFAIL groups:")
        for k, v in sorted(by_check.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"- {k}: {v}")
        print("\nTop FAIL items:")
        for msg in fails[:40]:
            print(f"- {msg}")
        if len(fails) > 40:
            print(f"- ... +{len(fails) - 40} more")
    if warns:
        print("\nWARN items:")
        for msg in warns[:20]:
            print(f"- {msg}")
        if len(warns) > 20:
            print(f"- ... +{len(warns) - 20} more")

    if verbose:
        print("\nRaw verifier output:")
        print(proc.stdout)

    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run wiring verification gate.")
    parser.add_argument("--verbose", action="store_true", help="Print raw go test output.")
    args = parser.parse_args()
    return run_verifier(args.verbose)


if __name__ == "__main__":
    sys.exit(main())

