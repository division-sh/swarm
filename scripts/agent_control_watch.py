#!/usr/bin/env python3

import argparse
import hashlib
import sys
import time
from pathlib import Path


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"control file not found: {path}")
    return path.read_text()


def file_hash(path: Path) -> str:
    return hashlib.sha256(read_text(path).encode("utf-8")).hexdigest()


def parse_simple_yaml(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    current_list_key = ""
    list_items: list[str] = []
    for raw_line in read_text(path).splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- "):
            if current_list_key:
                list_items.append(stripped[2:].strip())
            continue
        if list_items and current_list_key:
            data[current_list_key] = ", ".join(list_items)
            list_items = []
            current_list_key = ""
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value == "":
            current_list_key = key
            list_items = []
            data[key] = ""
            continue
        current_list_key = ""
        data[key] = value
    if list_items and current_list_key:
        data[current_list_key] = ", ".join(list_items)
    return data


def print_summary(path: Path) -> None:
    data = parse_simple_yaml(path)
    ordered = [
        "version",
        "agent",
        "state",
        "issue",
        "pr",
        "action_type",
        "action_status",
        "agent_mode",
        "next_step",
        "updated_at",
    ]
    print(f"Control-File: {path}")
    for key in ordered:
        if key in data:
            print(f"{key}: {data[key]}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for CONTROL.yaml changes and print the new control state.")
    parser.add_argument("--control-file", default="CONTROL.yaml", help="Path to CONTROL.yaml")
    parser.add_argument("--watch", action="store_true", help="Wait until the control file changes.")
    parser.add_argument("--interval", type=int, default=2, help="Polling interval in seconds for --watch.")
    args = parser.parse_args()

    path = Path(args.control_file).resolve()

    if not args.watch:
        try:
            print_summary(path)
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        return 0

    baseline = None
    try:
        baseline = file_hash(path)
    except FileNotFoundError:
        baseline = "__missing__"

    while True:
        try:
            current = file_hash(path)
        except FileNotFoundError:
            current = "__missing__"
        if current != baseline:
            print_summary(path)
            return 0
        time.sleep(max(args.interval, 1))


if __name__ == "__main__":
    raise SystemExit(main())
