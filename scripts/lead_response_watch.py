#!/usr/bin/env python3

import argparse
import hashlib
import sys
import time
from pathlib import Path


AGENT_RESPONSE = {
    "A": Path("worktrees/agent-a/RESPONSE.yaml"),
    "B": Path("worktrees/agent-b/RESPONSE.yaml"),
    "C": Path("worktrees/agent-c/RESPONSE.yaml"),
    "D": Path("worktrees/agent-d/RESPONSE.yaml"),
}


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text()


def file_hash(path: Path) -> str:
    return hashlib.sha256(read_text(path).encode("utf-8")).hexdigest()


def parse_simple_yaml(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data: dict[str, str] = {}
    current_list_key = ""
    list_items: list[str] = []
    for raw_line in path.read_text().splitlines():
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


def snapshot(repo_root: Path) -> list[tuple[str, Path, dict[str, str]]]:
    out: list[tuple[str, Path, dict[str, str]]] = []
    for agent, rel_path in AGENT_RESPONSE.items():
        path = (repo_root / rel_path).resolve()
        out.append((agent, path, parse_simple_yaml(path)))
    return out


def snapshot_hash(repo_root: Path) -> str:
    digest = hashlib.sha256()
    for _, rel_path in AGENT_RESPONSE.items():
        digest.update(file_hash((repo_root / rel_path).resolve()).encode("utf-8"))
    return digest.hexdigest()


def print_snapshot(repo_root: Path) -> None:
    for agent, path, data in snapshot(repo_root):
        state = data.get("state", "missing")
        issue = data.get("issue", "none")
        pr = data.get("pr", "none")
        response_type = data.get("response_type", "none")
        response_status = data.get("response_status", "none")
        control_version = data.get("source_control_version", "none")
        summary = data.get("summary", "")
        print(
            f"{agent}: file={path} state={state} issue={issue} pr={pr} "
            f"response_type={response_type} response_status={response_status} "
            f"source_control_version={control_version}"
        )
        if summary:
            print(f"  summary={summary}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for RESPONSE.yaml changes across agent worktrees.")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--watch", action="store_true", help="Wait until any response file changes.")
    parser.add_argument("--interval", type=int, default=2, help="Polling interval in seconds for --watch.")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    if not args.watch:
        print_snapshot(repo_root)
        return 0

    baseline = snapshot_hash(repo_root)
    while True:
        current = snapshot_hash(repo_root)
        if current != baseline:
            print_snapshot(repo_root)
            return 0
        time.sleep(max(args.interval, 1))


if __name__ == "__main__":
    raise SystemExit(main())
