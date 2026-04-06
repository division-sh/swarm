#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class Action:
    source: str
    target_number: int
    target_title: str
    target_url: str
    comment_id: str
    fields: dict[str, str]


def run_json(cmd: list[str], cwd: Path) -> Any:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return json.loads(proc.stdout)


def run_text(cmd: list[str], cwd: Path) -> str:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc.stdout.strip()


def git_branch(cwd: Path) -> str:
    return run_text(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_status_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        out[key.strip()] = value.strip()
    return out


def write_status_file(path: Path, fields: dict[str, str]) -> None:
    ordered = [
        "Agent",
        "State",
        "Issue",
        "PR",
        "Branch",
        "Control-Target",
        "Last-Seen-Comment-ID",
        "Last-Updated",
        "Next-Action",
    ]
    lines: list[str] = []
    for key in ordered:
        if key in fields:
            lines.append(f"{key}: {fields[key]}")
    for key in sorted(fields.keys()):
        if key not in ordered:
            lines.append(f"{key}: {fields[key]}")
    path.write_text("\n".join(lines) + "\n")


def parse_agent_action(body: str) -> dict[str, str] | None:
    lines = body.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip() == "[agent-action]")
    except StopIteration:
        return None
    fields: dict[str, str] = {}
    for line in lines[start + 1 :]:
        stripped = line.strip()
        if not stripped or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key:
            fields[key] = value
    return fields or None


def latest_matching_action(
    comments: list[dict[str, Any]], agent: str, expected_type: str | None = None
) -> Action | None:
    for comment in reversed(comments):
        fields = parse_agent_action(comment.get("body", ""))
        if not fields:
            continue
        if fields.get("Agent", "").strip() != agent:
            continue
        if expected_type and fields.get("Type", "").strip() != expected_type:
            continue
        return Action(
            source="",
            target_number=0,
            target_title="",
            target_url=comment.get("url", ""),
            comment_id=str(comment.get("id", "")).strip(),
            fields=fields,
        )
    return None


def find_open_pr_for_branch(cwd: Path, branch: str) -> dict[str, Any] | None:
    if branch in {"HEAD", "master"}:
        return None
    prs = run_json(
        [
            "gh",
            "pr",
            "list",
            "--state",
            "open",
            "--head",
            branch,
            "--json",
            "number,title,url",
        ],
        cwd,
    )
    if not prs:
        return None
    if len(prs) > 1:
        raise RuntimeError(f"multiple open PRs found for branch {branch}")
    return prs[0]


def pr_action(cwd: Path, pr_number: int, agent: str) -> Action | None:
    data = run_json(
        ["gh", "pr", "view", str(pr_number), "--json", "number,title,url,comments"],
        cwd,
    )
    action = latest_matching_action(data.get("comments", []), agent)
    if action is None:
        return None
    action.source = "pr"
    action.target_number = int(data["number"])
    action.target_title = data["title"]
    action.target_url = data["url"]
    return action


def assigned_issue(cwd: Path, agent: str) -> dict[str, Any] | None:
    issues = run_json(
        [
            "gh",
            "issue",
            "list",
            "--state",
            "open",
            "--label",
            f"agent:{agent}",
            "--json",
            "number,title,url",
        ],
        cwd,
    )
    if not issues:
        return None
    if len(issues) > 1:
        raise RuntimeError(f"multiple open issues assigned to agent {agent}")
    return issues[0]


def issue_action(cwd: Path, issue_number: int, agent: str) -> Action | None:
    data = run_json(
        ["gh", "issue", "view", str(issue_number), "--json", "number,title,url,comments"],
        cwd,
    )
    action = latest_matching_action(data.get("comments", []), agent, expected_type="next-task")
    if action is None:
        return None
    action.source = "issue"
    action.target_number = int(data["number"])
    action.target_title = data["title"]
    action.target_url = data["url"]
    return action


def control_state_changed(current: dict[str, str], fields: dict[str, str]) -> bool:
    watched = (
        "State",
        "Issue",
        "PR",
        "Branch",
        "Control-Target",
        "Next-Action",
    )
    return any(current.get(key, "").strip() != fields.get(key, "").strip() for key in watched)


def derive_status(
    agent: str, cwd: Path, status_path: Path
) -> tuple[dict[str, str], Action | None, bool]:
    current = parse_status_file(status_path)
    last_seen = current.get("Last-Seen-Comment-ID", "").strip()
    branch = git_branch(cwd)

    pr = find_open_pr_for_branch(cwd, branch)
    if pr is not None:
        action = pr_action(cwd, int(pr["number"]), agent)
        comment_id = action.comment_id if action else last_seen
        review_status = action.fields.get("Status", "").strip() if action else ""
        state = "in_pr"
        if review_status == "changes-needed":
            state = "changes_requested"
        elif review_status == "blocked":
            state = "blocked"
        next_action = (
            action.fields.get("Next-Step", "").strip()
            if action
            else "waiting for first structured PR review comment"
        )
        fields = {
            "Agent": agent,
            "State": state,
            "Issue": current.get("Issue", "unknown"),
            "PR": f"#{pr['number']}",
            "Branch": branch,
            "Control-Target": f"PR #{pr['number']}",
            "Last-Seen-Comment-ID": comment_id,
            "Last-Updated": now_iso(),
            "Next-Action": next_action,
        }
        actionable = (action is not None and action.comment_id != last_seen) or control_state_changed(current, fields)
        return fields, action, actionable

    issue = assigned_issue(cwd, agent)
    if issue is not None:
        action = issue_action(cwd, int(issue["number"]), agent)
        comment_id = action.comment_id if action else last_seen
        next_action = (
            action.fields.get("Next-Step", "").strip()
            if action
            else "waiting for structured assignment comment"
        )
        fields = {
            "Agent": agent,
            "State": "assigned",
            "Issue": f"#{issue['number']}",
            "PR": "none",
            "Branch": branch,
            "Control-Target": f"Issue #{issue['number']}",
            "Last-Seen-Comment-ID": comment_id,
            "Last-Updated": now_iso(),
            "Next-Action": next_action,
        }
        actionable = (action is not None and action.comment_id != last_seen) or control_state_changed(current, fields)
        return fields, action, actionable

    fields = {
        "Agent": agent,
        "State": "idle",
        "Issue": "none",
        "PR": "none",
        "Branch": branch,
        "Control-Target": "idle",
        "Last-Seen-Comment-ID": last_seen,
        "Last-Updated": now_iso(),
        "Next-Action": "polling for assigned issue",
    }
    return fields, None, control_state_changed(current, fields)


def print_summary(fields: dict[str, str], action: Action | None, actionable: bool) -> None:
    print(f"Agent: {fields.get('Agent', '')}")
    print(f"State: {fields.get('State', '')}")
    print(f"Issue: {fields.get('Issue', '')}")
    print(f"PR: {fields.get('PR', '')}")
    print(f"Branch: {fields.get('Branch', '')}")
    print(f"Control-Target: {fields.get('Control-Target', '')}")
    print(f"Actionable: {'yes' if actionable else 'no'}")
    print(f"Next-Action: {fields.get('Next-Action', '')}")
    if action is not None:
        print(f"Comment-ID: {action.comment_id}")
        print(f"Action-Type: {action.fields.get('Type', '')}")
        print(f"Action-Status: {action.fields.get('Status', '')}")
        prefix = "PR" if action.source == "pr" else "Issue"
        print(f"Source: {prefix} #{action.target_number}")
        print(f"Source-Title: {action.target_title}")
        print(f"Source-URL: {action.target_url}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll GitHub for agent assignments and PR actions.")
    parser.add_argument("--agent", required=True, help="Agent letter, for example A")
    parser.add_argument("--watch", action="store_true", help="Poll until a new actionable event appears.")
    parser.add_argument("--interval", type=int, default=600, help="Polling interval in seconds.")
    parser.add_argument(
        "--status-file",
        default="AGENT_STATUS.md",
        help="Path to the local status file. Default: AGENT_STATUS.md in the current worktree.",
    )
    parser.add_argument(
        "--verbose-wait",
        action="store_true",
        help="Print state on each wait iteration while using --watch.",
    )
    args = parser.parse_args()

    cwd = Path.cwd()
    status_path = (cwd / args.status_file).resolve()
    agent = args.agent.strip().upper()
    if not agent:
        print("agent is required", file=sys.stderr)
        return 2

    while True:
        try:
            fields, action, actionable = derive_status(agent, cwd, status_path)
            write_status_file(status_path, fields)
        except subprocess.CalledProcessError as exc:
            print(exc.stderr or exc.stdout or str(exc), file=sys.stderr)
            return exc.returncode or 1
        except Exception as exc:  # noqa: BLE001
            print(str(exc), file=sys.stderr)
            return 1

        if not args.watch:
            print_summary(fields, action, actionable)
            return 10 if actionable else 0

        if actionable:
            print_summary(fields, action, actionable)
            return 10

        if args.verbose_wait:
            print_summary(fields, action, actionable)
            print("---")

        time.sleep(max(args.interval, 1))


if __name__ == "__main__":
    raise SystemExit(main())
