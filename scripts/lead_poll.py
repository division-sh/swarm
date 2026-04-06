#!/usr/bin/env python3

import argparse
import hashlib
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AGENTS = ("A", "B", "C", "D")
WORKTREE_STATUS = {
    "A": Path("worktrees/agent-a/AGENT_STATUS.md"),
    "B": Path("worktrees/agent-b/AGENT_STATUS.md"),
    "C": Path("worktrees/agent-c/AGENT_STATUS.md"),
    "D": Path("worktrees/agent-d/AGENT_STATUS.md"),
}


@dataclass
class AgentView:
    agent: str
    assigned_issue: str
    open_pr: str
    pr_title: str
    pr_url: str
    review_status: str
    local_state: str
    local_control_target: str
    local_next_action: str


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


def latest_agent_review(comments: list[dict[str, Any]]) -> tuple[str, str]:
    for comment in reversed(comments):
        fields = parse_agent_action(comment.get("body", ""))
        if not fields:
            continue
        agent = fields.get("Agent", "").strip().upper()
        if agent in AGENTS:
            return agent, fields.get("Status", "").strip()
    return "", ""


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


def open_prs(cwd: Path) -> list[dict[str, Any]]:
    prs = run_json(
        [
            "gh",
            "pr",
            "list",
            "--state",
            "open",
            "--json",
            "number,title,url,headRefName,isDraft,statusCheckRollup",
        ],
        cwd,
    )
    out: list[dict[str, Any]] = []
    for pr in prs:
        details = run_json(
            [
                "gh",
                "pr",
                "view",
                str(pr["number"]),
                "--json",
                "number,title,url,headRefName,isDraft,statusCheckRollup,comments",
            ],
            cwd,
        )
        out.append(details)
    return out


def ci_state(pr: dict[str, Any]) -> str:
    rollup = pr.get("statusCheckRollup") or []
    states: list[str] = []
    for item in rollup:
        state = item.get("conclusion") or item.get("state") or ""
        if state:
            states.append(state)
    if not states:
        return "unknown"
    if any(state.upper() in {"FAILURE", "FAILED", "ERROR", "CANCELLED"} for state in states):
        return "failing"
    if any(state.upper() in {"PENDING", "IN_PROGRESS", "QUEUED", "EXPECTED"} for state in states):
        return "pending"
    return "green"


def build_snapshot(cwd: Path) -> tuple[list[AgentView], list[dict[str, str]], list[str]]:
    prs = open_prs(cwd)
    pr_by_agent: dict[str, dict[str, str]] = {}
    pr_by_number: dict[str, dict[str, str]] = {}
    unattributed_prs: list[dict[str, str]] = []
    for pr in prs:
        agent, review_status = latest_agent_review(pr.get("comments", []))
        record = {
            "number": f"#{pr['number']}",
            "title": pr["title"],
            "url": pr["url"],
            "review_status": review_status or "waiting-first-review",
            "ci": ci_state(pr),
            "head": pr.get("headRefName", ""),
        }
        pr_by_number[record["number"]] = record
        if agent:
            if agent in pr_by_agent:
                raise RuntimeError(f"multiple open PRs attributed to agent {agent}")
            pr_by_agent[agent] = record
        else:
            unattributed_prs.append(record)

    agent_views: list[AgentView] = []
    free_agents: list[str] = []
    for agent in AGENTS:
        issue = assigned_issue(cwd, agent)
        local = parse_status_file(cwd / WORKTREE_STATUS[agent])
        pr = pr_by_agent.get(agent)
        issue_ref = f"#{issue['number']}" if issue else "none"
        open_pr_ref = pr["number"] if pr else "none"
        view = AgentView(
            agent=agent,
            assigned_issue=issue_ref,
            open_pr=open_pr_ref,
            pr_title=pr["title"] if pr else "",
            pr_url=pr["url"] if pr else "",
            review_status=pr["review_status"] if pr else "",
            local_state=local.get("State", "unknown"),
            local_control_target=local.get("Control-Target", "unknown"),
            local_next_action=local.get("Next-Action", "unknown"),
        )
        agent_views.append(view)
        if issue is None and open_pr_ref == "none":
            free_agents.append(agent)

    return agent_views, unattributed_prs, free_agents


def snapshot_hash(agent_views: list[AgentView], unattributed_prs: list[dict[str, str]], free_agents: list[str]) -> str:
    payload = {
        "agents": [view.__dict__ for view in agent_views],
        "unattributed_prs": unattributed_prs,
        "free_agents": free_agents,
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def print_snapshot(agent_views: list[AgentView], unattributed_prs: list[dict[str, str]], free_agents: list[str]) -> None:
    print(f"Timestamp: {now_iso()}")
    print("Agents:")
    for view in agent_views:
        line = (
            f"- {view.agent}: issue={view.assigned_issue} pr={view.open_pr} "
            f"review={view.review_status or 'none'} local_state={view.local_state} "
            f"control={view.local_control_target}"
        )
        print(line)
        if view.pr_title:
            print(f"  pr_title={view.pr_title}")
        if view.local_next_action:
            print(f"  next_action={view.local_next_action}")
    print("Free-Agents:", ", ".join(free_agents) if free_agents else "none")
    if unattributed_prs:
        print("Unattributed-PRs:")
        for pr in unattributed_prs:
            print(
                f"- {pr['number']} ci={pr['ci']} review={pr['review_status']} head={pr['head']} title={pr['title']}"
            )
    else:
        print("Unattributed-PRs: none")


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll GitHub and local agent mirrors for lead-side control state.")
    parser.add_argument("--watch", action="store_true", help="Poll until the system snapshot changes.")
    parser.add_argument("--interval", type=int, default=300, help="Polling interval in seconds for --watch.")
    args = parser.parse_args()

    cwd = Path.cwd()

    try:
        agent_views, unattributed_prs, free_agents = build_snapshot(cwd)
        current_hash = snapshot_hash(agent_views, unattributed_prs, free_agents)
    except subprocess.CalledProcessError as exc:
        print(exc.stderr or exc.stdout or str(exc), file=sys.stderr)
        return exc.returncode or 1
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        return 1

    print_snapshot(agent_views, unattributed_prs, free_agents)

    if not args.watch:
        return 0

    while True:
        time.sleep(max(args.interval, 1))
        try:
            next_views, next_unattributed, next_free = build_snapshot(cwd)
            next_hash = snapshot_hash(next_views, next_unattributed, next_free)
        except subprocess.CalledProcessError as exc:
            print(exc.stderr or exc.stdout or str(exc), file=sys.stderr)
            return exc.returncode or 1
        except Exception as exc:  # noqa: BLE001
            print(str(exc), file=sys.stderr)
            return 1
        if next_hash != current_hash:
            print("---")
            print_snapshot(next_views, next_unattributed, next_free)
            return 10


if __name__ == "__main__":
    raise SystemExit(main())
