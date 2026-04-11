#!/bin/bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
primary_worktree="$repo_root"
mirror_worktree="$repo_root/worktrees/origin-master-current"
lock_dir="${TMPDIR:-/tmp}/swarm-origin-master-sync.lock"

if ! mkdir "$lock_dir" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$lock_dir"' EXIT

git -C "$primary_worktree" fetch --quiet origin master

target_rev="$(git -C "$primary_worktree" rev-parse origin/master)"

if [ ! -d "$mirror_worktree/.git" ] && [ ! -f "$mirror_worktree/.git" ]; then
  mkdir -p "$(dirname "$mirror_worktree")"
  git -C "$primary_worktree" worktree add --detach "$mirror_worktree" "$target_rev" >/dev/null 2>&1
  exit 0
fi

current_rev="$(git -C "$mirror_worktree" rev-parse HEAD)"
if [ "$current_rev" = "$target_rev" ]; then
  exit 0
fi

if [ -n "$(git -C "$mirror_worktree" status --porcelain)" ]; then
  echo "origin-master-current is dirty; refusing to auto-update" >&2
  exit 0
fi

git -C "$mirror_worktree" checkout --detach --quiet "$target_rev"
