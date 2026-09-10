# PR #2441: Typed Admission Rebase Accounting

Agent-g, 2026-09-09. Rebased with merge preservation onto
`origin/master@b38b84045d827bc097d505b8b2c693719c7c6838` (PR #2403).
The pre-rebase history is retained at
`agent-g/2321-pre-rebase-review1-f7605eb56`. Existing merged reset/diagnostic
repairs remain integrated; no unrelated worktree was changed.

## Integration Boundary

Ordinary runtime logging now consumes upstream's typed payload admission owner
and requires matching admission evidence before persistence. The diagnostic
receipt validator retains its exact event identity, mode, payload and no-delivery
comparison, using the existing selected-store payload admission owner rather than
the deleted validator. This read-only comparison is not an ordinary-write bypass.
The admitted-event guard lists those two exact read-only callsites.

Diagnostic-only logger fixtures pass the new third constructor argument as nil;
they do not emit ordinary logs. The supported fork negative fixtures obtain real
typed payload admission from their pinned source before persisting the forged
diagnostic payloads. They still test rejection by fork authority, not rejection
for missing payload evidence. Their first post-rebase run failed fixture setup;
the corrected run executes all four scenarios on SQLite and PostgreSQL.

## Rebased Checks

| Check | Result |
| --- | --- |
| `go build ./...` | PASS |
| CLI selection/config/structural and runtime logger controls | PASS, CLI 3.150s/runtime 1.551s |
| Actual API specification package | PASS, 2.451s |
| Persistence authority registry | PASS, 2.149s |
| Diagnostic identity/mode, atomicity, selected activation and event-boundary guard | PASS, 7.972s |
| Ordinary log admission and diagnostic payload-failure atomicity | PASS, 3.526s |
| `TestLifecycleDiagnosticForkActivationBothStores` | PASS, 8.193s |
| OpenRPC check | PASS, 70 methods/236 schemas/67 errors/30 mutators/5 subscriptions |
| `go run ./cmd/swarm-test -- -run '^$' ./...` | PASS, all test packages compile; NOT whole-suite execution |
| `git diff --check` | PASS |

Local receipts and SHA256:

- `/tmp/agent-g-pr2441-rebased-fork.log`: `5cb885b8509cf0a1b7ea35fca7b0f7113a800c72fad3faaa3357297437198137`
- `/tmp/agent-g-pr2441-rebased-compile.log`: `d93919cfb247d15e671f12d047ce57c1438021df65f52ebd69f37abb303f586e`
- `/tmp/agent-g-pr2441-rebased-diagnostics.log`: `ad0f951a7a4ceb56a366780bc32c4a87dd138487f4b0cf940bd4bc23d1bdd5c3`
- `/tmp/agent-g-pr2441-rebased-log-admission.log`: `0d867affefbd609dafccfd20f9e76c87b1b5a5bec87fe8556a6438c5ee14d428`
- `/tmp/agent-g-pr2441-rebased-apispec.log`: `9cd5116cc034bdceac27df9277b1421c8ae80885c7d429acfb864c30b82780cc`

## Qualification Remains Blocked

The full-profile attempt started at old head edb169908 finished exit 1. Its
coordinator retirement failure is LSF-030, with the separate 3/3 deterministic
probe and pending absorb/split request in `issue-2321-pr2441-review1-escalation.md`.
Full log SHA256: `d2e1b6f20a96262fb4db237c609bf5f2c0233001ccc72885b97351a76ac443e9`.
That attempt was not clean final-head qualification: audit and pack-guard edits
were made while it ran. No historical passing suite, compile-only check, focused
pass or subsequent CI result is substituted for final integrated qualification.

No production coordinator fix, new live-provider call or replay was performed.
The three proof audits remain separate and explicitly not review-ready. The
existing residual waiver for LSF-028/029 does not waive LSF-030. Await the lead's
bounded routing decision before repairing that additional runtime class.
