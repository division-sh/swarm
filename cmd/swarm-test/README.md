# Local Qualification

The reviewer chooses independent `CI-Tier` and `Local-Tier` PR-body lines. Use
`go run ./cmd/swarm-test --tier core`, `--tier lifecycle`, or `--full` for the
explicit local tier. Plain invocation is unqualified core developer feedback.
Explicit qualification requires clean tracked and non-ignored untracked source
before planning, before/after each unit, and after the joined aggregate.

Independent units execute as subprocesses. Capacity is conservative: at most four,
one per four effective CPUs and eight GiB RAM, including observable container
limits. Unknown capacity becomes one. `SWARM_TEST_RUN_SLOTS` may reduce that ceiling
but cannot exceed it or resize an occupied host registry. An explicit shared
PostgreSQL DSN stays at one unit; the existing 300-connection admission floor is
not evidence of capacity for several concurrent heavy units. Owned services stay
independent per worker. The parent does not hold an admission slot.

Cheap source/tool/host/scratch checks precede planning. Selected native PostgreSQL,
service capacity, and Unix socket checks run after binding and before workers.
Required native roots cannot silently skip. For server2, keep the worktree and a
short `TMPDIR` on disk, not its RAM-backed `/tmp`. To measure private-service
parallelism, deliberately omit the shared `SWARM_TEST_POSTGRES_DSN`; no command
silently replaces an explicit shared connection or changes its server settings.

The fixed plan is retained at `test-results/local/<tier>-<time>/proof-plan.json`.
`workers.json` records source, digest, admitted capacity, aggregate/unit intervals,
exit codes, and not-started units. Unit runner logs and canonical command receipts
remain alongside it. The first failure or signal stops launching work, interrupts
started workers, and waits for their existing descendant/service settlement.
Compilation bytes may be shared; state and passing test credit may not.

The 30-minute local goal is a measurement target, not a relaxed test deadline or
an assurance of success. Missing, failed, interrupted, and unstarted work earns no
full qualification. A killed supervisor does not release authority still retained
by a surviving descendant.
