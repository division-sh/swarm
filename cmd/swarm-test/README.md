# Local Qualification

Run `go run ./cmd/swarm-test preflight` on the clean committed candidate before
pushing. It always executes the same reviewed cross-package source guards,
canonical structural inventory, formatting, complexity, OpenRPC check and vet.
It performs no runtime/service bootstrap. `test-results/preflight/` retains
source/build/selection identity, phase timings, failures and unstarted work.
The five-minute warm-vemew target is soft: an overrun requires reviewed
re-ranking, never shorter test deadlines or automatic selector changes.

Preflight replaces focused proof's shared guards. Declare `Local-Tier: focused`
and an informational `Preflight: pass (Ns)` receipt; run affected behavioral,
race and stateful proofs separately, not a duplicate core/lifecycle tier.
Do not repeat a shared guard for two labels on unchanged source/variant.
Preflight is not a hosted tier and grants no completed hosted qualification.

The finite membership lives in `internal/testplanning/preflight.go`, projected
through active native root/child ownership. Every eligible hosted escape needs
a reviewed candidate addition or an explicit exclusion; removing a nonfiring
candidate also needs review. The debt ratchet, tagged complete candidate-overlay,
blocked-SQL/runtime origin and native-unused proofs are explicitly outside this
initial selection; their existing affected/hosted owners remain required.
Known flaky whole roots cannot be made eligible by filtering their flaky leaf.
The reviewed ledger is `.github/flake-ledger.json`, not a dynamic skip list.

For the single complete-command measurement, use
`/usr/bin/time -p go run ./cmd/swarm-test preflight`: the outer time includes
launcher compilation, while the retained receipt separates discovery/setup and
each actual phase including build/TestMain and joined cleanup. Do not warm up
with a whole suite or call a sum of early-failing root durations command latency.

The reviewer chooses independent `CI-Tier` and `Local-Tier` PR-body lines. Use
`go run ./cmd/swarm-test --tier core`, `--tier lifecycle`, or `--full` for the
explicit local tier. Plain invocation is unqualified core developer feedback.
Under the user's focused-only local policy, a reviewer may instead declare
`Local-Tier: focused` for named manual focused/race/both-store receipts. This is
PR-body review metadata, not `--tier focused`, a duplicate local tier run, or a
claim that core/lifecycle/full completed. Hosted CI selection remains independent.
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
