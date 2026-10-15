# Local Qualification

Run `go run ./cmd/swarm-test preflight` on the clean committed candidate before
pushing to a PR. First upload the signed candidate to a non-PR staging branch;
the command verifies that exact commit in the origin repository before any proof
work. Authenticate `gh` for github.com with repository/commit read access and
commit-status write access (private repositories require more than `repo:status`).
Unset `GH_REPO` and any conflicting `GH_HOST`; remote/environment changes refuse.
After a complete pass, push the SAME SHA to the PR. A rebase needs a fresh pass;
the command never pushes or transfers status to another SHA.

It always executes the same reviewed cross-package source guards,
canonical structural inventory, formatting, complexity, OpenRPC check and vet.
It runs serially without acquiring shared native capacity or provisioning a
test database/service. A private inherited lifetime descriptor joins launched
descendants, including work with redirected stdio, before a phase can complete.
Ordinary broad/native admission is unchanged. `test-results/preflight/` retains
source/build/selection identity, phase timings, failures and unstarted work.
The five-minute warm-vemew target is soft: an overrun requires reviewed
re-ranking, never shorter test deadlines or automatic selector changes.
An overrun alone does not make a complete passing command fail.

Execution order is formatting, OpenRPC, nine ranked existing package phases,
complexity, all remaining selected packages in canonical order, then full
`go vet ./...`. The ranked group is worklifetime, effects, deliverycontinuation,
manager, testutil, rewrite-stages-2566, backend/authoractivity, swarm-test-timing,
and testtiming. Rank changes only order, not membership: missing ranked packages
are never fabricated and future structural packages are never omitted. Cheap
census failures can surface before expensive checks, but complexity-only or
vet-only failures can arrive later. This is not an all-green speedup guarantee.
Every check must still pass for a complete receipt. Source/build fences run
before and after each phase, and joined cleanup precedes advancement.

Preflight replaces focused proof's shared guards. Declare `Local-Tier: focused`
and an informational `Preflight: pass (Ns)` receipt; run affected behavioral,
race and stateful proofs separately, not a duplicate core/lifecycle tier.
Do not repeat a shared guard for two labels on unchanged source/variant.
Preflight is not a hosted tier and grants no completed hosted qualification.
Its exact-head `preflight/local` status is required before ordinary PR CI starts.
Pending and terminal statuses share a unique attempt/receipt identity. CI accepts
only the latest context success matching the latest started attempt; late old
success cannot override a newer pending or failed attempt. Interrupted attempts
remain non-success. Publication/authentication errors fail the command separately
from proof results, with no automatic retry. Complete receipts and descendant
cleanup precede success. Token-holder honesty is required; this is an audit trail,
not cryptographic proof or protection against a manually forged status.

Admission uses read-only runner tools before checkout/Go. Admitted drafts retain
static/complexity feedback but no proof/tier qualification. Same-repository actual
`automation/*` PR heads, master push, schedule, dispatch (both keep-going modes)
and genuine merge-group events are exempt only from local-status admission;
native CI obligations remain. No merge-group trigger is enabled. Ordinary forks
need status on their own exact head repository/SHA; automation-named forks and
main push are not exempt.

The finite membership lives in `internal/testplanning/preflight.go`, projected
through active native root/child ownership. Every eligible hosted escape needs
a reviewed candidate addition or an explicit exclusion; removing a nonfiring
candidate also needs review. Any changed Go source, dependency/workspace input,
authority policy or path in the store trees requires the existing debt ratchet
and raw-authority fixture guard, including their native child obligations, once
in the existing store phase with its actual 20-minute command timeout. Both old
and new rename/copy/delete paths count. The receipt freezes origin/master, the
comparison and exact diff hash; ambient debt events/refresh/publication, missing
or ambiguous diff evidence and ref movement refuse. The unchanged native ratchet
owns comparison authority: its reported source must equal the frozen expectation.
Unrelated docs-only changes keep the existing finite membership. The tagged
complete candidate-overlay, blocked-SQL/runtime origin and native-unused proofs
remain outside this selection; their affected/hosted owners remain required.
Known flaky whole roots cannot be made eligible by filtering their flaky leaf.
The reviewed ledger is `.github/flake-ledger.json`, not a dynamic skip list.

For the single complete-command measurement, use
`/usr/bin/time -p go run ./cmd/swarm-test preflight`: the outer time includes
launcher compilation, while the retained receipt separates discovery/setup and
each actual phase including build/TestMain and joined cleanup. The receipt's
`end_to_end_seconds` covers compiled-command entry through joined execution,
including early repository/status admission. Receipt persistence and terminal
publication follow; terminal publication latency is printed separately;
retain the outer launcher-to-exit time separately, without subtracting compile
time. `queue_wait_seconds` is zero: preflight never enters the shared slot queue.
`execution_seconds` includes package build/TestMain and actual commands;
`cleanup_seconds` records descriptor and log cleanup. `setup_seconds` covers
discovery, source/variant/result validation and remaining command overhead.
The same execution/setup/cleanup costs and actual start/completion timestamps
are retained per phase. Do not warm up
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

Before slot admission, eligible commands warm Go's build cache with the same
arguments and `-exec=/bin/true`, through the existing joined native-process owner.
No TestMain, test body, service or slot runs in that phase. Successful warm-up
output is discarded; failure output goes to stderr as diagnostics, never JSON
proof credit. Signals, failed builds and unresolved descendants refuse admission.
The actual admitted command, runtime environment and result-cache semantics stay
unchanged. Preflight uses its direct commands and does not recursively warm itself.

Only a finite set of ordinary test/build flags is eligible. Unknown or ambiguous
flags, nonempty effective GOFLAGS (including `go env -w`), executor/tool overrides,
compile/output/profile/artifact/fuzz/work/dry-run forms, a missing fixed no-op
executor, and non-native or unsupported targets keep the original admitted path
without warming. Build environment and
toolchain are preserved; private DSN, lease and proof setup still follow admission.
Warm-up prints its environment/build/cleanup and total time separately. Linking
may repeat inside the slot: slot occupancy is not end-to-end latency, and neither
existing timing ceilings nor command receipts subtract this extra work. Compilation
outside admission can also add host CPU pressure; this is not a new compiler
concurrency limit or a guaranteed queue-latency improvement.

The 30-minute local goal is a measurement target, not a relaxed test deadline or
an assurance of success. Missing, failed, interrupted, and unstarted work earns no
full qualification. A killed supervisor does not release authority still retained
by a surviving descendant.
