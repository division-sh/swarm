# PR #2441 Review Pass 1: Qualification Failure And Scope Escalation

Agent-g, 2026-09-09. Repair/code-test head
`edb1699080a69630ebd0c2cd25275e98dd817975`; merged base
`47c0e70d8ef89a2c99f0be4af632f24692121b83`.

## Approved Corrections

The three review findings are repaired without runtime changes: obsolete CI config
and argument deleted, actual public live SQLite command retained and guarded,
diagnostic encoding/table summaries reconciled with the canonical derived event ID
and first-logger/receipt mode. Focused config/structural/spec and both-store
diagnostic controls pass. Actual compiled SQLite smoke exits 0 with three events
and one entity; the required CI SQLite job also passes at
https://github.com/division-sh/swarm/actions/runs/34410900554/job/102664809947.

CI then exposed a deletion consequence in the existing pack-config guard:
`TestPlatformPackBodiesHaveOneEmbedOwnerAndNoRetiredTeachingConfig` requires the
now-empty `.github/fixtures` directory, which Git does not retain. Failed job:
https://github.com/division-sh/swarm/actions/runs/34410900554/job/102665580583.
The bounded correction scans from `.github`, selecting fixture and workflow
consumers but not historical audit prose. It does not restore the retired fixture
or skip corpus checks. A local empty directory initially masked this CI condition;
the follow-up control removes that directory and executes the guard again.

## New Failure, Not Waived By The Existing Residual Ruling

Full-profile command:
`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -count=1 -timeout=30m ./...`
at edb169908 failed the SQLite cell of
`internal/apiv1::TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores`:

```text
--- FAIL: TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores (1.59s)
    --- FAIL: TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores/sqlite (0.58s)
        author_activity_test_context_test.go:255: API test delivery continuation failed: delivery continuation coordinator is retired
FAIL github.com/division-sh/swarm/internal/apiv1 236.984s
```

The attempt completed exit 1. Log SHA256:
`d2e1b6f20a96262fb4db237c609bf5f2c0233001ccc72885b97351a76ac443e9`.
Audit and pack-guard edits occurred during execution, so this failed attempt is
not clean final-head qualification. Subsequent rebase receipts are recorded in
`issue-2321-pr2441-rebase-accounting.md`; no coordinator fix was made.

Original log: `/tmp/agent-g-pr2441-review1-full.log`. A bounded isolated both-store
count=10 run passes (8.010s), not a fix: `/tmp/agent-g-pr2441-pin-cleanup-probe.log`,
SHA256 `1fb40c9260d7e3fa098ad1240a99cbcd93aea5aea31c1a0963e1a416a463807e`.
No CPU/load explanation or relation to LSF-028/029 is claimed. Other agents may be
active on this shared host; their activity was not controlled or attributed.

The coordinator's `Retire` publishes `retired=true` under its mutex, unlocks, then
cancels its worker context. An in-flight scan can see retirement in `observe`,
return the retired error, and reach `run`'s failure reporter before ctx.Err changes.
The API fixture reports that callback as a test failure. This is a runtime
retirement-versus-failure classification race, not grounds to ignore all fixture
errors or wait away the invalid state.

An investigation-only barrier in `/tmp/agent-g-pr2441-retire-probe` holds exactly
the interval before cancellation and releases a previously entered scan. The real
coordinator reports the same retirement error 3/3. Retained source:
`issue-2321-retirement-cancel-gap-probe.go.txt` beside this artifact. Command:
`go test ./internal/runtime/deliverycontinuation -run
'^TestRetirementBeforeContextCancellationDoesNotReportFailure$' -count=3`.
Log `/tmp/agent-g-pr2441-retire-gap.log`, SHA256
`87e4873f8dbfdcf4a9273027f111b830364ede0632327fce44de14716431c430`.
The original full run did not capture scheduler interleaving; exact attribution
of that one occurrence to the demonstrated window remains an inference, not a
trace claim. The deterministic runtime defect itself is reproduced.

Coordinator production source is byte-identical at the merged base and repaired
head (SHA256 `273a21f8ce3b56b67c2aefbb9cd0b1214004a8ced1a25d7377ab5dffe3f8641c`).
Neither the coordinator nor the API fixture/data test changed in this combined PR.
This does not prove the entire earlier full suite failed on master.

## Requested Disposition

Record this separately in the existing #2353 evidence ledger and request the lead's
bounded absorb/split ruling before changing the coordinator. #2436/B already owns
the flake-repair stream; do not silently take or widen that work. Existing #2250
is the architecture watchpoint, not authorization for a new lifecycle framework.
No runtime patch, error suppression, fixture retry, timeout increase, or new live
provider/Telegram message was made. The deterministic probe stays audit-only.

PR #2441 remains **not review-ready**: the new required full-profile failure is not
covered by the accepted missing-history residuals. Do not request merge approval
or call this head failure-class eliminated merely because isolated tests or CI
later pass. The required CI consumer repair can finish independently; the runtime
classification repair requires its own recorded disposition.
