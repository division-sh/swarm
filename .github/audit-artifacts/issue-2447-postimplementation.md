# Post-Implementation Proof Audit: #2447 R1.4

Agent-g. **LSF-044 test repair implemented; integrated qualification pending.**
Executable checker/test Go head: `3ba8c5678`; inventory-metadata heads: `ac3d00dde`
and `6019a729f` (plus this artifact's exact classification entry);
base: `27a9f1b8c` (current master integration,
including the separately merged scatter-gather safety tests). Subsequent changes
to this artifact are documentation-only; the baseline carries no circular SHA.

## Governing Contract and Closure Boundary

The [pre-audit](https://github.com/division-sh/swarm/issues/2447#issuecomment-5647890399)
and [approved first-slice gate](https://github.com/division-sh/swarm/issues/2447#issuecomment-5648061668)
govern, together with #2407 R1.4. The checked-in approved-gate addendum supersedes
the historical pending wording in the original pre-audit. There is no exact
platform-spec complexity contract: this is development/CI maintenance with no
runtime semantic change, so authoritative `platform-spec.yaml` is unchanged.

Exact concepts: admitted source inventory, pinned analyzer result populations,
stable callable occurrence identity, strict recorded measurement, independent
revision comparison and non-skippable CI acceptance. Chosen working class:
`source_complexity_growth_has_no_reproducible_ci_ratchet`. Parent: recovery-program
complexity control (#2447 / R1.4); broader parent: durable Recovery acceptance
boundaries (#2407). This was an absent acceptance invariant, not a local runtime
symptom. The approved slice closes only the missing ratchet, not hotspot reduction.

Intended closure level: **failure class eliminated**, not yet claimed because full
qualification and independent review remain outstanding. Current achieved level:
**touched seam canonicalized**. The whole known local/CI consumer family is migrated;
there is no second score implementation or local baseline ceiling that can approve
growth. Both counts >=30 must independently not increase. This is NOT a promise
that total complexity, every function, or all executable bodies can only decrease.

## Owners and Systematic Consumption

One development command, `cmd/swarm-complexity`, owns inventory, output admission,
normalization, baseline rendering/checking, comparison and event selection.
Unmodified gocyclo v0.6.0 and gocognit v1.2.1 remain the only score owners, provisioned
with version-qualified `go run` outside the product module. No vendoring, fork,
new exported library, production dependency or custom complexity algorithm.

| Consumer / gate, in execution order | Disposition and exact path |
| --- | --- |
| Local update/check | Moved to canonical owner: `main -> run`; update writes evidence, while a supplied base still independently rejects growth. |
| PR head/base selection | Moved to canonical owner: `applyEvent -> eventRevisions -> revision`; actual PR head, not test planner's synthetic merge. |
| Push before/after | Moved to same owner: exact nonzero SHAs required; missing/unavailable history fails. |
| Manual/nightly | Moved to same owner for current-baseline validation; no fabricated historical comparison. |
| Git inventory, build variants, tools/support | Moved to `inventory -> readBlobs -> stageSource`; all tracked regular authored non-test Go, flat staging avoids upstream directory skipping. |
| Generated/test exclusion | Same inventory owner; recorded per file. `ast.IsGenerated`, not filename guesses. Test content is not parsed. |
| Analyzer score production | Already consumes upstream owners via `upstream`; exact pinned invocation, diagnostic/exit failure propagation. |
| Analyzer callable population / provenance | Moved to `stageSource -> normalize`: syntax maps expected rows, never scores; suppressions and line directives refused. |
| Zero scores and duplicate occurrences | Moved to same owner: cognit `-over=-1`; metric-specific populations; lexical occurrence disambiguation without line identity. |
| Committed head and existing base artifacts | Moved to `checkBaseline` / `checkBasePolicy`; exact bytes and policy identity must match fresh independent measurement. Base artifact absence is allowed only for bootstrap. |
| Independent count ratchet / disclosure | Moved to `compare -> summaries/changes`; >=30 counts gate independently, >=50/maxima/per-function/file-set changes report. |
| Every CI profile and changed-path posture | Moved to unconditional `complexity` job, with no dependency on `ci-plan` or its profile selection. |
| Required aggregate | Moved to strict complexity success check; skipped/cancelled/missing/failure/unknown all fail. Existing other-job rules unchanged. |
| CI/local artifact readers | Consume `head.json` / `delta.json` through `emitEvidence`; exact revision evidence only in delta, not circular committed baseline. |
| Existing CI planner/timing tooling | Different concept, proven by unchanged execution-SHA planning and separate job; no parallel complexity interpretation there. |
| Historical score/keep-map readers | Historical only, not current approval authority; missing eligibility evidence continues to block future factoring. |
| Repository-wide route-authority drift census | Different semantic concept: reads callable names as searchable text, not scores or executable route authority. Exact baseline path classified in its existing inventory; no search exclusion or test-assertion change. |
| Retired-transport reference census | Different semantic concept: its existing exact-count table classifies the JSON's 75 metadata references and this audit's single test-name reference. No path-wide exclusion or assertion removed. |
| Monthly delta publication / protection administration | Explicitly split / tracked separately in #2407 R1.5 / R1.3. No second checker or invented publication framework. |
| Runtime/golden consumers | Different semantic concept; production and golden fixture code stays unchanged. The separately approved disposal-observation test repair is tabulated below. |

Every earlier gate must succeed before comparison is meaningful. Unsupported
sources, missing records and invalid facts fail before acceptance; downstream
baseline editing cannot compensate. No known same-class consumer still bypasses
the owner. Historical ad-hoc measurements survive only as non-authoritative history.

## Manifestation Proof Table

The named tests below replace the pre-audit's provisional test names. All use the
same command owners. Real upstream tools and disposable Git repositories supplement
unit controls; workflow assertions are not the only proof.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Host-selected / hidden / testdata / platform source omission | reproduced and fixed | `TestUpstreamPopulationAndSnapshot`: real pinned tools, tracked hidden testdata and Windows-only files, authored/generated/test classifications. |
| Dirty or untracked source changes measured evidence | reproduced and fixed | Same test overwrites tracked source and adds invalid untracked Go, then proves byte-equivalent measurement of unchanged HEAD. |
| Empty inventory, parse errors, unsupported files, absent revisions | reproduced and fixed | `TestSourceAdmission`, `TestMalformedSnapshotAndArtifactFacts`: empty/all-generated, malformed source, symlink, absent/empty revision and corrupt/truncated blob controls. |
| Canonical generated marker versus lookalike | reproduced and fixed | `TestUpstreamPopulationAndSnapshot`: pre-package canonical marker excluded; post-package lookalike remains authored. |
| Legitimate no-function source mistaken for dropped output | reproduced and fixed | `TestSourceAdmission/no-functions`: both real analyzers return empty populations legitimately. |
| Suppression hides authored complexity | reproduced and fixed | `TestSourceAdmission/cyclo`, `cyclo-space`, `cognit`: refused before analyzer execution. |
| Fictitious adjusted line provenance | reproduced and fixed | `TestSourceAdmission/line` and `block-line`; `TestNormalizationRefusesMissingDuplicateAndForeignRows`. |
| Repeated init / repeated multi-variable literal names collide | reproduced and fixed | `TestUpstreamPopulationAndSnapshot`: two init and two A-labelled literal rows retained with distinct occurrences. |
| Missing cognitive rows invented as zero | reproduced and fixed | Same real-tool test: only declarations in cognit, direct variable literals in cyclo; real cognit zeros retained; parenthesized literal remains explicitly unreported. |
| Output order, physical lines, file or receiver collision | reproduced and fixed | Line-only real-Git shift in `TestUpstreamPopulationAndSnapshot`; reversed output and distinct file/receiver controls in `TestCollectorFailuresAndCanonicalOrder/order-path-and-receiver`. |
| Missing/malformed/duplicate/foreign analyzer output | reproduced and fixed | `TestNormalizationRefusesMissingDuplicateAndForeignRows`; precise expected population, score and provenance rejection. |
| Tool missing/failing/skipping or invocation drifting | reproduced and fixed | `TestCollectorFailuresAndCanonicalOrder`: injected failure, absent Go executable, executable invocation/diagnostic control verifies exact pinned modules and cognit -over=-1. Real score controls remain upstream, not mocks. |
| Incorrect threshold / one metric offsets another | reproduced and fixed | `TestIndependentThresholdsAndReporting`: 29/30/31 each metric, independent grow/decrease, >=50/max worsening disclosure. |
| Missing/stale/inflated/partial/duplicate/unknown artifact | reproduced and fixed | `TestGitBaselineAndPolicyAdmission`, `TestMalformedSnapshotAndArtifactFacts`: real Git artifacts with inflation, missing/duplicate rows, unknown policy, trailing data. |
| Head self-bump or initial bootstrap bypass | reproduced and fixed | `TestRealGitIndependentGrowthAndPRHeadNotMerge`: measured updated head still refuses each metric's independent growth; `TestGitBaselineAndPolicyAdmission` proves valid first-baseline introduction. |
| Missing base / changed policy silently resets gate | reproduced and fixed | `TestGitBaselineAndPolicyAdmission`: nonexistent base and threshold drift refuse; canonical existing base must match independent measurement. |
| Synthetic merge measured instead of actual PR head | reproduced and fixed | `TestRealGitIndependentGrowthAndPRHeadNotMerge`: divergent real merge contains merge-only function, but event-driven run measures exact feature SHA and refuses its regression. |
| Same-count worsening hidden | reproduced and fixed | `TestIndependentThresholdsAndReporting`: passes prescribed count rule, emits exact before/after instead of claiming stronger policy. |
| Admitted/excluded file-set changes hidden | reproduced and fixed | `TestIndependentThresholdsAndReporting`: authored -> generated yields explicit removed/added facts; full repository delta names every new tool/test file. |
| Event facts absent/zero/unsupported or manual history invented | reproduced and fixed | `TestEventAdmission`; real event-driven run in Git integration proof; local/current exact-head command qualification. |
| Profile skip / aggregate treats skipped as success | reproduced and fixed | `TestWorkflowUnconditionalGateAndExecutableAggregate`: independent unconditional job for all event/profile postures; executes the real aggregate shell for success/skipped/cancelled/failure/empty/unknown. |
| Artifact output / local-CI owner divergence | execution-proven through the same corrected path | Exact command `-head HEAD -base origin/master -evidence ...` and real event-driven `run`, strict baseline check; workflow invokes that same executable. Actual hosted CI remains a separate merge check. |
| New artifact references unclassified by route-authority text census | reproduced and fixed | `TestFinalFlowInstanceAuthoringFixture_RouteAuthorityBypassInventoryStaysClassified`, `TestRouteAuthorityDriftInventoryCoversRepoWideSearchDimensions`, and `TestRouteAuthorityDriftInventoryRejectsNarrowOrStaleAudit` pass unchanged after exact metadata-path classification. No runtime routing or search bypass added. |
| New artifact references unclassified by retirement census | reproduced and fixed | `TestRetiredBuilderSemanticReferencesStayExplicit`: exact-count classification additions only; all existing search and refusal assertions retained. |
| Unchanged supported runtime workload | execution-proven through the same corrected path | Default full-suite releasee2e package PASS (636.150s, includes smoke); explicit full-profile dual-store restart/SIGKILL and burst iterations 1/2 PASS (330.937s). The whole suite did NOT pass; see the separate fork finding. |
| PostgreSQL failed-commit immediate competing-lock assertion | reproduced and fixed | #2353 LSF-044 bounded test-only approval; commit `054b888e5` replaces the false post-disposal timing requirement with actual server acquisition. Detailed sibling table and pending integrated qualification below. |
| Later factoring, runtime ownership and monthly publication | split / escalated as separate class | #2447 later family gate; #2443/#2250 and R4-R7; #2407 R1.3/R1.5. No closure credit assigned. |

## Qualification and Measurements

- `go test ./cmd/swarm-complexity -race -count=3 -timeout=5m`: PASS (70.062s), including final refusal controls.
- `go vet ./cmd/swarm-complexity`: PASS.
- `git diff --check origin/master...HEAD`: PASS.
- `go run ./cmd/swarm-complexity -head HEAD -base origin/master -evidence ...`: PASS; initial artifact is exact and counts do not grow.
- `TEST_POSTGRES_BIN=/usr/lib/postgresql/16/bin go run ./cmd/swarm-test -- ./... -count=1 -timeout=30m`: completed with failures. Three deterministic artifact-classification guard failures were repaired as above. The remaining PostgreSQL fork finding is #2353 LSF-044, reproduced on unchanged base. No whole-suite green claim.
- Explicit `SWARM_TEST_PROOF_PROFILE=full` managed releasee2e restart/SIGKILL and both burst iterations: PASS (330.937s), with SQLite and PostgreSQL subtests in all three tests. These are proof surface H (compiled mock lifecycle), not public live-provider proof. The default profile skips bursts; it is not counted as burst proof.
- Additional focused coverage run: PASS, 87.5% statement coverage; upstream invocation, normalization, suppression/provenance checks and count comparison are fully covered. Coverage is supporting evidence, not a substitute for the manifestation matrix.
- Existing route-inventory guards and negative controls: PASS (12.895s). Watchlist YAML validated through the existing Go YAML dependency; the host's Python environment did not have PyYAML, so that unavailable parser is not claimed as a pass.
- Final metadata guard recheck with this audit tracked: PASS (retirement4.647s, route inventory plus negative controls19.713s).
- An earlier full run was intentionally interrupted after additional checker refusal tests were added. It is not counted as qualification.
- The completed-source full qualification exposed two route-inventory failures and one retirement-census failure: generated JSON contains symbol names needing exact metadata classification. All existing search and refusal assertions are unchanged; the existing embedded exact-count metadata table is updated, not bypassed.
- The queued repaired-head full rerun was cancelled before admission once the separate PostgreSQL failure reproduced on unchanged master. This avoids a retry-to-green or an expensive run before lead disposition. No other agent's runner was interrupted.
- LSF-044 unchanged-base command: `go test ./internal/store/internal/backend/runforkpersistence -run '^TestConversationForkGracefulMutation$/^keyed$/^commit_failure$' -count=20 -timeout=5m`; one failure, 8.165s. No source modification in `/tmp/agent-g-2447-base-proof`.
- Full log SHA256 `92d029af28c17abf3b9ac86d371c8535bca26f275ed193533cc53f53f3e6887d`; unchanged-base fork probe SHA256 `59583d7931adb2289fe16111f95ce5b406c4d82f6155977198e5436b0376360b`; golden proof SHA256 `5c7817ab46f472f417651d95b3efa1b6b95957ca21077e12b7b80c24cb7d174a`. Logs are retained under `/tmp/agent-g-2447-{full-final,base-fork-proof,golden-full}.log`.

Measured base -> head: cyclo >=30 **236 -> 236**, >=50 **43 -> 43**, maximum
**184 -> 184**; cognit >=30 **518 -> 518**, >=50 **158 -> 158**, maximum
**294 -> 294**. Each metric's callable population is **18143 -> 18172** in this
repository (equality here does not imply equal upstream populations generally).
No runtime complexity reduction is claimed. Added authored files are exclusively
development tooling; generated baseline/audit/test lines are reported separately.
Non-test production `internal/**/*.go`, `platform-spec.yaml`, `go.mod`, `go.sum` and vendor
have zero changes. Existing test metadata changes classify the new JSON path in
the route inventory and the retirement guard's exact-count table, including this
audit's named-test reference. The separate approved test commit repairs only
post-disposal observations; no workload fixture or search/refusal assertion changes.

## Parent, Watchlist and Architecture Decision

#2447 and #2407 remain OPEN. The explicitly ordered acceptance-boundary slice does
not absorb hotspot extraction, branch protection or reporting. Parent sibling
probing and action remain the approved model: roughly **11-13** future factoring
families, low confidence until salvage eligibility and active-lane coordination;
other R1 acceptance rows remain independently tracked. Implementation does not
change that estimate. The separately merged scatter-gather safety additions are
included in this base, not claimed as G's work.

Watchlist: refine the existing `boundary_owned_decomposition` and
`invariant_suite_coverage` mapping, incorporating docs **b62ccb9** (built on
ff6c80e), then publishing **8027783** on `agent-g/2447-preaudit` for the discovered
artifact-text-census integration and separate LSF-044 qualification finding. It requires exact metadata classification, not
blanket exclusion or weakened guards. No new node, issue or potential-issues entry;
no change to the audited score/CI class or its approved gate. This proof artifact
and the issue update record implementation status; published docs refinement is
not claimed merged. #2353's canonical body now records LSF-044 and both failure
receipts; historical #2444 is not reopened. The subsequent bounded approval and
docs `ca5db26` authorize the test-only repair below, not runtime work or a
qualification exception. The original complexity gate remains unchanged.

## LSF-044 Test-Only Qualification Addendum

[Independent approval](https://github.com/division-sh/swarm/issues/2447#issuecomment-5648722245)
and the checked-in approved-gate amendment bind this additional class. Exact
concept: local disposal versus observed remote lock availability. Canonical
product owners remain PostgreSQL possession, the existing session/transaction
owners and fork/pipeline claim owners; none changes. Test-local observers in the
two existing test files now consume that contract. No new production interpreter.
The immediate helper and all acknowledged-unlock consumers remain unchanged.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Keyed fork failed COMMIT | reproduced and fixed | `TestConversationForkGracefulMutation/keyed/commit_failure`; one bounded actual acquisition, SQL23505, exactly one callback/commit, zero durable rows and fresh PID. |
| Terminal pipeline release, false result/query error | execution-proven through the same corrected path | `TestPostgresPipelineClaimReleaseFailureIsTerminalAndReclaimable`; stale claim, retired lease, one release attempt, actual acquisition then reclaim. |
| Setup issuer/eligibility/hydration with both cleanup modes | execution-proven through the same corrected path | `TestPostgresPipelineClaimSetupFailuresTerminallyReleaseAndReclaim`; primary/cleanup error retention and registry absence precede server proof. |
| Attach/publication poison | execution-proven through the same corrected path | `TestPostgresPipelineClaimPoisonBetweenLeaseAttachAndRegistryPublicationIsTerminal`; registry, scan and capacity checks precede server proof. |
| Scan-close failure followed by ClaimBatch | execution-proven through the same corrected path | `TestPostgresPipelineScanCloseFailureIsTerminalAndReclaimable`; stale scan/claim, actual server acquisition, then fresh nonblocking batch. |
| Terminal advisory failure / ambiguous acquisition | execution-proven through the same corrected path | `TestPostgresTerminalAdvisoryReleaseFailureDiscardsExactSession`, `TestPostgresAmbiguousAdvisoryAcquireDiscardsBorrowedSessionAfterTransaction`; independent errors and unusable retained session, then server acquisition. |
| A genuinely held server lock | reproduced and fixed | `TestForkPostDisposalObservationRejectsHeldLock`, `TestPostgresPostDisposalObservationRejectsHeldLock`; acquisition must fail at its safety deadline while held, succeed after explicit unlock, and pass the strict immediate control. |
| Acknowledged unlock / transaction state / remote lock under local fence | execution-proven through the same corrected path | Full fork matrix; unchanged borrowed/foreign, commit/rollback, retained-reference, cancellation and unlocked-close-error cases; `TestAuthorityTransactionOutcomeEvidence`, `TestPostgresSessionScopeMonitorReleasesHealthyBinding`, `TestSessionMonitorFenceBeforeSilentProofDrain`, generic-schedule and pipeline parent-fence controls. |

Caller census is exhaustive for the two helpers plus scan-reclaim: five original
disposal-helper callers moved, six original acknowledged-unlock callers retained,
and the generic-schedule acknowledged-unlock caller retained. The separate fork
helper changes only keyed failed COMMIT, not other cases or its healthy successor.
No passing sibling is misrepresented as an independently reproduced failure.

Original full-suite and unchanged-base failures above remain valid historical
receipts. The repair removes an invalid distributed timing guarantee, not a
production cleanup bug. Achieved test-class closure is pending integrated suite
and review; #2353 LSF-044 stays repair-open. Architecture tracking remains in
existing nodes, docs `ca5db26` incorporated by fast-forward. Effort/ROI: one small
test-only patch removes a false full-load failure without runtime machinery.
No new issue, platform behavior, compatibility or factoring is introduced.

Architecture feedback is tracked in existing #2447/#2407 and #2443/#2250. Scores
can reward relocating complexity without repairing semantic ownership; keep
future extraction behavior-preserving and family-approved. Long-run direction is
the already-owned decomposition program, not a new metrics/runtime framework.
Effort/ROI: the baseline is one bounded tooling PR with broad ongoing CI coverage;
factoring remains approximately one bounded PR per eligible family, low confidence
and conditional ROI until direct test coverage/eligibility are verified.

Limits/residual risk: same-count worsening is allowed and disclosed; upstream
unreported bodies remain unreported; path/name/ordinal changes are add/remove or
ordinal re-pairings without heuristic matching. Code review and R1.3 must protect
changes to the checker/workflow itself. Local workflow proof does not claim hosted
CI success or independent merge approval. No runtime/provider/live-call evidence
is invented, and no paid-provider proof is required by this gate.
