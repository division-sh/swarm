## Independent Gate: Approved as First Slice

**Outcome: `approved as first slice`.** Reviewed pre-audit `ea71f436a` on merged master `90809b1cb`; this ruling is a binding pre-audit addendum, not an implementation/merge proof. G may implement the baseline-only PR after incorporating this record. No further prose-only gate cycle is requested. Runtime factoring remains unapproved.

### Findings and Class Assessment

The category is correctly **high-risk maintenance of a CI acceptance boundary**, not a runtime semantic change. #2407 R1.4 and #2447 explicitly authorize baseline-first sequencing. The working class is broad enough for that first slice: reproducible pinned-analyzer source measurement and non-increase admission across local tooling, revision selection, normalization, artifact validation, CI events/profiles and required aggregation. The parent complexity-reduction program remains open. Neither a baseline file alone nor a shorter serve function would close this class.

Two collector assumptions needed explicit repair in the proposed audit. These are concrete implementation requirements supplied by this addendum, not reasons to reopen a runtime architecture design:

1. **Successful, nonempty analyzer output does not establish complete admitted coverage.** Both pinned analyzers honor per-function suppression comments. My fixture has a function scoring gocyclo **31** / gocognit **30**; adding `//gocyclo:ignore` and `//gocognit:ignore` removes it from both successful reports while an unrelated visible function keeps each report nonempty. Independent base/head measurement would still accept that apparent decrease unless source admission rejects suppression. The audit's malformed/empty-output tests did not explicitly cover this manifestation. Add it before claiming ratchet closure.
2. **The proposed path/package/receiver/name key is not sufficient, and the two tools do not enumerate identical functions.** A valid file with two `init` declarations produces duplicate keys. For `var A, B = func(){...}, func(){...}`, gocyclo v0.6.0 reports two rows both named `A`; gocognit v1.2.1 reports neither. A `//line fictitious.go:77` directive changes the reported filename/position. Do not overwrite duplicate rows, manufacture a zero cognitive score, assume one-to-one metric rows, or use adjusted diagnostic paths as physical tracked-file authority. The audit must not describe upstream hotspot counts as comprehensive measurement of every executable Go body.

No missed live repository complexity checker was found. The newly identified interpreters are the pinned analyzers' own reporting/admission conventions. They belong inside the already-proposed collector boundary, not in a new framework or third-party fork. Existing source contains no suppression directives or line directives in the included non-test files; the counterexamples protect future changes and validate the proposed representation before coding it.

### Independent Checks and Evidence

- Read #2447's full body/thread, its checked-in audit and gate request, #2407 body/thread, the R4/R5/R6/R7 boundaries (#2410/#2411/#2412/#2413), and #2443's activation/lifetime exclusion. #2407/#2447/#2410/#2411/#2412/#2413/#2443/#2250 are OPEN. #2446 is actually MERGED at `90809b1cb`; #2441 is MERGED historical evidence, not an open dependency invented by this review.
- Verified G's analysis worktree is clean. `90809b1cb..ea71f436a` contains only the audit Markdown. No implementation predates this gate.
- Independently ran the exact pinned tools over the tracked non-test inventory and the two identified generated-file exclusions. Reproduced **236 cyclo >=30 / 43 >=50**, and **518 cognit >=30 / 158 >=50**. These verify the exploratory scope, not an immutable numeric allowance for a later base.
- Confirmed current `serveapp.Run` = **37**, `buildRuntimeComposition` = **184 cyclo / 286 cognit**, and `Service.driveLocked` = **76**. Inspected the real serve acquisition/defer/lifetime code and its shared live/test construction, not just its score. The named old decoder has no current definition/caller. Missing historical keep-map/data files are not available in the checked source/docs revision or tracked docs history searched. I do not ratify a replacement keep-map by inference.
- Inspected the single current CI workflow: PR/push/manual/schedule events, all four profiles, independent planning/execution SHA handling, and `required-tests`. Its current generic aggregate accepts `success|skipped`; adding a complexity dependency alone would therefore not enforce the proposed non-skipping rule. The new complexity result needs an explicit success-only requirement.
- Executed the positive/suppressed/duplicate-init/multi-variable-function/line-directive miniature probes against both unmodified pinned tools. Probe files are outside the source worktree at `/tmp/gate2447-analyzer-probes`. Analyzer versions and upstream implementation were inspected locally; no custom scorer or product patch was used.
- Verified the named golden workload tests exist. They were not run during this gate, and no unimplemented checker test is claimed passed. The checked-in pre-audit correctly distinguishes planned proof from executed measurement.

### Binding Implementation Conditions

1. **One small development owner.** Proceed with one development-only command and its focused tests, deterministic baseline/delta artifacts, and one CI job. Pinned upstream tools remain the score owners. Ordinary tool provisioning is allowed; vendoring is not. No product dependency workaround, local scoring algorithm, generic metrics framework, runtime edits, ownership movement, or test weakening. Tooling additions are explicitly allowed because they buy the missing executable ratchet invariant; report them separately rather than claiming product-code reduction.
2. **Exact source inventory.** Measure tracked authored non-test Go across build variants, including authored tools/support. Exclude canonical generated files using actual Go generated-marker semantics and record their paths. Inventory and comparisons must resolve the requested Git snapshots, not a dirty working tree, host-selected `go list`, untracked files, or a fallback current checkout. Reject absent revisions, empty repository inventory, parse errors and unsupported/unreadable tracked inputs. Files legitimately containing no functions are not missing analyzer output. Report changes to the admitted/excluded file set.
3. **Analyzer admission and identity.** Reject `gocyclo:ignore` / `gocognit:ignore` suppression directives in included authored source. Include zero cognitive rows with `-over=-1`. Specify each metric's actual upstream callable population; absence from the other metric is not a measured zero. Preserve every distinct reported occurrence with collision-safe deterministic identity; line-only shifts must not rewrite identities. Use syntax/provenance mapping where necessary, not independent complexity scoring. For adjusted `//line` provenance that cannot be normalized reliably, fail explicitly rather than trusting fictitious paths or silently dropping rows. Do not ban valid repeated init declarations merely to fit a map key. Add exact fixture controls for all counterexamples above and for generated-marker/zero-function files.
4. **Unchanged count policy, honest limits.** Both >=30 hotspot counts independently cannot increase. >=50, maxima and per-function increases are reported, not new blocking policies. Equal-count worsening, upstream-unreported body shapes, and add/remove identity changes remain explicit limitations; no claim that total complexity or every function can only decrease. This gate does not authorize modifying analyzers to eliminate their limitations.
5. **No self-approved baseline or policy reset.** Fresh head measurement must equal the checked-in artifact. Independently measure exact base and head under the same admitted tool/policy identity, including the first baseline PR. Editing ceilings or the baseline cannot authorize growth. Changed analyzer versions, thresholds, exclusions or schema require an explicit policy review and comparable evidence, not an automatic reset. Keep revision evidence in run artifacts without a self-referential HEAD requirement in the committed baseline. A local update operation produces evidence, not CI approval.
6. **Actual CI enforcement.** Use actual PR head/base facts for this checker without changing existing test-planner synthetic-merge semantics. Push uses exact before/after; unavailable/all-zero comparison facts fail clearly. Manual/nightly validate current baseline without inventing history. Every existing profile runs the independent job. Add it to `required-tests` with a strict `result == success` check; skip/cancel/missing/error are not success. Upload reproducible metric/delta evidence. Repository protection/identity administration and monthly publication stay #2407 R1.3/R1.5, not scope added here. This is not a claim of tamper-proof enforcement against a PR that rewrites its own workflow/checker; that trust boundary still needs code review and R1.3 controls.
7. **One complete proof pass.** Implement the original audit's threshold, normalization, baseline-exactness, disposable-Git revision, event/profile and aggregate counterexamples, plus the added suppression/population/identity/provenance cases. Preserve real upstream analyzer runs and real Git base/head tests; a YAML string assertion alone is insufficient. Prove head-vs-merge distinction, dirty/untracked isolation, false baseline inflation, missing base, initial bootstrap, and independent metric regression. Then run the unchanged golden smoke, restart/SIGKILL and both burst iterations, plus final full qualification through `go run ./cmd/swarm-test`. A PR proof audit must identify actual passes and limitations, not merely copy this planned matrix.
8. **Keep factoring separate.** First PR says `Part of #2447` and `Part of #2407`; it closes neither issue. No serve extraction to offset checker line additions. Later factoring needs the original salvage evidence or an explicit lead-ratified replacement table, coordination with active lanes, and approval of the current function family. Interpret the later <=1500 changed-production-line cap conservatively as additions plus deletions, retaining net-neutral/negative product lines. A high score, keep-package adjacency or a wrapper rename does not authorize `buildRuntimeComposition` restructuring. Preserve original defer/cleanup lifetime and shared serve/test semantics; actual ownership changes remain #2443/#2250 and the R4-R7 lanes.

### Architecture and Tracker Disposition

Architecture smell: metrics can reward moving complexity without removing semantic ownership, especially in the shared serve composition. **Disposition: attach to existing #2447/#2407 and #2443/#2250, not escalate the baseline PR into a broad refactor.** This is honest because the checker closes a distinct acceptance-boundary gap and does not claim to repair runtime architecture. Missing eligibility evidence still blocks later factoring, not this no-product-change baseline.

Watchlist decision: **refine existing nodes** `boundary_owned_decomposition` and `invariant_suite_coverage`. G's published `ff6c80e` was independently verified; reviewer refinement `b62ccb9533ff59add07453987f44206716d97a22` is YAML-validated and published on docs branch `review/2447-gate`, based on G's commit, not claimed merged to docs master. It records the suppression/population/provenance findings and this scope. No new issue, architecture queue or `docs/POTENTIAL_ISSUES.md` entry is necessary; the concrete work remains in #2447.

**G's next work item:** incorporate this gate addendum and the reviewer docs refinement, then implement and qualify the baseline-only PR in one pass. No further pre-audit approval is needed for these specified collector corrections. Stop only if this cannot be delivered with the one existing-purpose tooling owner, requires product semantics/legacy/vendoring, changes the approved metric policy, or would need to weaken runtime tests. Do not start hotspot factoring under this approval.

## Bounded Qualification Amendment: LSF-044

Independent [approval](https://github.com/division-sh/swarm/issues/2447#issuecomment-5648722245)
amends the existing-test prohibition only for tests that mistake local unsafe
socket disposal for synchronous remote advisory-lock release. No runtime repair,
factoring, score-policy change or fresh semantic gate is authorized or needed.
The governing `platform-spec.yaml` owner is
`backend_neutral_runtime_mutation_write_boundary`: local connection closure does
not prove remote SQL completion or advisory possession disappearance.

The fork failed-COMMIT symptom is the entry point, not the boundary. The chosen
additional class is false synchronous-release/reclaim assertions after unsafe
disposal. The parent is truthful distributed authority observation; product
transaction/lifecycle ownership is unchanged. One test-only commit can close the
identified assertion class without claiming a runtime defect was repaired.

All original `assertIndependentAdvisoryLockAvailable` consumers were classified:

| Consumer | Observation contract / repair |
| --- | --- |
| Fork keyed failed commit | After disposal: one bounded real server acquisition and checked unlock on the same independently pinned connection. Preserve SQL23505, one callback/commit, zero durable rows, fresh successor PID. |
| Fork other scenarios and healthy successor | Immediate try-lock unchanged after acknowledged unlock or the explicitly already-unlocked lost-lock case; error/state/PID assertions unchanged. |
| Pipeline terminal release (false/error) | After disposal, following stale-claim and lease-retirement assertions. |
| Pipeline setup issuer/eligibility/hydration, fail-once/persistent | After disposal, following primary plus cleanup error and registry-absence checks. |
| Pipeline attach/publication poison | After disposal, following exact registry/capacity/scan retirement checks. |
| Pipeline scan-close failure then ClaimBatch | After disposal, following stale scan/claim checks; server acquisition must succeed before fresh nonblocking reclaim. |
| Terminal advisory release / ambiguous acquisition | After disposal; retain independent errors and reject SQL through the retired session before server observation. |
| Borrowed/foreign transaction claims, commit/rollback, retained reference, caller cancellation | Acknowledged unlock: immediate helper unchanged. |
| Successful unlock followed by session-close failure | Acknowledged unlock: immediate helper unchanged. |
| Generic-schedule terminal preparation, healthy schedule release, pipeline parent fence | Separate acknowledged-unlock or held-lock control; unchanged. |
| Transaction outcome / invalid-authority tests | Already bounded real acquisition after disposal; unchanged. |
| Healthy session monitor / silent monitor fence | Immediate acknowledged release versus remote lock still held under local fencing; unchanged. |
| API, serve, selected-fork contention, test-server manager | Admission/held-lock barriers, not disposal-release observations; unchanged. |

The old immediate helpers remain authoritative for acknowledged unlock. Only
named disposal callers use the new test-local observers. These issue one real
blocking acquisition under a finite safety deadline, then check unlock and
connection cleanup. Errors/deadlines fail the proof. No polling, sleep, mutation
retry, pool reset, runtime owner, timeout-policy change or third-party code.
Both observer variants require a still-held-lock negative control, followed by
successful acquisition after explicit release. Existing local-fence/remote-held
controls retain their opposite assertion.

Required execution: complete changed fork/persistence matrix under race detection,
100-repeat held-lock controls and original keyed commit case, healthy sibling
controls, dual-store golden restart/SIGKILL/bursts, then fresh integrated managed
whole suite. Existing failed qualification and unchanged-base receipts remain
historical failures, not overwritten by targeted passes.

Tracker/watchlist decision: absorb only this test-contract repair in the current
PR, retain #2353 LSF-044 repair-open through qualification and review, incorporate
published docs `ca5db26` (`invariant_suite_coverage`). No new issue or architecture
entry. The broader #2447/#2407 factoring parent stays open with the original tail
estimate. No spec semantic change: these tests now respect the existing contract.
