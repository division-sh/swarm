# Pre-Implementation Coverage Audit: #2550

Implementation qualification exposed the additional L04/L07 private-service phase
counterexamples in [the focused service amendment](issue-2550-local-service-amendment.md).
The independent delta gate is approved under
https://github.com/division-sh/swarm/issues/2550#issuecomment-5974227335.
The seven additional L04/L07/L14 rows are binding within the same one-PR boundary;
no full or review-ready closure is claimed.

Agent: agent-g. Phase: implementation approved under the independent gate below.
Intake: 2026-10-03. Source: master `1afb7f20315e99400b7398ef13c205836abd5f98`,
tree `069470d56363e7ed9695fb21e8f6a5dba66738b9`.
Issue: https://github.com/division-sh/swarm/issues/2550

## Independent coding gate and binding amendments

Approved 2026-10-03 by reviewer-g:
https://github.com/division-sh/swarm/issues/2550#issuecomment-5972816660
This approval supersedes the historical pending-gate wording below. One PR is
the ceiling for all six families; no gross-line ceiling. A complete describe-first
split requires demonstrated material multi-day conflict/requalification cost and
an explicit record before splitting. No such split is claimed now.

Binding corrections supersede the original proposals:

- C04: every qualifying complexity event requires an immutable lineage-valid
  comparison. PR compares its head with merge-base of the exact event base tip;
  push compares before/after; schedule/manual must follow an explicit tested
  branch ancestry rule. Measure-only must never qualify the required summary.
- Registry: 11,947 reviewed rows / 598 source files must NOT become per-file
  shards. First test file-first deterministic ordering in the single TSV by
  conflict replay; otherwise use a small stable set of coarse source-owner
  partitions. Preserve exact identities/judgments and all negative guards.
- Preflight is two-stage: cheap source/host/tool/scratch before planning; exact
  selected-root/native/PG/socket admission after plan binding and before workers.
- Zero eligible classified regression findings yields escape rate N/A, not 0%.
  #1967 is closed history; material publication and cadence acceptance are
  tracked only by open #2535. B05 does not reopen #1967.
- Fresh exact-head server2 local full and hosted full remain mandatory; the
  #2548 waiver does not transfer. All prior characterization is baseline only.

## Disposition and binding context

Category: high-risk maintenance, qualification/source-identity parity and test
lifetime ownership. This is the user-approved batch of SIX separately owned
classes, not a claim that one new framework owns them all. Requested order is
describe (#2), local execution (#10), complexity (#1), other testdata (#9),
timing publisher (#4), full-cadence observation (#8). One PR is proposed for all
six. #3 and #6 are dropped; #7 is watchlist-only; #5 is already done. No branch
protection, production routing, concurrency-model, unused-timeout or vendor change.

Observed symptoms: unrelated source/spec changes rewrite global oracle files;
local qualification runs units serially and discovers prerequisites late;
weights-only bot qualification defaults to full; delayed exhaustive discovery has
no honest escaped-regression/detection-lag report. The issue is a broad batch of
symptoms, made acceptable here by explicit separate owner and proof boundaries.
The observed files/failed qualification are entry points, not the audit boundary.

Chosen working classes to close entirely:

1. Describe regression comparison conflates presentation semantics with relocatable
   source coordinates/current embedded source identity and hides changes in hashes.
2. Local plan execution lacks bounded independent-unit scheduling and early
   prerequisite admission, while preserving exact source, receipts and child lifetime.
3. Complexity admission conflates independently measured revision deltas with a
   committed generated score population.
4. Testdata representation conflates reviewed authority judgments or fixed semantic
   output with volatile/aggregate generated evidence.
5. The existing weights-only publisher does not declare its justified qualification
   scope, although its model still has material consumers.
6. Exhaustive-run observation lacks a source/attempt/root-bound distinction between
   confirmed core escapes, unknown failures and detection lag.

Immediate parent: qualification/repository-friction ownership under #2535 and
#1196; registry/oracle integrity also intersects #2407 and #2447. Broadest parent:
maintainable, effective and economical regression proof. These parents stay open.
No runtime failure class is being absorbed. Intended closure: eliminate the six
listed bounded classes; do NOT claim the 20-minute fleet target, server2's
30-minute local target, a measured week of cadence acceptance, all model debt,
or production regression-oracle closure from this PR.

Exact governing spec sections read:

- `platform-spec.yaml#test_specification.internal_catalog_conformance.qualification_tiers`
  (22707): independent plain CI-Tier/Local-Tier, exact clean source and execution
  identity, complete tier membership, per-command isolation/deadlines/receipts,
  build reuse without result credit, conservative master replay.
- Adjacent `compiled_process_golden_profile`, `compiled_process_full_lifecycle_profile`
  and `compiled_process_numeric_feed_profile`: retained both-store/restart/soak
  obligations and explicit surface credit cannot be thinned to make a faster run.
- `cli_specification.foundations.output_contract.command_support.output_conformance_registry.rows.describe`
  and `.describe_routes`
  (25561/25574): preserve source authority, graph, topology, diagnostics, roots,
  flows, policy/rules and approvals; structural validity is not live readiness.
- Source authority: `filesystem_source_model`, including `local_root_selection.invocation_invariance`
  and `canonical_owner: internal/sourceartifact.AdmittedSourceArtifact`
  near 39840, and `flow_routing` lowered routing/topology contracts. Source identity
  and authored provenance are real production facts; only test comparison changes.
- `engine.runtime_core_persistence_store_contracts.selected_contracts[1].structural_enforcement`
  (6624): exact resolved finding identity,
  reviewed dispositions, and unknown/stale/unclassified fail-closed enforcement.

No exact platform section specifies golden-file storage layout, complexity artifact
layout, host resource sizing, a 30-minute local target, bot cadence or escaped-regression
metrics. For those the full #2550 body/user disposition, existing executable
owners, #2544's accepted qualification contract, and #2447's pinned independent
ratchet are the binding context. Proposed deltas below need independent gate approval
and authoritative spec updates in the implementation PR, not an audit-only spec.
`IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md` were re-read.

## Verified intake corrections

- Timing bot IS useful: package weights drive LPT broad partitioning; exact selector
  weights control eligible short-job packing; weights also supply duration/ETA.
  They do NOT select required roots or set hard deadlines. Preserve the bot.
- Publisher ALREADY runs daily, schedule-only after successful required proof
  (`03:17 UTC`). Do not introduce a fictitious per-push throttle fix. PR #2325 is
  open at `5cba2a4e6`; its body has no CI-Tier, hence full fallback is genuine.
- Run admission ALREADY is host/account-local through XDG_STATE_HOME/user home,
  not a cross-machine queue. Its default is one; explicit capacity already exists.
- Authority TSV is NOT merely disposable generated evidence. It carries reviewed
  classifications of independently discovered declarations. Regenerating accepted
  rows from observed code would disable the guard.
- There is ONE live `*.expected.json` oracle, static_data_invocation, read by both
  releasee2e and cataloge2e. Bundle hash is authority, not generic noise; static IDs
  and content are independently reviewed expectations, not values to erase.
- Server2 observed 16 CPUs/~64 GiB RAM, Docker 26.1.5, native PostgreSQL available,
  and test cluster port 5433 `max_connections=300` via read-only observation.
  CPU count alone therefore does not justify four heavy units on that shared server.
  No server setting, role, other agent process or database was changed.

## Proposed implementation, without new owners/frameworks

### 1. Describe comparison

Replace the one hash/header document with readable goldens, one directory per
fixture and one output per surface. Preserve all five fixtures, all nine surfaces,
both repetitions, stderr/exit assertions and bounded real command execution.
The corpus is fixed, not discovered only from whichever files happen to survive.
Reject missing, duplicate, extra or wrongly assigned cells.

Normalization is test-only, CLOSED and surface-aware. Existing invocation-path
replacement remains. Normalize only named presentation coordinate fields/recognized
renderer locations and exact current source-identity positions. Preserve file
identity, coordinate presence, authored selector/site, messages, diagnostic severity,
event/schema fields, edges, resolution/carries, stage transitions, approvals,
policy/rules, stable IDs and ordering. No recursive key deletion, global numeric/hash
regex, or swallowing malformed/unknown output. Assert actual source hash against
independently admitted fixture identity BEFORE a placeholder is used. Keep exact
provenance/coordinate tests at authoring/topology/lexer owners; a wrong or missing
source/hash must not become equal just because it is masked. If another derived ID
turns out coordinate-dependent, stop for gate clarification rather than masking it.
Production CLI output and SourceArtifactFact remain unchanged. Contract-change
justification belongs in the PR/audit, not a globally rewritten header.

### 2. Local independent-unit execution

Extend `cmd/swarm-test` completion orchestration with a bounded queue of unit
SUBPROCESSES using the existing planned-unit executor, recorder, RunAdmission and
service/process-tree owners. Do not concurrently call runTestArgs in one process:
environment and signal handling are process-global today. Parent owns planning,
source revalidation, cancellation/join and final aggregate; each worker owns its
one admission, exact unit, raw JSON and receipt. No nested parent slot that blocks
its own children; no separately implemented receipt validator. A parent-issued
reviewer-bound worker invocation must enforce the SAME clean-source contract,
not silently inherit current `--planned` developer-feedback permissiveness.

Capacity proposal for the gate: use a stable effective CPU/memory limit (including
container limits), conservative ceiling FOUR unit workers, no more than one per
four CPUs or eight GiB RAM, minimum one. These are scheduling limits to be measured,
not a semantic proof that every workload uses only those resources. Preserve
`SWARM_TEST_RUN_SLOTS` as an explicit positive override, subject to admitted bounds;
unknown resource information is conservative one, not unrestricted.

For independently runner-owned PostgreSQL instances, every concurrent unit retains
its own service and unchanged 300-connection floor. For an explicit shared DSN,
observe the existing capacity owner first and conservatively budget 300 connections
per concurrent unit; a 300-connection server stays at one heavy unit. This allocation
is conservative scheduling policy, NOT a claim that the service floor is a measured
unit footprint. Never override an explicit DSN with Docker, resize PostgreSQL, or
raise a live registry's capacity. Different requested capacities while live work
exists keep the existing fail-closed conflict, not an implicit takeover. Apply
the common admitted capacity to focused and planned entrances, not only --full.
Per-host measurement will explicitly identify owned-service versus shared-service
posture. Server2 can exercise existing runner-owned Docker instances without
changing its shared server; a faster shared-DSN budget would require separate
evidence/gate, not an invented safe multiplier.

Cancellation/failure: stop launching units on the first genuine failure, preserve
every completed/failed/not-started receipt, cancel active children, join their exact
trees/descendant leases and service cleanup, then publish failure. No early unlock,
detached cleanup, lost independent failure, retry-to-green or partial-full success.
Source dirt/head change before, during or after workers fails reviewer-bound proof.
Workers share only compilation bytes through the existing integrity-checked cache,
not stores, workspaces, configuration, test state or execution credit.

Preflight precedes expensive inventory/queue/execution: validate source/toolchain,
selected PostgreSQL prerequisites/capacity or Docker availability, native binary
availability for planned native-required roots, writable disk-backed scratch,
bytes/inodes, and native socket geometry. Test the selected private-cluster
construction with its real maximum path geometry, not just len(TMPDIR). The
serveapp private outage helper uses t.TempDir and must be covered along with
capacity_probe's MkdirTemp helper. Neither touches the shared server. If geometry
needs repair, use their existing disposable lifetime with short socket placement;
no new general PostgreSQL service owner. Required native proof cannot silently skip.
Small prerequisite/canary work runs before long units; reorder execution only,
never alter logical membership, selectors, counts or deadlines. Record host,
admitted capacity, plan/source, queue/execution/cleanup/unit intervals and complete
wall time. The server2 <=30-minute target remains a measured goal, never a timeout
change or a promise. Document server2 invocation; do not make a local command
silently SSH to another host or transmit credentials.

### 3. Complexity delta, not score snapshot

Continue independent source measurement at head and comparison revision with
pinned gocyclo/gocognit, complete authored/all-build-variant census and separate
cyclo/cognitive >=30 non-growth. Retain base/head/delta artifacts as CI output.
Do not accept measured current output as its own ratchet baseline.

Minimal policy-preserving layout proposed: replace the contents of the existing
`.github/complexity-baseline.json` path with a tiny REVIEWED POLICY-ONLY record
(schema, tools, threshold, source-scope rule); no files/metrics/source scores remain
committed. The filename is retained to keep one policy reader and avoid a bootstrap
fallback or a second legacy/policy authority. It is not a generated snapshot after
cutover. Head score-population fields are invalid; historic comparison revisions'
existing policy field is still checked, while their score populations cannot affect
the independently computed verdict. Remove the update-and-commit-score workflow/
`-update` behavior; measurement is evidence-only. Missing/malformed/unknown policy,
tool drift, threshold drift, scope drift, wrong revision and analyzer failures all
refuse. Every qualifying event requires a lineage-valid independent comparison;
no-base invocation refuses, rather than giving qualified measure-only success.

Migrate retired-builder symbol accounting and route-authority drift inventory too:
they currently classify names contained only in the generated metric population.
Removing those generated names does NOT remove a real source-level retirement guard.

### 4. Other testdata

Keep one TSV with file-first deterministic ordering, retaining every exact
declaration/member/type key and disposition. Independently discover every finding;
reject missing/duplicate/malformed/unclassified/unknown/stale rows and filesystem
errors. The updater preserves judgments and labels new findings unclassified.
Six representative disjoint whole-source edit pairs on the original 11,947 rows
produce one kind-first Git conflict hunk and zero file-first hunks. This is limited
concrete reduction evidence, not replay of all historical fleet conflicts or a
claim of conflict freedom. All four guard families retain their complete reader;
no shard inventory, fallback, path allowlist or generated acceptance is introduced.

Static-data expected output retains exact reviewed content/static IDs. Derive only
the current bundle hash once from the independent canonical admitted fixture,
then require the compiled runtime's same exact authority in every path/backend/
restart cell. Do not learn an oracle from runtime output. Both catalog identity and
release readback consumers must use the same authored expectation. Keep geometry,
foreign-cwd refusal and invocation invariance; deliberate byte/ID/hash corruption
must still fail. No production source-artifact format or fork authority change.

### 5. Timing publisher

Preserve the useful daily successful-full-evidence publisher, diff allowlist, one
canonical bot PR, master-only GitHub App/environment and zero automatic dispatch.
Write `CI-Tier: core` and `Local-Tier: core` to BOTH create and existing-PR update
paths ONLY for independently validated model-only refresh. Keep the honest
successful-source-run/model selector binding, all root coverage and hard budgets.
No title/branch-based general tier exception and no App access in feature branches.
Prove weight perturbations change balancing/packing while required roots do not
change; a stale/malformed model or non-model diff still fails publication.
Actual material scheduled publication stays an explicit post-merge acceptance
obligation under open #2535; #1967 is closed history. No credit from a no-op or simulated shell.

### 6. Full cadence observation

Use `internal/testtiming` and `cmd/swarm-test-timing` report/evidence ingestion,
plus existing always-retained workflow artifacts; no new service, dispatcher or
automatic cadence controller. A full-run observation is bound to actual run/
attempt/execution tree, plan/tier/venue and terminal root/backend evidence. Emit
JSON/Markdown with failures, incomplete/canceled/missing evidence, coverage,
confirmed core escapes, unclassified candidates and known/unknown lag.

Full-only failed roots are CANDIDATES, not automatically confirmed regressions.
Review-cycle-1 correction: report reviewed regression and confirmed escape counts
separately. Escape rate is explicitly unmeasured/N/A, including a known escape;
the independent comparable escape/non-escape population and longitudinal rate
acceptance remain open under #2535. Never divide confirmed escapes by themselves
or count unknown findings as non-escapes.
Use existing issue/review attribution when present; absent causal attribution means
unknown, never infer that the latest merge introduced a failure. Lag requires a
confirmed introducing commit on the detected master lineage; count first-parent
merges from it to first qualifying full detection, retain run retries as attempts
of that detection, and reject foreign/future/nonancestor attribution. Missing
prior core proof cannot prove a core escape. Infra/harness/budget failures remain
separately identified, not runtime regressions. No automatic bisect framework or
new machine-review authority. Failed runs retain observation without weakening
required summary or creating a success receipt.

Keep nightly+manual cadence. Collect/review roughly a week under #2535; neither
auto-tighten cadence nor claim operational cadence acceptance from synthetic data.
Unavailable historic artifacts are explicit unknowns, not fabricated history.
Hosted observations fetch complete history; causal enrichment is retrospective
through the existing report command, archived full/core plan and command evidence,
successful exact core run metadata and reviewed attribution. The checked-in command
guide names the complete collection/replay procedure and source-alias refusal.
PR core commands retain their actual execution identity. Only the existing merged
qualification observer may associate them with an exact master landing after
independent tree/PR/protected-check/run/attempt/digest validation; a caller-supplied
SHA or master replay receipt alone is insufficient. No new provenance owner.
Real archived-artifact replay and shallow-history rejection must be proven; no
claim that unattended observations alone classify regressions.

## Systematic consumption census

Statuses: A = already consumes canonical owner; M = moved in this work; D =
different semantic concept with proof below; S = explicitly split/tracked.
Owners are existing semantic owners, not merely the first failing file encountered.

| Owner | Exhaustive currently known live consumer set and disposition |
|---|---|
| CLI authoring/structural owner: cliapp describe + runtime/authoringview, routingtopology, sourceartifact | A production describe/text/JSON/quiet/graph and describe routes; M read_proof_describe_test.go comparison/corpus only; A describe_factoring, payloadless, structural-reader and exact topology/provenance tests. D API runtime source read/fork identity: sourceartifact/runtime authority remains unchanged; retained exact-hash mutation proof guards that distinction. |
| Compiled describe corpus | M all 45 baseline rows (five named fixtures x nine surfaces x two invocations); M only live baseline consumer read_proof_describe_test.go. Historical issue/audit prose is non-authoritative history, not another execution reader. |
| Local qualification: cmd/swarm-test/completion.go + testplanning RunPlan/BindExecution + testtiming ValidateCommandEvidence | M no-context core, explicit --tier and --full orchestration; M parent-authorized planned-unit workers; M focused -- passthrough capacity/preflight integration while retaining plan-free execution; A main.go child executor/creator and completion receipt validation. D hosted logical-unit packing: testplanning/batches.go and ci.yml already process-isolated and retains its unchanged membership tests. |
| RunAdmission/RunLease + ServiceRegistry/Service | M capacity resolution and preflight at runner entrances; A Acquire/FIFO/capacity agreement, inherited lease, process-tree Join/Complete, owned private service creator/reconciliation/cleanup; A run_admission*_test, service_registry*_test and cmd process_tree_* controls. D application SessionAuthority and runtime cleanup: existing proof, no production changes. |
| PostgreSQL test capacity/connection/private probe owners | A ConnectionFromEnvironment/ChildEnvironment/ValidateServerCapacity, ManagerFromEnvironment and testutil consumers; M runner selected-mode preflight; M native scratch/path preflight and existing serveapp private-cluster socket placement if required; A floor 300/native capacity controls, private probe start/stop cleanup. S broader isolation/performance changes #1702/#2394, not new registry or client-pool redesign. |
| Build products: testplanning/build_products.go and execution build context | A unchanged exact clean identity, external GOWORK bypass, manifest/byte integrity; M parallel callers only; A failure/corruption/concurrent-build controls. No new cache or result reuse. |
| Complexity: cmd/swarm-complexity collector/inventory/model/events | M CLI main admission/update flag/README and complexity tests; A pinned tools, inventory, callable identity, compare and emitEvidence; M ci.yml static job label/invocation/evidence presentation; M serveapp retired_builder_transport_guard exact generated-text counts/classification; M conformance route_authority_drift_inventory and its full-census tests. No runtime reader of score snapshot found. |
| Resolved persistence findings + reviewed classifications | M store registry reader/updater and hostile tests; M public_capability_architecture_test adjacent2149 guard; M credentials/currentness_owner_guard_test retired-epoch search; M serveapp/retired_builder_transport_guard_test exact artifact text classification; M two classification markdown docs and spec structural_enforcement owner. A independent typed/raw/effective-method/context finding discovery and compound/run/durable fixture guards. |
| Static-data authored oracle + canonical sourceartifact identity | M releasee2e/static_data_invocation_test.go six compiled both-store shards; M runtime/cataloge2e/static_data_invocation_identity_test.go; A sourceartifact/contracts independent canonical admission, semanticview static content/ID admission; D production bundle/fork authority unchanged, explicit wrong-hash/foreign-cwd mutation proof. |
| Timing weights/model publication: testplanning model/plan/batches and schedule publisher | A broad LPT/short packing/model ETA and strict LoadWeightModel/selector binding; M publisher ci.yml both PR create/update bodies; A cmd/swarm-test-timing updateWeightModel/ValidatePublicationDiff and master-only App boundary. A published bot PR is ordinary native PR qualification, not a second dispatcher. S literal material acceptance under open #2535; #1967 remains closed history. |
| Timing/report evidence | M internal/testtiming report/observation and cmd/swarm-test-timing report entrance; M existing workflow artifact/step-summary presentation, failed-run preservation; M local completion wall/queue/capacity report. A budget evaluator, required-tests aggregate, run/attempt/head/job checks and full-root ownership. S confirmed regression classification #2353 and runtime perf #2394 remain independent; observation does not grant closure. |

Searches included all tracked Go/workflow/spec/docs reads of both baseline paths,
registry TSV/update env, `.expected.json`, RunCapacityFromEnvironment, runCompletion/
runPlanned/executeCompletionUnit/runTestArgs, service/native-cluster construction,
model path/publication modes, full-profile selectors and report/evidence consumers.
Other agents' scratch trees are not production authorities; no files there changed.

Old invalid/removal paths: monolithic describe hashes/header; committed complexity
score population and score updater instructions; kind-first registry ordering;
static oracle's committed current bundle hash; serial completion and late-only
environment refusal; bot PR body without declared model-only scope; unbound
inference that any full failure is a core escape. Production source hashes,
coordinates, raw-authority rules, receipts, capacity floor, exact cleanup and
hard deadlines survive as authoritative. All migrations are all-consumer, with
no dual active readers or backward-compatibility fallback.

## Complete execution paths and gates

1. Describe: authored source -> canonical source admission -> shared structural
   verification -> authoring/topology projection -> selected text/JSON/quiet/graph
   renderer -> compiled command exit/stderr -> normalized golden plus exact
   source/provenance checks. Source/structural/render semantics are D (existing
   describe/topology controls); comparison/source-carriage gates are same chosen
   class. A semantic mismatch cannot be masked at the final snapshot.
2. Local: clean source -> preflight -> inventory/policy/model -> exact tier/plan/
   build context -> bounded worker -> shared host admission -> selected PG service/
   isolated test resources -> exact unit/process tree -> root/backend evidence ->
   child/service join -> source revalidation -> complete aggregate/metrics. Every
   admission/receipt/lifetime/scheduling gate is same chosen class; product test
   semantics are D and retain their own assertions. CI/local profile selection is
   A existing owner, not a new approval interpreter.
3. Complexity: exact event/revision admission -> policy/tool equality -> independent
   base/head source census/measurement -> independent metric deltas -> non-growth
   verdict -> retained evidence. Every gate is same chosen class except upstream
   analyzer algorithms (D pinned tools, no vendor changes).
4. Registry/static: independent source/finding admission -> reviewed expectations ->
   exact classification/identity -> actual CLI/runtime selected-store readback ->
   unknown/corrupt/refusal verdict. Registry representation and authored-oracle
   equality gates are same chosen class; application/fork mutation owners are D.
5. Publisher: successful scheduled full -> exact attempt plan/receipts -> model
   selector validation -> model-only diff -> App/branch/native PR owner -> explicit
   core body -> required normal checks. Same chosen class except full workload
   execution D; first material post-merge acceptance S under #2535.
6. Cadence: actual full attempt/artifacts -> exact source/root/coverage validation ->
   join optional independently recorded defect attribution/prior core proof ->
   honest classification/lag -> report. Same chosen class; defect repair/classification
   itself D/S #2353/#2394. Nothing changes nightly cadence without later evidence.

## Manifestation matrix and exact planned proof

All rows are PLANNED, NOT repair proof. D01-D45 expand to one row per named
fixture/surface cell below; each has two actual compiled invocations and its own
readable golden, exit/stderr and independent identity assertion.

| IDs | Exact fixture | Surfaces in ID order (one ID each) |
|---|---|---|
| D01-D09 | examples/routing/template-select-or-create | describe-text, describe-json, describe-quiet, describe-no-color, describe-graph-text, describe-graph-json, routes-text, routes-json, routes-quiet |
| D10-D18 | examples/routing/template-create-minted-key | describe-text, describe-json, describe-quiet, describe-no-color, describe-graph-text, describe-graph-json, routes-text, routes-json, routes-quiet |
| D19-D27 | examples/integrations/telegram-agent | describe-text, describe-json, describe-quiet, describe-no-color, describe-graph-text, describe-graph-json, routes-text, routes-json, routes-quiet |
| D28-D36 | examples/routing/fan-in/barrier | describe-text, describe-json, describe-quiet, describe-no-color, describe-graph-text, describe-graph-json, routes-text, routes-json, routes-quiet |
| D37-D45 | internal/releasee2e/testdata/golden_agent_workload | describe-text, describe-json, describe-quiet, describe-no-color, describe-graph-text, describe-graph-json, routes-text, routes-json, routes-quiet |

| Row | Manifestation | Exact planned proof |
|---|---|---|
| D46 | coordinate/spec-identity-only drift | Golden normalizer characterization: harmless line insertion/fixture relocation/current embedded identity changes compare equal; actual identity assertion still matches independent admission. |
| D47 | semantic change or unsafe normalization | Reverted mutation arms for event type/edge/carries/stage/approval/policy/rule/severity/file/ID/hash/missing-coordinate; each must fail the specific compiled/golden or provenance assertion. No broad regex. |
| D48 | corpus loss/extra/malformed output | Fixed 45-cell census rejects deleted/duplicate/extra/wrong fixture surface, bad JSON/shape; all current describe/topology exact-location controls remain. |
| L01 | bounded independent unit execution | Compiled runner process harness, at least three barrier-held unit workers; prove overlap and bound, exact unique unit/root receipts and no duplicate result credit. |
| L02 | no self-deadlock / nested slot | One-slot subprocess harness completes multiple units; parent owns no worker admission slot; existing inherited-lease tests retained. |
| L03 | stable CPU/RAM/cgroup limits and override | Capacity table tests include small/unknown/container-limited host, positive/zero/malformed/over-bound override and live-capacity mismatch; actual server2 observation receipt. |
| L04 | shared PostgreSQL capacity vs private services | Real 299/300/600 capacity probes and actual explicit/owned mode controls: no shared-server resize or Docker fallback; separate owned services verify exact floor and cleanup. |
| L05 | SIGINT/SIGTERM queued/running | Compiled multiworker runner signal test verifies canceled ticket, no new launches, all trees/services joined, signal exit and unsuccessful partial aggregate. |
| L06 | parent death / surviving descendants | Existing RunSlotSurvivesSupervisorDeathUntilChildExit and RunCompletionDoesNotUnlockSurvivingDescendant plus parallel parent/worker death barrier; successor cannot acquire retained authority early. |
| L07 | first failure and independent cleanup error | Controlled one failing/one held child plus service cleanup failure: preserve both errors/receipts, cancel/join, mark not-started units, never full success. |
| L08 | source identity changes | Dirty-before/after, untracked/root embed edit, mid-run HEAD change and worker-start drift controls; explicit full refuses; developer feedback remains nonqualification. |
| L09 | shared immutable build cache | Existing warm-root-Go/embed/external-GOWORK/hash mismatch tests plus concurrent worker build/rebuild/poison control. Compilation is not test credit. |
| L10 | early scratch bytes/inodes/socket refusal | Deterministic preflight injection proves no test worker starts on missing binaries/unwritable/full/inode-starved/long socket geometry; real short disk-backed native cluster/control. |
| L11 | all native socket consumers | Real capacity probe and serveapp PostgreSQL outage startup/stop with long parent path, exact private server ownership and required native execution; no shared-server stop. |
| L12 | execution reordering | Plan/selector/root/deadline digest and required children equality before/after ordering; short prereq units execute first without omission. |
| L13 | host/account isolation and FIFO | Existing admission FIFO/corrupt/state authority/process tests plus two source modes/two independent state roots; no fleet/global remote lock or barging. |
| L14 | actual complete local full | Fresh explicit --full on server2, clean final head, all plan units/roots/backends/soaks and retained receipts; actual capacity/posture/wall metrics, no transferred #2548 waiver. Compare with preserved serial historical receipt, label non-controlled performance evidence honestly. |
| C01 | independent ratchet without score snapshot | Exact base/head compiled CLI with source changes, no committed population update; emit independent inventory/metrics/delta. |
| C02 | cyclo-only / cognitive-only growth | Both existing adversarial >=30 growth arms must refuse independently; decrease/neutral controls pass. |
| C03 | policy/tool/scope mutation | Threshold/tool/schema/scope/missing/unknown/trailing policy mutations fail before non-growth credit; no auto reset. |
| C04 | wrong base/event / no comparison | Existing wrong/missing revision, event override/zero SHA controls plus no-base refusal; PR/push/daily/manual lineage remains explicit. No head-only qualified ratchet. |
| C05 | exhaustive analyzer source census | Existing generated/test/all-build variants, duplicate literal identity, suppression/line directive, unreadable source/analyzer missing/duplicate/invalid rows tests. |
| C06 | no snapshot-dependent bypass | Retired-builder transport guard and full route-authority inventory pass after score removal; injected retired production symbol still fails. |
| G01 | lossless reviewed file-first registry | Compare every original finding/disposition to the reordered set; full independent registry/effective-method-set suite and six-pair real Git layout replay. |
| G02 | duplicate/malformed/missing/unreadable registry | Hostile TSV fixtures individually fail; exact file remains part of finding identity; no alternative reader or shard machinery. |
| G03 | unknown/stale/unclassified/raw bypass | Existing resolved types, transitive methods, hostile local operation, compound event/run/durable fixtures plus new unclassified updater arm all refuse. |
| G04 | secondary registry consumers | Public adjacent2149, credentials retired-epoch and serve retired-builder guards retain exhaustive TSV reads and detect hostile trailing sentinels. |
| G05 | static oracle content/ID integrity | Existing catalog admitted-identity proof plus corrupt expected content/static ID/fixture byte counterexamples; only bundle authority independently derived. |
| G06 | static authority/runtime parity | All six TestDurableDataInvocationInvarianceSQLitePostgresShard1..6, real compiled verify/lifecycle readback and refusal/geometry cells; wrong runtime bundle hash fails. |
| B01 | model still has material consumers | Perturb valid weights: different broad partition/packing/ETA, identical exhaustive logical roots/children/tier/deadlines; existing balance/packing tests retained. |
| B02 | stale/invalid model | Strict schema/version/negative/nonfinite/selector mismatch/refused evidence/publish diff controls; no unbound weights or unsafe core publication. |
| B03 | bot create vs update scope | Actual shell/API-payload harness verifies both body entrances set core/core only after model-only validation; existing body prose/issue link preserved. |
| B04 | no App leakage or duplicate dispatch | Existing workflow static/App/master-only/native draft-ready checks; feature/draft/no-op paths acquire no publisher credential; zero automatic dispatch callers census. |
| B05 | material daily publication | Literal successful post-merge scheduled material update/native bot PR/normal core checks; S under open #2535 until it actually runs. #1967 is closed history; no-op is not proof. |
| F01 | complete successful full, no eligible finding | Source/attempt/plan-bound fixture with all terminal roots, valid prior core; report full coverage and rate N/A with explicit zero eligible denominator. |
| F02 | failed full-only root, no attribution | Observation reports unclassified candidate, unknown lag, not zero escaped regressions or verified runtime bug; preserve failed summary. |
| F03 | confirmed core escape and exact lag | Synthetic linear/squash/merge commit history with prior actual successful core, omitted root and recorded introducing SHA: exact first-parent merge count and first detection; duplicate rerun not second regression. |
| F04 | not a core escape | Full-selected failure already caught by core, infra/timeout and harness attribution fixtures remain separate; no inferred causal/latest-head repair. |
| F05 | incomplete/foreign/stale evidence | Missing/duplicate/canceled/skipped roots, wrong backend/attempt/head/plan, expired artifact and foreign/future/nonancestor introducer refuse confirmation and expose incompleteness. |
| F06 | actual hosted observation | Replay real green #2548 full run 37143254718 and retained red qualification receipts with unknown attribution; then new-head hosted full publishes its own exact JSON/Markdown artifact, including failing-run control. |
| F07 | nightly/manual/fleet boundary | Workflow census retains current daily/full-only manual/protected checks; no automatic cadence change or newly successful skip. One-week acceptance remains parent work. |
| Q01 | canonical local/hosted qualification | Focused affected/race proof -> fresh explicit local full -> exact-head hosted full with both 900s soaks/macOS/native union/normal protected contexts, CI-Tier full and Local-Tier full. |
| Q02 | final deletion/spec/owner census | No committed scores/global hash header/kind-first registry ordering/missing bot scope/unowned worker receipt; all spec/directory guards/current sources parse and exact complexity ratchet passes. |

Total: 88 planned rows (45 D cells plus D46-D48, L01-L14, C01-C06,
G01-G06, B01-B05, F01-F07, Q01-Q02). No row earns credit from shared-owner
architecture alone. B05 and one-week cadence/fleet acceptance are explicitly
post-merge parent acceptance, not falsely included in premerge class closure.

## Baseline evidence actually run

- `go test ./cmd/swarm-test ./cmd/swarm-test-timing ./cmd/swarm-complexity
  ./internal/testplanning ./internal/testtiming -count=1`: PASS. Respective
  package durations 25.395/14.976/13.330/28.819/8.290 seconds. Log
  `/home/youmew/dev/agent-g-2550-baseline-focused.log`.
- `go test ./internal/cliapp -run '^TestReadProofFactoringCompiledDescribe$'
  -count=1 -timeout=15m`: PASS, 172.278 seconds, all 45 cells x2 real compiled
  invocations against the old hashes. Log
  `/home/youmew/dev/agent-g-2550-describe-characterization.log`.
- Read-only current issue/thread, bot PR, open-PR owner/path census and server2
  resource/PG setting inspection. No server/database configuration changed.

These are baseline characterization, NOT new normalization/parallel execution,
mutation qualification, candidate savings, full-suite or closure proof.
No full run, live provider, paid call or production implementation was performed.

## Parent probes, tracker/watchlist and feasibility

Parent sibling probes: existing draft/ready admission, verified master replay,
source/cache identity, hosted logical/physical plan evidence, fixed tier source
inventory, PostgreSQL service/resource leases, unused native diagnostics, source
artifact/CLI structure, reviewed raw authority, real runtime concurrency model.
The watchlist's `harness_reliability_and_local_smoke`, `invariant_suite_coverage`
and `boundary_owned_decomposition` nodes name these siblings. This supports
absorbing all SIX requested tooling classes now, not resetting runtime perf,
broader R1, flaky-test classification or app authority. Parent action: keep those
different obligations open; no new umbrella or speculative issue needed.

Tracker decision: #2550 body needs the six verified factual corrections and this
audited class boundary before coding; append them without deleting original
evidence. #2544 is CLOSED on merged 1afb with final independent approval and
retained 48-row receipts; #2535 remains OPEN with literal scheduled/material
publisher/fleet acceptance and these tooling followups identified. No superseded
runtime issue, new child issue or POTENTIAL_ISSUES entry. Refine existing watchlist
nodes, including preservation of policy/classification authority when generated
evidence moves out of committed shared files.

Watchlist promotion check: existing node already covers complete-artifact timing,
test concurrency/lifetime ownership, proof identity and lost diagnostics. Its
additional live sibling families are explicit above; none is a second describe
oracle, complexity ratchet, bot dispatcher or local completion runner. Broader
#2535 target needs real busy-fleet/post-merge acceptance, not another code patch.
Remaining parent tail estimate: 2-4 groups (publisher/master/scheduled literal
acceptance; busy-fleet and full-cadence week; runtime performance #2394/2549;
independent #2353/#1196 proof-health work), medium confidence; not a claim these
are two-to-four already-gated PRs. No reduction claimed from this pre-audit.

Feasibility: six finite existing owner families and 88 proof rows are
closeable in one PR without application schema, production semantics, new service,
vendoring or compatibility machinery. The compiler/service/admission already
exist. Prepare the entire batch before opening the PR; make #2 the first
implementation commit and keep new goldens reconciled to genuine accepted
contract changes, not stale hash regeneration. Do not open a days-long WIP PR.

Active source overlap: PRs #2545 (F), #2531 (D), #2526 (C), #2525 (E) all touch
the generated baseline(s); F/E also touch static expected JSON; D/C also touch
the registry. Their production owners remain theirs. #2325 is the bot-owned
weights file. Baseline cutover needs a concise migration handoff to those lanes;
do not edit their branches, accept their changed semantics without assertions,
or use repeated rebase-only pushes. Integrate actual required owner changes once
before final qualification; do not impose zero-behind-master merge policy.

If the combined implementation would leave the describe cutover in an open PR
for days, ask reviewer-g for the ONE justified split: land complete describe
normalization/corpus first, then the already-audited other families. Do not split
individual consumers or drop proof to create a superficially smaller PR.

Architecture feedback: committed evidence, reviewed judgments and semantic identity
need distinct roles; serial orchestration and globally scoped process effects hide
parallelism/lifetime obligations. Long-run direction is independently measured
evidence, stable reviewed per-owner expectations and process-isolated execution
through existing owners, NOT a proof framework or runtime scheduler. Tracking:
promote these six in #2550 now; reusable residual capacity/measurement/ownership
risks refine existing watchlist and #2535/#1196. Rough effort 3-5 engineer-days
including characterization/integration/full proof, medium confidence. High ROI
from eliminating two fleet-hot generated populations and shortening safe local
qualification; actual cost/time reductions must be measured, not estimated as closure.

## Gate request / stop conditions

Request reviewer-g's independent semantic/maintenance coding gate for this six-family
one-PR boundary, normalization allowlist strength, policy-only ratchet, conservative
capacity/posture limits, corpus/classification migration and honest cadence report.
Proposed final review scope: CI-Tier full, Local-Tier full, fresh final-head proof;
the user waiver for #2548 DOES NOT carry to #2550.

Implementation remains frozen until the explicit gate is recorded on #2550.
Stop and repair the gate if another live same-concept owner appears, normalization
needs to erase semantic identity/diagnostics, parallel safety requires a new service/
admission framework or application changes, borrowed/shared PG cannot be safely
bounded, cleanup cannot retain/join exact authority, a real assertion/ratchet/deadline
would be weakened, or hosted publication/metrics require a new credential/review
authority. Unknown causal attribution is allowed only as an explicit observation,
not as confirmed cadence acceptance. No implementation is authorized by the
baseline green tests or this author's closure-feasibility assertion.
