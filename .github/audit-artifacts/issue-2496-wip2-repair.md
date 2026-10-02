# R5.1 WIP Pass 2 Repair

## Master f805cdb7f Reconciliation

Rebased all repair commits onto master f805cdb7f. The only conflict was the
compiled describe characterization: master adds pack/platform source provenance
and R5.1 adds the constructor projection. Both implementations remain intact.
The first exact compiled root is RED89.615s, limited to ten JSON surfaces. For
each, removing only the 2488 newly added provenance entries reproduces the old
exact stdout hash; all other output is byte-identical. The baseline retains
the new provenance, all 45 cells, two repetitions and 35 unchanged hashes.
The normalization remains repository/isolated-scope absolute paths only.
Detailed before/after receipts are in issue-2496-describe-baseline-migration.json.
The earlier mistyped TestReadProofFactoringDescribe selector ran no tests and
receives no credit. A fresh correct-root qualification remains required.

The new physical deployment/scenario constructor matrix initially failed the
fieldless fixture's empty entities.yaml admission on both stores (RED54.119s).
Omitting that undeclared file preserves strict source admission. The corrected
three-root matrix passes through swarm-test at race/count3: PASS58.109s, both
stores, including rollback-before-ack, commit-before-lost-ack, exact replay,
cancelled admission and fieldless header without a field companion. This is
native transaction proof, not public launcher qualification.

Against f805cdb7f, no-rename non-test Go accounting is 194 files, +7502/-7683,
net -181. Readiness core is 386 lines. Exact-snapshot measurement at e7dc25131
reduces cognitive hotspots >=30 from 581 to 578 and cyclomatic hotspots >=30
from 266 to 262. The policy and thresholds are unchanged. Default managed
qualification, remaining race supplements and exact-head CI are still pending.

Binding issue authorization: 5947674701. Independent review/checklist:
5947664407 / 5947675035. This additive receipt is not a final proof audit or
merge-readiness claim. No new grammar, framework, compatibility path, or A/B
ownership change is introduced. P01-P60 / C01-C20 remain binding.

## Canonical Owners And Consumers

- `RequireStandingConstructionPath` consumes the existing constructor for every
  ancestor and the root's complete keyless tree before standing reconciliation.
  Disk/reconstructed declaration admission agree. Unrelated keyed branches stay
  deferred; keyed ancestry refuses before mutation.
- Pipeline standing preparation constructs each selected generation's exact
  run-owned root recursively and atomically through the existing constructor.
  `StandingForGeneration` returns the declaring descendant with its actual
  parent. Independent service identities and generations remain independent.
- `LoadConstructedFlowInstance` consumes the strict paired target reader.
  Manager verifies every expected descendant before process attachment. Missing
  members refuse without repair; terminal descendants are not reattached.
  Active members consume the existing exact readiness/lifecycle owners.
- The pre-start owner stages all members, keeping executable completion behind
  Manager startup. Acknowledged construction errors no longer run that finalizer
  early. Later preparation errors preserve construction evidence. Child plans
  carry exact standing replacement authority without repeating construction.
- Service publication follows construction of the entire selected set and
  retains each generation's exact run/source context. Second-tree construction
  failure publishes no member.
- Both generic activation adapters, and selected activation through their
  lineage, inventory strict canonical headers, including fieldless instances.
  State-only imports do not count. Unused `FlowInstances` / `SourceFlows` and
  their instance-path-derived scope interpreter are deleted.
- Agent replay receivers consume the same fixed-revision historical header
  projection/paired reader as materialization. Identity, route, template, field
  contract, lifecycle/configuration and fields must agree before replay writes.
  The shared runnable snapshot loader consumes canonical metadata attachment,
  including recorded header stage-entry time, not mutation-log time.
- Exact historical reuse compares progress only through the header; optional
  field rows own business fields, not a second progress authority. Header JSON
  objects decode strictly. Non-agent/timer/deferred replay refusals remain.

## Development Evidence

These receipts precede final frozen-head qualification; earlier REDs remain RED.

| Proof | Actual result |
| --- | --- |
| Standing ancestry | Initial RED on root/intermediate ancestry; disk/reconstructed cases and root-constructor control PASS0.192s. |
| Constructed inventory | Managed RED2.624s (two headers counted as one on both fieldless backends); corrected four-cell inventory included in native matrix PASS5.775s. |
| Pre-start/handoff Manager controls | Three named roots PASS0.410s. |
| Complete tree/replay/inventory native matrix | Three roots, both stores PASS5.775s; missing sibling/fields and foreign-parent refusals preserve construction history. |
| Expanded replay matrix | Both stores PASS3.803s; foreign template, wrong stage, malformed bookkeeping, state-only receiver and missing fields refuse without execution-history mutation. Agent replay, source isolation, reconstructed readback and repeated-activation refusal remain asserted. |
| Root provider | Managed both-store text/callback journey including beta PASS4.476s; stronger independent-generation complete-tree readback PASS5.649s. Compiled served fixture with mocked provider, not paid provider qualification. |
| Partial set | Both stores PASS2.111s: second construction fails, all service publication sequences remain zero. |
| Proof-plan partition | PASS0.009s; new native backend leaves mandatory in existing units. |

Authoritative `platform-spec.yaml` states these tree, eligibility, restart,
pre-start, inventory and receiver rules in this same repair. Managed race,
public restart/reset/default qualification, complexity measurement, exact-head
CI and the final proof audit remain required before final review.

## Qualification Cutover Checkpoint

The complete native-package diagnostic at698b5e1ea is RED600.049s and timed
out before all roots ran. Sampled unchanged-master controls at9a1f27fbd pass
8.053s, establishing cutover obligations rather than a blanket flakiness waiver.
Native header fixtures now declare complete identity, progress, clocks and
JSON objects. Execution fixtures use the canonical constructor before handlers,
and real Manager attachment follows execution admission. Ordinary Initialize
and node-first construction are not restored. The retired receiver SQL allowance
is deleted; its guard requires zero SQL in that consumer.

The connected-receiver matrix keeps exact scoped targets, sibling isolation,
terminal/draining refusal, persisted publication replay and late-row isolation.
Missing-target upserters now refuse, including after a field-only row appears.
Duplicate field rows cannot elect a second owner. Wrong-owner controls declare
the original exact constructed target instead of deriving an entity from the
retired handler-initialization interpreter. This is native routing evidence,
not public constructor qualification.

| Additional executed development proof | Result |
| --- | --- |
| Keyed ancestry native refusal | Both stores/root/intermediate PASS2.109s; complete snapshots unchanged. |
| Expanded historical receiver coordinates | Both stores PASS3.083s, including wrong path and foreign-run header refusal. |
| Generic/selected inventory | Both stores/fieldless/fielded PASS3.211s. Selected inventory precedes its named binding refusal; not selected execution success. |
| WIP2 deterministic race/count3 matrix | Native package PASS161.188s; Manager PASS9.823s. Standing tree/partial set/ancestry, replay/inventory, lost response/cancellation/native cleanup, pre-start handoff and actual load-budget roots all executed. |
| Existing public standing component/lifecycle controls | Managed PASS39.319s at698b5e1ea; not the new compiled launcher journey. |
| Mixed fan-out, B07/B08/B18 recovery/control | Constructor and actual attachment migrated; focused diagnostic PASS28.607s. No prefix/disposition assertions removed. |
| Terminal admission/log and mixed execution | Five focused roots PASS4.583s, preserving lifecycle/accounting and immutable replay assertions. |
| Header history/reset | Two focused roots PASS1.815s after correcting incomplete native clocks and historical state evidence. |
| New public standing journey | RED27.071s on test setup; next RED10.893s reached hash-only restart on both stores but rejected its declared standing_reconcile reason. Corrected assertion preserves exact run, generation, status and start time; reset proof still pending. |

No development receipt is relabeled final-head qualification. The old A native
join fixture's ordinary Initialize seed was cross-recorded on #1994 in
5948846117; A's production collection/join files and lifecycle signature remain
unchanged. Managed default/public/recovery qualification, exact complexity and
exact-head CI remain outstanding. This is not a final audit or merge claim.

## Reconciled Qualification Checkpoint

Rebased the complete branch onto origin/master9a1f27fbd. The authority registry
keeps A's native delivery-cardinality witness; the retired receiver-dependent
materializer helper is absent rather than restored during conflict resolution.

The mock-agent crash fixture now declares Mock in its initial constructor
context and successor context, with a current attachment clock. Its SQLite
after_commit_before_ack cell passes6.763s after the mode/clock setup REDs.
No persisted mode rewrite, new continuation owner, or admission bypass occurs.

The connected wrong-owner negative explicitly routes the original constructed
entity, then corrupts native header identity. Its focused SQLite control passes
0.979s and rejects the changed entity before publication. Complete matrix
qualification remains required.

A's physical driver-COMMIT fixture now seeds an explicit native header beside
its native field row and requests only UpdateStateAndCompanion. All eight
SQLite/PostgreSQL commit, rollback, lost-acknowledgment and deferred-rejection
cells pass3.333s with the original arm, deadline, revision and retry assertions.
This is physical store-boundary proof, not compiler/public construction proof.
A's production collection/join files and lifecycle signature are unchanged.

Managed public standing restart/reset, complete connected/crash matrices,
default suite and exact-head CI are still outstanding; no final closure claim.

## Broad Qualification Fixture Repairs

The bbdd8a9a0 connected/crash/A2/freeze aggregate is RED54.486s: connected
receivers, all four real mock-agent SIGKILL cells and the A2 native cells pass;
selected freeze used an unrelated catalog under the actual gate bundle hash.
The gate fixture now declares its own bare root input and subscriber, and both
generic/selected freeze controls consume that exact source catalog. All sixteen
freeze/history/rollback cells pass6.423s with unchanged history/frontier oracles.

The first default run on the reconciled source is RED/incomplete, deliberately
terminated in broad-01 after five affected roots failed. No later unit is
credited as executed. Repairs retain the boundaries rather than grant exceptions:

- Standing commit-error preparation does not execute readiness early, even
  when durable construction was acknowledged; normal activation retains its
  acknowledged-error finalization.
- The compiled standing fixture moves unchanged behind a closed canonical
  fixture API, rather than weakening the repository routing guard.
- The ordinary native fork-tool fixture has an explicit complete constructed
  header and canonical config before revision capture; field-only imports do
  not acquire executable fork authority. The focused control passes1.277s.
- Root historical replay carries explicit template `.`. The focused control
  passes0.007s with exact child identity projection.
- The deleted metadata write allowance is removed. Shared snapshot attachment
  remains its sole producer; the hostile ownership guard passes1.030s.

Manager commit-error controls pass0.255s. These are targeted development
receipts, not a complete repaired-head default or public qualification result.

The closed-fixture compiled standing journey passes both stores13.192s. Its
selector also named a nonexistent progressive-presence root; that name receives
no proof credit. The actual progressive-presence selector remains required.

The census update classifies all71 changed records:11 typed process-local,
four typed public-facade,32 private backend and24 private header projection
findings. Stale signatures/removed calls are deleted. Exact registry plus raw
effective-method and hostile resolved-type controls pass8.690s. No raw authority
exception or guard threshold changes. The closed routing guard passes12.869s;
proof partition controls pass1.515s.

## Discard Consumer And Settlement Proof

The second default run, source0ff2b6cb9 followed by metadata-only02b776816,
completed RED in broad-01 at the required-missing selected receiver on both
stores. No later unit ran. Removing its declared fields correctly causes the
new inventory to refuse activation. The old negative expected successful
activation, and post-return readback lost the rows removed by joined discard.

A held existing post-settlement probe now verifies the exact terminal claim,
absent required row, unchanged source and sibling state before release, then
checks the named activation refusal and discard. It exposed a real omitted
retirement consumer: retained-completion discard removed fields but left three
constructed headers on both stores. The same named atomic discard now removes
those headers and cascaded readiness/construction rows. Retention, story order,
completion tombstones, dependency refusal and rollback remain unchanged.
The before-edit class/census repair is recorded on2496 at5950195540, under the
existing O2/C11/P35-P36 retirement boundary; no additional semantic owner.

The supported receiver/control matrix passes6.392s after the repair. Native
unretained, retained-completion, cancellation and rollback controls now include
explicit physical header/attachment/construction fixtures; these are cleanup
proofs, not public constructor admission. Final managed qualification follows
the committed repair; earlier green matrices are not relabeled this head.

The fourteen-root managed WIP2 race/count3 supplement on02b776816 passed:
runtimepersistence291.210s and Manager9.608s. This precedes the discard edit.
The public/volume count3 command remains running, also preceding that edit.

## Qualification Receipts After Discard Repair

The real-schema cleanup and selected-receiver race/count3 matrix passes:
pipeline71.171s and runtimepersistence40.251s. Retained/unretained completion,
cancellation and rollback include headers and cascading attachment/construction
evidence. This source is c76849f82; the later writer census correction is test
metadata only. Authority registry/hostile guards pass4.344s and complexity
policy passes without changing thresholds or regenerated head scores.

The cffa774ea default managed suite is RED in broad-01. Its only failed root is
TestDeleteSelectedContractForkStatePreservesCompletionTombstones: its hand-built
backend schema omitted flow_instances, so both retain modes failed with no such
table. The fixture now includes that table and explicitly asserts its removal
in both modes. The focused root passes count3,11.383s. No production change or
guard exception is needed; no later default unit receives proof credit.

Public standing/progressive-presence/sequential-run count3 passes259.878s at
02b776816. Sequential runs use compiled internal mock-lifecycle processes with
public RPC readback, not real-provider/public-launcher qualification. The same
aggregate's volume package is RED600.031s at the unchanged package-wide ten-minute
limit during repetition3. It is not a completed volume count3 receipt. Separate
SQLite and PostgreSQL count3 managed jobs retain the original test/deadlines and
assertions; their results, the repaired default suite and exact-head CI remain
pending. No deadline inflation, capacity bypass or partial GREEN claim.

The thirteen-root attachment race/count3 aggregate is RED600.054s: its ABA root
passed a pre-Abandon superseded observation to Begin after durable state became
aborted, and both backends correctly rejected it as stale. The test now asserts
that refusal, then validates/reloads the exact settled predecessor before its
successor request. All ABA/hash/ordinal/late-callback assertions remain; no
production admission change. The aggregate later reached Go's unchanged
ten-minute limit in a timer leaf that had run less than a second, not a ten-minute
case deadlock. Smaller managed root groups retain race/count3 and every original
case deadline; this aggregate receives no completed matrix credit.

## Eventless Producer Handoff And Qualification Fixtures

The Gate E producer accounting (5916752092, C07/C09/C20 and P19/P55/P59)
also covers feed-only run.start and public scenario import-to-execution.
Both now prepare the no-argument root under its exact selected run, commit
the complete eligible tree and route topology in the creating transaction,
and dispatch every acknowledged lifecycle/attachment result. No creating
business event, ordinary-handler construction, or implicit import permission.
Scenario setup applies explicit validated stage/fields/gates only after
independent constructor admission, before A's entry planning. The constructor
persists that root; a second field-only insert no longer rejects its canonical
bookkeeping. Other imported rows remain non-executable. The public API resolves
the exact selected runtime owner; the old raw Setup option is retired.
The new role is injected by selected-store composition, not discovered by the bus.

Development receipts (dirty source; not final-head qualification):
- Root scenario CLI, eight semantic numeric modes, API setup controls and native
  scenario import/transaction matrix: managed PASS, apiv1 4.863s, serveapp
  10.349s, runtimepersistence 3.857s. Both stores; no skips. The served scenario
  controls use a retained internal MockOnly lifecycle with public RPC, not a
  public private-launcher or paid-provider qualification.
- Native deployment creation COMMIT-loss/rollback, exact receipt reconciliation
  and no synthetic event: managed PASS 2.450s, both stores. Native scenario matrix
  includes cancellation, seed disagreement before mutation and exact replay.
- Bus acknowledged/unknown/cancelled/field-only controls: race/count3 PASS 1.062s.
- Corrected catalog concurrent root, query group-by, immutable duplicate outcome
  and static invocation checks: managed PASS 60.309s. Concurrent positive setup
  constructs the root before racing input; query declares a presence filter for
  the sparse root field. The duplicate oracle waits for its existing terminal
  boundary, keeping its full immutable snapshot assertion and original deadline.

Earlier reds remain: native scenario compilation refusal, followed by both-store
  duplicate-root bookkeeping refusal; public root setup RED on the same source.
  The corrected transaction owns the root once rather than weakening validation.
Native physical faults supplement, not replace, public constructor qualification.

The static invocation golden now consumes the canonically admitted artifact hash
and its two bundle-scoped static IDs; content bytes are unchanged. The node-ID
golden change is not merely formatting: the barrier's obsolete duplicated derived
transitions are removed and its canonical collector transition now contains the
declared join rather than a copied emit. All 25 node identities, raw handlers,
connects, event owners and other corpus projections are unchanged; ordered barrier
execution remains a separate mandatory proof. No arbitrary golden acceptance.

The exact persistence-authority census adds the named setup command/typed facade
and migrated native transaction operations (26 changed findings, five existing
bounded owners). Proof-plan backend/cut requirements cover both native producers.
Full frozen-head managed qualification, affected race supplements and CI remain
pending. No failure-class closure or final review request is made here.
