# Additive C10/C11 Header-Consumption Accounting

## Subsequent Executed Coverage

The ordinary source matrix is expanded to eight manifestations on each store:
absent imported source, zero loop generations, constructed fieldless source,
missing header, corrupt accumulator, wrong header flow, wrong header type and
an original-declaration loop generation. Preparation compares the complete
source/child/application snapshot before and after every row. The fieldless
fixture proves one root header and zero root entity_state rows before loading;
its selected artifact preserves the fieldless contract instead of replacing it
with a fielded contract. The exact original admitted source supplies its node
routing fact. Both backend leaf sets are mandatory in the proof plan and the
partition guard.

Actual receipt: /tmp/agent-e-2496-header-consumption-expanded-correct-source.log,
aggregate RED22.687s. Absence, zero and original-loop controls pass on both
stores. Fieldless source fails at the entity_state-only loop lookup; missing,
corrupt and wrong-coordinate headers are ignored on both stores. Every
completed preparation preserves the full snapshot. The earlier fieldless
setup failure used a fielded replacement and is retained separately in
/tmp/agent-e-2496-header-consumption-expanded.log; it is not credited as the
fieldless reader reproduction. A focused corrected SQLite fieldless receipt
is RED0.681s at the same entity_state-only lookup.

The activity projection fixture now constructs its source through O2 at
9ab42140a. Root construction includes its declared keyless child and the fixture
requires both entities; static producer construction requires one. The actual
rehomed-child rejection control passes on both stores3.727s. This does not
credit the four uncorrected header readers or their broader sibling families.

Implementation checkpoint, not closure or another full pre-audit. Gate E
5916752092 remains binding. Measured committed head: f2fe842b9, including merged
A/#2515 at 8466039d008a9daa1a948e8c00dd48d732a2650f. The working test delta uses
the existing O2 constructor, preserves initial-entry evidence, and does not edit
production readers.

## Reproduced Omission

`TestSelectedContractOrdinarySourceStatePresenceBothStores` now creates its source
through `PrepareFlowInstanceActivation` and `CommitFlowInstanceActivation`, not
the retired state-only writer. Missing and corrupt child `flow_instances`
accumulators are ignored on both stores. The unchanged absent/zero/loop cases
and all four uncontended activation controls pass. Receipt:
`/tmp/agent-e-2496-historical-constructor-controls-v2.log` (8.141s, aggregate RED).
This is selected execution's consumption of constructed state under C10/C11,
not permission to relax historical replay admission.

`runforkpersistence.loadRunForkEntityActivations` still reads only
`entity_state.accumulator`. It neither consumes the exact constructed header nor
supports a complete fieldless header without a field row. Its four consumers:

| Consumer | Disposition | Required execution proof |
| --- | --- | --- |
| prepareRunForkSelectedContractSourceEvent | Move to strict constructed-header consumption in C11 | Ordinary source absent/zero/missing/corrupt/loop matrix on both stores; add fieldless and wrong-header-coordinate controls; rejected preparation preserves the complete source/child snapshot. |
| selectedContractActivityLineage (run_fork_activity_lineage.go:129) | Move with the same bounded selected reader | Exact request/result/diagnostic lineage, malformed/unrelated request refusal, loop generation match and wrong child header on both stores. |
| materializeRunForkProposedEffectCards (run_fork_gate_activation.go:192) | Move with the same bounded selected reader | Existing gate snapshot/remint and missing/corrupt/wrong-child refusal, without additional effect execution. |
| projectRunForkFanOutCapsule (run_fork_fan_out_generation.go:133) | Move with the same bounded selected reader | Fixed original declaration/child generation and fan-out projection controls, missing/corrupt/wrong-child refusal; preserve #642 refusal. |

The proposed owner is the existing selected-store run-fork header projection,
bound to exact run/entity/route/type and fixed-revision construction evidence.
Field-row presence remains required iff fields are declared. No absent-to-empty
coercion, companion repair, fallback read or additional execution grant is allowed.
Existing loop/gate/fan-out semantic decoders retain ownership of their values.

## Parent Sibling Probe / Decision Request

Searching beyond the failing helper finds five further reader families that
still consume accumulator/lifecycle data from entity_state:

- activityjournal.loadLoopState: activity generation eligibility and result
  admission, including optional entity fields;
- pipelinepersistence.requireWorkflowJoinAdmissionTx: A's stage-entry/arm
  publication fence;
- pipelinepersistence.fanOutBarrierGenerationCurrent: existing barrier generation
  admission, before advancement/settlement;
- decisioncard.summarizeGates: public diagnostic projection;
- decisionpersistence.supersedeRunGateActivations: terminal run gate mutation.

These are not constructor producers and do not justify expanding R5.1 into a
replacement loop/join/decision framework. They are also not proven harmless by
their different package names. A's join ownership must remain intact. Their
exact fieldless behavior and read/write authority require an explicit disposition
before claiming complete systematic header consumption. Parent #2411/#2250
remain open. Proposed tracking: refine their existing owner nodes, no new issue;
please confirm whether the bounded four selected-reader consumers are ordinary
C10/C11 migration and which additional header-consumption integrations belong in
this landing. No production SQL change to these families has been made.

The chosen construction/attachment class and P01-P60 commitments are not narrowed.
This repairs an omitted consumer census, not evidence of closure. P11's separately
posted admission-response-loss contract request (5941372820) remains pending;
other approved API, fixture, validation and qualification work continues.

## Actual Qualification Receipts

Frozen a82888074, managed supported-surface supplement, no race flag or deadline
changes: both-store progressive-presence public serve/restart PASS; unchanged
sequential-run/public-readback PASS (56.18s); unchanged 1362 fan-out/settlement/
public readback/hash-only restart PASS on SQLite and PostgreSQL (200.94s total,
PostgreSQL 81.56s). This is not final-head or full-suite qualification.

Default managed suite at a82888074 remains RED on five CLI roots. Targeted
repairs now committed at f2fe842b9: retired create_entity grammar refusal through
public verify; in-memory runtime refusal; A's merged stage-graph oracle; restored
describe whitespace normalization. Writer handoff/terminal race controls pass on
both stores under race x3 (62.719s), without weakening rollback or terminal truth.
Default qualification of f2fe842b9 is running. No PR or GREEN closure claim.

## Authorized R2 Cutover, Current WIP

The historical requests above were answered by the independent WIP ruling
5944826849 and issue authorization5944833985. All four selected and five sibling
families below have migrated. This is implementation status, not automatic
execution credit or an exhaustive final closure claim.

| Consumer | Current canonical consumption and exact proof obligation |
| --- | --- |
| prepareRunForkSelectedContractSourceEvent | Exact fixed-revision declaration/child correspondence, header route/type and iff-declared fields; ordinary-source presence and rehomed-child matrices, unchanged rejected snapshots. |
| selectedContractActivityLineage | Exact header owner under selected source, retaining fresh-request parent execution and malformed/unrelated lineage refusal; reminted write/read-only activity and source-projection matrices. |
| materializeRunForkProposedEffectCards | Exact child header activations before fresh card/continuation creation; native proposed-effect remint and gate refusal controls, no added effect execution. |
| projectRunForkFanOutCapsule | Exact header generation for the admitted original-role/child relation, retaining #642 pending-work refusal; fixed-revision fan-out materialization and origin controls. |
| activityjournal.loadLoopState | Header gates/loop buckets and only declared business fields; exact FlowID binding and canonical decoder retained. The28-leaf native activity matrix passed race3, including fieldless/current/stale/missing/wrong-flow controls. |
| requireWorkflowJoinAdmissionTx | Constructor lock plus exact header route/template and stage-entry/arm MatchCurrent; header-input integration only, A's model unchanged. Native fieldless entry/arm matrix passed race3; composed arm/arrival and initial activation tests remain in the queued supplement. |
| fanOutBarrierGenerationCurrent | Exact registration route/template/header generation, optional fields, existing fan-out/loop decoder. Current/stale/supersession/winner controls are named in the queued supplement, not credited by the shared helper. |
| decisioncard.summarizeGates | Header accumulator, exact declared FlowID, strict gate decoding; open/committed/superseded/malformed projection controls passed race3. Native constructed gate-freeze/summary passed race3; real run-completion controls remain required. |
| supersedeRunGateActivations | Header selection and revision-CAS writer, exact template, existing journal; no obsolete field-row gate writer. Native fieldless/field-bearing freeze and actual generic/selected terminal contention passed race3 with exact journal/readback/summary assertions. Other terminal producer controls remain required. |

The shared workflowheader.LoadForMutation projection is transaction-local and
strict, not a semantic decoder. PostgreSQL consumers pass their adapter dialect
so it locks the exact header and declared field row. An absent fieldless row is
the declaration's valid shape, never an empty-row repair. Header mismatch,
duplicate field ownership and malformed persisted authority fail closed.

The post-cutover direct read/write census also found generic historical replay
receiver verification and both generic activation inventories using entity_state
as executable owner evidence. These surviving same-concept consumers are
unchanged and explicitly escalated at5946306433, pending absorb/split disposition.
Import/scenario state-only rows, optional business-field collections, immutable
historical field snapshots and deliberate hostile test rows remain separate
contracts; they do not acquire executable header authority. This census does
not claim owner-complete closure while the generic readers survive.
