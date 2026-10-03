# Pre-Implementation Coverage Audit Addendum: #2550 L04/L07

Agent: agent-g. This is a focused delta to gate5972816660, not a new issue or a claim of qualification. The other five families and the one-PR ceiling remain unchanged.

## Independent delta gate

Approved within the existing one-PR gate by reviewer-g:
https://github.com/division-sh/swarm/issues/2550#issuecomment-5974227335.
This supersedes the pending-request/freeze wording retained below as historical
pre-implementation context. No further lead ruling is required inside this boundary.
Current rows must authorize namespace observations; pre-ID creation requires exact
live service AND creator locks, while published identity requires constructor and
cidfile equality. Concurrent retirement requires exact absence, without recreating
authority. Both Provision cleanup and pre-start runner settlement must retain
independent errors. The seven added proof rows, real focused/planned consumers,
fresh four-service server2 full and hosted full remain required; no closure waiver.

## New execution evidence and tracker decision

Fresh server2 Local-Tier: full at bf695ea171c5b14af44501dadc6d4b4cb96f3dab used private services, native PostgreSQL16 explicitly, disk TMPDIR, capacity4, and an unchanged65-unit full plan. It failed after151.397s: delivery-continuation-full refused provision because another legitimate lease did not match its registry container. Four units passed; four failed/interrupted;57 were not started. All started workers joined. No managed containers remained afterward. The error was service provisioning, not an application delivery test. Preserve the failed aggregate; no full or speed claim.

Archive full-2550-failed-bf695.tar.gz SHA256 2caebcc259f64cb5bcde1ba04f89479beb023224e9804c1ee13bfd6abf705134 retains plan, workers, runner logs and canonical command evidence. The real full run used clean committed source. Counterexamples use an external Go overlay that modifies no tracked production/test files; the service owner has NOT been modified. Audit documentation was being prepared during the diagnostic probes; those probes are not reviewer-bound qualification receipts.

Two controlled same-owner counterexamples each reproduce3/3:
- An actively leased ServiceCreating row also holds its exact creator fence and has an exact labelled container but has not published ContainerID. Reconcile refuses it as a mismatch before inspecting the active lease.
- A valid actively leased ready row, with terminal cidfile identity and the creator fence released, is published by a second registry instance after registrySnapshot but before managedContainers. Reconcile refuses it as rowless because validation consumes the stale snapshot.

The existing service/creator/corruption/death controls pass under race. This exposes an omitted phase-consumption obligation in L04/L07; it is not permission to weaken namespace validation. Update #2550 and its existing harness watchlist; absorb this bounded owner repair here, subject to independent delta approval. No child issue, parent expansion, vendor, production-runtime change or new framework is proposed. The original six-family implementation is committed locally, but there is no review-ready PR or closure audit.

## Class and complete path

Category: qualification lifecycle/ownership parity. Exact concept: contemporaneous namespace evidence versus a live service construction/teardown phase. Chosen class remains independent local plan execution with exact existing service/descendant ownership. Immediate parent remains #2535/#1196 tooling/lifetime reliability; broader regression-health work remains #2353/#2407. The helper is an entry point, not the boundary.

Governing context: `platform-spec.yaml#test_specification.internal_catalog_conformance.qualification_tiers.execution` and `.local_completion` preserve independent processes, stores, services, admission, descendant joins and receipts. No exact section currently specifies the ServiceRegistry construction/publication namespace algorithm. The binding context for that seam is #2550 gate5972816660, #1196's existing ownership boundary, and the executable ServiceRecord/creator/service-lease/cidfile controls. The implementation spec delta, if approved, will make this phase-consumption obligation explicit alongside `.local_completion`, not replace the service owner or alter product/runtime authority. `IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md` were re-read.

Path: parent clean-source/cheap preflight -> bound unchanged plan/selected prerequisites -> unit subprocess -> RunAdmission.Acquire -> ServiceRegistry.Provision -> Reconcile namespace observation -> prepared/creator-starting/creating -> RunCreator exact Docker/cidfile handoff -> create-succeeded/starting/ready -> MarkChildRunning/inherited lease -> unit execution -> Service.Close/absence/row retirement -> RunLease.Join/Complete -> command and aggregate evidence.

Every phase of private service observation is same chosen class. Explicit shared-DSN database capacity, native private cluster lifecycle, application runtime ownership and hosted scheduling remain separately owned adjacent concepts; they do not use this Docker namespace interpreter. No shared server is resized or stopped.

## Exhaustive owner consumption

| Canonical owner | Consumers / proposed disposition |
|---|---|
| ServiceRegistry namespace and durable ServiceRecord phases | Reconcile registrySnapshot/managedContainers/validateManagedNamespace/reconcileOrphanAuthority/reconcileRecord; Provision initial reconciliation and record writes; RunCreator terminal handoff; Service.Close exact teardown/record removal. These must agree on live versus abandoned phase and current identity. |
| Creator fence/cidfile/exact inspected container | cmd/swarm-test internal-create -> runCreator -> RunCreator; creatorProcessCommand and inherited file-lock handoff. Retain the existing exact labels/name/image/spec/cidfile result; no reader invents a container ID. |
| Exact service lease and descendant possession | Provision acquisition, Service.InheritLeaseTo, MarkChildRunning, Close and Reconcile per-lease admission. Existing creator-death, surviving-descendant and corrupt/no-row controls stay authoritative. |
| RunAdmission / planned runner / receipts | runTestArgs is the only non-test Provision caller, consumed by both focused and planned workers. Parent remains slot-free; per-worker stores/services/leases remain independent; first failure cancels and joins siblings without credit. |

No second private Docker creator or cleanup owner was found. testpostgres.Manager manages per-test database clones, not this service-container namespace. Native capacity probes and serve outage helpers remain different process owners with their own required proofs.

Old invalid interpretation: a pre-enumeration registry snapshot is authoritative for every later observed container, including another actively owned intermediate phase. Still valid: unknown namespace, foreign identity, duplicate resources, abandoned ambiguous creation, mismatched terminal cidfile, and unsafe cleanup must refuse and leave evidence/resources untouched.

## Bounded design amendment requested

Prefer a phase-aware, contemporaneous namespace observation through existing ServiceRegistry, ServiceRecord, service/creator fences and exact container identity. A live lease is not blanket success: require the appropriate construction authority and exact constructor labels/name/image/spec, distinguish live construction from orphaned creator state, and revalidate current rows/remote evidence at the existing owned decision boundary. Do not silently skip foreign/duplicate resources, synthesize terminal IDs, suppress errors, retry to green, reduce full membership, or lower capacity as a hidden workaround.

A global constructor/recovery framework, a second registry or broad scheduler is outside scope. If preserving current safety requires such an owner, stop for a different ruling. The authoritative spec plan is to bind parallel service observation to existing live/terminal phase authority, without changing product runtime contracts.

## Additional proof rows (within L04/L07)

| Row | Manifestation | Planned execution proof |
|---|---|---|
| L04a | live creating before exact ID publication | Existing failing overlay, active service AND creator authority, held publication cut, exact no-removal assertion; same cut with foreign label/name/image/spec and duplicate candidate refuses. |
| L04b | legitimate row published after namespace snapshot | Deterministic fake Docker barrier + second registry instance; read current row/authority and preserve exact service. Existing failing overlay reproduced3/3. |
| L04c | creator terminal publication during observation | Hold/read/publish cuts across creating/create-succeeded/ready; abandoned ambiguous creator and malformed cidfile still refuse. |
| L04d | concurrent teardown/absence/row retirement | Exact deletion and registry/remote observation cuts; acknowledged absence is distinct from unknown row or foreign surviving container. |
| L04e | no live owner / foreign namespace | Keep current no-row, duplicate, foreign identity, corrupt phase and orphan/in-flight creator-death refusals; no skipped namespace family. |
| L07a | independent unit/provision/cleanup failures | Preserve primary AND unrelated cleanup errors, failed/not-started receipts, exact service retirement and no premature slot release. |
| L14a | actual four-unit private-service completion | Fresh unchanged server2 full plan on the repair head, independent owned services, native proof, all65 units including both900s soaks; no transferred waiver or deadline/assertion changes. |

Intended closure remains elimination of the six bounded tooling classes, not parent/fleet/cadence closure. Feasibility: the owner already persists all required phases; one bounded amendment is plausible, but current safety preservation must be independently checked before editing it. The remaining parent2-4 acceptance groups are unchanged; this adds one in-PR service-phase repair, not a new parent child stream.

Architecture feedback: namespace observation is using final-state identity rules before a contemporaneous lifecycle admission. Better direction is consume existing typed phase/authority at the observation boundary, not add another owner. Track here and in harness_reliability_and_local_smoke; estimated repair/proof effort1-2 days, moderate confidence, high ROI because any explicit concurrent private invocation can encounter it.

## Other verified progress (not closure)

All45 compiled describe cells x2 passed after integrating source-identity retirement; human labels remain exact and JSON hash is independently admitted. Policy-only complexity independently measures master a2d84f049 against bf695ea17: cyclomatic263->263, cognitive569->568 at threshold30. Registry/secondary guards and static content/ID controls pass. Compiled signal/death controls pass; both green and red historical full-evidence observations retain N/A with no qualification credit. A red workflow whose Go roots all pass is not a discovered runtime regression.

Request one focused independent delta gate for the L04/L07 owner amendment before service-lifetime edits. Full qualification, hosted full, final88-row proof audit and review remain outstanding.

## Qualification delta within the approved boundary

The fresh four-worker server2 run at 7dde578f388d7b430ebadd7d5f8bc1288ba1cf9e
failed at the retirement census: the policy-only complexity file no longer contains
retired action names, but the current-only corpus still classified it as generated
measurements. Remove only that stale current-only row; preserve all 163 historical
rows and strict unknown/stale refusal. The aggregate has 17 passed, four failed or
interrupted, and 44 not-started units; it earns no complete qualification credit.
Every started worker joined and no managed service container remained.

Hosted run 37161729285 also identified three new runner launches missing from the
async-site ledger. Classify the parent relay, bounded worker pool and subprocess
relay explicitly. The subprocess relay must actually stop and join before return,
not merely receive a stop notification. The existing bounded-worker root now has a
held-relay negative control; existing signal/death and hostile inventory controls
remain required. This is L05/L07/Q02 consumption proof, not a new lifetime owner.

The real Docker focused test requested two slots on a hosted machine whose admitted
ceiling is one. The test now proves exact fail-closed refusal before any service or
admission authority is created when the requested capacity exceeds the actual host
ceiling. It does not claim that refusal earns two-service execution credit. Both
actual one-slot FIFO and two-slot independent-service execution remain mandatory on
server2, alongside an over-ceiling negative control. Its early-exit observer also
retains the joined process result for failure cleanup, avoiding a second unbounded
wait after evidence has already been consumed. No cap, service isolation, deadline,
unit membership or application-runtime behavior is relaxed. Fresh clean-source full
qualification on server2 and hosted full remain required after these corrections.

L03 also includes the automated review's effective-cgroup counterexample: fixed
mount-root reads can miss nested process/ancestor restrictions. Resolve the current
unified membership and complete visible mount, apply every visible ancestor CPU and
memory limit, and conservatively admit one for missing, partial, ambiguous, v1 or
hybrid evidence. No cgroup modification, new resource owner, hidden capacity override
or broad compatibility parser is authorized. The existing L03 root adds controlled
leaf, parent, root, malformed, missing, unreadable and unsupported-layout proofs.
The 96f qualification was explicitly interrupted and joined for this correction;
its partial passing units cannot qualify the replacement head.

Retained counterexample source and raw red log: `docs/audits/2550-service-namespace-probe_test.go.txt` and `docs/audits/2550-service-namespace-probe.log` in swarm-docs. These are diagnostic overlay evidence, not checked-in executable roots or green repair receipts. The current source-code service owner remains byte-identical to master.
