# Receiver And Generation Authority Registry Accounting

This records the exact finding refresh following Q1/Q2 and E's C1/C2 integration
at `5f487bda4`, with the subsequent test-only `afb20f2b2` checkpoint. It is not
a class-closure claim or a replacement for execution proofs.

The compiler-resolved registry changed by 149 added and 63 removed exact
findings. Each entry retains its full resolved type, enclosing function and
operation ordinal; no directory or whole-file exemption was added to the guard.

| Changed owner family | Classification and reason |
| --- | --- |
| `backend/agentpersistence` execution authority, lifecycle and receiver execution | Private backend. Shared generation fence and exact transactional admission replace the narrower retained-only helper; receiver state and grant reads remain inside store adapters. |
| `backend/authoractivity`, `backend/generationauthority`, `startupownership` | Backend mutation-order delegate and private startup domain adapter. Pooled and retained grant mutations share the existing selected-store order before domain/grant locks. No raw capability is exported through the facade. |
| `backend/delivery` adapter, lifecycle, receiver materialization | Private backend. Claim, continuation, successful/failed settlement and dependent terminalization consume exact materialization/readiness evidence transactionally. SQL publication enumeration is separately guarded at its exact function/callsite. |
| `backend/pipelinepersistence` receiver materialization and workflow target reads | Private backend. Facade and transactional receiver checks now delegate to the same one-statement state/companion snapshot. Separate dialect statements are mutually exclusive returns; hostile extra-query and split-read tests remain required. |
| `backend/pipelinepersistence` selected target enumeration | Private backend. The existing exact receiver inventory now reads lifecycle availability as evidence; it does not introduce business-key selection. |
| `backend/fanoutorigin`, event record and event persistence consumers, fan-out writer | Private backend. Exact inherited-origin lineage and committed ordinal validation move into shared transaction-compatible owners; causal parents remain same-run. Complete event codecs remain private. |
| Removed pipeline selector reader methods and facade forwarders | Remove stale typed-process-local and typed-public-facade entries. Handler receiver election and its SQL queries no longer exist; generic query APIs and the exact-scope state owner remain. |

No unknown raw runtime/facade authority was accepted. The finding registry,
effective-method no-raw-authority proof and hostile resolved-type proof must all
pass after regeneration. Their success accounts for the capability boundary,
not correctness of argument provenance or integration behavior. Full-suite and
manifestation-level proof obligations remain independent.
