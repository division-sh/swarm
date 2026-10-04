# Credential Admission Authority Registry Accounting

This accounts for the exact registry refresh in #2319. It is a capability-boundary
audit, not a substitute for selected-store or public lifecycle execution proofs.

The refresh adds 31 exact findings and removes 11 superseded findings. Each entry
retains its resolved type, enclosing function and operation ordinal. The scanner,
raw-authority dispositions and public effective-method restrictions are unchanged.

| Changed owner family | Classification and reason |
| --- | --- |
| `channelactivation.Owner` admission parameters and retained snapshot callback | Typed process-local. They validate an exact compiled publication or admitted lease; no SQL, transaction or generic persistence capability crosses the callback. |
| `RuntimeContextManager.AdmitChannelStandingTarget` lifecycle barrier | Typed process-local test observation. The existing `TestLifecycleBarrier` observes the admitted boundary; it cannot replace the canonical mutation or manufacture credential evidence. |
| Prepared activity HTTP admission and provider HTTP preflight | Typed process-local. These callbacks revalidate existing admitted authority before effects. Their only result is an error, not a raw persistence carrier. |
| Public-ingress selection callbacks and readiness observation | Typed process-local. Exact registration pairs are checked against canonical currentness; callbacks return a boolean/error, never a store, connection or transaction. |
| Delivery/native-setting settlement evidence scans | Private backend. Existing transactional guards read latest-attempt authority separately from immutable operation/first-attempt history; the two old scan shapes are removed. |
| Standing-service reconciliation, disablement and enabledness readers | Private backend. Two private transaction parameters, scoped reconciliation delegates and disablement calls remain inside the existing selected-store adapter. Reads and reset updates include exact binding enabledness. Eight replaced inline-delegate/scan/update shapes are removed. |
| Standing restart-disposition reader | Private backend. The existing exact run reader adds binding enabledness; its old scan shape is removed. |

No unknown runtime/facade raw authority is accepted. The registry, effective-method,
hostile resolved-type, transitive-carrier and unknown-local-operation tests must
pass after regeneration. Behavioral correctness remains independently covered by
the #2319 manifestation matrix and final-head qualification.
