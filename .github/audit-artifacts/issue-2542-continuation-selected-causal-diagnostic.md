# #2542 continuation: selected causal diagnostic storage

## Owner and Consumption

Governing boundary: selected_contracts.selected_runtime_store_projection.raw_sql_policy.
The existing eventrecord owner now owns fixed global outbox-payload event lineage
and cardinality; eventpersistence owns exact outbox projection bytes and pending
cardinality. The existing test_event_support adapter composes each pair in one
original selected native read transaction. Public storetest ports return detached
facts only, with no SQL, table/dialect selector, callback or reconstructed store.
The event import boundary remains unchanged; the adapter lives in its existing
closed fixture owner rather than adding an allowed import location.

Both remaining raw paths in selected_fork_causal_diagnostic_test.go consume these
owners: successful explicit/subject projection readback and missing-parent
event/outbox conservation after each of two actual projection refusals. The real
selected grant, producer lineage, lifecycle commit, pending-operation match,
activation, retry count and complete receipt assertions are preserved. The real
consumer disables h.db before execution, making accidental raw fallback fail.

All six original physical queries are compared verbatim after whitespace
normalization. Event reads retain global payload-outbox scope, every event name
and run, PostgreSQL casts and strict string scanning. No NULL default, event-name
restriction, run filter, pagination or duplicate collapse is introduced. Receipt
bytes remain exact; missing receipts and any late read failure discard the
complete witness. Pending and projected NULL receipts retain their distinction.
The coherent transaction intentionally strengthens the former independent cuts.

The existing run-scoped lifecycle diagnostic inventory is a different physical
witness: exact-run receipts plus global runtime-log rows. It does not replace
these all-event, exact-outbox causal observations. Its existing tests remain.
No parallel interpretation of runtime admission, acknowledgment or replay is
introduced; physical receipts are never grant authority.

## Manifestations and Proof

| Manifestation | Exact proof |
| --- | --- |
| Explicit and subject selected causal publication and replay | TestSelectedContractActivationAllowsCausalForkLocalRuntimeLogDiagnostic under race on both stores, twice projected with strict run/cause/receipt/count assertions |
| Missing causal parent | Same real root, missing variants on both stores, two actual projector refusals and unchanged zero-event/one-pending evidence |
| Foreign run, different diagnostic name and duplicate outbox payload | TestSelectedCausalDiagnosticStoragePreservesGlobalMultiplicityBothStores retains both physical causal rows and exact receipt bytes |
| NULL lineage, missing receipt, invalid/foreign/canceled/closed/unavailable owner and late outbox read failure | TestSelectedCausalDiagnosticStorageDiscardsFailedCutsBothStores plus missing-identity controls; no partial evidence; conservation still counts NULL-attribution rows |
| Callback, receipt assertions and physical SQL drift | Complete finite inverses, all-query equality, hostile owner/identity/multiplicity/lineage/receipt/refusal mutations and candidate type overlays |
| Post-rebase finalization semantics | ScatterGatherSafety ordered, duplicate_publication and held_finalization, actual SQLite/PostgreSQL under race at integration head f4df780b0 |

Initial adversarial setup failed schema checks before reaching its oracle; those
failed receipts are retained and receive no proof credit. Final probes use actual
persisted foreign-run identities and permitted physical diagnostic names. NULL
run corruption also clears source_event_id to preserve the schema's causal check.
No production/schema change, guard exception, timeout increase or assertion
weakening was used. The initial adapter import refusal was repaired by consuming
the existing closed event-support location; the guard itself is unchanged.

The parent failure class remains open under #2542/#2151. This cohort closes the
causal diagnostic observation family, not shared catalog construction, recovered
readiness faults, other migration debt or either global completion obligation.
Existing fixture-authority watchlist/gate remains sufficient. No new framework,
compatibility, tracker or authoritative runtime-spec change is required.
Exact counts and final receipts are recorded in the increment issue comment.
No core/full, hosted CI, fork-deadline or parent closure is claimed.
