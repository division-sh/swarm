# Cohort 131: typed capture-only logger collaborators

Binding ruling: reviewer-b comment6070596271, implementing the capture-only
portion of request6070493603. The native persistence migration and authoritative
spec clarification follow in the same long-lived branch; this is not closure of
the mixed logger family or parent.

Thirteen capture-only roots (three direct constructions and ten of the original
17 helper callers) use runtimeLogPersistenceCapture directly as a typed
RuntimeLogPersistence collaborator. It stores supplied record facts verbatim,
records exact lineage arguments and supports a typed lineage/persistence error.
It never opens storage, consults persisted subjects, creates runs, synchronizes
counters or implies durable success. The shared logger construction helper now
accepts RuntimeLogPersistence instead of a raw database plus mixed stub.

Remove unused sqlmock connections and empty SQL-expectation checks from the ten
units. Those checks had no query or write expectations and did not prove a
persistence gate. Preserve every payload/normalization/envelope/flight-recorder
assertion and injected error identity. The one real mock query expectation was
the lineage failure unit; replace it with exact typed method arguments and zero
record assertions while preserving the same error and no-recorder assertions.
Payload refusal also asserts zero records. A direct control retains exact
EventID, historical CreatedAt, run, parent, payload and mode and refuses to infer
durable lineage from a subject UUID.

The mixed stub no longer has a capture field or capture branch. Seven original
persisted-log helper callers keep an explicit persistence handoff temporarily;
their raw implementation, seven recovery consumers and seed/story/counter helpers
remain enumerated native-migration debt. Do not credit these units as missing-run,
terminal preservation, lineage, transaction or durable-replay proof. The named
RunOwnerFailure/PostAppendOwnerFailure units remain simulated errors, not native
post-append transaction controls. Native failure cuts must be proven separately.

Finite whole-function recipes cover all 13 units, seven unchanged persistence
handoffs and the one typed constructor. Their independent oracle permits only
this collaborator/setup correction and added refusal evidence; original
meaningful assertions and workloads are retained. No production contract,
compatibility behavior, retry, timeout/budget or tier change in this cohort.
