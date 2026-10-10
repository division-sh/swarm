# Catalog Latest Platform Pipeline Receipt

Routine exact-storage read migration under #2151/#2542. All three catalog
interpreters of the latest platform pipeline receipt now consume one named
read in the existing pipeline owner through the original selected coordinator:
assertTriggerReceipt, assertHandlerOutcomeForEntity and loadCatalogReceipt.
No generic SQL selector, callback, reconstructed owner or public raw handle.

Selection remains exact event ID, platform subscriber, pipeline or pipeline:
prefix, processed_at DESC and LIMIT 1. The witness retains subscriber,
outcome, side effects and raw failure bytes. SQL NULL remains distinct from
stored JSON null: the former is absent in the witness; callers retain their
original null/default formatting. No field-level failure policy is introduced.
Missing rows remain no evidence, and errors are never converted to absence.

| Manifestation | Exact proof |
| --- | --- |
| Trigger outcome/class/detail/attributes | Whole-body inverse oracle preserves every assertion after the reviewed native prelude; actual conflicting-duplicate creation passes on both stores under race. |
| Handler success assertion | Same oracle preserves success-only authority and every failure assertion; actual canonical configured/duplicate creation and the native receipt control pass both stores. |
| Stored receipt replay | Same oracle preserves trimming, canonical side effects, typed failure decode, NULL exclusions and errors. Native control invokes loadCatalogReceipt on a real published event and compares subscriber/outcome with native readback. |
| Exact native read lifetime | Canonical real publication, joined runtime shutdown, one original read commit/zero writes/no active transaction; missing, canceled, closed and foreign readers return no evidence. |
| Wrong owner/event, lost refusal, altered failure attributes or replay bytes | Hostile whole-body controls reject each mutation; independent fixed owner projection checks exact dialect predicate/order/limit and SQL NULL distinction. |

Failure-only handler_error event-log diagnostics are a distinct eventrecord
observation and remain explicitly tracked raw debt. Their query and failure
message are unchanged, not claimed canonicalized by the receipt migration.
Catalog raw opening/manual dependency assembly also remain tracked.

Finite inventory: 278 recipes. Qualification before push includes full finite/
candidate/type/hostile controls, 78 guards, decreasing census/registry with all
16 native-family children, contracts, native unused, inert replay and exact
committed-head complexity. Vemew receipts:
/home/youmew/.cache/swarm-2542-local-20261008/catalog-pipeline-receipt-*.
Initial controls caught a missing import and a test input not marked for the
existing published-ID set; fixed without relaxing the exact-root assertion.
No new runtime policy, spec change, collector permission, abstraction,
compatibility, retry, skip or timeout increase. Existing watchlist mapping
suffices; parent zero debt, strict guards, fork deadline and integrated proof
remain open.

Measured debt: 13,945 findings / 10,269 raw sites becomes 13,937 / 10,261:
eight findings / eight sites removed, zero added. Five exact private read/
scan/coordinator occurrences are source-classified fixture-2151 within the
unchanged policy. Explicit byte allocation preserves even empty-present
storage distinctly from SQL NULL; final qualification uses that exact copy.
Final selected sweep: 267 passing roots, zero failures/skips; native unused,
inert replay and exact committed-head complexity must finish before push.
