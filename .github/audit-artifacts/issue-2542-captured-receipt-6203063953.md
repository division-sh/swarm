# Captured receipt closed-data repair

Track: FULL. Part of #2542; does not close #2542 or #2151.

Binding authorization: https://github.com/division-sh/swarm/issues/2542#issuecomment-6203063953.
Parent source: `12c8895925cfb500da50dd0408840ce1285ae5ca`, with pinned
master input `13c70dbca4f20867d757d93ef403aa2429274f3f`. No later master or
E capacity changes are included.

## Owner and complete consumer set

The changed concept is the opaque selected-coordinator fault receipt's captured
representation, not executable runtime authority. Retire the private
`capturedResourceReceiptTx` interface. `CapturedResourceReceipt` now retains
exactly one of five concrete private payload pointers. The durabledata owner
validates exactly-one before dispatching Install/Restore to the original private
methods. No `any`, function carrier, copied payload, registry or new SQL owner.

All five getter declarations, seven public effective methods, and both public
Install/Restore signatures are preserved. The three promoted receiver types
still retain the same embedded atomic payload. The runtime consumer source
`runtimepersistence/test_durable_receipt_fault.go` is byte-identical: its only
two consumption expressions remain the calls at lines 190 and 204. Selected
coordinator, mutex, cleanup-required-before-write, uncertain acknowledgement,
reverse cleanup and foreign/replaced receipt refusal remain with those owners.

| Payload / manifestation | Status | Exact proof |
| --- | --- | --- |
| Single corruption pointer and mutable images | execution-proven through the same corrected path | `TestCapturedResourceReceiptRetainsEveryPayloadPointer`; receipt and payload COMMIT-cut cells |
| Atomic pointer and three promoted receivers | execution-proven through the same corrected path | same pointer test, all promoted cases; clock COMMIT-cut cells |
| Import provenance removal | execution-proven through the same corrected path | provenance COMMIT-cut cells |
| Provenance read-arm addition | execution-proven through the same corrected path | added-provenance COMMIT-cut cells |
| Nonmutating head-history addition | execution-proven through the same corrected path | eight added head-history COMMIT-cut cells using a genuinely accepted native check |
| Seven raw effective-method findings | reproduced and fixed | original exact-head preflight RED; unchanged `TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority` alone GREEN |
| Nil/zero, typed-nil or ambiguous payload | execution-proven through the same corrected path | all 32 presence masks, five typed-nil captures and nil token in `TestCapturedResourceReceiptRejectsEveryAmbiguousPayload` |

The refusal tests cover cancellation, replacement identity and original SQLite
writer admission. No assertion, timeout, cleanup, raw-return hostile control,
embedding hostile control or production consumer is weakened.

## Finite source and registry correspondence

The unchanged registry generator produces exactly 11 removals and 10 additions:
seven raw effective methods and two private-interface method rows disappear;
two former interface dispatch rows become ten concrete owner dispatch rows.
Classify only those ten new calls as the existing `private-domain-adapter` role.
Keep the two original transaction-parameter rows and every unrelated row
unchanged. The unrelated-registry comparison is empty. No SQL occurrence is
deleted or hidden, and no scanner, role policy, source pin or accounting
exception is changed. No history record names this owner source, so no
historical re-pin is needed.

The remaining registry RED is exact retained unclassified debt: 4,813 unique
rows, with zero unknown/stale rows. Its sorted diagnostic set equals the
committed unclassified set, SHA-256
`5f5c9cf2ea361070e0bf3dba00f2029bf9205d7befcfe5ba793bf00fafc823cf`.
The prior 4,824-row RED is preserved. This correspondence is not a registry PASS
or an admission exception; that decision remains separate with the user.

The debt baseline is unchanged, SHA-256
`df5bd6ff2a246fe67d01dc564ea81d8cf603934ea980c55377ca5eda90cb0daa`.
Real ratchet PASS, 208.97s; native-family guard PASS, 20.40s. Actual census:
54,567 total findings, 38,393 raw-operation sites; debt 3,806 with 2,152 confirmed
raw-operation debt sites and 66 unresolved excluded occurrences, identical to
the parent's debt totals. Collector remains
`f6d712543a35e26917682ae29347e1eb5c293082f0ac6bd500d63a1418577a31`.
No inferred seven-site debt credit is claimed. The parent and the 194/155
qualification holds remain open, as do Q1 and the separate registry disposition.

## Focused receipts

Receipt prefix: `integration-2696-receipt-` in the accompanying bounded archive.
Archive: `issue-2542-captured-receipt-6203063953.receipts.tar.gz`, SHA-256
`1e25c14d34945f93c8901e28e5f854c3e73c6e1589dca74fa25a8a96fa1f734d`.
The four-file code/registry patch included with those receipts has SHA-256
`336a44e2c5e78ae6f149a8aedf1902d9db95d3fd4fcc76d2d9d4475bac07b44c`.

- `effective-guard.jsonl`: unchanged effective-method guard alone PASS, 9.534s.
- `payload-final-race.jsonl`: three owner roots / 51 named passes; all five payloads, three promoted receivers, mutable state and invalid masks.
- `native-race.jsonl`: SQLite's 24 COMMIT-cut cells, cancellation/replacement and original writer admission PASS. Aggregate RED is preserved: the initial shell lacked the explicit PostgreSQL DSN.
- `postgres-race.jsonl`: only the unexecuted PostgreSQL cells, two roots / 27 named passes, PASS 22.996s using B's existing isolated PostgreSQL service; no skips.
- `hostile-guards.jsonl`: five unchanged owner/raw-signature/embedding controls PASS, 17 named passes.
- `vet.log`: affected store, durabledata and runtimepersistence packages PASS.
- `payload-final-vet.log`: final typed-nil owner controls PASS.
- `cognitive.log` / `cyclomatic.log`: pinned upstream analyzers; touched-file maxima 28 / 15, below existing 30 thresholds.
- `registry-final.jsonl`: explicitly RED only for the exact retained 4,813-row set above; not claimed as normal qualification.
- `ratchet-native.jsonl`: real ratchet and native-family guard PASS, package 229.926s; no baseline refresh.

The four previously unstarted canonical preflight phases were independently
investigated at the parent: runtimepersistence 46 roots / 16.100s, testplanning
four / 11.266s, codemod two / 10.310s, and `go vet ./...` PASS. These are independent
receipts, not edits or completion attestations to the original incomplete
canonical receipt. Unaffected receipts carry; the affected owner controls above
are fresh. A fresh successful canonical preflight is still required before an
ordinary PR push. No ordinary push, hosted tier, waiver or forged receipt.

No platform runtime semantics or architectural contract is changed, so
`platform-spec.yaml` remains unchanged. Closure level: touched fixture seam
canonicalized; broader parent open. The existing #2542 ownership watchlist and
opaque-token feedback remain the tracking location; no new framework or issue
is proposed. The small fixed-data sum replaces the unnecessary raw-signature
interface rather than adding another semantic owner.
