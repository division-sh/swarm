# Schedule Original-Reader Repair

Track: FULL. Bounded authorization:
https://github.com/division-sh/swarm/pull/2698#issuecomment-6225992056.
This is a staged repair receipt, not final-head or landing approval.

## Boundary

The finite affected-proof sweep at signed107f786b05 reproduced the original
schedule parity root failing on both stores: its local start closure omitted
ReceiverStateReader. Capture the existing native EventStore inspection reader
in the existing persistence hook and supply it after every ready start,
including restart. No new owner or pool, SQL, production behavior, assertion,
numeric payload, identity, schedule, timeout or backend-selection change.

Historical Before/After records and all old packets remain byte-for-byte.
One exact preservation-only inverse binds the complete source at signed
ff1ea5580af1ecf153490f5e71e9f2ea66da3616 to the three-line wiring change:
746c7c357dad92054381626d95617e18d186d61e0a0142c23b2893ba86b08e5f
to8de80abaf8df0ecae4fb4fcb7c7a1d3bdbcfd5b80e157084410562af09552119.
Copied bytes must match actual source before historical lookup. Foreign,
missing, changed, stale, reverse, repeated and extra-caller cases are refused.
The copied readback control now includes its original git custody; it still
mutates actual copied source and manifest and keeps both negative assertions.

The mirrored strict source verifier has one append-only collector edge:
f5da43ac97f3155bef4d51ac115b6500ffdcd4e8d50cbd3e9f09a20f08aba296
tod7f0674dc6481d92aef2af07f8a5eb3a349dddf5ec5c167f872e36a0cb7a3ec1.
Old edges are not retargeted. The live census reads actual current source;
historical reconstruction grants no debt transfer or deletion credit.
The exact historical commit is provisioned only for its actual consumers;
the source-derived inventory and depth-one fetch controls pass.

## Receipts

Raw streams are retained under
/home/youmew/.cache/swarm-2542-family145/.

| Stream | Result | SHA256 |
| --- | --- | --- |
| pr2698-gap-behavior-serveapp-other-serveapp.jsonl | Original schedule omission RED on both stores; other selected roots continued. | f2b1e325ec929480db953d1a5b706e453b6d2f7a02ba61a5f494525b6eb62dee |
| pr2698-schedule-original-reader-race3.jsonl | Both stores PASS in all three race iterations;55.735s package. | fb2bd951d2403fd06916c099cfdc28cd22645f58ebe688171745ff5dd8de438a |
| pr2698-schedule-accounting-controls.jsonl | Fingerprint, forward-only source policy, copied/warm/extra-call controls PASS;15.280s. | 48d0a2b56d37e1641af56cb88cd56963cb6a0fa1148a369f95aaba2ed28fcaf8 |
| pr2698-schedule-source-controls-repaired.jsonl | Complete readback source/history/warm-cache controls and repair source controls PASS;17.850s. | 5274fd4c5b57beb733ef05a94a7d5dce104f32005cbbaa387686736e82e60be1 |
| pr2698-schedule-history-provisioning.jsonl | Exact source-derived historical inputs and depth-one fetch controls PASS;0.300s. | f818f3e37c050469f1e9ae84b121e637cdb105f9b2553dc833e9e9ef93dd7be1 |
| pr2698-schedule-real-ratchet-refresh.jsonl | Real ratchet PASS;125.009s;0 removed,0 added. | ed9514e16612564eecfa65b08472e0454d8964f50aa59b01dd41c23674a1768d |

Vet passes for store, the source owner and serveapp. Baseline TSV and findings
are unchanged:3798 findings,2150 confirmed raw sites,66 unresolved excluded
occurrences. The4704-key a1 allowance is unchanged. No additional credit.
Only collector metadata changes, retaining the same baseline and predecessor
digests. Final read-only ratchet/preflight and final required runtime evidence
remain separate obligations.

The accepted clock repair is the parent. Current onboarding/reporter
uncertainties and the newly captured H2 RED remain explicit in the final
finite-ledger packet; this fixture repair does not dispose of them.

The complete retained410-root codemod selection reached409 PASS and one
source-pin RED in367.676s. That guard still compared the live schedule fixture
directly with the immutable historical recipe. Compose only this exact
file/declaration through the authenticated current-source inverse before the
same exact predicate comparison. No row or assertion is skipped, and the
immutable recipe remains unchanged. The affected guard then PASSes0.259s
(pr2698-schedule-delivery-pin-composed.jsonl); the original full-unit RED is
retained, not presented as a whole-unit PASS. Subsequent complete-unit evidence
belongs to its exact frozen source, not this prose addendum.
