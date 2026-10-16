# Issue 2700: Custom Native Search Request Budget

Part of #2700. Track: FULL. Reviewer-c gate: 6212062423.
Base: `56c8969ea64d2bc0b90f80eff47bce676bf0a26c` (#2708 landed).
This is N1 only, not whole-issue closure. Local qualification is focused.

## Existing Owner And Bounded Delta

`ToolHTTPExecution.Prepare` already returns the custom provider's admitted
duration, including the omitted-value default of 30 seconds. Previously,
`executeCustomWebSearch` discarded that duration and the shared client's
independent 30-second cap controlled the request instead.

The custom caller now passes that duration to `doNormalizedSearch`. Its three
hardcoded-provider callers explicitly pass zero, retaining their original
client behavior. After the existing rate-admission boundary, custom requests
derive a bounded context from the request's existing context and shallow-copy
the original client, clearing only that copy's `Timeout`. Headers, body,
`GetBody`, transport, redirect policy and cookie jar remain intact. Earlier
caller cancellation/deadlines win. The original authority context continues
to own authorization, launch and settlement, independently of request expiry.

The one physical primitive remains
`internal/runtime/tools/executor_native.go:doNormalizedSearch:http_do:1`.
Normalization, credentials, policy provenance, effect classification,
acknowledgment, fences, uncertainty and no-replay behavior are unchanged.
There is no new effect owner, retry, table, public API, production test hook,
SQL test site, source grammar, timeout allowance or baseline increase.

The spec adds only a cross-reference in the existing platform-executed custom
web-search configuration section: prepared headers/body budget after rate
admission, default 30 seconds rather than a maximum, earlier cancellation
wins. Hardcoded providers and CLI-native search remain outside that budget.

## RED Evidence

Receipt directory: `~/.local/state/agent-a-2700/`. All original results stay
retained. `native-timeout-baseline-red.log` ran normal managed
`Execute("web_search")` against unchanged production at the base:

| Cell | Baseline result |
| --- | --- |
| N01 explicit 1s / 10s / 120s | RED: all three outgoing requests carried the client's approximately 30s deadline. This first component fixture used the existing Go-field-name policy serializer; canonical authored spelling has its separate counterexample below. Omitted/default 30s passed separately. Exact context-creation interval oracle, not an elapsed-performance ceiling. |
| N02 authored 1s / real 2s response | RED: succeeded after 2.002334478s, incorrectly returning the normalized result. One request. |
| N03 authored 120s / real 45s response | RED: failed after 30.001726539s with exact native transport uncertainty; one request. The server's original bounded handler joined at 45s; package 47.034s. |

## Execution Proof Map

The decisive roots use ordinary Execute, actual actor native-capability
admission, custom policy resolution and preparation. Managed-harness controls
and real native-store evidence are distinct, not interchangeable.

| Cell | Active roots / required accounting |
| --- | --- |
| N01 | `TestNativeWebSearchCustomTimeoutDeadlineIdentity`: explicit 1/10/120s and omitted 30s, exact outgoing deadline interval, prepared header/body/GetBody, normalized result, single attempt/settlement, replay refusal. Controlled 50ms substitution must be rejected. |
| N02 | `TestNativeWebSearchCustomTimeoutShorterThanSharedCap`: actual 2s response, declared 1s, exact native transport uncertainty plus DeadlineExceeded, one request/attempt/settlement, joined handler, no redispatch. |
| N03 | `TestNativeWebSearchCustomTimeoutBeyondSharedCap`: real 120s/45s baseline/fixed pair, count1 only, exact normalized success, one attempt/settlement and no replay. |
| N04 | `...ResponseBody`, `...CallerCancellation`, `...EarlierCallerDeadline`: body beyond shorter injected cap, stalled body, explicit cancellation at transport/read, exact earlier deadline. Native uncertainty retains its own transport/read stages and causes. Fast race x3. |
| N05 | `...OverlappingCallsPreserveClient`: two admitted custom budgets concurrently share the original client, actual 307 body replay/cookies/redirect policy, independent cancellation, exact original pointer and settings during/after, one attempt/settlement per occurrence. Race x3. |
| N06 | `...AdmissionBoundary`: normal first and second requests, original shared provider bucket, wait precedes budget; timed-out admission has no second dispatch/attempt. Keep existing inherited-policy/actor/flow controls. |
| N07 | `...PreservesBuiltins`: Brave/Serper/Tavily normal Execute retains original 30s client, mappings and credential-client pointer. Preserve existing authored HTTP, token, capability and policy tests. |
| N08 | `...SettlementBothStores`: actual selected SQLite/PostgreSQL effect owners for success, deadline uncertainty, prelaunch cancellation, native stale-launch refusal, acknowledged cleanup and unacknowledged settlement. Exact durable operation/attempt state/count and no redispatch. Native acknowledged-cleanup errors remain visible, not silently converted into the authored HTTP response behavior. Existing authored both-store acknowledgment proof stays separate. |

Runtime GREEN receipts, the mutation-control result, exact-head static checks
and preflight are recorded with the RED-to-GREEN and FINAL HEAD checkpoints;
this table alone does not grant execution credit.

### Canonical Policy Decode Gap Found During N08

The first component suite passed, but both real-store timeout cells took 30s
despite the canonical `timeout_seconds: 1` policy. Inspection found that the
existing native policy resolver JSON-decodes into the YAML-tagged HTTPToolSpec;
the underscore-spelled timeout is silently discarded before Prepare. The
older rate-limit fixture serializes Go field names instead, so its component
proofs cannot discharge authored-policy fidelity. Those original results stay
retained, not promoted to whole-N1 GREEN credit.

The decisive fixtures now use canonical policy keys, and N08's timeout cell
returns a real response after 2s so a dropped 1s budget cannot satisfy it merely
by eventually timing out at 30s. `native-canonical-policy-deadline-red.log`
records explicit 1s/10s/120s all RED with a 30s outgoing deadline (0.019s);
omitted/default 30s remains PASS. A bounded existing-contract admission seam
was needed before the final authored-policy proofs could turn GREEN. The
existing native policy decoder now gives that one authored spelling a typed
integer projection into the same HTTPToolSpec and its unchanged admission
owner. No shared JSON tag, persisted encoding, historical packet, replacement
parser or additional public API changes. The existing field-name decoding is
not redefined, and the default prepared duration remains 30s.

The strengthened N08 counterexample is separately retained in
`native-canonical-policy-stores-red.log`: SQLite returned success after
2.053371018s, PostgreSQL after 2.014271304s, despite the declared 1s budget.
After the projection repair, `native-canonical-policy-fixed.log` passes all
four exact-deadline cases and all twelve native-store cells (package 5.566s).
The timeout cells fail correctly after 1.011779676s / 1.007494693s, retaining
their exact native transport uncertainty and durable terminal evidence.

## Focused Receipts

| Receipt | Result / scope |
| --- | --- |
| `native-50ms-negative-red.log` | Expected RED, 0.016s: replacing only the custom 10s request budget with 50ms fails exactly that deadline-identity cell; 1s/120s/default controls pass. Diagnostic mutation removed. |
| `native-http-timeout-fast-race3.log` | Fifteen unchanged native/HTTP fast roots pass all three repetitions, 105 PASS records. Package RED 13.811s solely on the newly added invalid-budget test expecting the internal config error at the public Execute boundary. Preserve this RED; it is not a whole-package pass. |
| `native-invalid-budget-phase-race3.log` | Corrected phase oracle PASS, 1.044s, race x3: direct config admission must retain the exact negative-duration refusal AND normal Execute must retain its exact unsupported-tool capability refusal, with zero physical requests/attempts/settlements. No production or other test change after the preceding run. |
| `native-preserved-store-source-controls-race1.log` | PASS: tools 28.433s, effects 9.163s. Native selected-store twelve-cell matrix under race; authored both-store acknowledged cleanup remains separately proved. Existing rate/provenance, credentials, launch/cancellation/identity, CLI/fallback capability controls and four exact primitive/source guards pass. |
| `native-canonical-slow-fixed.log` | Canonical authored-key 120s/45s journey PASS, count1/non-race, 45.045128301s request / 45.066s package. Exact normalized result, one physical request/attempt/settlement, replay refused. The earlier component pair retains separate credit; this validates the corrected policy ingress without another baseline repetition. |

| Retained file | SHA-256 |
| --- | --- |
| `native-timeout-baseline-red.log` | `9b7825186597186de130ba2bd92e05fb85188e1c823416051d3eb627375848d1` |
| `native-timeout-fixed-first.log` (component/weak initial N08, not authored-budget credit) | `670f4ef036597a178409cdf7c9bf7073e4690825005ade0dd5c0c58d55f2fcaf` |
| `native-canonical-policy-deadline-red.log` | `05c8deace68c4762270b10fa802e124a8b81711572f5658d65f5f9328e499b3c` |
| `native-canonical-policy-stores-red.log` | `93e03e840ae611ab80ea39c39c3ba4e57ff6c31e69b383abfe4ce8a4d087236b` |
| `native-canonical-policy-fixed.log` | `ded5139c0dc69b20d55789740d6f8e83d7dac7f6d1a31e8053d812f1ac774551` |
| `native-50ms-negative-red.log` | `c6d9bc21a9bc9e905f023504915d1d2d9cd35eb228eb2e617743011c329b54e7` |
| `native-http-timeout-fast-race3.log` | `36c9d90ad2f178a7bb75db48466661eb8f4acb082f12321afa5698852bbc1b83` |
| `native-invalid-budget-phase-race3.log` | `0609fa6ff909948ebba862d82dd4fd8cc6d78402cb8d77818f571106fe61b432` |
| `native-preserved-store-source-controls-race1.log` | `0ee8866eb0971569317d1f28302455d14058da51ac4e3bcaee5eb3cd0429d7b6` |
| `native-canonical-slow-fixed.log` | `46624cff3d24c7a6bdd54e44c0d7aa66bccd8c313c51059520ebdfce5af7624a` |

## Retained Parent Rows

| Row | Status / owner boundary |
| --- | --- |
| A0 authored HTTP cap | Closed by #2708; its original receipts remain separate. |
| N1 custom native-search cap | This bounded implementation and proof set; not whole-parent closure. |
| P1 activity cap | Gate allows validation only through the real ordinary activity dispatcher and client supplier. Read-only activity, non-idempotent activity and injected-client paths remain separately enumerated. No activity production edit here. |
| E1 effect authority / U1-U2 | USER/LEAD decisions: extending declared read-only authority to ordinary agent effects and any live/recovery/replay consequences. No class flip, caller override, uncertainty removal or retry. |
| E2 reported stuck run | Not attributed to uncertainty: the effect summary treats it as terminal. Identify the actual remaining session/delivery/pipeline/reservation owner before proposing a semantic repair. |
| Registration / U3 | Nil/default and explicitly supplied positive-client caps differ. Validation and supplier/policy trace only; no override change without a concrete caller and policy ruling. |

## Integration And Qualification

The #2592 integration preempts N1 when B lands. At the initial comparison,
B's published #2698 `e857a1e482` and readable local `f5c57acafd` did not change
the native-search runtime owners, but did share `platform-spec.yaml` and
generated proof/debt metadata. This is not a claim of repository-wide
disjointness. Recheck actual master before landing; do not rewrite immutable
history or raise authority debt. No merge during B's preflight-to-push hold.

Hosted core; local focused. Validate the execution-bound canonical selection
at FINAL HEAD, including every new native root, existing HTTP/credential/
admission controls, real selected-store proofs and source guards. Add only a
disjoint exact unit if core omits a required root. No full fallback, duplicate
local tier or server2 qualification.
