# Issue 2700: Authored HTTP Request Budget

Part of #2700. Track: FULL. Gate: reviewer-c, issue comment 6209671391.
Base: `9beb615299e14741752ea6c92128b8e6036e9ee6`.

## Owner And Scope

`contracts.ToolHTTPExecution.Prepare` continues to admit the authored timeout
(default 30 seconds). `tools.execHTTPRequestOnce` already constructs its request
context after dispatch-rate admission and reads the complete response body
before canceling it. Its shared client's independent 30-second timeout previously
shortened longer authored budgets.

The repair shallow-copies that existing client for this one call and sets only
the copy's timeout to zero. The bound request context remains the timeout owner.
Transport, connection pooling, redirect policy and cookie jar are preserved;
the original client and other consumers are not changed. The physical `Do`
remains under the existing exact effect primitive
`internal/runtime/tools/executor_http.go:execHTTPRequestOnce:http_do:1`.

No retry, additional launch, effect classification, authority, source grammar,
schema, persistence, package limit or existing test deadline changes. The spec
clarification is confined to
`tool_model.http_tool_definition.schema.http_block.optional.timeout_seconds`:
the default is not a maximum, response headers/body share the declared budget,
an earlier caller cancellation/deadline wins, and rate admission precedes the
budget as before. Authentication, admission waiting and settlement are not
redefined as part of this request budget.

## Proof Ledger

Evidence directory: `~/.local/state/agent-a-2700/`. Original REDs are retained;
only the changed candidate can receive GREEN credit.

| Cell | Active proof and receipt |
| --- | --- |
| H01 | `TestHTTPToolDeclaredTimeoutBeyondSharedClientCap/declared_120s`: real authored YAML, normal Execute, declared 120s, actual 45s local upstream. Baseline `declared-timeout-baseline-red.log`: RED at 30.028423958s, one request, canonical transport uncertainty. `declared-timeout-fixed.log`: GREEN, exact provider receipt after 45.042525619s, one request/attempt/settlement, no redispatch. |
| H02 | Same root `/declared_10s`: same 45s upstream, declared 10s, exact transport uncertainty, one attempt/settlement, no replay, joined server. Baseline PASS at 10.006409063s; fixed PASS at 10.001389213s. Whole fixed package 55.802s, count1/non-race. Strengthened deadline-cause assertion subsequently PASS at 10.007339173s (package 10.019s). |
| H03 | `TestHTTPToolTimeoutIncludesResponseBody`, `...CallerCancellation`, `...StalledBody`, `...DefaultAndEarlierCallerDeadline`: body beyond injected shorter cap, explicit caller cancellation at headers/body, bounded stalled body, omitted 30s/default and earlier caller deadline. `http-timeout-fast-race3.log` PASS, 4.535s. |
| H04 | `TestHTTPToolTimeoutOverlappingCallsPreserveClient`: simultaneous requests, separate cancellation/deadlines, original client pointer/timeout/transport/redirect/jar, actual redirect and cookie flow. `...PreservesOtherClientConsumers`: original token-client identity and actual native-search 30s outgoing deadline. Same fast race x3 PASS: six roots x3, 30 passing records, zero failures/skips. |
| H05 | Existing after-admission timeout, rate-limit/no-retry, canonical failure taxonomy, managed launch/cancellation/identity/settlement, credential refresh/refusal and native-search controls; existing acknowledged-settlement cleanup proof on both native stores. `preserved-http-effect-controls-race1.log` PASS, 8.980s, 15 roots / 42 passing records. Both-store acknowledged-settlement root PASS, 6.60s; expected injected post-commit cleanup diagnostics retained. |
| H06 | Exact effect/source-owner guards: `effect-source-owner-guards.log` PASS, four roots, 1.287s. Formatting/diff-check PASS. Static/unused/preflight results are per-head attestations published with FINAL HEAD, not implied by these runtime receipts. |

H01/H02 use a real waiting HTTP server that observes request cancellation; its
handlers are joined. All new HTTP execution proofs use the managed-effect
controller, with existing post-launch uncertainty and logical-replay refusal.
Fast cancellation tests assert the canonical `platform.outcome_uncertain` /
`http_tool_attempt_outcome_unconfirmed` envelope from `tool-executor`, operation
`execute_http_tool`, with exact `transport` or `read_response` stage.

| Retained receipt | SHA-256 |
| --- | --- |
| Baseline RED | `ebbe2319a0165991c61d58f1c54b3d3dda1e2e615da0db502a7b63cb090614c8` |
| Fixed slow proof | `a8fd5b5801e3394a79ede2e3b831afdc9bc79de497ecf7b2fa22f2f87a4d85e7` |
| Fast race x3 | `a921821482560f87582d5c6311b089e27ba181a9fd29f74d7c0cad2b239d6df6` |
| Existing controls | `f5bfd029129afd1889ed2acaebc2aaaaafb1cfad4fbff2dc377562891ec7dc97` |
| Effect/source guards | `c1951688ec0b491332dcfaa0d3e2c9028b8149b05387a7e0d46cbda3d8f372ef` |

## Explicit-Deadline Oracle Correction

Reviewer-c accepted the production repair and H01 at `d21e2868f1` in comment
6210089436, but identified a P2 proof gap: the original H02 test logged elapsed
time without asserting the admitted duration. A substituted 50ms budget could
therefore satisfy that old failure oracle. Those original receipts remain
visible; they are not credited as proving exact authored-budget application.

`TestHTTPToolTimeoutExplicitAuthoredDeadline` now exercises normal YAML
admission/Prepare/Execute with explicit 1s, 10s and 120s declarations and no
caller deadline. Its outgoing test transport checks the actual request deadline
against the interval of possible context-creation coordinates. This is a
deadline-identity assertion, not a new request-performance ceiling. The actual
10s failure also requires `errors.Is(err, context.DeadlineExceeded)`.

One controlled substitution of 50ms for only the 10s request, immediately before
`context.WithTimeout`, was rejected by the new fast root: expected RED,
package 0.013s. The temporary diagnostic change was removed; production is
byte-identical to the accepted `d21e2868f1` repair. No other assertion changed.

| P2 receipt | Result / SHA-256 |
| --- | --- |
| `p2-50ms-negative-red.log` | RED, `7021c49ccce86a70849a455d43ae74a16bd3732a7f6482bd1f4252a63275f9a2` |
| `p2-50ms-negative-mutation.patch` | Removed diagnostic only, `31ca3d36777108350e04a4173b9abd1bd924334ddff6b94bd1f95f3b5fd705c7` |
| `p2-http-timeout-fast-race3-green.log` | PASS 4.596s, seven roots x3 / 42 records, `2d00d9bbfb331d9f9a7554e1b5762ada76882f2e6b00dcde5b1725b010903484` |
| `p2-real-10s-deadline-cause-green.log` | PASS 10.019s, original request bound/envelope plus deadline cause, `b35ab65015c00adebb7c8e03c74885e76dadef7a18fea9d0a502728b7a636882` |

The actual 45s success and unchanged managed-effect, credential, both-store
settlement and source-owner receipts carry under 6210089436; no duplicate
45s journey or local tier was run.

## Open Siblings (Not Discharged)

| Row | Disposition |
| --- | --- |
| Custom native web search drops prepared timeout | Same-concept sibling attached to #2700 by gate 6209671391. `executeCustomWebSearch` prepares an authored duration but uses the original context; reviewer's declared 1s probe observed 29.99998684s. Deferred to a separately bounded native-search repair. Its existing behavior is preserved here, not silently repaired or claimed qualified. |
| Read-only authored tools versus primitive effect class | `authored_http_tool` remains registered `write_or_unknown`. Transport/body failures after launch remain uncertain and never trigger automatic replay. Changing effect authority requires LEAD/user disposition; GET does not establish absence of an effect. This PR does not close the issue. |

## Qualification

Hosted `core`; local focused only, per gate. Validate the exact canonical
root-to-unit ownership on the landing head and add a named unit only if core
omits a required root. Expected path-to-family: executor/test changes to the
tools proof family, effect primitive/source guards to structural owner guards,
and the narrow spec prose to spec/plan consistency checks. No workflow/planner
change, blanket full, local tier duplication or server2 run is authorized.

The #2592 integration takes priority when B lands. This repair is independent
of that held Q1 slice; recheck the actual master and overlap before landing.
