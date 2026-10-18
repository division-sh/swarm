# #2729 Credential-Source Projection Accounting

Gate D: https://github.com/division-sh/swarm/issues/2729#issuecomment-6252188543

This is operator readback, not new admission authority. The existing
`verify_admission_predicate_census.json` is refreshed only through
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` and its canonical test. Existing
classification rows and proof-family assignments remain unchanged.

| Changed pinned owner | Existing rows / proof families | Exact role and preserved control |
| --- | --- | --- |
| `LocalPreflightReport.checkContractSecrets`, `WriteLocalPreflightText`, `loadLocalPreflightCapabilitySource`, `runLocalClaudeCLIPreflight` | A15-A18/A27; P13-P16 | Retain source facts from the same required observations and append value-free presentation. Existing blockers, severities, reachability and agent-free/Claude decisions stay unchanged. `TestProviderIngressCredentialTierDoesNotAdoptToolEnvironment` adds exact-source preflight and redaction reaches; existing source-free doctor and secret controls remain. |
| `ProviderCredentialResolver.Resolve`, `Inspect`, `storeValue` | A12; P10/P21 | Use the existing native snapshot port for value and source together; retain legacy non-snapshot store fallback without fabricating provenance. No environment fallback for provider keys. `TestProviderCredentialResolver_StoreWinsOverEnvForActiveProfiles` requires exactly one atomic native observation; missing-port, env-only, read-failure redaction and credentialless-profile controls remain. |
| `ProviderCredential.SourceFact` (new pinned method) | A12; P10/P21 | Pure getter of an already captured, value-free source. No lookup, refusal, grant, cache or admission clause. |
| `RuntimeDeps.validated`, `newRuntime`, `validateSelectedBackendCredential`, `validateSelectedBackendCredentialForDeclaredAgents`, `validateSelectedBackendCredentialForActiveAgents` | A05/A08/A11-A13/A18/A23-A25; P04/P08-P11/P15/P20/P21 (existing per-owner subsets) | Thread the existing provider validation's source result and retain successful static-credential observations. Caller error handling, missing-credential warnings/policy escalation, mock waiver and restored-agent checks are preserved. Existing runtime admission/boot-graph and live/mock controls remain. |
| `Runtime.prepareStartLocked` | A05/A08/A11/A13/A18/A23-A25; P04/P08/P09/P11/P15/P20/P21 | Retain the active check's existing successful fact before readiness, under the existing credential-readback mutex. Error, injected-runtime, recovery and startup ordering are unchanged. Boundary, native hydration/order and served presentation evidence are separately attributed below. |
| `ValidateWorkflowContractSurface` | A05-A10/A13/A14/A29; P00/P04-P08/P11/P12 | Retain source facts from the same existing provider-trigger owner after capability evaluation; no extra inspection or new admission condition. All post-registry clause accounting and refusal controls remain. |
| `WorkflowContractValidationResult.CredentialSources` (new pinned method) | A05-A10/A13/A14/A29; P00/P04-P08/P11/P12 | Copy-only projection of retained boot/static/provider facts, not new admission authority. |
| `buildRuntimeComposition` | A01-A04/A10/A19-A22/A26/A27; P01-P03/P08/P10/P16-P22 (existing exact assignments) | Pass construction, channel and frozen standing facts to the sole serve presenter. No change to lease, store, source, listener, readiness, registration or startup ordering. The new `TestServeRequiredCredentialSourceReadbackBothStores` proves the public reach on both stores in concise/verbose modes. |

Credential owner/watchlist mapping stays with `runtime/credentials` and its
existing snapshot, receipt, seal and usability interpreters. Source locators
are private on observations/metadata; `sameAtomicSnapshot` and its JSON-based
authority comparison are unchanged. `TestCredentialSnapshotOverlayPreservesSourcePrecedenceAtomically`
covers same-value raw/normalized/uppercase source changes without token changes,
real value/tier changes, frozen provenance, and absent/empty/refused/corrupt
sources. Existing currentness and ingress owner guards stay mandatory.

Reviewer-d's owner-readback P2 is repaired in that same snapshot owner and root:
https://github.com/division-sh/swarm/issues/2729#issuecomment-6253270672
`Observe` refreshes its retained snapshot after every successful observation,
including an unchanged-authority observation, under its existing mutex. Token
rotation still depends only on `sameAtomicSnapshot`. The source remains outside
authority JSON, receipts and seals; no second lookup is introduced. The proof
checks `owner.SourceFacts()` at each winning environment spelling while the
already-frozen projection keeps its original locator, with the existing exact
metadata/token stability and real-tier/value invalidation assertions intact.

The active-provider fact-return repair and its proof composition follow:
https://github.com/division-sh/swarm/issues/2729#issuecomment-6253978913
The existing private manager-only fixture now checks the fact-return boundary
using the real file credential owner, including file-only missing-provider
refusal, absent-agent live/mock controls, identical declared/active source facts,
and value-free readback. Existing mock/live-descriptor refusals are retained.
This is a boundary proof, not a native recovered-boot reproduction. Its RED
uses a signature-only adapter that still discards the successful observation;
the expected file fact is empty until the actual return repair is applied.

The Start caller retains only a successful, nonempty fact, before readiness;
the injected-LLM branch and original error/boot ordering are unchanged. The
existing credential-readback mutex protects this append and the readback copy.
No second resolution, credential fallback or new production helper is added.

`TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity`
separately preserves genuine both-store reconciliation/hydration/delivery order.
Its declarations and injected LLM are unchanged; it does not exercise the
repaired active-provider observation. The supported both-store served proof
still covers the runtime-to-readback/presenter layer with tool/channel sources,
not manager-only restored-provider admission. The existing presenter check
coalesces identical facts; the boundary check establishes that declared and
active checks return that identical pair. No lawful manager-only served
reproduction has been established: reconciliation retires unmatched static
actors before hydration. No recovery or retirement gate is bypassed.

Proof cost: adds 0 roots in lifecycle; extends the existing credential boundary
and mock-control roots. Existing native hydration and served roots retain their
original assertions and distinct proof attribution.

Channel role mapping remains in `compileChannelBindings` and the existing
mapped store; source facts are captured during its original inspection, not by
another read. The existing configured-channel root adds the exact mapped-key
reach. Identical facts alone are coalesced; same-key tool-environment and
provider/channel-file sources both remain visible. Doctor remains source-free.

No guard assertions, classifications, budgets, timeouts or accounting exceptions
are relaxed. These two new getter entries are newly pinned source bodies, not
new deployment predicates or increased allowed-debt baselines. No C2
certification is claimed.
