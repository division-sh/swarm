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

Channel role mapping remains in `compileChannelBindings` and the existing
mapped store; source facts are captured during its original inspection, not by
another read. The existing configured-channel root adds the exact mapped-key
reach. Identical facts alone are coalesced; same-key tool-environment and
provider/channel-file sources both remain visible. Doctor remains source-free.

No guard assertions, classifications, budgets, timeouts or accounting exceptions
are relaxed. These two new getter entries are newly pinned source bodies, not
new deployment predicates or increased allowed-debt baselines. No C2
certification is claimed.
