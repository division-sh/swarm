# #2555: post-master core fixture and inventory addendum

Baseline: `52b954ec2`; observed clean source: `accc049ab`.
The worker-owned-service core exposes four branch-owned fixture/inventory
failures. These do not add a production admission interpreter or widen the
approved execution-target class. The original failed aggregate remains RED.

## Unit doubles

`agents.agentTestRuntimeAdapter` and `tools.mockCapabilityRuntimeStub` expose
the current Mock provider descriptor but lack its declared startup interface.
Their tests concern prompt/agent behavior and native capability selection,
not activation or workspace transport. Implement the interface with an
explicit error and nil response; retain assertions that these doubles cannot
claim a successful startup observation. Do not clear the descriptor's probe
requirement, fabricate evidence, or change production admission. These unit
tests earn no A/N/M transport credit.

## Checked YAML fixture

`internal/releasee2e/testdata/workspace_mcp` is the new compiled mock HTTP
transport source. Register it in the existing canonical-routing artifact
census as a different-concept transport proof, with the direct executable
`TestWorkspaceMCPCompiledHostConformance` call to `canonicalrouting.Prove`.
That same test actually consumes the fixture in compiled public host test.
The fixture has already passed canonical source admission and the finite
scalar rewrite; no legacy input or extra routing framework is restored.

## Async-site classifications

The existing source guard finds exactly four new sites. Register their exact
counts and named owners without altering the guard or executable ownership:

| Site | Count | Owner and boundary | Named execution proof |
| --- | --- | --- | --- |
| `after_func`, `worker/remote.go:runRemote` | 2 | Native worker input cancellation/deadline closure; the outer `RunWorker` joins the whole native process. Neither callback submits runtime work. | `TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest`, `TestRemoteWorkerDisconnectCancelsAndJoinsRequest` |
| `after_func`, `worker_remote_execution.go:runDockerWorker` | 1 | Existing worker owner closes the exact attachment, waits for its client and proves exact remote absence before success. This callback only closes its owned pipe. | Seven `TestWorkerRealDocker*` process/request-absence controls |
| `go`, `cliapp/doctor_gateway.go:probeDoctorGateway` | 1 | Free probe's temporary server; deferred Shutdown/Close and exact result channel join precede return. | `TestDoctorGatewayConcurrentHostProbesJoinOwnedResources`, `TestDoctorGatewayProbeCancellationRetainsCancellation`, `TestDoctorGatewayPartialDockerCreationOwnsCleanupAndReportsFailure` |
| `go`, `worker/remote.go:runRemote` | 1 | Exact acknowledged attachment observer; input closes and its joined channel completes before the native result is published. | `TestRemoteWorkerDisconnectCancelsAndJoinsRequest`, `TestWorkerRealDockerLostClientJoinsGatewayRequest` |

These are synchronously joined different-concept subprocess/transport
mechanisms under existing owners, not independent durable runtime tasks.
Their source census is necessary bookkeeping, not behavioral proof by itself.
The prior actual Docker proofs remain separately receipted; final integrated
core/full and hosted acceptance are still required.

No production change, new owner, registry framework, fixture compatibility,
deadline adjustment, vendoring or reduced tier is authorized by this addendum.
