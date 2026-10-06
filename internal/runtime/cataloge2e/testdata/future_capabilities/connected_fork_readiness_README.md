# Completed Dynamic-Connection History (#642)

Binding split: https://github.com/division-sh/swarm/issues/2304#issuecomment-5648278784
Lead correction: https://github.com/division-sh/swarm/issues/2304#issuecomment-5648237091

The two `.go.txt` files preserve the full connected success oracle and its fixture
at `e9f0323a2` (the rebased `1106a5d29`).
`TestConnectedForkFutureSuccessArtifactsPreserved` pins both SHA256 hashes.
The oracle retains the entire node, agent, activity, mixed-frontier, recovery,
readiness, identity and source-isolation assertion matrix. No assertions or
production admission rules are replaced by the overlay.

This exact future capability is **split / escalated to #642**: a real root input
traverses a compiled dynamic connection, the child entry handler publishes a
genuine local event, and selected-contract execution admits that pending event
despite its completed dynamic ancestor. Current admission fails with
`flow_route_history_unproven`: the route-history evaluation does not prove the
completed ancestor's dynamic topology. This is not a claim that all dynamic forks
are unsupported.

Run the original narrow success oracle from the repository root:

```sh
go test -overlay=internal/runtime/cataloge2e/testdata/future_capabilities/connected_fork_readiness_overlay.json \
  ./internal/runtime/cataloge2e \
  -run '^TestSelectedForkFlowOwnedReadinessBothStoresInitial/[^/]+/declared_0/node/initial$' \
  -count=1 -timeout=90s
```

For the full original matrix, use `go run ./cmd/swarm-test --` with the same
overlay and `-run '^TestSelectedForkFlowOwnedReadinessBothStores(Initial|Staged)$'`.
Configure `SWARM_TEST_POSTGRES_DSN` for the narrow direct invocation; the wrapper
provisions PostgreSQL when it is unset. Both backends are required evidence.

The active `TestConnectedForkCompletedDynamicHistoryRefusalBothStores` uses the
same real source path, joins source-runtime writers, and proves the exact typed
store refusal with identical complete application-table snapshots on initial and
repeated execution. That passing refusal is not positive readiness proof.

The active readiness matrix retains its success and recovery assertions, using a
genuine local source without a completed dynamic-connection ancestor.
`TestLocalReadinessCreationSourceBothStores` now proves actual activation and
compiled receiver creation-event admission, then observes the exact completed
entry and pending node-produced local frontier without waiting for global idle.
It does not forge root authority for a private event.

`TestLocalReadinessForkRecipientBothStores` compares selected-frontier recipients
with the actual concrete source's ordinary local consumer. Live publication and
selected frontier/history now consume the same admitted-source lookup-key owner;
historical targets do not supply source identity. The active readiness matrix
requires actual child workflow, readiness and delivery evidence, not publication
count. Loop cases capture their genuine creation event while paused so the loop
start remains in the fork frontier, without introducing an extra loop handler.

This local-source repair does not admit the completed dynamic ancestor in #642.
Neither success matrix is waived, skipped or converted to a refusal assertion.
Full-suite and PR-readiness results are recorded separately on the issue/PR.

The frozen oracle also retains the superseded ordinary-final retirement probes
and assertions from that historical checkpoint. The ratified #2269 D1 model,
composed under #2564 ruling6021549457, distinguishes final-stage entry from
operational retirement. Its active counterparts now retain accepted agents and
structural routes, observe real handler completion/settlement, and test explicit
termination separately. The two pinned `.go.txt` files are intentionally not
rewritten: future #642 activation must reconcile their old final-entry assumptions
with the then-current contract without losing the retained routing, identity,
restart and source-isolation obligations.
