# Manager lock order

This is the observed partial order within the manager. An arrow means that
the lock on the left is already held when acquiring the lock on the right.
Separate branches are not a global rank. The selected store owns durable
agent and attachment phases; these locks own process resources and views.

## Source and attachment transitions

- Live handoff and source-set rebind acquire
  `dynamicFlowRetirementMu -> dynamicFlowAttachmentMu` (write).
  Rebind then takes `sourceSetPublishMu` (write) before resolving and locking
  the affected lifecycle cells, and releases those locks in reverse order.
  Multiple `cell.opMu` locks are acquired in identity fingerprint order.
  See [live handoff](live_handoff.go) and
  `PrepareDurableTopologySourceSetRebind` / `release` in
  [static topology](static_topology.go).
- Readiness reconciliation takes `dynamicFlowAttachmentMu` (read) and uses
  short `dynamicFlowReadinessMu` sections for the pending flag, current attempt
  and local source indexes. A readiness attempt can later take publication and
  cell locks through replacement. Do not keep the readiness mutex held for
  native attachment verification: snapshot the exact local attempt, verify
  the store proof outside the mutex, then revalidate that attempt. See
  [readiness reconciliation](flow_runtime_readiness.go) and
  [committed attachment verification](committed_agent_attachment.go).
- After the manager join, `retireDynamicFlowAttemptsAfterJoin` takes
  `dynamicFlowRetirementMu` before the short `dynamicFlowReadinessMu` snapshot.
  The durable retirements are outside the readiness-map lock. This does not
  permit moving the preceding work/physical join under the retirement lock.

## Publication and lifecycle cells

- Ordinary execution replacement uses
  `sourceSetPublishMu` (read) -> `executionPublishMu` -> `cell.opMu` ->
  coordinator `mu` when those locks nest. The initial lookup in
  `lockIdentityOperation` drops coordinator `mu` before waiting on
  `cell.opMu`, then revalidates the cell under `mu`. Never block on a cell
  while holding coordinator `mu`.
- Begin-run takes the same publication pair and then coordinator `mu` ->
  `workMu` to install the run's work owner. `workMu` protects the process
  work/retirement view; do not enter coordinator `mu` from that lock.
- `storeMu` protects the lifecycle persistence pointer, not a durable row.
  `persistence` snapshots the owner and drops the pointer lock before using it.
  Do not infer that all native calls are outside manager locks: the native
  compare-and-set/current-row path in `replaceLoopLocked` has seats under
  coordinator `mu`. Calls through these seats must not reenter the locked
  coordinator or wait for a handler that needs it.

See [execution replacement](runtime.go) and
[the lifecycle coordinator](lifecycle_coordinator.go).

## Callbacks, waits, and unlock boundaries

- The predecessor execution join in `replaceLoopLocked` drops coordinator
  `mu`, the cell, and the two publication locks. It reacquires the publication
  pair before the cell, then revalidates under `mu`. Loop release drops its
  publication read lock after acquiring the cell, and drops the cell before
  waiting on outstanding execution leases; it revalidates on resumption.
  Keep these distinct from the bus's own route
  retirement/join in [prepared route publication](../bus/eventbus.go).
- Do not join an accepted finalizer while holding locks that it needs for
  publication or settlement. `joinShutdownDrain` runs the work wait and
  dependency callback outside coordinator `mu`, on the transition's shared
  deadline. Completion/cancellation reporting remains with that transition.
- Prepared source-set rebind and terminal preparation have separate `mu`
  fields for settlement of their own reservation. Commit, abort, and release
  transfer or drop the locks through the prepared object. They are not locks
  on a second copy of durable phase. See [source-set rebind](static_topology.go)
  and [terminal preparation](terminal_retirement.go).

## Other local state

- `AgentManager.mu` guards local topology bookkeeping and snapshots, and is
  not a replacement for the coordinator's lifecycle operation locks.
- `startupEffectsMu` serializes startup effect recovery and its local
  completion bit. `ReconcileExternalEffectsForStartup` holds it over the native
  reconcile calls; the native owner still decides the persisted outcomes.
- `runMu` guards the local auth breaker; `poisonMu` and `deadLetterMu` guard
  their local panic/diagnostic maps. Keep these short state sections separate
  from route/worker joins.
- `deliveryLaneMu` guards only the map of per-identity delivery lanes.
  [Lane acquisition](claimed_attempt_executor.go) drops the map lock before
  waiting for the lane token.

`readinessMu` is no longer present. The readiness-map mutex and the two
publication mutexes still guard process-local facts; none can be removed
merely because the store owns the durable transitions.
