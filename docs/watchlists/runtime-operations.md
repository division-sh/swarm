# Runtime Operations Watchlist

Canonical home for execution-boundary, security, concurrency, replay, shutdown, multi-runtime, and operational config work.

## Active Issues

- `#111` Unify tool transport context-token policy across `/mcp` and `/tools/*`.
- `#112` Make direct-recipient delivery an explicit semantic path or route it through the delivery planner.
- `#116` Make workflow instance mutation atomic across load, mutate, and upsert.
- `#136` Make managed-agent startup validation prove the real filtered tool transport path.
- `#137` Unify agent-visible tool availability across prompt and transport surfaces.
- `#139` Make gateway auth fail closed and remove caller-owned privilege data from tool authorization.
- `#140` Authenticate builder RPC and WebSocket as a privileged control plane.
- `#141` Cap delegable privileges on agent hire and reconfigure.
- `#142` Require authentication for dashboard and runtime-control surfaces on the default listener.
- `#143` Drain in-flight work before runtime shutdown tears down dependencies.
- `#144` Make receipt retry counting and delivery-state updates atomic.
- `#145` Add explicit shared-store ownership for replayed deliveries and restored timers.
- `#146` Add multi-runtime fencing or explicit single-instance enforcement.
- `#147` Make operational config surfaces fail closed and remove inert controls.

## Reserve Backlog

- `5.` Add watchdogs for long-running/no-output turns and stalled runs.
  - combined follow-on priority: issue `#169`
- `8.` Make delivery lifecycle first-class and observable.
  - combined follow-on priority: issue `#169`
- `9.` Improve restart/recovery observability for in-flight turns.
  - combined follow-on priority: issue `#169`
- `25.` Introduce an explicit runtime dependency graph object for boot wiring.
  - follow-on priority: issue `#170`
