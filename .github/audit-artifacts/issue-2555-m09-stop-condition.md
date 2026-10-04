## Implementation stop condition: M09 lost-client remote join

Fork-chat target isolation is implemented under ruling 5978140439. Host and
real-Docker public fork/chat/continuation/refusal/replay pass on both selected
stores with exact source lifecycle/container/files/session noninterference.
Claude's early backing observation now receives its existing selected-store
controller; its caller regression and six first/resumed/tool-result,
streaming/buffered gateway-loss launch cases pass race x3. These are partial
uncommitted WIP receipts, not final class closure or paid-Claude proof.

The next original M09 lifetime proof exposed an additional gap in the existing
worker owner, not a second implementation or a proposed framework:

`workspace.RunWorker -> workerCommand -> exec.Cmd.Wait` only joins the local
attached Docker client. Killing **only that test-owned client** after the real
container-native worker reaches a held HTTP `tools/list` makes `RunWorker`
return while its exact worker and request remain live.

`TestWorkerRealDockerLostClientJoinsGatewayRequest` fails **3/3** on server2.
Every failure retains this actual Docker process:

```text
execution=platform.dependency_unavailable workspace_worker_launch_failed
owned child left its HTTP request live
PID       COMMAND
...       sleep infinity
...       /opt/swarm/bin/swarm --internal-workspace-worker
```

The same checkpoint's `TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest`
control passes **3/3**. Its native request deadline cancels the HTTP request
and joins the worker, but that successful control does not prove client loss.
The combined command exits 1 in 21.908 seconds:

```sh
SWARM_TEST_WORKSPACE_MCP_DOCKER=1 SWARM_TEST_WORKSPACE_MCP_NETWORK=bridge \
go test ./internal/runtime/workspace \
-run '^TestWorkerRealDocker(LostClient|HTTPDeadline)JoinsGatewayRequest$' \
-count=3 -timeout=90s -v
```

The proof removes only its dedicated test container during cleanup, joins the
held server, and sends no model/Telegram call or business effect. This is
explicit hardened-host transport/lifetime proof, not default-Linux acceptance.

**Consumption census:** mock model evaluation, pre-launch gateway observation
and mock HTTP tool calls all use the same `RunWorker`. No second remote worker
interpreter was found. M09 already requires child/request join; its definition
is now explicit for lost-client as well as normal deadline cancellation.
The 48-row chosen class and one-PR ceiling remain unchanged. The issue body,
audit amendment and existing transport watchlist are repaired; docs commit
`8c040df`, evidence `docs/audits/2555-m09-lost-client-counterexample.md`.

**Requested bounded disposition:** extend the existing worker/workspace owner
to retain exact remote execution ownership and join or dispose that child
when the local client disappears. Completion of the local client must not
stand in for remote cleanup. No blanket retirement of live source/ordinary
containers, guessed timeout completion, lost-reply success, committed-tool
replay, attempt/PID ledger, compatibility path, driver dependency or generic
supervisor/framework. The spec/proof amendment must distinguish remote cleanup
from local attachment completion and preserve independent failures/uncertainty.

Remote-cleanup implementation is frozen pending the independent bounded ruling;
unrelated proof can continue. No new issue or lead/product redesign is requested.
E/#2525 remains the sole compensation owner, and its actual merge plus G's
crossed L01/L04 refusal/retry proof are still required. No compensation hunk
was duplicated or cherry-picked. I am not claiming only #2525 remains or that
the original full/full/hosted/final-audit obligations have passed.
