### Additional implementation stop-condition: Docker fork-chat target

The approved L07/M08 proof found a target-authority contradiction. The concrete
source actor remains in the immutable conversation-fork snapshot, as required.
Both the mock fork-chat path and Claude fork-state workspace now use the
capability-admission target. Its concrete-identity branch resolves the ordinary
source container without the source data-projection identity; the existing
identity-checked container owner therefore removes/replaces that live source
container to create the admission target.

`TestForkChatWorkspaceAdmissionCannotRetireSourceExecution` fails **3/3**:
`fork-chat admission retired 1 source execution containers; err=<nil>`.
This is a faithful injected Docker-state workspace-owner counterexample, not
native-Docker transport credit. The public host fork-chat journey passes on both
stores but cannot clear this Docker source-noninterference requirement.

Relevant owners/consumers:
- `apiv1.conversationForkChatActor`: immutable snapshot actor remains concrete.
- `workspace.ResolveWorkspaceForCapabilityAdmission`: defective shared entrance.
- `MockRuntime` fork-chat workspace resolution and `ResolveClaudeWorkspace`
  fork-private-state base: both consume that entrance.
- Existing workspace identity/cleanup and fork-chat sandbox authorities remain
  canonical; no duplicate owner or need for a framework was found.

Requested bounded disposition, alongside the pending activation-compensation
request in https://github.com/division-sh/swarm/issues/2555#issuecomment-5977855287:
keep the exact source snapshot identity for provenance and sandbox authorization,
but project the existing fork-chat authority into its execution target rather
than the ordinary source actor container. Do not erase identity, fabricate a
normal source-run context, materialize current source data, share provider state,
or add another workspace/attempt owner. Require both-store host/real-Docker
public fork-chat and continuation, source container/labels/files/loop unchanged,
exact snapshot/stub restrictions, typed pre-model refusal, and cleanup of only
owned sandbox resources.

The checked-in pre-audit owner row and implementation addendum are corrected;
the issue body now records the original coding approval and these outstanding
stop conditions. The existing watchlist node is refined, no new issue is needed,
and the original one-PR/48-row ceiling remains. Production edits on compensation
and sandbox target projection are frozen until the bounded disposition; unrelated
transport proof continues. Please record the allowed existing-owner repair.
