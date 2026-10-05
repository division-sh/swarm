# Qualification Addendum: Exact Root-Agent Preparation

This is an omitted case in the already approved startup readiness consumer,
not a new construction or lifecycle owner. The class and existing Gate E
boundary remain #2496/#2525: exact constructed-instance readiness, stopped
preparation, admitted execution and owned retirement. Parents #2411/#2250
remain open. No new framework, root-route inference, executable preparation or
historical replay is proposed.

## Reproduction And Owner

`d32c350338d58ff0c75040e5f2eef3356fede87b`, on master `276d7723f`, passes
the provider matrix's other scenarios but fails `agent-consumers` and
`agent-replay` on both stores before public startup. The exact refusal is
`dynamic flow agent root-agent ... root ... is not an exact stopped preparation`.
An additive deterministic Manager probe at `589cbb8ae` reproduces the same
refusal after successful canonical preparation (0.128s). An earlier probe used
a memory-enabled root agent and failed earlier memory admission; that probe
does not prove the startup comparison and is not credited.

The relevant execution path is canonical root construction -> immutable
readiness plan -> non-executable declared-agent preparation -> ordinary grant
admission -> `PrepareAdmittedDynamicFlowAgentsForStart` -> Manager.Run ->
post-run route/timer/creation completion. The failure is the pre-Run admission
gate, not provider ingestion or historical replay. Construction and route
ownership remain their existing canonical owners.

At `flow_readiness_startup.go:214`, the startup consumer compares the readiness
instance path directly with `Identity.FlowInstance()`. Root declarations
intentionally have `RootRoute()` and no flow route fields, while their readiness
belongs to the concrete constructed root. `RunScopedFlowInstance.MatchesAgentRoute`
already defines exact root/present route matching through
`Route.AgentIdentityRoute`; it is the semantic owner, not a new helper.

## Bounded Consumer Correction And Proof

Consume that existing route owner against the current persisted plan, retaining
the exact run, stopped/registered phase, attempt ordinal, source coordinate and
declared-agent membership checks. Do not rewrite root agent identities to
borrow a node path, accept a merely same-named agent, skip a current-row load,
or admit a child/root crossing. This is the existing startup consumer's
incorrect comparison, not another live construction authority or a wider
parent obligation. The issue/gate accounting is repaired by this explicit
addendum; the existing approval to finish startup consumers applies.

Proof rows: deterministic constructed-root positive; foreign run, foreign
instance route, obsolete declaration, changed attempt and non-stopped cell
refusals with no executable route/timer publication; existing standing-attempt
and root-blueprint controls; unchanged complete provider matrix on both stores,
including its replay and denied-subscriber cases. The fresh compiled describe
and managed default suite are independent qualification, not proof inferred
from this probe.

Tracker/watchlist decision: additional startup manifestation in existing
#2496/#2525, mapped to construction/attachment and exact lifecycle topology;
no new issue or split, parent tail unchanged. Long-run direction remains
systematic use of typed run/route ownership instead of comparing optional
descriptor projections. This bounded comparison repair has high ROI; broader
decomposition remains #2250. No failure-class closure or merge claim.
