# Qualification Addendum: Optional Receiver Header Writes

Source: `4edd83eca`, based on `origin/master@276d7723f`. This is a
qualification interpretation request, not a closure claim or an oracle waiver.

The unchanged SQLite `TestSelectedForkReceiverPolicyPermutationExecutionBothStores`
leaf `default_sqlite/recipient_permutations/012` reproduces at its source-run
prerequisite. The payload-only optional receiver preserves its business fields,
stage and readiness attempt, but its constructed header changes revision 1 to
2, update time, and acquires the redundant runtime `config.status = active`
projection. The raw before/after comparison remains in the permanent test;
only its failure message was separated from the business-row comparison.

Execution path: eager static constructor -> seeded header/readiness -> ordinary
node delivery admission -> payload-only guard -> Executor.persist ->
pipelineEngineMutationOwner.CommitEngineMutation -> exact constructed-target
prepareMutation -> commitWorkflowInstanceHeader -> atomic claim settlement.
`workflowInstancePersistedProjection.ConfigPayload` adds the status projection;
`DecodeWorkflowInstancePersistenceRecord` uses the status column, not that JSON
copy, for runtime lifecycle authority. This is not yet evidence of a competing
status reader. The historical entityless-mutation branch skipped state writes
for this receiver; it is retired by the approved constructed-target contract.

The related current assertions that an optional static declaration has no row
also conflict with eager construction: a declared empty `receipt` field
contract is initialized as `{}`, whereas a genuinely fieldless instance has
only its header. Neither can borrow the producer's entity. Both declarations
must still execute only the admitted target, preserve authored fields, and
retain exact source/fork isolation.

Proposed proof disposition, requiring independent confirmation: preserve the
canonical constructed-header commit/claim boundary and migrate these historical
receiver controls to assert the exact initialized header/optional field
companion, no authored-field mutation, and the exact expected owned header
revision changes. Continue byte-exact source preservation during subsequent
fork execution. Do not accept arbitrary companion changes, restore lazy or
entityless node execution, or remove claim/failure checks. If a payload-only
handler instead must perform no header write, that requires a bounded named
constructed-target settlement operation, not revival of the old entityless
branch.

Known consumer families: receiver policy permutations, optional receiver
execution, nested static isolation, repeated occurrences, geometry/post-revision
refusal, and supplemental acquisition. Their shared fixture and assertion
helpers are entry points, not independent proof. Every positive/negative root
and backend remains required after the disposition; none receives PASS credit
from this diagnostic leaf. Business-only readers have separately been migrated
to read stage from the canonical header, with the unchanged both-store
settlement/fencing matrix passing at `f9c371829` (32.319s package time).

Class/trackers remain #2496/#2525, parents #2411/#2250 open. Watchlist mapping
remains canonical construction/attachment and receiver ownership; no new issue,
compatibility reader, broader replay authority or framework is proposed. The
failure-discard ordering addendum remains separately awaiting confirmation.
Production behavior and these strict optional-receiver assertions are unchanged
pending the qualification interpretation. No final-review request or push.
