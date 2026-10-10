# Family 141: Retire the entire pipeline callback context protocol

The test-private pipeline transaction helper exposed context keys, queues and
drains for post-commit/rollback callbacks. After families 136-138, exactly one
test still queued an action, through recordingRuntimeMutationRunner. No
production consumer can observe those keys. The rollback collection and the
separate SQL connection-context key had no consumers at all.

All fourteen callback/connection helper functions, three context keys and the
OwnerAction type are removed. The recording runner no longer constructs,
attaches or drains callback collections on either acknowledgment branch. The
existing transaction attempt, cancellation, acknowledgment and returned errors
are unchanged. Remaining transaction-context/raw workflow fixture ownership is
still explicitly parent-owned; this does not claim that entire fixture is native.

The original mutation-runner root retains its exact transaction requirement,
one runner invocation, callback write and error assertions. It now observes one
mutation callback directly rather than treating a private callback queue as
production post-commit evidence. The semantic after-commit obligation is proven
by the existing TestExecuteNodeContractHandlerDefersCommittedEmissions on both
stores: an original selected mutation receipt, exactly one committed emission,
zero dispatch before handoff and exactly one after real receipt consumption.
The full-window observer and failed-handoff control remain authoritative.

Two finite source-pinned recipes constrain the entire runner/consumer rewrites.
A canonical-source walker and hostile identifier/parse controls close the
removed capabilities without a test-source exemption. No context protocol is
renamed or moved into another package; no second codemod inventory is added.

Focused race receipts retain rollback-on-callback-error, exact active transaction,
unowned raw transaction rejection and PostgreSQL no-retry assertions alongside
the mutation-runner and real receipt/dispatcher proofs. A temporary source
overlay invokes the actual premature committed handoff before the deferred
assertion, and must fail both backend cells at zero-before-handoff rather than
compile, setup or timeout. The overlay is not a production change.

Source-pinned receipts: ~/.cache/swarm-2542-local-20261009-family141-*.
The injected real premature handoff fails both backend cells at the exact
zero-before-deferred-dispatch assertion; the unchanged positive race selection
passes seven roots in 10.737s. Two affected recipe/retirement guards pass 0.194s.
The frozen-source ratchet and registry pass 62.297s: 12,388 / 9,089 becomes
12,383 / 9,088 (five findings, one raw site removed; zero added; 67 uncertainties).
Final ratchet/registry, native-unused, complexity and hostile execution disposition
are recorded in the issue checkpoint. Existing #2151/#2542 approval and
watchlist mapping cover this deletion. No new PR, production semantics, timeout,
compatibility path, fixture restoration framework or aggregate qualification.
