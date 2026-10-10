# Cohort 76: source admission without dummy transaction protocols

The adjacent route and post-commit source tests no longer import database/sql or
the fake transaction/action namespace. A zero-value sql.Tx and unused action
arrays never represented a production transaction, callback owner or persistence
gate. Those non-authoritative setups/assertions are removed, not retained under
a replacement context key. They are not claimed as lost native atomicity proof.

Actual foreign-source rejection before route persistence/publication, immutable
source on successful add/remove, exact local route table, delivery-session zero
mutation and the real post-commit claim/settlement counts are preserved. The
post-commit root is renamed to its genuine source-identity obligation. The
existing probe captures the actual ClaimEvent context under its existing mutex,
and the root now requires the exact bus source at BOTH claim and settlement.

Finite complete snapshots allow only fake protocol retirement and these stronger
source observations. Focused race proof also checks publisher admission order,
pending-work retention on source rejection and exact owner-bound sweep siblings.
Two committed negative controls alter the REAL production claim/settlement
handoffs through Go overlays to discard their source context. Both must fail the
renamed root at its exact missing-source assertion, not setup/build/timeout.
Production source is unchanged. This component policy proof is not backend I/O;
both-backend source persistence proof remains the separate cohort 74 control.

Existing immutable-source/work owners and the approved #2542 fake-protocol
retirement boundary govern. No new key protocol, semantic owner, compatibility,
framework, timing change, tracker or spec behavior. Parent closure remains open.
