# Telegram signing option 1: existing registration operation

LOCAL S3 DRAFT. Option 1 is classified INSIDE by LEAD's ruling:
https://github.com/division-sh/swarm/issues/2764#issuecomment-6489106841
The hunk remains under reviewer-g; this is not push/landing approval or a claim
of completed Telegram runtime activation.

The existing deployment registration reserves its admitted Telegram profile's
signing role as well as its provider-token role. Service.admitCredentials and
Service.admitSigningCredential generate the private value once through the
existing operation receipt, credential writer, checkpoint and teardown.
There is no extra operation, lifecycle, table, listener or manual-secret step.

The signing occurrence is account-registration scoped. It is not a source or
operator authorization: the original identity proof still cites only the
provider-token admission. Separate sources must still obtain exact admitted
bindings from the snapshot/registration owners. This branch does not fabricate
a learned activation or complete that source-consumer handoff.

Receipt replay retains the generated value after a lost checkpoint. A separate
registration gets a separate occurrence. A missing or foreign receipt, unusable
value or read error refuses rather than adopting shared credentials or
silently rotating. Existing legacy registrations are not backfilled or reset.
No key lifetime beyond the operation, rotation or new source authority is
authorized. HITL Inbox option (a)/(b) remains a separate pending R38 choice.
