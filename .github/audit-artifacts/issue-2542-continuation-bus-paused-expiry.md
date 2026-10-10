# Cohort 93: paused live-claim expiry consumer

The stale-in-progress branch already has the real claim. It now passes that claim
through its shared helper to the existing ExpireExactDeliveryClaimFault owner,
instead of opening a connection transaction and updating by delivery ID alone.
No new fault interpreter, SQL allowance or reconstruction is introduced.

The original two-hour start/one-hour expiry cuts and obligation/open-attempt
atomicity remain. The canonical fault additionally requires the exact run,
version and token, preventing an obsolete fixture claim from aging another
attempt. Both updates require one row inside the original selected transaction.

Three whole-function recipes pin the sole caller substitution, all pending/failed/
stale branches, park-without-wake checks, manager/coordinator synchronization,
continue dispatch and successful final settlement. Existing native owner controls
prove exact mutation and dual-row rollback plus invalid/cancelled/closed authority
on both stores; the actual parking/continue journey also runs under race.

The native exact controls reproduced an existing consumer mismatch: the canonical
whole-store snapshot now encodes Column/Type/Value cells, but the fault comparator
still expected untyped arrays and therefore found zero matching rows. This bounded
proof repair decodes that existing format explicitly, matches string versus byte
keys by their declared driver type, and preserves raw JSON values (including large
integers, null and byte encoding). Column/type drift is rejected even for the
allowed timestamp cells; whole-store equality and exactly-one-row checks remain.
No inference or permissive fallback is introduced. Pure hostile controls reject
untyped/incomplete cells, wrong column identity and malformed/unsupported keys.

This is consumer closure using a previously approved named live fault. Existing
#2542 approval and raw_sql_policy govern; no runtime/spec/framework/architecture
change or compatibility path. Other raw control/card/construction/live-lock debt
and the final parent completion obligations remain open.
