# #2542 continuation: terminal-run bus refusal storage

Both terminal-run refusal roots replace their raw status/event/delivery SELECTs
with one fixed consumer helper. The helper consumes existing RunLifecycleReadPersistence,
canonical event-record read and closed delivery diagnostic read owners. It keeps
the exact run and event keys, explicitly refuses a borrowed run, maps canonical
event primary-key presence to 0/1, and counts every physical delivery row by the
same event ID without status/run/subscriber narrowing. Any reader/decoder failure
returns no partial status/count tuple. There is no new SQL or storetest port.

The old independent read cuts remain independent: no coherent transaction is
claimed across three different semantic readers. Canonical decoding strengthens
malformed-row refusal without converting failures to zero. Existing original
selected stores supply every port, with no raw getter, wrapper reconstruction,
callback authority or backend selector. The six local query/scan pairs and three
SQLite getter calls are invalid in this family.

Complete finite root inverses change only the observation callback body. Run
creation through lifecycle ownership, actual terminalization, exact cancelled
status and every publish/publish_acknowledged/publish_direct refusal/no-event/
no-delivery assertion remain unchanged. The helper's whole shape and hostile
run/event/presence/delivery/error controls protect its entire fixed tuple.

Actual race proof: both TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwner
roots and all three entrypoints. A new both-store positive control creates actual
admitted event/delivery evidence through existing semantic fixture recipes, then
requires running/1/1, running/0/0 for a missing event and no partial tuple under
cancellation. It also proves no writer commit or retained selected transaction.
Thus an always-zero observation cannot satisfy this family. Initial missing
import/incomplete proof-store interface compile failures are retained and earn
no execution credit; only declared semantic ports were added to that test type.
The debt guard also rejected the new positive probe's initial raw PostgreSQL
setup. It now consumes the existing StartPostgresRuntimeStore fixture, without
a pool getter or independently admitted store. No baseline addition or exemption
was allowed; the repaired source-bound census has zero new sites.

Governing raw_sql_policy and existing gate/watchlist suffice; no new architecture,
framework, compatibility, tracker or runtime-spec behavior. This canonicalizes
terminal-run refusal observation, not other bus duplicate/fault/context/setup
families or #2542/#2151. Exact counts/final receipts are on the issue. No aggregate
core/full, hosted CI, server2, fork-deadline or parent closure is claimed.
