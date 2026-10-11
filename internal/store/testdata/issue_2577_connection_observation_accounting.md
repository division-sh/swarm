# #2577 Exact Connection Observation Accounting

The public onboarding DTO intentionally omits private connection/account evidence.
The distinct-successor oracle therefore consumes one closed read-only fixture
operation, rather than exposing those fields or calling a runtime getter through
an unadmitted inspection. The latter alternative was rejected by the unchanged
schema guard and its RED is retained.

Four exact findings are `fixture-2151`:

- backend `ReadSessionConnectionForTest`: its private original `*sql.Tx` parameter;
- backend `ReadSessionConnectionForTest`: delegation to the canonical `loadOperation` decoder;
- runtimepersistence `ReadServedChannelConnectionForTest`: the existing private read-transaction adapter;
- runtimepersistence `ReadServedChannelConnectionForTest`: delegation to the backend projection.

The public storetest operation accepts the existing independently owned read-only
inspection and runs under `InspectSnapshot`. Only one retained opaque connection
UUID leaves the private owner. No SQL selector/handle/callback, account keys,
runtime admission or provider permission is returned; no row is written or seeded.
Malformed IDs, missing rows, foreign/uninitialized owners and contradictory
session evidence refuse. Both-store genuine served logout/reconnect proves distinct
original/successor coordinates through this exact observation path.

No scanner, allowance, fixture-family policy or debt baseline changed. The complete
finding registry and governed native-family/debt unit remain required separately.
