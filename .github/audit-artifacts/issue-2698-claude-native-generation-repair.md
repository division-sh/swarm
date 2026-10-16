# Claude Native Generation Repair

Track: FULL, consolidated ruling2698/6205327685. The original channel ledger
had six Claude roots with zero provider calls. Their fixture installed a literal
execution authority, not an issued generation. Its handwritten SQLite bootstrap
also omitted the canonical event payload admission used by lifecycle diagnostics.

The whole constructor chain now consumes the original selected store, admitted
source, complete resolved actor, native process capability and issued generation.
Preparation precedes bus/coordinator construction. Materialization consumes the
already committed actor, and readiness uses the existing manager finalizer. The
logger forwards ordinary and lifecycle diagnostics to RuntimeLogger with the same
payload admission as the bus; a projection error fails the test. SQLite uses the
existing StartSQLiteRuntimeStore owner instead of handwritten schema setup.

At the original restart cut the old manager/coordinator are shut down, the work
occurrence is joined and the grant retired. The retained capability issues the
actual next generation, and the existing source_set_rebind transition preserves
the exact actor/configuration while advancing its lifecycle generation. Replay
does not reuse a synthetic generation-one context or redispatch the provider.
No Claim auto-heal, production change, assertion removal or timeout increase.

Logs in /home/youmew/.cache/swarm-2542-family145/:

| Receipt | Result |
| --- | --- |
| pr2698-served-channel-original-ledger.jsonl | Original aggregate RED: six Claude roots had no provider progress; the three unrelated channel roots passed. |
| pr2698-claude-native-mode-diagnostic-control.log | Strict diagnostic oracle RED on SQLite: missing payload admission in handwritten bootstrap. PostgreSQL passed. |
| pr2698-claude-canonical-native-and-diagnostic-control.log | Complete first-claim root PASS11.954s under race on both stores, including diagnostic persistence. |
| pr2698-claude-native-complete-final-race.log | Seven complete roots PASS115.638s under race on both stores, including restart/no-redispatch, uncertain settlement, missing backing, terminal retryability, stateless/memory format parity and direct dead-letter envelope. |

Original exact failure classifications, provider/process/turn counts, spend,
canonical envelopes, session/memory ownership and restart assertions remain.
Unrelated historical guard/corpus composition, final quality/ratchet checks,
frozen-head HumanTask proof and canonical preflight remain outstanding. This is a
local signed repair, not the final PR push or hosted qualification.
