# Source Freeze, Replay State And Startup Owners

Track: FULL, consolidated2698/6205327685. Three remaining original failures
were positive fixture setup defects, not permission to alter their subject gates.

- Source freeze: the live delivery uses its original selected PostgresStore and
  explicitly prepared source identity before committing/claiming work. Historical
  queued delivery remains pending-only. Other live/historical session, activity,
  directive and external-effect cuts are unchanged; the callback retains the
  selected store instead of reconstructing its delivery owner from SQL.
- Replay state: construct the genuine ordinary source turn before installing the
  deliberately incomplete fork actor and changing only the turn's classification
  run coordinate. All three original invalid fork replay states remain present;
  exact blocker and taxonomy assertions are unchanged. Seed callbacks receive
  their actual subtest, so fixture cleanup and FailNow do not escape to the parent.
- SQLite startup/trace: the original incumbent, canceled standby/no-mutation and
  exact successor evidence/release assertions all remain in order. Only AFTER
  both startup process owners release does the trace phase acquire its own native
  owner in a child fixture scope. It does not reuse a released capability. The
  original full actor configuration, memory lease, conversation, physical trace,
  event/delivery and diagnostic visibility assertions remain unchanged.

Receipts under /home/youmew/.cache/swarm-2542-family145/:

| Receipt | Result |
| --- | --- |
| pr2698-source-freeze-replay-state-native-race.log | Complete source-freeze root PASS13.24s, all ten live/historical cases. The aggregate remains RED for the replay fixture's initial native-plan contamination; not an aggregate PASS. |
| pr2698-replay-state-native-before-fault-race.log | Complete replay-state root PASS9.005s under race after valid source construction precedes deliberate fork corruption; all three cases. |
| pr2698-startup-trace-native-owner-race.log | Complete SQLite startup/trace root PASS8.469s under race, including the real ownership/standby sequence and all trace assertions. |

The Docker smoke's new local quiescence failure and the selected-fork history
fixtures are NOT qualified by these receipts. No timeout, native guard or failure
assertion changed. Final finite composition, quality/ratchet, frozen HumanTask
proof and canonical preflight still precede the one repaired PR push.
