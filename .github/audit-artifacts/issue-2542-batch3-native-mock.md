# Batch 3 Local Family: Native Mock Activity Admission And Replay

Parent: #2542 / #2151. Predecessor: `ab709b124`, based on masteraf250de63.
Chosen class: the four mock activity consumers' reconstructed journal/setup
and raw attempt/story observations. The failing helper was the entry point,
not a boundary excluding its admission and restart consumers.

## Canonical Owners And Complete Cohort

All four roots consume the original selected `WorkflowPersistence`, canonical
run construction and native coordinator fixture. The selected native event
bus owns delivery and obligations; the recording bus remains the result oracle.
Explicit mock-only posture now survives coordinator and event-bus construction;
only omitted posture receives the normal live fixture default. Replay joins,
closes and reopens the original fixture location before constructing its second
coordinator. No live pool, alternate store or fake transaction protocol remains
in this cohort.

The existing detached journal reader adds exact-run physical attempt count and
the original whole-store mock author-story count. The latter retains both
`execution_mode=mock` and `source_owner=activity_attempts`, deliberately without
a run predicate. A two-run live/mock control independently proves the different
scopes and exactly one original read transaction. Late errors return an entirely
empty value. The authoritative selected-runtime projection `raw_sql_policy.
activity_journal_observation` states these scopes and fixture-posture semantics.

| Manifestation | Exact preserved proof |
| --- | --- |
| TestPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournal | Compiled generated response, two duplicate deliveries, zero HTTP/credential access, succeeded mock journal, original payload and identical publication ID; exactly two global mock activity stories. |
| TestPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlan | Original success followed by true native reopen; replay without a current response plan returns the journaled result ID and cannot launch HTTP or credentials. |
| TestPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTP | All four original refusal cases and codes: missing response, non-provider tool, read-only effect and invalid response; exact-run attempts and credential reads remain zero. |
| TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP | Original live-intent refusal under a preserved mock-only ceiling; physical attempts, credential reads and HTTP calls all remain zero. |

Each root now executes on both stores; the two originally SQLite-only roots
gain PostgreSQL execution without losing their original cell. The original
provider workload, invocation count, execution-mode assignment, refusal codes,
payload/cardinality assertions and no-redispatch checks remain.

## Codemod And Completion Proof

Four exact current-source before/after recipes extend the existing finite
executable64 ->68. The original64 encodings remain byte-for-byte unchanged.
Independent controls compare mutation/result/refusal conditions, literal error
arguments, intent assignments, generated/explicit mock plans, HTTP callback
workload, schema/credential setup, dispatch calls, repeated delivery bounds and
all four admission-case literals. Weakened refusal or changed mode controls fail.

The real executable applies exactly those four functions to an ab709b124
proof worktree after complete overlay type checking. Its second application
reports zero changes. The output agrees with the committed function snapshots
under the existing canonical AST comparison; harmless layout is not authority.
Snapshot identity, unknown replacement and all-or-nothing preflight tests pass.

The new family guard uses the unchanged exhaustive collector. On the actual
compiled507395616 predecessor it fails26.69s on original mock constructors,
pool propagation and SQL observations. Migrated roots and hostile raw argument
or callback controls pass. No exception, invented field discriminator or
collector narrowing was introduced.

## Measured Evidence And Remaining Tail

- Focused Vemew race: four roots / eight backend cells / twenty passing records,
  zero fail/skip, package19.684s.
- Original native-reader snapshot/refusal controls: two roots / four backend
  cells, zero fail/skip, package15.104s. The independent live/mock two-run oracle
  distinguishes global story count from requested-run attempt count.
- Downward census: `14,915 ->14,880`, **35 removed / ZERO added**; confirmed
  raw-operation debt `11,065 ->11,040`, all67 excluded-source uncertainties
  unchanged. The complete registry verifies12,474 facts; only the two existing
  private reader operation signatures changed. No ordinary escape was relabeled.
- Native mock guard11.15s, hostile control0.08s and complete registry2.52s;
  downward census30.74s. Closed-delivery/revision inventories pass1.192s.
- Initial preparation refused an invalid refresh value and left the baseline
  untouched, then flagged changed private operation signatures as unclassified.
  Those receipts remain visible; the correct downward-only refresh and exact
  private signature classifications subsequently pass. There was no retry loop
  seeking timing or workload success.

Receipts: `/home/youmew/.cache/swarm-2542-local-20261003/batch3-native-mock-*`.
Collector494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5
is unchanged. Cumulative local batch reduction is152 findings, ZERO added.
No server2, aggregate tier or exhaustive matrix run was used mid-migration.
Committed-head complexity and reviewer-selected final-head lifecycle qualification
remain before publication because this batch includes actual reopen.

Closure is all four mock consumers, not #2542 or #2151. The neighboring channel,
loop, post-acknowledgment fault and remaining general fixture families are still
tracked under the existing parent, not unnamed follow-ups. Existing watchlist
mapping and gate remain sufficient; there is no new ownership decision, duplicate
G/C/A first-landing port, compatibility layer or separate framework.
