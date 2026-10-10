# Cohort 101: one native SQLite fixture construction owner

Binding context: approved #2542/#2151 broad migration; selected construction,
original coordinator and raw_sql_policy. No production semantics or new framework.

StartSQLiteRuntimeStorePair and StartSQLiteRuntimeStoreWithContext now consume the
existing StartSQLiteRuntimeStoreWithReopen owner. The former still opens two independent
native stores at one file-backed location; the latter still forwards its exact context.
File-existence checks remain. Duplicate schema bootstrap, payload admission and
independent close callbacks are deleted, not wrapped or retained as alternatives.

The canonical reopen owner registers one reverse-order close ledger before consumers,
records each constructed store before bootstrap can fail, uses the same platform plans,
storetest origin and payload admission, and never recovers a pool/coordinator. The four
pair consumer families are catalog restart, delivery-lifecycle parity, API durable-data
run lifecycle and external workflow-gate recovery. All ordinary SQLite fixture callers
also consume this owner through unchanged StartSQLiteRuntimeStore/WithContext signatures.
No caller propagation or new raw permission is required.

Focused proof: native location/peer controls on both stores, lifecycle materialization
and refusal controls, plus the new SQLite reopen-owner teardown control. A real channel-gated
consumer is released in consumer cleanup, reads through both original owners and joins
before both stores close; after the subtest both stores must refuse reads. Distinct
handles, identical nonempty paths and peer readback are asserted. Affected pair roots
cover actual delivery lifecycle, API run creation/reconstructed readback and workflow
startup recovery on both stores. Catalog construction is included in the complete
candidate type overlay; no full catalog workload qualification is claimed here.

Two finite recipes and hostile foreign-test/file-error controls prove exact wrappers,
while the existing canonical reopen oracle protects location/bootstrap/payload/close
behavior. Fresh census must remain strictly downward before commit.

Reliability value: shared constructor and cleanup-owner consolidation, not a measured
flake-rate change. Parent generic getters, raw protocols, zero debt, strict completion
guards, unchanged SQLite fork deadline and integrated qualification remain open.

Ratchet disclosure: the initial new test called the tuple-returning pair wrapper,
which the census correctly refused as a new construction occurrence. The proof now
consumes the existing named canonical constructor at the identical original location
for both independent owners. Neither a tuple-pair wrapper nor an unresolved function
value adds a fresh construction site. The pair's returned factory remains existing
conservative construction debt (decreased, not newly exempted). The exact wrapper
oracle and three existing pair-consumer roots prove its connection to that owner.
No classifier, exemption, baseline addition or inventory suppression was introduced.
Existing watchlist/architecture disposition applies; no new split or compatibility.
