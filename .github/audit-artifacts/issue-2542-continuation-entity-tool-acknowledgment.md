# Selected Entity Field Acknowledgment Readback

Parent #2542 / #2151. The both-store acknowledgment/refusal root replaces its
raw revision and mutation-count queries with the existing selected tracked entity
projection owner. Revision is the same physical integer at exact run/entity;
history remains unfiltered and complete. The read is performed on the original
base owner, not the deliberate post-commit/rejection collaborator. No new receipt
policy, context reconstruction, SQL callback, raw getter or dialect branch.

Baseline and committed revision/count facts are read in one original native read
transaction each; the final refused history is read through the same owner. All
original exact +1 revision/mutation and writes==1 assertions remain, as do response
shape, retry=false, typed unacknowledged refusal, diagnostic cause/revision and
secret-redaction assertions. The fault adapter and all invocation timing/workload
are unchanged. Missing, invalid, cancelled or unavailable storage yields no
partial evidence. Existing conformance/type/history consumers are unchanged.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Acknowledged commit with cleanup error | reproduced and fixed | TestSaveEntityFieldAcknowledgedErrorReturnsCommittedToolResponseOnBothStores under race: committed receipt, no retry, exact revision/mutation increment, one native write and private diagnostics. |
| Unacknowledged refusal produces no extra write/history | execution-proven through the same corrected path | Same root's original typed write_failed/nil-result and unchanged exact count assertion. |
| Physical revision and complete scoped history | execution-proven through the same corrected path | TestTrackedMutationProjectionPreservesScopeOrderAndNullBothStores includes exact revision7 plus original scope/order/null/one-read controls. |
| Raw/invalid/cancelled/closed/late failure | execution-proven through the same corrected path | Existing tracked projection refusal root on both stores, zero partial evidence. |
| Workload/owner/assertion drift | reproduced and fixed | Whole-function finite recipe; independent complete-body equivalence and changed base owner/revision/count/write/retry/refusal controls; whole-file raw sibling guard. |

Consumer audit: the acknowledgment root is the sole new revision consumer. Existing
physical contract, sparse/history and conformance consumers retain their original
selected owner and assertions; only an additional physical revision column is
returned. Other fixture authority remains parent debt. Closure: this complete
acknowledgment observation family, not the entire tool harness or parent. Existing
tracker/watchlist mapping remains sufficient; no new architecture issue/gate.
Spec: selected_runtime_store_projection.raw_sql_policy.
Receipts:/home/youmew/.cache/swarm-2542-local-20261007/increment18-entity-ack-*.
Grouped census/registry/guards/overlay/complexity/unused are required before push.

Grouped increment18 evidence: eight focused roots/34 passing race records across
the HTTP, role-scoped bookkeeping and entity acknowledgment families, zero
failures/skips. All162 finite recipes, complete actual candidate overlays,
workload/hostile controls, the real handler premature-dispatch negative control
and inert replay pass. All78 structural guards and partition/timing/spec checks
pass. Fresh downward census:14,543/10,777 ->14,515/10,753 findings/raw sites,
28/24 removed and ZERO added/increased identities; all16 native-family children
and hostile guard controls pass. Collector and67 excluded uncertainties unchanged.
Exact registry passes12,771 facts after individually classifying ten new private
effect-backend read facts and the changed private projection Scan signature; no
external exception or collector change. Exact-head complexity and definitive
native unused qualification accompany the increment before push.

Qualification disclosures: a default Go1.25.5 oracle invocation failed during
stdlib/toolchain setup, not at a migrated assertion. Pinning the already-installed
Go1.26.8 used by native qualification passes; the original setup error is retained.
Cold type-export compilation made the combined census run expire at8m while
loading the registry, AFTER the debt/native-family/hostile roots passed. Only
the unfinished registry root was run separately, first refusing its unclassified
facts and then passing after exact source classification. No test limit, budget,
workload, backend obligation or guard was weakened; all red receipts are retained.
