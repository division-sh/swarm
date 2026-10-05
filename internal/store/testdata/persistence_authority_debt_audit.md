# Guard-First Authority Debt Ratchet

Binding approval: #2542 comment 5999476669. Governing contract:
`platform-spec.yaml#engine.runtime_core_persistence_store_contracts.selected_contracts`:
`selected_runtime_store_construction`, `selected_runtime_store_projection`, and
its `implementation_topology` / `structural_enforcement`. The authoritative spec
records this temporary regression barrier, not a new permission registry.

## Boundary

This extraction contains only the existing expanded census mechanics, the exact
debt snapshot, monotonic/trusted-source enforcement and structural controls.
There is no fixture migration, product behavior change, cleanup repair, planner
or workflow change. The original master registry and selected-boundary guards
are unchanged. Known-red expanded zero-closure guards remain on B's integration
branch; neither existing guard consults the debt snapshot as permission.

The existing source-owned census records resolved operations, inferred locals,
aliases, promoted methods, callbacks, context protocols, and constructor recovery.
The same checkout ownership and inactive-source scan are retained. The debt set
also consumes selected-boundary syntax findings and non-SQL construction and
forbidden fixture-consumption facts. Unknown ordinary authority remains debt.
Private/backend/construction/infrastructure roles remain distinct; changing a
disposition label cannot authorize ordinary runtime/test SQL.

Closed native fixture construction operations are not same-DB recovery. Their
exact semantic factory names remain usable; raw-input admission, uninitialized
concrete literals and unknown reconstruction remain debt. The current scanner
source and role policy are digest-bound after bootstrap, so no later narrowing
or permission relabel can silently erase debt.

## Exact Identity And Validation

Each sorted row contains finding kind, consumer file, enclosing declaration,
normalized member, resolved signature, family, canonical-owner routing hint and
multiplicity. Occurrence ordinals are removed from identity, not evidence counts.
Identical occurrences retain multiplicity. No line number or SQL-text permission
defines ownership. Owner hints are diagnostics; missing exact owners explicitly
route to #2542 rather than creating a generic query escape.

For actual head A, head snapshot H and trusted integration-base snapshot B:
`A subset H subset B`, with multiplicity, plus `A subset actual-base-source`.
The last comparison prevents an obsolete snapshot row from authorizing
resurrection. Totals are derived, not independently editable ceilings. Missing,
corrupt, duplicate, reseeded or raised snapshots and partial/type-failed scans
fail closed. Trusted base is Git's actual common integration base; hosted PR
events supply the authenticated target, and shallow checkouts retrieve history.
Behind-master alone is not a qualification failure.

Normal tests/CI are read-only. One explicit local mode,
`SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT=downward`, removes eliminated rows and
prints exact deltas only after validation. Initial bootstrap requires actual
base debt equality. A removed baseline in landed ancestry cannot be bootstrapped
again. No caller-selected baseline file or source override exists.

## Bootstrap Evidence

Extraction base: `52b954ec26c85a47605cefb8d7030f2a842f8c24`.
Source configuration: module-wide `go/packages`, test syntax/types/imports and
effective method sets, checkout-owned Go sources, inactive parsing and selected
boundary scope; ambient GOFLAGS and GOWORK source-selection overrides cleared.

Measured final-policy receipt: `ratchet-bootstrap-final-policy-20261005.jsonl`
under `/home/youmew/.cache/swarm-2542-local-20261003`.

- Collected findings: 53,203.
- Raw-operation occurrences across all roles: 38,167.
- Exact debt occurrences: 15,107.
- Distinct debt identities: 11,851.
- Raw-operation debt occurrences: 11,391.
- Guard-only delta: added 0, removed 0; initial snapshot equals actual base debt.

These are separate metrics. They are not B's migrated integration census of
8,476 findings / 6,567 residual raw-operation sites, nor a number of bugs.

## Proof And Residual Closure

`TestPersistenceAuthorityDebtRatchet` executes the complete head/base census and
comparison. Ten named hostile/control roots cover new sites, equal/lower-count
payment, duplicates, raised/reseeded snapshots, stale resurrection, corrupted
data, aliases/embedding/callbacks/opaque construction, inactive imports, role
relabeling, failed/missing census, unchanged legacy debt, actual deletion, benign
line movement and earlier-ordinal removal. A named membership proof requires the
ratchet to remain selected by existing `store-admission-full` at core, lifecycle
and full; no planner or shard change is introduced.

Failed initial receipts are retained. They exposed the missing non-SQL
constructor debt selection and git archive's PAX header, both repaired directly.
Positive package success is checked alongside every expected root/backend cell;
no skip, retry-to-green or timeout inflation supplies closure evidence.

PR1 closes the new/resurrected-debt regression barrier, not inherited authority
migration. #2542/#2151 remain open. Five grouped migration extractions and final
closure own the tail; strict zero debt, both global guards, SQLite fork-deadline
qualification and integrated full remain final obligations. Existing watchlist
mapping and approved owners remain sufficient; no new tracker or framework.
