# Frozen Landing Rebase And Hosted Failure Repairs

Part of2542; related to2151. Supersedes c28171f5b's approval pin, not the frozen
class boundary or prior proof. Rebase target:
055bbbaba13e8d604ed80b73af98ca97c3b7acfb, including2602/2603. All201 commits
replay without conflict; range-diff records201 exact patch matches and no
omissions. Incoming CI publisher, unit selection and authoritative spec rules
are preserved. Qualification remains hosted full, with no CI-Units declaration
or duplicated local tier. No production runtime or timing policy changes.

## Broad-10 Diagnosis And Complete Bounded Repair

Hosted run38032086679/job114156135905 panics in projectionTestBus.send at its
continuation acquisition. The branch removed the generic manager fixture's
implicit persistence fallback, but four executable projection witnesses still
used that construction-only fixture. Their bus continuation owner was unset;
one also supplied an explicit continuation spy but still lacked persistence.
The newer master commits do not change these manager sources. The exact
adoption panic is reproduced locally, as are recovery/spawn and self-retirement
failures. Omitting the explicit native dependency in the repaired fixture
reproduces the original adoption panic separately on SQLite and PostgreSQL.

The four original root names remain executable, now as external both-store
consumers of the existing ManagerDeliveryNativeFixture. One small fixture
composition helper borrows that original store and its activated authority,
admits its run, and binds the manager context to the same execution/source.
It opens no store, issues no substitute claim and interprets no transaction.
Every event/obligation is published explicitly before send; no claim-time seed
or implicit fallback returns. Pure projection construction remains store-free,
with an added subcase in the existing unit separation control.
That subcase initially confused the bus's nonnil typed provider with valid
authority; the provider correctly refuses before configuration. The repaired
assertion checks actual refusal, absent persistence/continuations and invalid
authority, retaining the original plain/options controls unchanged.

Adoption preserves standard/authoritative modes, exact current token, phase,
subscription counts, single route, generation replay and receiver build. It
also proves absent-before-publication and exact persisted delivery settlement.
The existing lifecycle probe waits for that settlement before cleanup; handler
arrival and queue continuation consumption are not persistence completion.
Recovery preserves hydration-before-route and generation4 ->5; spawn retains
exact execution/route identity; self-retirement retains the blocked handler,
two buffered carriers, one consume/one return, one handler and two resolutions.
Its release barrier now also opens on assertion failure before joined cleanup.
No receiver deadline, grace, workload or meaningful assertion is weakened.

## macOS Possession Diagnosis And Repair

Hosted job114156134757 rejects duplicate effective YAML key serve before store
admission. This is deterministic source construction, not a macOS lock/driver
failure. The same public possession journey fails locally on Linux. The shared
golden config already emits the ephemeral listener block; the possession caller
still appended its older local block. It now consumes only the shared producer.
Normal/dev configuration and every process ownership/restart assertion remain.
A cheap source-consumption control rejects the exact injected duplicate block;
the complete possession journey and restored guard pass on Linux. Native macOS
qualification remains the normal hosted job; no cross-platform pass is claimed.

## Source Pins, Regeneration And Consumption

Two active immutable input pins now name their byte-identical rebased ancestors:
1df1e21aa ->0fa8f19fe for the finite Q6 reconciliation input, and6a03e25a2
->73c42be19 for the historical collector policy. Exact recipe bytes and all four
hashed policy files respectively compare unchanged. Both new pins are ancestors
of this branch, available in fresh clones. The18-transition digest remains
1690ca4675c7189489cf78faba03e913b1e4ac2fa3bf8953d1b8d1919e180c7c.
No historical transform, classifier or allowed fingerprint pair changes.

Facade, Python manifest, describe corpus, startup-predicate, node-ID, registry
and collector sidecar regenerate unchanged. Fresh census:54777 total findings,
38919 total raw operation sites,10698 debt,7754 raw debt sites and67 unresolved
occurrences. Full identity comparison against the prior source adds exactly five
non-raw typed factory callbacks: four ProveNative projection consumers and the
composition helper's AgentFactory parameter. No finding is removed; canonical
debt rows compare byte-identical. Baseline permission and historical role
extraction equivalence are not broadened or relabeled. Registry14631 remains
fully classified. The collector stays3958381013996586ea697cd868abcacec29e9316706276f603f32523f74b5cb6;
baseline checksum stayseb156b5f2a8f2c78a8bd211015ba2eb15d01b30470428ac3a74586b4e9fe992a.

## Manifestations And Evidence

Receipts: ~/.cache/swarm-2604-rebase055-* on vemew, Go1.26.8, count1,
GOMAXPROCS3, native SQLite/PostgreSQL and unchanged deadlines.

| Manifestation | Coverage status | Exact proof |
| --- | --- | --- |
| Adoption missing explicit execution dependency | reproduced and fixed | Original panic; same missing-dependency injection panics on each store; restored native race proof covers both modes/stores, exact identity/replay and durable settlement. |
| Recovery/spawn execution dependency | reproduced and fixed | Separate original failures; both-store race proofs retain route/generation/build assertions with explicit native publication and checked send results. |
| Deferred self-retirement dependency | reproduced and fixed | Original failure; both-store race preserves blocked/accepted/buffered cuts and exact resolutions; final assertion-safe barrier proof passes. |
| Possession duplicate config producer | reproduced and fixed | macOS hosted and Linux original rejection; exact duplicate injection fails the new guard; restored guard and full public journey pass on Linux, native macOS pending. |
| Rebase source/qualification integration | execution-proven through the same corrected path |201 patch matches; unchanged input bytes/digests; planner/CI unit/publisher/timing/catalog, recipe/owner/boundary, census/metadata/family and retirement controls pass. |
| Prior interrupted/served aggregate reds and remaining parent debt | split / escalated as separate class | Retained in rebase336 audit and2353; prior isolated passes are not aggregate qualification. Complete normal hosted proof and final2542/2151 closure remain required. |

Named receipts include native-projection-race, selfretirement-final-race,
projection-unit-separation, adoption-unwired-sqlite/postgres, possession-red,
possession-fixed, possession-regression-injection, possession-guard-restored,
planner-guards, recipe-boundary-guards, owner-inventories, census-regeneration,
cli-regeneration and node-regeneration. Native unused default/race/issue2413
exits0; no Linux/Darwin union claim. Signed-head complexity is measured before
push and reported on the PR. Both hosted reds remain recorded, not erased.

Canonical owners and all unchanged cohort consumption/receipts carry from the
rebase336 audit. This delta completes the four missed execution consumers and
the possession caller, not the remaining parent migration. Watchlist/tracker
decision remains existing2542/2151/checklist; no new issue or framework. Final
review must re-pin the new SHA. Merge requires full exact-head gate success and
rebaseable linear history; no pending native macOS or aggregate red is waived.
