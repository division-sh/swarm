# Batch 3: Source-Bound Guard Cost Repair

The retirement wording repair at `2024a7b7f` left a genuine branch-owned CI cost
regression. The old-head Local core completed all 22 units on server2, but its
hosted store-admission command exceeded the unchanged 312s buffered ceiling.
That old qualification is retained as historical evidence, not credited to this
subsequent repair. Server2 was released after all workers joined.

## Exact Diagnosis

Compared hosted `c17380b44` / run37497773815 / job112388444924 with
`2024a7b7f` / run37513890481 / job112443421460. The added runtime/API fixtures
do not execute in this unit. Its new completed-family guards independently
invoke the same full source-owned, test-inclusive typed census.

| New guard | Hosted root seconds |
| --- | ---: |
| Inbound setup, first cold census | 210.44 |
| Native activity | 11.90 |
| Native channel/terminal | 12.36 |
| Native journal | 12.68 |
| Native loop claim | 12.51 |
| Native mock | 12.64 |
| Native API setup | 12.38 |

These seven scans account for 284.91s of added root work. The six repeated warm
scans alone cost 74.47s. The largest common-root increase is only 3.82s; it does
not explain the step change. The CI command/package regression is thus repeated
whole-checkout type/export preparation in guard setup, not slower native writes,
extra lifecycle persistence or a backend fixture moved into this unit.

## Bounded Repair And Preserved Coverage

`TestNativeFixtureFamiliesDoNotReceiveRawAuthority` obtains one fresh complete
census and runs the seven original verifier bodies as separately named subtests.
No finding filter, memoization, alternate collector, cached source, permission
change, conditional skip or assertion removal is introduced. All seven original
family predicates and their typed hostile controls remain unchanged.

The existing `persistence-authority-debt-census` unit owns this parent alongside
the original ratchet root, in core, lifecycle and full on both venues. Its plan
requires every original family child explicitly; an omitted child cannot earn
execution credit merely because its parent passes. The positive RE2 admission
complement excludes exactly these two roots and retains all prefix/suffix
siblings and every testpostgres root. No new proof unit or tier is introduced.

Partition/envelope controls verify exact single ownership of every current root,
both census roots and all seven mandatory children across six venue/tier cells.
Negative controls reject a missing parent, omitted/duplicated child, removed
child map, duplicated ownership, retiering, counts, environments and deadline
changes. Timing and catalog contract checks preserve the same unit inventory.

The store-admission baseline/ceiling stays 240s/312s. The existing census
baseline/ceiling stays 480s/624s. No budget, deadline, job concurrency, fixture,
production owner, lifecycle policy, source-ownership walker, debt collector or
baseline identity changes. Runtime supported-surface proofs remain required.

The old hosted census command was 500s (470.07s package work). Moving seven
independent scans without sharing would consume most of its remaining margin;
the shared fresh scan removes six redundant loads instead. Actual repaired
command evidence is still required; this estimate is not a measured pass.

Future migrated native families must join this shared-scan parent with explicit
required-child coverage rather than reintroducing a fresh full census into the
ordinary admission unit. Parent #2542/#2151 migration remains open.

## Evidence Status

Preparatory partition controls PASS (15.208s); timing contract and catalog
selection controls PASS (0.006s / 0.741s). Repaired-head complexity, planned-unit
measurements, new managed Local core and exact-head hosted CI remain pending at
the time of this implementation note. No earlier failure is relabeled as a pass.

Old-head receipts are retained under
`/home/youmew/.cache/swarm-2542-local-20261003/batch3-2024a7b7f-*` and
`batch3-server2-core-2024a7b7f`; the initial missing native-tool preflight refusal
is retained separately from the successful 22-unit managed core. Fresh old-head
API six roots, private-store five roots and inbound four roots passed under race
with no selected skips; they do not substitute for the new plan's qualification.
