# #2566 Finite Corpus Rewrite

`intent.json` records exact edits against the integration baseline, retaining
the independently reviewed positive entry/end decisions and explicitly marking
generated/partial source and negative-oracle edits. The default is a **dry run**:

```sh
go run ./scripts/rewrite-stages-2566 -root .
```

No production or corpus source is changed by that command. The script is a
finite applier, not a YAML dumper, another runtime reader, or a store migration.
It validates every file hash and edit anchor before writing anything. Unknown
input fails; reviewed output is accepted unchanged on a second application.
Apply with `-write`, check with `-check`, or obtain the one-time exact replay
receipt with `-prove`. The latter reads the recorded Git inputs, compares every
output byte with the reviewed tree, and proves second-application idempotence.
It is a qualification command, not a recurring freeze on future fixture bytes.

Preparation uses the existing `yamlsource` and typed schema owners to
identify stage declarations. It removes only stage-owned `initial`
and renames only stage-owned `terminal` to `final`. Business data, stage IDs,
expression quoting, comments outside removed fields, timers, gates and stage
metadata remain unchanged. Two explicitly reviewed fixtures move their old
entry declaration to the front before removing `initial`; the script does not
sort any mappings or infer an entry from a stage name.

The retained baseline decisions state that old marked entry equals the new first
declaration and old terminal choices equal new final choices. Full generated
branches also retain their original assertions: entry-selection variants now
move exact declarations instead of setting markers. No generic YAML re-dump is
used, and preparing a non-first marked entry fails for explicit review.

`entries.json` permanently checks 566 baseline-reviewed source/flow/entry
tuples: 214 disk declarations and 352 embedded declarations. A
sorted-but-still-reachable hostile fixture proves reachability cannot replace
this guard. Later intentional entry
changes must update the affected golden explicitly; this does not freeze other
fixture bytes or require maintaining the historical rewrite hashes forever.

Parameterized source generators need actual branch execution as well as literal
goldens. `TestEntityToolFixtureInitialStageBranches` calls the real
`loadWave1EntityToolBundleWithInitialStage` for `queued` and `marginal_review`,
checking compiled entry, declaration order and final `closed` in both cases.
The unchanged `TestEntityTools_BracketListTypeRefsAcrossConsumers` exercises
the latter branch's persisted search and one-row assertion. The source's
`intent.json` review records these proofs; they are not additional independent
literal decisions in the 566 count.

The original preparation was independently reviewed before the grammar cut.
Post-rebase capture preserves those decisions instead of deriving intent from
the new parser. To refresh exact integration edits after a reviewed repair:

```sh
go run ./scripts/rewrite-stages-2566 -refresh-baseline c0f7c2ac5
go run ./scripts/rewrite-stages-2566 -prove
go test ./scripts/rewrite-stages-2566 -race -count=3
```

The corpus receipt and permanent entry tests are not runtime closure by
themselves. Finite initiation, catalog consumers, timer applicability,
construction/restart/fork and completion require the separate execution proofs.

`incoming-fixtures.json` records the three cycle-1 merge-composition sources
against 5ea6d33d6: workspace MCP, the finite clock fixture, and the opt-in
generated Docker emission source. Apply/prove with the existing `-ledger`
option. Their independently reviewed entry/final decisions are also included
in `intent.json` and `entries.json`. Embedded sources added after the original
census pin their own `entry_source_revision`; this is offline proof provenance,
not a runtime fallback. The cheap incoming-source test reads the actual Docker
source without taking its opt-in skip; real transport still needs the opt-in run.

`rebase-2582-fixtures.json` records six complete Go literals, five child schemas
and the already-reviewed explicit root-close fixture against c0f7c2ac5. The
main inventory retains 552 original decisions, the three earlier incoming
decisions and 11 new A decisions. One deactivation-test golden is retired with
the removed capability; its replacement is the public H2 completion proof.
The relocated branch-point fixture keeps its pending/later order and final
membership through an explicit source-selector update, not a fallback search.

`finite-fixtures.json` is the supplemental plan for the release and generated
finite-call fixtures whose roots previously had no final stage. It
does not reinterpret the original entry/end equivalence decisions or mark
service examples final. Apply and replay it with the same applier:

```sh
go run ./scripts/rewrite-stages-2566 -ledger scripts/rewrite-stages-2566/finite-fixtures.json -write
go run ./scripts/rewrite-stages-2566 -ledger scripts/rewrite-stages-2566/finite-fixtures.json -prove
```

The finite-caller regression checks 18 actual loaded root/constructor/connection
closures, including generated feed sources and ordinary source verification.
The no-final census explicitly classifies the 43 remaining disk roots, and
deliberate service variants still refuse finite initiation. Reusable collectors
have an explicit close path, not end-on-first-row or decorative final stages.
Real release execution still must
prove that an ended routing root does not finish a run while child or pinned
fan-out work is outstanding.
