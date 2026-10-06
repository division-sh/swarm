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

`entries.json` permanently checks 204 baseline-reviewed disk source/flow/entry
tuples against current typed admission. A sorted-but-still-reachable hostile
fixture proves reachability cannot replace this guard. Later intentional entry
changes must update the affected golden explicitly; this does not freeze other
fixture bytes or require maintaining the historical rewrite hashes forever.

The original preparation was independently reviewed before the grammar cut.
Post-rebase capture preserves those decisions instead of deriving intent from
the new parser. To refresh exact integration edits after a reviewed repair:

```sh
go run ./scripts/rewrite-stages-2566 -refresh-baseline af250de63 \
  -reviewed-plan-revision bd43e4a8d
go run ./scripts/rewrite-stages-2566 -prove
go test ./scripts/rewrite-stages-2566 -race -count=3
```

The corpus receipt and permanent entry tests are not runtime closure by
themselves. Finite initiation, catalog consumers, timer applicability,
construction/restart/fork and completion require the separate execution proofs.
