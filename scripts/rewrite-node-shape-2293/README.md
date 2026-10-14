# Node shape rewrite for #2293 slice 1

This committed corpus tool implements the carrier portion of the ratified Q1
6170174660 and Q2 6170252119 rules. It is not a runtime compatibility reader or a
public migration command. Select complete `nodes.yaml` files explicitly:

```sh
go run ./scripts/rewrite-node-shape-2293 path/to/nodes.yaml
go run ./scripts/rewrite-node-shape-2293 -write path/to/nodes.yaml
```

Dry run is the default. The complete selection is checked before any write.
Only node-level `execution_type`, `subscribes_to`, and `produces` are removed;
the `event_handlers` members are inserted in place, retaining declaration and
handler order. Read-only `yamlsource` syntax coordinates drive source-span edits;
there is no off-owner YAML decoder, mutable syntax carrier or map re-emission.
This retains quoted/block scalar bytes, handler bodies, nested data, timers, and
the state fields owned by slice 3.
Already rewritten input returns its original bytes unchanged.

Node aliases and merge references retain their original anchor ownership.
Inline/quoted node-field edits, handler-container anchors/aliases, nonmapping containers, cyclic or duplicate
effective mappings require an explicit source-ledger disposition instead of
being silently expanded or repaired. Non-node files and historical sources are
not discovered or selected implicitly. Generated Go sources and intentional
negative oracles need their separate finite corpus ledger.

The runtime decoder cut, scalar guard/disposition rewrite, explicit activity-ID
materialization, generated-source rewrite, and final consumer/pin reconciliation
remain in the same atomic slice-1 PR. This first carrier tool must not be used to
claim those obligations are complete. No corpus output has been applied yet.
