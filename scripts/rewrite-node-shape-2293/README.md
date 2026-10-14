# Node shape rewrite for #2293 slice 1

This committed corpus tool implements the carrier portion of the ratified Q1
6170174660 and Q2 6170252119 rules. It is not a runtime compatibility reader or a
public migration command. Select complete `nodes.yaml` files explicitly:

```sh
go run ./scripts/rewrite-node-shape-2293 path/to/nodes.yaml
go run ./scripts/rewrite-node-shape-2293 -write path/to/nodes.yaml
go run ./scripts/rewrite-node-shape-2293 -prepare-disk path/to/nodes.yaml
go run ./scripts/rewrite-node-shape-2293 -prepare-go path/to/fixture.go:FunctionName
go run ./scripts/rewrite-node-shape-2293 -go-plan path/to/reviewed-inputs.json -write
go run ./scripts/rewrite-node-shape-2293 -capture-activities path/to/source-bundle
```

Dry run is the default. The complete selection is checked before any write.
Only node-level `execution_type`, `subscribes_to`, and `produces` are removed;
the `event_handlers` members are inserted in place, retaining declaration and
handler order. Read-only `yamlsource` syntax coordinates drive source-span edits;
there is no off-owner YAML decoder, mutable syntax carrier or map re-emission.
This retains quoted/block scalar bytes, handler bodies, nested data, timers, and
the state fields owned by slice 3.
Already rewritten input returns its original bytes unchanged.

Before changing guard/rule positions or requiring explicit activity IDs, capture
the old effective names through the admitted bundle's existing activity/result
owners. Read-only capture includes the source artifact hash and admitted source
coordinates; it neither reimplements the default-ID algorithm nor skips failed
bundle admission. The four result-name fields record identity, not a claim that
all four event kinds exist without activity approval. Generated sources and
negative fixtures still need explicit ledger coverage before output is applied.

Node aliases and merge references retain their original anchor ownership.
Inline/quoted node-field edits, handler-container anchors/aliases, nonmapping containers, cyclic or duplicate
effective mappings require an explicit source-ledger disposition instead of
being silently expanded or repaired. Non-node files and historical sources are
not discovered or selected implicitly. Generated Go sources and intentional
negative oracles need their separate finite corpus ledger.

`go-projection-inputs.json` selects fourteen complete literals by source file,
function and string-literal ordinal. Both the original token and reproduced
output are retained. Application validates the complete selection and refuses
foreign edits, forged output or missing/duplicate coordinates before writing.
Partial string generators and malformed-YAML negative controls are not rewritten
implicitly; their flat-source corrections remain explicit fixture changes.

`go-producer-inputs.json` banks the complete emit-lowering fixture literal.
`generated-producer-dispositions.json` lists the four explicit fragment-generator
edits and their original source/proofs. These generators have no activities, as
asserted through the admitted bundle owner, so no activity identity is synthesized
or renamed by their carrier-only cutover.

The runtime decoder cut, scalar guard/disposition rewrite, explicit activity-ID
materialization, generated-source rewrite, and final consumer/pin reconciliation
remain in the same atomic slice-1 PR. This carrier tool must not be used to
claim those obligations are complete. The 254 selected disk files have been
rewritten. `disk-inputs.json` retains their immutable before sources, hashes and
counts; tests reproduce those outputs independently of subsequent live-corpus
edits. Live files are checked for retired carriers, not locked to whole-file pins.
