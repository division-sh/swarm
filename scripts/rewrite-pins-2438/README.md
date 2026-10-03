# #2438 Slice 2b Corpus Rewrite

One-shot implementation script for the user-ratified allocation:
https://github.com/division-sh/swarm/issues/2438#issuecomment-5970351985

```sh
go run ./scripts/rewrite-pins-2438 -root .
go run ./scripts/rewrite-pins-2438 -root . -write
go test ./scripts/rewrite-pins-2438
```

The default prints a sorted plan without writes. All selected tracked sources are
parsed and the reply's exact paired connections checked before applying a plan.
Only pins and the affected reply connect section are re-encoded; other schema
sections remain byte-identical. Unexpected forms fail rather than being guessed.

The closed deletion set is the seven grant-bearing schema fixtures and the
obsolete harness-injection example. The single checked-in reply moves onto its
response connection. The deferred initialize passenger and ordinary connection
resolution/key_from are preserved. No activation, constructor or fan-in change.

This is local preparation, not the complete slice or a current-runtime admission
claim. Do not merge script/output separately from the interface-owner removal.
Before the eventual one-PR publication, finish the schema reader and consumer
cut, generated producers, retired fixture/reference removal, authoritative spec,
I01-I29 proofs allocated to 2b, both-store supported surfaces and required CI.
The constructor chain/I16 remains in 2c after E's qualified handoff.

## Local Preparation Receipt

`preview.json` is the unapplied plan at `origin/master@d043efecf`: 260 schema
rewrites and 13 file deletions (seven grant schemas and six files in the retired
harness example). Corpus event order/identity/cardinality and all three deferred
initialize entries are preserved by the checked-in corpus test. The corpus
itself has not been rewritten yet; regenerate the preview before applying it.

`go test ./scripts/rewrite-pins-2438 -race -count=3` passes all eight roots.
`go vet ./scripts/rewrite-pins-2438` passes. The repo-pinned gocognit/gocyclo
tools report no script function at or above the 30-point hotspot threshold.
These are script proofs, not runtime, both-store or merge qualification.
