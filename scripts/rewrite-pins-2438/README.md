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
Parsing consumes `yamlsource.Load` and its immutable `Value` view; the script has
no raw YAML decoder or `yaml.Node` reader. Typed rewrite projections are encoded
only after the complete source preflight. It adds no decoder-census exemption.
Only pins and the affected reply connect section are re-encoded; other schema
sections remain byte-identical. Unexpected forms fail rather than being guessed.
Preflight rejects duplicate or non-local event names, padded identities,
redundant option mappings, empty initialization and nonexact reply correlation.
Tracked symlinks fail before writes; application preserves file permissions.

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

`preview.json` is the unapplied plan, originally collected at `d043efecf` and
rechecked unchanged on the approved local `#2533@77e5cbee4` base: 260 schema
rewrites and 13 file deletions (seven grant schemas and six files in the retired
harness example). Corpus event order/identity/cardinality and all three deferred
initialize entries are preserved by the checked-in corpus test. The corpus
itself has not been rewritten yet; regenerate the preview before applying it.

`go test ./scripts/rewrite-pins-2438 -race -count=3` passes all ten roots, including
actual dry-run/write/repeated-write, exact reply relocation, file-mode preservation
and fail-before-write symlink controls. Seven added invalid-form cases failed
before the preflight repair and pass afterward. The full corpus property test
still preserves event order/identity/cardinality and the three initialization
carriers; this is not current-runtime admission of the rewritten grammar.
`go vet ./scripts/rewrite-pins-2438` passes. The repo-pinned gocognit/gocyclo
tools report no script function at or above the 30-point hotspot threshold.
These are script proofs, not runtime, both-store or merge qualification.

The whole-file deletion proposal is held: the three generic bundle schemas also
contain live stage, instance and deferred constructor declarations. Those facts
must not disappear as a side effect of retiring grants. No corpus writes have
been made while that allocation conflict is being clarified.
