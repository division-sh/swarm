# #2438 Slice 2b Corpus Rewrite

One-shot implementation script for the user-ratified allocation:
https://github.com/division-sh/swarm/issues/2438#issuecomment-5970351985
The three generic schemas follow the bounded preservation correction:
https://github.com/division-sh/swarm/issues/2438#issuecomment-5974062605

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
Untracked files inside a retired tree fail before any writes. Empty directories
are removed after their tracked files so retired fixtures cannot remain discoverable.

The closed deletion set is the two tier11 grant-mechanism trees and the obsolete
harness-injection example. The three generic-swarm-bundle schemas survive with
only their grants removed and input lists flattened (3/2/2 events); grant-only
outputs disappear without replacement events. All other sections, including
delivery's instance and deferred constructor facts, survive unchanged. The single
checked-in reply moves onto its response connection. The deferred initialize
passenger and ordinary connection resolution/key_from are preserved. No
activation, constructor or fan-in change.

This is local preparation, not the complete slice or a current-runtime admission
claim. Do not merge script/output separately from the interface-owner removal.
Before the eventual one-PR publication, finish the schema reader and consumer
cut, generated producers, retired fixture/reference removal, authoritative spec,
I01-I29 proofs allocated to 2b, both-store supported surfaces and required CI.
The constructor chain/I16 remains in 2c after E's qualified handoff.

## Local Preparation Receipt

`preview.json` records the applied plan on top of `b099a8aba`: 262 schema rewrites
and 31 file deletions (25 files in the two retired tier11 trees and six in the
harness example). Reapplying the script produces no changes. The 14 retired
field entries were nine grants in the preserved generic schemas and five in the
four deleted tier11 grant schemas. Historical #2143 census artifacts stay intact;
current catalog counts and producer-routing proof dispositions are reconciled.

`go test ./scripts/rewrite-pins-2438 -race -count=3` passes all thirteen roots, including
actual dry-run/write/repeated-write, exact reply relocation, file-mode preservation
and fail-before-write symlink controls. Seven added invalid-form cases failed
before the preflight repair and pass afterward. The full corpus property test
still preserves event order/identity/cardinality and the three initialization
carriers; this is not current-runtime admission of the rewritten grammar.
`go vet ./scripts/rewrite-pins-2438` passes. The repo-pinned gocognit/gocyclo
tools report no script function at or above the 30-point hotspot threshold.
The added required-survivor oracle failed against the old `b099a8aba` deletion
selection. Deleting any survivor or altering any non-pin section now fails the
mutation controls. A permanent test reapplies retired grants to the live generic
schemas so grant removal stays exercised after the corpus is names-only.
Unknown grant locations, wrong directions and malformed values fail preflight.

The independent ancestor connection formerly carried by test-data-pin-wiring is
preserved by `TestGrantFixtureRetirementPreservesCanonicalAncestorConnection`
using the canonical test-child-flow-absolute-path fixture. It passed three race
repetitions before corpus application; rerun it through names-only admission
when the remaining reader cut is finished. Deleted mechanism-specific callers
are not credited as execution of the removed trees. Ledger rows B059-B063 now
identify explicit removal and their actual replacement proofs.

These are script and focused checkpoint proofs, not runtime, both-store or merge
qualification. Names-only admission, generated producers, harness consumer
retirement and reply owners still need the rest of the local 2b implementation.
The eventual PR must retain reviewer-set `CI-Tier: lifecycle` and
`Local-Tier: lifecycle`. No push or PR is authorized at this checkpoint.
