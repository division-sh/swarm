# #2566 Corpus Rewrite Preparation

`intent.json` freezes exact byte edits and entry/end equivalence for the reviewed
positive sources on its named baseline. The command defaults to a **dry run**:

```sh
go run ./scripts/rewrite-stages-2566 -root .
```

No production or corpus source is changed by that command. The script is a
finite applier, not a YAML dumper, another runtime reader, or a store migration.
It validates every file hash and edit anchor before writing anything. Unknown
input fails; reviewed output is accepted unchanged on a second application.
After the coverage gate and completion of the remaining dispositions, the
implementation can apply the ledger with `-write` and assert it with `-check`.

The preparation test uses the existing `yamlsource` and typed schema owners to
identify positive stage declarations. It removes only stage-owned `initial`
and renames only stage-owned `terminal` to `final`. Business data, stage IDs,
expression quoting, comments outside removed fields, timers, gates and stage
metadata remain unchanged. Two explicitly reviewed fixtures move their old
entry declaration to the front before removing `initial`; the script does not
sort any mappings or infer an entry from a stage name.

Preparation proof: old marked entry equals the new first declaration, old
terminal booleans equal the new final booleans, and the complete typed schema
with only initial removed is otherwise equivalent (apart from those two ordered
declaration moves). Final spelling is parsed as source only at this checkpoint:
the production final grammar has not been implemented and is not claimed proven.

This ledger is **not yet the full migration output**. The census separately
classifies partial generators/replacement anchors, negative oracles, typed Go
fixtures, semantic/spec prose and historical ledgers. They need exact reviewed
edits in the same implementation PR; they are not silently skipped as closed.
Embedded spec examples also await their authoritative semantic rewrite. Current
production code and the actual corpus stay untouched while Gate D reviews.

Regenerate the positive preparation from its exact baseline (diagnostic test
files must be present):

```sh
ISSUE2566_PLAN_OUTPUT=scripts/rewrite-stages-2566/intent.json \
  go test ./internal/runtime/contracts -run '^TestIssue2566PrepareCorpusPlan$' -count=1
go test ./scripts/rewrite-stages-2566 -race -count=3
```

The plan records source-site identities for this one-time equivalence proof.
The implementation still owes the permanent corpus/describe entry golden,
strict retired-key rejection, final catalog handoff, finite-start and execution
proofs. This historical plan must not freeze future corpus changes.
