# Batch 2 Pipeline And Connector Observations

This finite codemod ships with its reviewed caller output. Run from the repository
root:

```sh
go run ./tools/fixture-codemod/pipeline-observations
go run ./tools/fixture-codemod/pipeline-observations -write
```

The 28 recipes cover exactly 11 owned files. Each identifies an enclosing function
and its complete before/after syntax, not a SQL-text pattern or arbitrary query
translation. Source positions and inert comments do not change the match. Changed
bindings, effectful work, missing/ambiguous functions and malformed source refuse
the rewrite. All files are prepared, formatted and type-checked as a candidate
overlay before any write; an already-migrated tree produces zero changes.

The recipes retain unrelated inherited event/setup observations rather than
copying G/C's allocated owners or modifying execution, fault, restart or teardown
behavior. Those remaining parent seams are listed in the batch audit, not declared
closed by this codemod. The source-bound ratchet and completed-family positive and
hostile guards accompany the migration; the recipes are not a permission ledger.
