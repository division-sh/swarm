# #2556 Corpus Rewrite

This is a one-shot authoring change, not a migration command or runtime reader.
`intent.json` is the finite reviewed input/output ledger. Each entry identifies
the file, exact input/output hashes, line edits, executable slots,
and the chosen rewrite. Unknown source bytes and missing replacement anchors
fail before writing any file. A second application is byte-identical.

Build once, then run from any directory with an explicit checkout root:

```sh
go -C /path/to/swarm build -o /path/to/rewrite2556 ./scripts/rewrite-value-slots-2556
/path/to/rewrite2556 -root /path/to/swarm
/path/to/rewrite2556 -root /path/to/swarm -write
/path/to/rewrite2556 -root /path/to/swarm -check
```

The source decisions were reviewed against the approved #2556 checked-in/generated
census. The committed tool applies exact edits, not source inference or another
YAML decoder. Text and reference dispositions are explicit, not punctuation guesses.
The final `-capture <base-commit>` operation freezes the reviewed whole-file
edits after generator/negative-oracle integration. This includes `fmt` arguments,
Go concatenations and both replacement anchors, rather than pretending partial
strings are standalone YAML. The compact ledger contains only changed lines;
input/output hashes refuse any ambiguous application. Ordinary application
does not require Git history or census artifacts. Historical rewrite tests do
not freeze future modifications to the corpus.

`2496-intent.json` is the finite R5.1 fixture addendum, captured against
`620b66fd7` after rebasing onto the #2556 value semantics. It records exact
constructor predicate, standing guard, native integer and literal-text choices.
The numeric-registration generator entry separately records its exact
`12c14366e` input; core exposed that quoted supporting predicate after rebase.
Apply it with `-ledger scripts/rewrite-value-slots-2556/2496-intent.json`.
It does not replace the original historical corpus ledger or reinterpret
schema defaults, refusal fixtures, routing identities or lifecycle assertions.
Reviewed flow-style sources containing CEL that requires block scalars are
expanded to block mappings. Native number, bool and null values preserve their tags.

Scenario documents, policy R3, schema/event defaults, provider data, manifest
data, CLI input and structural paths are not expression slots. The ordinary
business object in `TestProducerRoutingRetirementGuardIgnoresNestedLiteralEmitMap`
is explicitly excluded: its `literal` key is not an escape. Negative fixtures
retain their offending declarations and owner. Supporting values alone move to
the new grammar. Grammar-oracle tests separately assert the new classification;
they do not preserve a raw-text escape or typed quoted sole interpolation.

Partial strings are replacement anchors, not independently admitted bundles.
Both anchors and their enclosing sources must agree after rewriting; runtime
source-proof and generated-fixture tests qualify that agreement. The ledger is
not a claim of runtime proof by itself.
