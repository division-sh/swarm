# Exact-snapshot complexity ratchet

This development command implements #2407 R1.4 and #2550's policy-only ratchet.
No runtime semantics or factoring are included. The unmodified pinned
gocyclo v0.6.0 and gocognit v1.2.1 commands own all scores; `go run module@version`
provisions them in the normal Go cache, without vendoring or product dependencies.

## Check

Commit source changes first. Measurement never reads dirty or untracked Go files:

```sh
go run ./cmd/swarm-complexity -head HEAD -base "$(git merge-base HEAD origin/master)" -evidence test-results/complexity
```

`.github/complexity-baseline.json` contains reviewed policy only, never scores.
Both independently remeasured >=30 counts must be non-increasing. Missing policy,
unknown head fields, missing comparison, self-comparison and unrelated comparison
revisions fail closed. Historical base artifacts contribute only their reviewed
policy; their score populations cannot influence independent measurement. Tool,
schema, threshold or scope changes require explicit review and comparable evidence.

PR events measure actual head and its merge-base with the event's base tip, not the
synthetic merge or unrelated newer-master changes. Push measures before/after and
refuses unavailable, all-zero or non-ancestor history. Manual runs require a named
branch; scheduled runs require the repository default branch. The selected head
must be on that observed remote branch lineage. An unmerged feature compares its
merge-base with the observed default branch; scheduled/default-branch and already
merged heads compare their exact first parent. An independent CI
job runs on every proof profile, and the aggregate requires success, not skipped.
`base.json`, `head.json` and `delta.json` carry reproducible measurement and revision evidence;
the delta records the comparison SHA and reviewed-policy verdict.

## Scope and Identity

All tracked regular authored non-test `.go` files are admitted, across every build
variant, including tools, support, hidden directories and testdata. Git blobs are
parsed then staged flat so tool directory filters cannot omit them. Canonical
`ast.IsGenerated` files and `_test.go` files are recorded as exclusions. Empty
authored inventory, parse failures, unsupported files, suppression directives and
line directives fail closed. Valid files without functions remain admitted.

Syntax is used only to check the upstream output population and provenance, not
to calculate complexity. Both analyzers report function declarations; gocyclo also
reports direct package-variable function literals. Cognitive zero is measured
with `-over=-1`; an unreported shape is never invented as a zero. Identity is
file/package/kind/name/lexical occurrence, so repeated `init` and multi-variable
literals are preserved without depending on line numbers. Receivers use the
upstream display spelling (generic arguments omitted). Adding/removing/reordering
same-name occurrences can change their pairing and is disclosed in the delta.

## Limits

This ratchets hotspot counts, not total complexity or every function. Same-count
worsening is allowed and reported, including >=50 counts, maxima and per-callable
changes. Tool-unreported shapes (for example parenthesized package initializers)
remain an upstream limitation. Nested closures follow each tool's own rules.
File additions, removals and generated/test classification changes are reported
for review. Editing the checker/workflow itself is not a tamper-proof boundary;
code review and #2407 R1.3 protection own that policy. Later hotspot factoring,
historical eligibility evidence and runtime architecture remain separate work.
