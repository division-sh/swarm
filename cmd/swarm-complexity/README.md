# Exact-snapshot complexity ratchet

This development command implements #2407 R1.4 under #2447's approved baseline-only
gate. No runtime semantics or factoring are included. The unmodified pinned
gocyclo v0.6.0 and gocognit v1.2.1 commands own all scores; `go run module@version`
provisions them in the normal Go cache, without vendoring or product dependencies.

## Update and Check

Commit source changes first. Measurement never reads dirty or untracked Go files:

```sh
go run ./cmd/swarm-complexity -head HEAD -base origin/master -update -evidence /tmp/complexity
git add .github/complexity-baseline.json
git commit -m 'chore: record exact complexity baseline'
go run ./cmd/swarm-complexity -head HEAD -base origin/master -evidence /tmp/complexity
```

The update writes measured facts, not permission to grow. Both independently
remeasured >=30 counts must be non-increasing, even on the initial baseline PR.
The committed head artifact must exactly equal deterministic measurement. Existing
base artifacts must match their source and policy too; only genuine baseline
absence is allowed for initial bootstrap. Tool, schema, threshold or scope changes
require explicit policy review and comparable evidence, never an automatic reset.

PR events measure actual head/base SHAs, not the synthetic merge used by the test
planner. Push measures before/after and refuses unavailable or all-zero history.
Manual and scheduled runs validate the current snapshot only. An independent CI
job runs on every proof profile, and the aggregate requires success, not skipped.
`head.json` and `delta.json` carry reproducible measurement and revision evidence;
the committed baseline deliberately contains no self-referential commit SHA.

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
