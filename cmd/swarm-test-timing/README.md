# Full Cadence Observation

The useful daily timing-model publisher remains unchanged in frequency. Weights
drive balancing, eligible packing, and ETA, never required root membership or hard
deadlines. Valid model-only create and update bodies select core qualification.

`-observe-full-cadence` consumes the existing full plan and canonical timing
evidence without making a passing receipt or changing required checks:

```sh
go run ./cmd/swarm-test-timing -observe-full-cadence \
  -plan artifacts/ci-plan/proof-plan.json -evidence-root artifacts \
  -workflow-run-id 123 -workflow-attempt 1 \
  -result-json artifacts/full-cadence.json -markdown artifacts/full-cadence.md
```

Failed roots are unclassified candidates by default. Optional independently
reviewed `-cadence-classifications` supplies package, root, classification, review
URL, introducing commit, and first detection run ID. A confirmed core escape also
requires `-prior-core-plan`, `-prior-core-evidence-root`, and `-prior-core-run`: an
actual successful preceding CI core attempt whose complete evidence covers the
introducing lineage but does not select the failed root. Master first-parent
lineage is read from Git, not inferred from the newest merge. Unknown introducer,
missing coverage, and unavailable historical evidence stay unknown.

Zero eligible classified regressions means **N/A**, not zero escapes. Observations
retain incomplete and failed evidence separately from infra, harness, budget, or
non-regression classifications. The conditional rate is not a claim that all
unclassified failures have been explained. First-parent detection lag requires
causal review evidence; repeated attempts do not invent a second introducing
commit. Nightly/manual frequency and one-week operational acceptance remain open
under #2535. #1967 is closed historical context.
