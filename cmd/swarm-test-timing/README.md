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

Reviewed regression and confirmed escape counts are distinct. Escape rate is
always **N/A/unmeasured**, even with confirmed escapes: #2535 must first define and
prove a comparable escape/non-escape population. Unknown findings are not non-escapes.
Observations retain incomplete and failed evidence separately from infra, harness,
budget, or non-regression classifications. First-parent detection lag requires
causal review evidence; repeated attempts do not invent a second introducing
commit. Nightly/manual frequency and one-week operational acceptance remain open
under #2535. #1967 is closed historical context.

## Retrospective Causal Replay

The hosted full job fetches complete Git history and saves its plan, all command
receipts and an unclassified observation, even for failed/incomplete work. It does
not guess attribution or a prior core run. Enrichment happens after independent
review through this same report entrance, not a new dispatcher or metrics service.

Select the actual full run/attempt and the preceding successful **core** run/attempt
from the CI artifacts. Use `gh run download` with `-n ci-plan` and
`-p 'proof-*-timing'`, into separate `full/` and `core/` directories. Preserve the
original files; do not edit identity, profile, outcomes or missing evidence to make
them comparable. Obtain core metadata from the exact attempt API:

```sh
gh api "repos/division-sh/swarm/actions/runs/$CORE_RUN/attempts/$CORE_ATTEMPT" > core-run.json
git fetch origin '+refs/heads/master:refs/remotes/origin/master'
# If this checkout is shallow, fetch its complete history before replay.
if test "$(git rev-parse --is-shallow-repository)" = true; then
  git fetch --unshallow origin
fi
go run ./cmd/swarm-test-timing -observe-full-cadence \
  -plan full/ci-plan/proof-plan.json -evidence-root full \
  -workflow-run-id "$FULL_RUN" -workflow-attempt "$FULL_ATTEMPT" \
  -prior-core-plan core/ci-plan/proof-plan.json -prior-core-evidence-root core \
  -prior-core-run core-run.json -prior-core-landing-sha "$CORE_MASTER_LANDING" \
  -cadence-classifications reviewed-findings.json \
  -result-json full/reviewed-cadence.json -markdown full/reviewed-cadence.md
```

`reviewed-findings.json` is an array of the existing typed attribution records:
`package`, `root`, `kind`, `review`, and, for causal regressions,
`introducing_commit` and `first_detection_run_id`. `review` must cite the
independent issue/review disposition; filenames or a newest-merge guess are not
causal evidence. Empty attribution retains honest unclassified observations.

Known lag requires the detected execution and introducer on observed master's
first-parent lineage, and complete successful core evidence after introduction
but before detection, with the failed root absent from that core selection.
For a PR core proof, `CORE_MASTER_LANDING` is the actual master merge commit, not
the PR head or a renamed plan source. The **existing merged-proof observer** must
independently verify exact PR association, equal execution/landing trees, current
protected checks, latest successful run/attempt and unchanged plan digest against
read-only GitHub/Git facts. The original core plan and command SHAs remain intact;
only this verified landing is used for master first-parent position. A direct
same-source master core proof does not require the landing flag.

Foreign branches, retired/unavailable artifact schemas, wrong attempts, superseded
runs, expired artifacts and unverified source aliases do not gain master lineage
or core credit. They remain explicit unknown/problems; do not rename their SHAs.
No merged-tree replay receipt by itself supplies the missing actual core commands.
The output preserves original full failure and never changes protected
checks. Archive the inputs, reviewed attribution and outputs together with the
run/attempt links. A successful command is report generation, not proof that a
regression, rate or lag is known.
