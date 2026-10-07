# #2566 Scenario Overlay Reconciliation

This fixture-only amendment follows the failed exact-head continuation at
`c5895fdd2aad8c32dfd0a873e5aba8159d727d81`, recorded on #2566 in comment
`6044297467`. That attempt retains 30 passing lifecycle units, one failing
unit, 18 unstarted units and all 13 unstarted supplements. Its receipts are
not replaced or relabelled. The six earlier-head credits remain separate.

## Ownership And Changes

The semantic concept is preservation of finite fixture completion after
scenario-specific contract overlays. The chosen defect is the two consumers
of `WriteNovelDerivedScenarioBundleWithRootInput` that overwrite the child
handlers and discard its only lawful `pending -> done` path.

All contract overlay declarations now belong to the existing
`canonicalrouting` fixture owner. Production ordered admission, compiled
topology, final membership, handler eligibility, publication preflight and
the ten-owner run-completion conjunction are unchanged.

- Numeric: `InstallNovelNumericScenarioLifecycle` preserves the arithmetic,
  explicit double, event shape and collector, then advances on consumption
  of `fulfillment.completed`. All four scenario modes retain their original
  numeric/public-readback assertions and additionally prove run completion.
- Authored: `InstallNovelAuthoredScenarioLifecycle` preserves the activity
  and response handlers and declares a fieldless fixture close event. Its
  root starts `active`, rather than already ended, and root/worker both
  advance to `done` on that event. The driver publishes it only after the
  original response/three-publication readback and quiescence. Both original
  runs are proved completed through public `run.get`; profile bytes, identity,
  effective-source mismatch refusal, restart and no-dead-letter assertions
  remain. Scenario data files and their interpolation semantics are unchanged.
- Existing base-constructor consumers in dev scratch, context-construction
  failure, fan-out worker boot, configured channel, admitted-source posture
  and numeric-ingress setup do not replace these child handlers. The base
  constructor and its source bytes are unchanged. No global fixture variant
  or runtime refusal exemption was introduced.

No new semantic owner, parser, shim, compatibility path or product capability
is introduced. The first close experiment demonstrated existing rejection
of later publication to an already-ended root; the repair keeps this
reusable fixture active rather than changing E's refusal owner. Runtime/spec
semantics are not changed by this amendment.

## Proof Matrix

| Manifestation | Replacement proof |
| --- | --- |
| Numeric overlay drops its completion edge | `TestServedSemanticNumericScenarioModes`: both stores, all four modes, original numeric assertions plus public completed-run readback |
| Authored response overlay drops its completion edge | `TestServedParityHarnessAuthoredScenarioMaterializationLifecycle`: both stores, one-request and three-request runs, response/profile/restart/readback assertions preserved, explicit actual close and completed-run readback |
| Base-only constructor coverage misses subsequent overlays | `TestRewrite2566FiniteInitiationSourcesHaveCompleteClosure` now loads and structurally verifies both actual overlay variants |
| Source order/final evidence omits overlay outputs | `TestRewrite2566GeneratedSourcesMatchReviewedEntryGoldens` covers both variants through loaded, persisted and retained artifacts |
| Removing the last completion edge is silently admitted | `TestRewrite2566ScenarioOverlaysRejectMissingCompletion`: both variants must produce the exact unreachable-state finding for `fulfillment.done` |
| Same-unit sibling regression | Complete unchanged `serveapp-journeys-m-z` selector, local managed execution |

Focused served roots and source/retained/negative controls are run with
`-race -count=3`. The complete failed selector, repository-wide
`Guard|Inventory|Census|Registry` sweep, all 78 structural guards, actual
persistence ratchet, vet, rewrite equivalence and independent complexity
measurement qualify the delta before its single review-branch push. Exact
commands/results are recorded in the SHA-bound #2566 comment; no complete
lifecycle, hosted CI, or merge-readiness claim follows from these checks.

Local failed probes remain disclosed: the initial focused-wrapper invocation
omitted its required `--` separator and executed no tests; the first negative
control used the wrong Finding field and did not compile; the first actual
authored-close probe refused the already-ended root on both stores. Subsequent
source changes fixed those defects without retries to green on an unchanged
candidate or any timeout/skip/assertion relaxation.

## Continuation And Tracking

Proposed continuation is the failed lifecycle unit plus the 18 unstarted
units, then all 13 supplements. The 30 fresh prior-head passes are proposed
for explicit reviewer carry, not relabelled as new-head execution. No local
shutdown exception transfers. The old #2353 cancellation and all earlier
failed/interrupted receipts remain visible. Server2 requires the next user
handoff; none of this local repair uses that slot.

Tracking remains #2566 and its existing lifecycle/grammar watchlist mapping.
The parent-class tail and A/E composed proof obligations are unchanged; no
new issue or universal source-mutation framework is justified. Broader guard
coverage is not claimed: actual overlay source and runtime proofs, not merely
introducing a shared helper, support this bounded reconciliation.
