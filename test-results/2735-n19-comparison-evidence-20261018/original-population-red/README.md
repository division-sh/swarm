# #2735 embedded-spec population RED diagnosis

Source: 24045c6bf5040d2663a1b1724fce0bbdd0b48056.
Pinned master control: 909222b50cf88232d81af10b6fcc46ab78916fbe.
Hosted failure:
https://github.com/division-sh/swarm/actions/runs/39151917009/job/117748302305

Bounded local diagnostic at the unchanged RED source:

    go run ./cmd/swarm-test -- ./tools/fixture-codemod/... -count=1 -json

387 roots / 2166 cases pass; the only failing root is
TestEmbeddedSpecSuccessorsPreserveFiniteHistoryAndAllOtherWork, with population
b3ce87fbab293fbd9165c96d9e6c7dd4a56ebfd0bc7f5392c07b380cdc529758 instead of
f0a2a47c6b09e1abd5b0ed69c3862bb3bfbb22b5c918f7715d7c3f14f2e3247e.
The five Action:skip records are generator packages with no tests, not skipped
guard tests. The complete root list is retained as a TSV alongside the full log.

One pinned-master diagnostic of the exact failed root passes, package 4.896s:

    go run ./cmd/swarm-test -- ./tools/fixture-codemod/pipeline-observations -run '^TestEmbeddedSpecSuccessorsPreserveFiniteHistoryAndAllOtherWork$' -count=1

The already completed catalog-required-verify invocation at this same unchanged
source passed 894 roots / 4106 cases with no failures or skips; see its prior
immutable evidence and checkpoint:
https://github.com/division-sh/swarm/pull/2735#issuecomment-6253213037
It was not repeated to obtain another pass.

The latest fixture commit does not change recipes.json; blob
92e5340ff9aad03e79dbad9c28f35b8af44a6ee5 is identical at 0e78f146d8 and
24045c6bf5. Neither changed CLI test file has a recipe row. Relative to master,
all 893 rows retain their identity/order and Before/After fields. The ONLY
committed recipe delta is row index 424's appended N19 Successor for
native-fanout-barrier-authority / TestFanOutDeliveryBarrierMixedDispositionLifecycleOnBothStores.
It was added by d4d76eb2dd under reviewer-e6247003514. The frozen embedded-spec
packet and its expected population hash are unchanged. Its isolated historical
comparison already reverses other independently reviewed successors, but not
this N19 successor. Therefore this is a previously hidden branch-owned
history-composition gap, not a consequence of the latest mock-array edit.

Proposed repair only: use the existing exact N19 validator to validate the
installed live successor, then reverse that ONE appended field in the temporary
historical comparison copy. Keep the installed recipe, every Before/After,
frozen packet/hash, 893-row population, 12 prior successors, Q10 count 1 and the
16-function/27-call N19 invariant intact. Add strict missing/duplicate/crossed/
changed-current-source controls inside existing roots. No source edits, guard
relaxation, repin, push or hosted run were performed for this proposal.
