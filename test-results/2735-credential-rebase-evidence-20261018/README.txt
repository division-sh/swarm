#2735 conflict-only rebase for reviewer-e LIGHT confirmation.

Old published head: 95464fccbd3d3efa4238b71325d3685f25c93644.
Old base: 909222b50cf88232d81af10b6fcc46ab78916fbe.
Delivered master: 1d3cb55f05f738dc40fb112bac78bd975490e778.
New signed head: 7dc9ac1eab1391d2371e4accb150ed635085d4d3.
New tree: 0e92e69144156acd20f275655ff6cef8bdee21af.

Range-diff: 26 patches equal; one patch differs only in the generated
buildRuntimeComposition census hash. No dropped or added patches.
Single conflict: internal/cliapp/testdata/verify_admission_predicate_census.json.
Canonical owner refresh: SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1,
TestVerifyBootStartupPredicateCensus, through swarm-test admission; PASS.
Master-only hash: 8c08d353f298f0725cc7bb0bb4f5118e29395b41e71f391ffdbb996cebf1d3c7.
Old branch-only hash: 54637bd3422f5b199870e509cdac5468148d6b1f6944d05a49afa31b5bc08c75.
Composed hash: 443280ee7978335baff695af3af2fd042812aae26dc2ad5765dc0243c266f6a6.

No production repair, timeout change, test deletion or additional diagnostic
invocation. This rebase does not classify away the retained RED.
Reviewer-e classification: known pre-existing #2701 recovery-scope defect;
#2735's contribution to reaching it remains UNCLASSIFIED.
https://github.com/division-sh/swarm/pull/2735#issuecomment-6255113595
Orchestrator disposition authorizes one normal qualification at the rebased head:
https://github.com/division-sh/swarm/pull/2735#issuecomment-6255124674
Same signature recurrence routes to #2701, with no further run.

No PR push, hosted run or final qualification claimed in this packet.
