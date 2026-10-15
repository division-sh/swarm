# Deferred selected-session execution

`q1_session_sibling_refork_execution_test.go.txt` preserves the positive
execution probe from signed `0d742b0ccee5da858e8f9536dae64d704970d3b0` unchanged.
Its original caller is the `q1_session_sibling_refork` subtest of
`TestSelectedCompletion2270EvidenceBothStores` at that commit.

Both stores refuse the second same-source sibling as
`selected-contract source event admission state forked is unsupported`, and
the child-input refork as
`selected-contract committed replay-scope marker policy blocked: source_committed_replay_scope_advanced_after_fork_point`.
Both refusals reproduce on master `13c70dbca4` and the Q1 candidate. The positive
execution journeys remain open under #642, owned by its source-admission lane;
they are not successful Q1 coverage or permission to relax either refusal.

Reviewer-c disposition: #2592 comment 6202442147. Original candidate/base RED
receipts: #2592 comment 6202380810. The active Q1 tests separately prove supported
first-child execution and preservation of these exact existing refusals.
