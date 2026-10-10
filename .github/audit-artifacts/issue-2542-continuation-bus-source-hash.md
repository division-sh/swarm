# Cohort 74: exact admitted run-source hash

Both publication fixtures use native selected-store construction. Direct
publication seeds its identical event-origin/source run through the existing
lifecycle fixture. Actual admitted source artifacts, explicit SourceArtifactFact,
exact direct recipient identity/subscription, publication calls and source-hash
assertions are unchanged.

Both exact run-row SELECTs consume the existing ReadSelectedForkRunBundleHash
port. Despite its historical fixture name, its implementation reads only
bundle_hash by exact canonical run ID; no fork status, eligibility, current-head
selection, fallback, reconstruction or projection inference participates.
No new SQL owner or facade is introduced. The whole-query oracle proves that
this is the same storage witness as both former direct SELECTs.

The two actual PostgreSQL publication roots and the existing reader's new
both-store controls prove exact source hash, original read transaction/no writes,
foreign run isolation and invalid/canceled/closed/missing owner refusal without
partial evidence. Whole-function finite snapshots preserve the complete workload.
This is approved fixture/observation consumption under the existing raw_sql_policy,
not new runtime semantics; no spec amendment or tracker/watchlist expansion.
Remaining parent migration and final integrated closure are still open.
