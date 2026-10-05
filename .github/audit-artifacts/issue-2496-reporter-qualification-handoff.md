# #2394 Qualification Handoff From #2496 / #2525

Source: `a73cabe917975637ab655bc682239ec8d3c18859`, based on
`origin/master@276d7723f`. Remote #2525 remains `a9ba49485`; no checkpoint push.

The managed old-CI non-native census requested 137 roots across six packages.
It completed 89 root receipts: 77 PASS, 12 FAIL. The serveapp package hit its
unchanged 600s aggregate deadline with 48 requested roots unreported. This is
RED/incomplete qualification, not green, flakiness, or proof of all 137 roots.
Exact census: `issue-2496-non-native-census-a73cabe91-red.json`.

Reporter findings, retained without changing source/oracles/deadlines:

- `TestIssue2394ReporterFiveHundredDelayedCommitsBothStores/postgres` issued
  all 500 rows in 68.056s, within its unchanged 120s target. At the later
  quiescence failure, its summary had cursor/committed 500, owed 0, settled 491,
  unsettled 9 and 29 active work leases; `WaitForQuiescence` timed out waiting
  for 26 leases. The conformance package failed in 427.615s.
- `TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores/postgres`
  failed its isolated served subprocess after 469.45s. The containing serveapp
  aggregate subsequently hit 600.016s while its reporter root remained active.
  The remaining 48 requested roots receive no passing credit.
- The failed conformance attempt's owner snapshot reports 2,646 begun
  transactions, 1,468 read commits and 1,178 write commits, no commit/cleanup
  failures, and 13m13.8s summed injected delay. This is diagnostic aggregate
  work across the attempt, not wall-clock elapsed time or a production timeout.

Full raw receipt is local
`/tmp/agent-e-2496-non-native-ci-revalidation.log`. The two reporter roots retain
their existing exact cardinality, durable chunk acknowledgement, descendant
settlement, quiescence, transaction-owner and timing assertions. The initial
matrix also selected ordinary API/runtime/catalog/public consumers; subsequent
reporter isolation must not be presented as a passing aggregate receipt.

Following the existing division of ownership, reporter timing/pressure remains
A's #2394 responsibility, not a speculative additional E performance refactor.
E is correcting its stale constructed-header/public-lifecycle test consumers
and has separately escalated the actual operation-free failure-discard ordering
defect on #2496. Neither is a waiver of these reporter failures. Please reconcile
the current reporter receipt against #2394 and indicate the bounded disposition
or required exclusive rerun; no merge approval or deadline increase is requested.
