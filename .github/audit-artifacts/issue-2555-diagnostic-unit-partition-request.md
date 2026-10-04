## Q06 diagnostic unit-cost disposition requested

The pre-#2525 clean `77e9de2ac` full run is diagnostic-only and RED. No final
qualification or complete-class closure is claimed. The user's ladder is
focused iteration, core, then one final full after actual #2525 integration;
no further full has been started and G has released server2's test capacity.

### Concrete fixture repair

`TestProviderAliasesDeliverOnlyLocalAndConnectedConsumersBothStores` failed
only its agent-consumers and agent-replay scenarios on both stores: their
workspace stub cannot execute the required native worker. The one-line
fixture correction at `5f3bae30f` selects the existing real host-workspace
helper only for the explicitly agent-bearing scenarios. All 14 scenarios pass
on both stores (88.756s, captured before commit). Authentication, routing,
exact receipts, replay and source/cardinality assertions are unchanged.

### Distinct accumulated unit cost

`serveapp-other-late` also hit its unchanged cumulative ten-minute Go timeout.
The interrupted direct-restart root had been running for 15 seconds, with
the PostgreSQL static subcase at four seconds. Its separate focused root
passes all six backend/geometry cases in 15.376s; that is not a fix for the
aggregate timeout.

The exact unit records show the existing
`TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores` consumed
266.162 seconds between its run/pass records. Its parent `Elapsed: 0` excludes
the parallel isolated child lifetime and must not be used as its cost.
The served reporter unit independently consumed 659.899 seconds, so moving
this proof into that existing 15-minute unit is not a safe remedy either.
Neither reporter test nor its existing child owner is changed by #2555.
New native mock execution does add real work to other roots in this unit;
the timing finding is not attributed solely to an unrelated master defect.

### Bounded proposal

Request reviewer-g's disposition for isolating the complete one-second
preservation root in one mandatory lifecycle/full unit through the existing
planner, removing only that exact root from `serveapp-other-late`. Keep both
stores, every assertion, the child/parent timeout policy, count=1, selected
environment, proof frequency and exact-one-owner census unchanged. Do not
raise a performance deadline, lower a tier or omit any proof. No new runner
or orchestration owner is proposed.

This is a proposed partition, not implemented or qualified. The full RED log
is retained at `docs/audits/evidence/2555/worker-77e9-independent-full.log.gz`
in swarm-docs; source checkpoint and limits are in the existing progress and
proof-accounting artifacts. #2525 remains independently E-owned. Existing
#2353 owns the test-health/timing record; no new issue is requested.
