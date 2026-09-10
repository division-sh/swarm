# Cycle-3 Full-Load Startup Evidence And Abort Dependency

The single authorized full-profile qualification at e1b6ecd52 completed exit1.
Exactly two packages failed: apiv1 (two tests) and releasee2e (one test).
Full log /tmp/agent-g-2441-r3-full.log, SHA256
82ff13118d8dd43c4abe82358bac97c71c4e5bc2027148d4e98f6e5370600cd3.
No full-suite rerun was launched.

## Captured Failure, Not Historical Reconstruction

TestCompiledProcessLifecycleStartupEvidence failed its first real dev-mode
readiness wait after88.19s, before its deliberately canceled observer exercise.
The H child is PID1271104, first/only child of that test; compiled internal
retained mock lifecycle, not a live provider. Its evidence handler captured:

- ready=false, failed=true.
- Startup phase16 manager_event_loop_start FAILED:
  **pipeline recovery blocked before explicit exhaustion**.
- SQLite fresh dev-scratch selected store; no persisted recovery snapshot;
  source topology/hydration/workspace/provider preflight phases succeeded.
- Runtime shutdown reported one active work lease after its grace budget.
- Main goroutine was blocked in RuntimeOccurrence.RetireAndWait(background),
  Runtime.stopWithOptions(runtime.go:2125), PreparedStartup's failure defer
  (1731), PreparedStartup.Start (1525), then serve composition release/rollback
  (main.go:2586/2641). There is no manager/coordinator worker in the captured
  stack. The stack capture completed normally (8248 bytes).

This is a new captured manifestation in the same dev fixture family used by J5.
It is NOT evidence that the missing historical LSF-032 cause has been recovered,
nor that the exact J5 row failed in this run. The full J1-J5 profile produced no
failure, while this separate compiled evidence test failed on initial startup.

## Source-Level Dependency Cycle

The supported composed-start path has a concrete circular cleanup dependency:

1. prepareServeRuntimeContextSet prepares a runtime and standing targets, then
   RuntimeContextManager.Register publishes the runtime context before release.
2. Register calls newStandingOccurrencesLocked (context_manager.go:483/596).
   Each active standing service creates a child of RuntimeOccurrence and retains
   its parent lease for the standing occurrence lifetime.
3. PreparedStartup.Start's error defer calls Runtime.stopWithOptions itself
   (runtime.go:1731). That joins the whole RuntimeOccurrence before returning.
4. The context-manager-owned standing occurrence is retired by
   DeactivateBundleHashWithOptions (context_manager.go:2341-2357), not by the
   lower runtime's manager/route cleanup.
5. The composition rollback that invokes that deactivation is deferred until
   release.Start returns (serveapp/main.go:2543-2551 and2641).

Therefore, a failed release with a registered standing child can wait for a
standing occurrence whose owner cannot retire it until that same release
returns. The captured stack follows that lower-level join and reports one lease,
consistent with the one standing service in the fixture. The signal does not
expose the lease's identity, so I do not label that identity directly observed.
This is the precise dependency-cycle stop condition in the cycle-3 ruling,
not a request to suppress shutdown errors or detach work after grace.

Separately, RecoverToExhaustion's Blocked comes from the existing pipeline owner:
bus/sweeper.go can report paused ingress or a locally blocked claimed pass;
pipeline/coordinator_recovery.go:59 fails before explicit exhaustion. The captured
message does not identify which blocked event/cursor/branch caused this pass.
That admission failure must remain visible and needs exact classification; do
not automatically retry it or declare an empty fresh store proof of exhaustion.

## Requested Bounded Disposition

Keep the implemented continuation-before-manager repair; this cycle is a
different owner edge, composed startup rollback versus lower runtime teardown.
Please ratify the exact existing composition/PreparedStartup cleanup ownership
repair and the required blocked-pass proof before changing it. The direction is
to let the owner of registered standing contexts retire those children before
joining their parent, while preserving standalone Runtime.Start cleanup. No
second startup framework, timeout workaround, lifetime detachment or blanket
recovery retry is proposed.

Keep tracking on #2321/#2353 with #2250 architecture feedback. The separate
PostgreSQL query-provenance ruling is on existing #2442 (issuecomment-5611438051).
No new issue or POTENTIAL_ISSUES entry. Production is frozen at e1b6ecd52 and the
PR remains not review-ready. Full selected-store runtimepersistence passed507.803s,
runtime116.100s and conformance178.834s, but none waives these three full-load
failures. No additional live calls or settled-delivery replay.
