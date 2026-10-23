package main

// #2720/6247003514 authorizes only these two observations at these exact cuts.
const n19FanOutBarrierIdentity = "native-fanout-barrier-authority/internal/store/internal/runtimepersistence/fan_out_delivery_barrier_selected_store_test.go/TestFanOutDeliveryBarrierMixedDispositionLifecycleOnBothStores"

const n19ArmedBarrierAnchor = "\t\t\tassertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)\n"

const n19ArmedBarrierObservation = `			report, err := owner.(selectedFanOutDiagnosticOwner).LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if projected := operatorread.ProjectRunOperationalStatus(report); report.TestQuiescence.FanOutBarriers != 1 || projected.State != "running" || len(projected.Attributions) != 0 {
				t.Fatalf("armed delivery barrier was diagnosed as an agent stall: status=%+v work=%+v", projected, report.TestQuiescence)
			}
`

const n19PendingBarrierAnchor = `			if summary.BarrierArmed != 0 || summary.BarrierPending != 1 || summary.BarrierTerminal != 0 || !summary.BlocksCompletion() {
				t.Fatalf("mixed barrier public summary = %#v", summary)
			}
`

const n19PendingBarrierObservation = `			report, err = owner.(selectedFanOutDiagnosticOwner).LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if projected := operatorread.ProjectRunOperationalStatus(report); report.TestQuiescence.FanOutBarriers != 1 || projected.State != "running" || len(projected.Attributions) != 0 {
				t.Fatalf("pending barrier outcome was diagnosed as an agent stall: status=%+v work=%+v", projected, report.TestQuiescence)
			}
`
