package pipeline

import "testing"

func TestQ10NoStageHistoryRetainsInternalLifecycle(t *testing.T) {
	record := WorkflowTargetPersistenceRecord{
		Presence: WorkflowTargetPersistenceCompleteFieldless,
		Lifecycle: WorkflowLifecycleCompanionPersistenceRecord{
			StageDefined: false,
			State: WorkflowInstancePersistenceRecord{
				CurrentState: "pending",
				Bookkeeping:  []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`),
			},
		},
	}
	got, err := trackedWorkflowTargetProjectionForTest(record)
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentState == nil || *got.CurrentState != "pending" {
		t.Fatalf("constructed internal lifecycle lost: %v", got.CurrentState)
	}
	record.Presence = WorkflowTargetPersistenceStateOnly
	record.State.Fields = []byte(`{}`)
	got, err = trackedWorkflowTargetProjectionForTest(record)
	if err != nil || got.CurrentState != nil {
		t.Fatalf("import acquired lifecycle: %+v err=%v", got, err)
	}
}
