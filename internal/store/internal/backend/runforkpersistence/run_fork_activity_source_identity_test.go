package runforkpersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func continuedActivityIdentityFixture(t *testing.T) (selectedInputReadFixture, runfork.RunForkSelectedContractSourceEvent, activityidentity.Fact) {
	t.Helper()
	f := newSelectedInputReadFixture(t)
	f.record.EventName, f.lineage.EventName = runForkActivityRequestEvent, runForkActivityRequestEvent
	request := runForkActivityRequestPayload{SourceRunID: f.record.RunID, SourceEventID: f.record.EventID, EntityID: f.record.RunID,
		FlowID: ".", NodeID: activityidentity.MustAgentOwner("provider").Key(), HandlerEventKey: "request.started", ActivityID: "compute", Tool: "compute",
		SuccessEvent: "compute.succeeded", FailureEvent: "compute.failed"}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	fields["input"] = map[string]any{}
	f.record.Payload, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := f.record.Decode()
	if err != nil {
		t.Fatal(err)
	}
	original := admitted.Event()
	source := runfork.RunForkSelectedContractSourceEvent{SourceEventID: original.ID(), EventName: string(original.Type()),
		ExecutionMode: original.ExecutionMode(), RoutingSource: original.RoutingSource(), Payload: original.Payload()}
	fact, err := runForkActivityFact(request)
	if err != nil || activityidentity.RequestEventID(fact) == original.ID() {
		t.Fatalf("fixture does not separate physical publication from journal identity: %+v %v", fact, err)
	}
	return f, source, fact
}

func TestContinuedActivityIdentityRequiresExactCommittedPublication(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, test := range []struct {
			name     string
			lineages int
			mutate   func(*selectedInputReadFixture)
		}{
			{"exact", 1, nil},
			{"missing_lineage", 0, nil},
			{"duplicate_lineage", 2, nil},
			{"foreign_source", 1, func(f *selectedInputReadFixture) { f.lineage.SourceRunID = uuid.NewString() }},
			{"foreign_child", 1, func(f *selectedInputReadFixture) { f.lineage.ForkRunID = uuid.NewString() }},
			{"foreign_request", 1, func(f *selectedInputReadFixture) { f.lineage.SourceEventID = uuid.NewString() }},
			{"foreign_publication", 1, func(f *selectedInputReadFixture) { f.lineage.ForkEventID = uuid.NewString() }},
			{"different_name", 1, func(f *selectedInputReadFixture) { f.lineage.EventName = "different" }},
			{"different_authority", 1, func(f *selectedInputReadFixture) { f.lineage.SelectionAuthority = "different" }},
			{"different_time", 1, func(f *selectedInputReadFixture) { f.lineage.CreatedAt = f.lineage.CreatedAt.Add(time.Second) }},
		} {
			t.Run(map[bool]string{false: "postgres", true: "sqlite"}[sqlite]+"/"+test.name, func(t *testing.T) {
				f, source, fact := continuedActivityIdentityFixture(t)
				if test.mutate != nil {
					test.mutate(&f)
				}
				tx, mock := startSnapshotTransaction(t)
				mock.ExpectQuery(`FROM events e`).WithArgs(source.SourceEventID).WillReturnRows(selectedInputRecordRows(f.record, sqlite, true))
				expectSelectedAttachmentBinding(mock, f.attachment.evidence.binding)
				expectSelectedInputLineage(mock, f, test.lineages)
				request, continued, err := admitRunForkActivitySourceIdentity(context.Background(), tx,
					runForkSourceStateAdmission{postgres: !sqlite, snapshot: &runForkRevisionSnapshot{RunID: f.record.RunID}}, source, fact)
				if test.name == "exact" {
					if err != nil || !continued || request != activityidentity.RequestEventID(fact) {
						t.Fatalf("exact evidence did not return only the journal identity: request=%s continued=%t error=%v", request, continued, err)
					}
				} else if err == nil || continued || request != "" {
					t.Fatalf("contradictory committed evidence became activity admission: request=%s continued=%t error=%v", request, continued, err)
				}
			})
		}
	}
}

func TestOrdinaryActivityIdentityStillRequiresExactRequest(t *testing.T) {
	f, source, fact := continuedActivityIdentityFixture(t)
	tx, _ := startSnapshotTransaction(t)
	admission := runForkSourceStateAdmission{snapshot: &runForkRevisionSnapshot{RunID: f.record.RunID}}
	source.SourceEventID = activityidentity.RequestEventID(fact)
	request, continued, err := admitRunForkActivitySourceIdentity(context.Background(), tx, admission, source, fact)
	if err != nil || continued || request != source.SourceEventID {
		t.Fatalf("ordinary identity changed: request=%s continued=%t error=%v", request, continued, err)
	}
	if _, _, err := admitRunForkActivitySourceIdentity(context.Background(), nil, admission, source, fact); err == nil {
		t.Fatal("identity admitted without a native attempt")
	}
}
