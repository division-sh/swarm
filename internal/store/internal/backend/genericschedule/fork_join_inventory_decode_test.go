package genericschedule

import (
	"database/sql/driver"
	"encoding/json"

	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func forkJoinInventoryWire(t *testing.T, row runtimegenericschedule.Activation) []driver.Value {
	t.Helper()
	payload, err := canonicaljson.Encode(row.Command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := json.Marshal(row.Command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	o := row.ForkJoinOrigin
	return []driver.Value{
		row.ID, row.Command.ScheduleKey, row.ImmutableHash, row.Command.RunID, row.Command.EntityID, row.Command.FlowInstance,
		string(row.Command.OwnerKind), row.Command.OwnerID, "", "", "", "", "", row.Command.EventType, payload, routing,
		string(row.Command.ExecutionMode), "", string(row.Command.Due.Kind), row.Command.Due.Absolute, "", "", row.Command.TaskID,
		row.ImmutableHash, row.AdmittedAt, row.InitialDueAt, row.CurrentDueAt, "", nil, string(row.Status), "", nil, nil, nil, nil, "", "", nil,
		o.SourceActivationID, o.SourceRunID, string(o.PointKind), o.PointRevision, o.PointEventID, o.SourceAdmittedAt, o.Owner,
	}
}
