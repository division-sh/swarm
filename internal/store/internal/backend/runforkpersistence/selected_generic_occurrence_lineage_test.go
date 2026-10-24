package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func selectedGenericPublicationFixture(t *testing.T, root, completion bool) (genericschedule.Activation, eventrecord.Record) {
	t.Helper()
	_, _, _, rows := arrivalJoinInventoryFixture(t, root, completion)
	activation := rows[0].Canonical()
	for _, row := range rows {
		if row.Status == genericschedule.StatusActive {
			activation = row.Canonical()
			break
		}
	}
	activation.Status = genericschedule.StatusFired
	activation.CurrentEventID = genericschedule.OccurrenceEventID(activation.ID, activation.CurrentDueAt)
	activation.CurrentEventAdmittedAt = activation.CurrentDueAt.Add(time.Hour)
	if activation.CurrentEventAdmittedAt.Before(activation.AdmittedAt) {
		activation.CurrentEventAdmittedAt = activation.AdmittedAt
	}
	activation.FiredAt, activation.AcceptedAt = activation.CurrentEventAdmittedAt, activation.CurrentEventAdmittedAt
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Encode(activation.Command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	event := retainedRuntimeControlPublicationFixture(t, activation.Command.RunID, events.EventFacts{ID: activation.CurrentEventID, Type: events.EventType(activation.Command.EventType),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: genericschedule.OccurrenceProducerID()},
		TaskID:   activation.Command.TaskID, Payload: payload,
		Envelope:      events.EventEnvelope{EntityID: activation.Command.EntityID, FlowInstance: activation.Command.FlowInstance},
		RoutingSource: activation.Command.RoutingSource, CreatedAt: activation.CurrentDueAt, ExecutionMode: activation.Command.ExecutionMode,
	})
	digest, err := events.JoinOccurrencePayloadDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := events.NewPlatformPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), EventKey: activation.Command.EventType,
		SchemaDigest: digest, SchemaClass: events.PayloadSchemaPlatform,
	}, events.EventPayloadProducer(event, ""), events.JoinOccurrencePayloadReference)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := events.NewPayloadAdmission(payload, schema)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, admission)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryMatchedNoRecipient, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	return activation, record
}

func retainedRuntimeControlPublicationFixture(t *testing.T, runID string, facts events.EventFacts) events.Event {
	t.Helper()
	event := eventtest.RuntimeControlWithRoutingSourceAndMode(facts.ID, facts.Type, facts.Producer.ID, facts.TaskID,
		facts.Payload, facts.ChainDepth, runID, "", facts.Envelope, facts.RoutingSource, facts.CreatedAt, facts.ExecutionMode)
	expectedEnvelope := facts.Envelope
	expectedEnvelope.Source = facts.RoutingSource.Route()
	if event.ID() != facts.ID || event.Type() != facts.Type || event.RunID() != runID || event.ParentEventID() != "" ||
		event.AdmissionClass() != events.EventAdmissionRuntimeControl || event.ProducerType() != facts.Producer.Type ||
		event.Producer().ID() != facts.Producer.ID || event.TaskID() != facts.TaskID || event.ChainDepth() != facts.ChainDepth ||
		event.ExecutionMode() != facts.ExecutionMode || event.RoutingSource() != facts.RoutingSource || !event.CreatedAt().Equal(facts.CreatedAt) ||
		!reflect.DeepEqual(event.Payload(), facts.Payload) || !reflect.DeepEqual(event.Envelope(), expectedEnvelope.Normalized()) {
		t.Fatal("retained runtime-control fixture changed the original publication facts")
	}
	return event
}

func TestSelectedGenericOccurrenceLineageRequiresExactPublicationBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, root := range []bool{false, true} {
			for _, completion := range []bool{false, true} {
				for _, fault := range []string{"exact", "missing_event", "read_error", "inventory_error", "foreign_run", "prepared", "wrong_event", "wrong_task", "wrong_payload", "wrong_mode", "duplicate"} {
					t.Run(map[bool]string{false: "sqlite/", true: "postgres/"}[postgres]+map[bool]string{false: "flow/", true: "root/"}[root]+map[bool]string{false: "timeout/", true: "complete/"}[completion]+fault, func(t *testing.T) {
						activation, record := selectedGenericPublicationFixture(t, root, completion)
						before := activation.Canonical()
						inventory := []genericschedule.Activation{activation}
						var inventoryErr error
						switch fault {
						case "foreign_run":
							inventory[0].Command.RunID = uuid.NewString()
						case "prepared":
							inventory[0].Status, inventory[0].FiredAt, inventory[0].AcceptedAt = genericschedule.StatusActive, time.Time{}, time.Time{}
						case "inventory_error":
							inventoryErr = errors.New("native census failed")
						case "wrong_event":
							record.EventID = uuid.NewString()
						case "wrong_task":
							record.TaskID = "unrelated-task"
						case "wrong_payload":
							record.Payload = []byte(`{"hostile":true}`)
						case "wrong_mode":
							record.ExecutionMode = executionmode.Mock
						case "duplicate":
							inventory = append(inventory, activation)
						}
						tx, mock := startSnapshotTransaction(t)
						if fault != "foreign_run" && fault != "prepared" && fault != "inventory_error" {
							query := mock.ExpectQuery(`FROM events e`).WithArgs(activation.CurrentEventID)
							if fault == "read_error" {
								query.WillReturnError(errors.New("exact event read failed"))
							} else {
								query.WillReturnRows(selectedInputRecordRows(record, !postgres, fault != "missing_event"))
							}
						}
						ids, err := selectedContractGenericScheduleLineage(t.Context(), tx, activation.Command.RunID, postgres,
							func(_ context.Context, actual *sql.Tx, dialect bool, runID string) ([]genericschedule.Activation, error) {
								if actual != tx || dialect != postgres || runID != activation.Command.RunID {
									t.Fatal("lineage changed the exact native frame")
								}
								return inventory, inventoryErr
							})
						if fault == "exact" {
							if err != nil || !reflect.DeepEqual(ids, []string{activation.CurrentEventID}) {
								t.Fatalf("canonical publication lineage: ids=%v err=%v", ids, err)
							}
						} else if err == nil || ids != nil {
							t.Fatalf("unproven publication became lineage: ids=%v err=%v", ids, err)
						}
						if !reflect.DeepEqual(before, activation) {
							t.Fatal("lineage read changed the historical activation")
						}
						if err := mock.ExpectationsWereMet(); err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}

func expectEmptyGenericOccurrenceCensus(mock sqlmock.Sqlmock, runID string) {
	mock.ExpectQuery(`FROM timers WHERE run_id = .* AND status =`).WithArgs(runID, "fired").WillReturnRows(sqlmock.NewRows([]string{"activation"}))
}
