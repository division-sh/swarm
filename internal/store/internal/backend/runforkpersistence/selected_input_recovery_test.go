package runforkpersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

type selectedInputReadFixture struct {
	attachment selectedAttachmentFixture
	source     runfork.RunForkSelectedContractSourceEvent
	schema     events.PayloadSchemaBinding
	lineage    runfork.RunForkSelectedContractExecutionLineage
	record     eventrecord.Record
}

func newSelectedInputReadFixture(t *testing.T) selectedInputReadFixture {
	t.Helper()
	f := newSelectedAttachmentFixture(t, runfork.RunForkPointRunStart)
	routing, err := events.NewRootRoutingSource(f.evidence.binding.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	source := runfork.RunForkSelectedContractSourceEvent{
		SourceEventID: uuid.NewString(), EventName: "task.start", ExecutionMode: executionmode.Live,
		Payload: []byte(`{"large":9007199254740993,"decimal":1.0000000000000000001}`), RoutingSource: routing,
	}
	schema, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: f.snapshot.BundleHash, FlowID: ".", EventKey: source.EventName,
		SchemaDigest: "sha256:" + fmt.Sprintf("%064x", 1), SchemaClass: events.PayloadSchemaAuthored,
	})
	if err != nil {
		t.Fatal(err)
	}
	lineage := runfork.RunForkSelectedContractExecutionLineage{
		ForkRunID: f.evidence.binding.ForkRunID, SourceRunID: f.evidence.binding.SourceRunID, SourceEventID: source.SourceEventID,
		ForkEventID: activityidentity.ForkLineageEventID(f.evidence.binding.ForkRunID, source.SourceEventID), EventName: source.EventName,
		SelectionAuthority: "original-publication-owner", CreatedAt: time.Unix(100, 0).UTC(),
	}
	selected, err := events.NewSelectedForkLineage(lineage.ForkRunID, lineage.SourceRunID, lineage.SourceEventID, lineage.SelectionAuthority, "", source.ExecutionMode)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Lineage: selected,
		Facts: events.EventFacts{
			ID: lineage.ForkEventID, Type: events.EventType(source.EventName), Payload: source.Payload,
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: lineage.SelectionAuthority},
			Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, routing.Route()), RoutingSource: routing,
			CreatedAt: lineage.CreatedAt, ExecutionMode: source.ExecutionMode,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := events.NewPayloadAdmission(source.Payload, schema)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, payload)
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
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteSelectedForkPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	return selectedInputReadFixture{attachment: f, source: source, schema: schema, lineage: lineage, record: record}
}

func selectedInputRecordRows(record eventrecord.Record, sqlite, found bool) *sqlmock.Rows {
	columns := make([]string, 32)
	for i := range columns {
		columns[i] = fmt.Sprintf("column_%d", i)
	}
	rows := sqlmock.NewRows(columns)
	if !found {
		return rows
	}
	var createdAt driver.Value = record.CreatedAt
	if sqlite {
		createdAt = record.CreatedAt.Format(time.RFC3339Nano)
	}
	return rows.AddRow(record.Class, record.EventID, record.RunID, record.EventName, record.TaskID,
		record.EntityID, record.FlowInstance, record.Scope, record.Payload,
		record.PayloadSchemaBundleHash, record.PayloadSchemaFlowID, record.PayloadSchemaEventKey, record.PayloadSchemaDigest,
		record.PayloadSchemaClass, record.ExecutionMode, record.ChainDepth, record.ProducedBy, record.ProducedByType,
		record.SourceEventID, createdAt, record.RoutingSourceKind, record.RoutingSourceAuthority,
		record.SourceRoute, record.TargetRoute, record.TargetSet, record.RouteSettlement, record.OperatorReferencedEventID,
		record.SelectedForkSourceRunID, record.SelectedForkSourceEventID, record.SelectedForkAuthorityStamp,
		record.SelectedForkLineageOwners, record.InheritedFanOutOrigin)
}

func expectSelectedInputLineage(mock sqlmock.Sqlmock, f selectedInputReadFixture, count int) {
	rows := sqlmock.NewRows([]string{"child", "source", "source_event", "child_event", "name", "authority", "created"})
	for i := 0; i < count; i++ {
		lineage := f.lineage
		rows.AddRow(lineage.ForkRunID, lineage.SourceRunID, lineage.SourceEventID, lineage.ForkEventID, lineage.EventName, lineage.SelectionAuthority, lineage.CreatedAt)
	}
	mock.ExpectQuery(`FROM run_fork_selected_contract_executions WHERE`).
		WithArgs(f.attachment.evidence.binding.ForkRunID, f.source.SourceEventID,
			activityidentity.ForkLineageEventID(f.attachment.evidence.binding.ForkRunID, f.source.SourceEventID)).WillReturnRows(rows)
}

func TestSelectedForkInputReadRequiresCanonicalRequest(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*selectedInputReadFixture)
	}{
		{"authority", func(f *selectedInputReadFixture) { f.attachment.authority = effects.Authority{} }},
		{"source_id", func(f *selectedInputReadFixture) { f.source.SourceEventID += " " }},
		{"source_name", func(f *selectedInputReadFixture) { f.source.EventName += " " }},
		{"mode", func(f *selectedInputReadFixture) { f.source.ExecutionMode = "" }},
		{"payload", func(f *selectedInputReadFixture) { f.source.Payload = []byte(`[]`) }},
		{"schema", func(f *selectedInputReadFixture) { f.schema = events.PayloadSchemaBinding{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSelectedInputReadFixture(t)
			test.mutate(&f)
			if err := validateSelectedForkInputReadRequest(context.Background(), f.attachment.authority, f.source, f.schema); err == nil {
				t.Fatal("invalid lookup request admitted")
			}
		})
	}
	f := newSelectedInputReadFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateSelectedForkInputReadRequest(ctx, f.attachment.authority, f.source, f.schema); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestSelectedForkInputPublishedStrictBothDialects(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, test := range []struct {
			name       string
			lineages   int
			eventFound bool
			corrupt    bool
			mutate     func(*selectedInputReadFixture)
		}{
			{name: "exact_old_publication", lineages: 1, eventFound: true},
			{name: "absent"},
			{name: "lineage_without_event", lineages: 1, corrupt: true},
			{name: "event_without_lineage", eventFound: true, corrupt: true},
			{name: "multiple_lineages", lineages: 2, corrupt: true},
			{name: "wrong_source", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.SourceRunID = uuid.NewString() }},
			{name: "wrong_child", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.ForkRunID = uuid.NewString() }},
			{name: "wrong_source_event", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.SourceEventID = uuid.NewString() }},
			{name: "wrong_child_event", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.ForkEventID = uuid.NewString() }},
			{name: "wrong_name", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.EventName = "task.other" }},
			{name: "wrong_authority", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.SelectionAuthority = "another-publisher" }},
			{name: "wrong_time", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.lineage.CreatedAt = f.lineage.CreatedAt.Add(time.Second) }},
			{name: "mode", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.ExecutionMode = executionmode.Mock }},
			{name: "number_loss", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) {
				f.record.Payload = []byte(`{"large":9007199254740992,"decimal":1.0000000000000000001}`)
			}},
			{name: "byte_change", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.Payload = append([]byte(" "), f.record.Payload...) }},
			{name: "schema_digest", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.PayloadSchemaDigest = "sha256:" + fmt.Sprintf("%064x", 2) }},
			{name: "schema_bundle", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) {
				f.record.PayloadSchemaBundleHash = "bundle-v2:sha256:" + fmt.Sprintf("%064x", 2)
			}},
			{name: "schema_flow", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.PayloadSchemaFlowID = "other" }},
			{name: "schema_key", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.PayloadSchemaEventKey = "task.other" }},
			{name: "schema_class", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.PayloadSchemaClass = events.PayloadSchemaGenerated }},
			{name: "source_route", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) {
				f.record.SourceRoute = []byte(`{"entity_id":"` + uuid.NewString() + `"}`)
			}},
			{name: "unknown_envelope", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.TargetRoute = []byte(`{"unknown":"corrupt"}`) }},
			{name: "missing_joined_lineage", lineages: 1, eventFound: true, corrupt: true, mutate: func(f *selectedInputReadFixture) { f.record.SelectedForkLineageOwners = 0 }},
		} {
			t.Run(map[bool]string{false: "postgres", true: "sqlite"}[sqlite]+"/"+test.name, func(t *testing.T) {
				f := newSelectedInputReadFixture(t)
				if test.mutate != nil {
					test.mutate(&f)
				}
				tx, mock := startSnapshotTransaction(t)
				expectSelectedInputLineage(mock, f, test.lineages)
				if test.lineages < 2 {
					mock.ExpectQuery(`FROM events e`).WithArgs(activityidentity.ForkLineageEventID(f.attachment.evidence.binding.ForkRunID, f.source.SourceEventID)).
						WillReturnRows(selectedInputRecordRows(f.record, sqlite, test.eventFound))
				}
				published, err := readSelectedForkInputPublishedTx(context.Background(), tx, f.attachment.evidence.binding, f.source, f.schema, sqlite)
				if test.corrupt {
					if published || !errors.Is(err, eventrecord.ErrCorrupt) {
						t.Fatalf("corruption became missing/matching: published=%v err=%v", published, err)
					}
				} else if err != nil || published != test.eventFound {
					t.Fatalf("published=%v err=%v want=%v", published, err, test.eventFound)
				}
			})
		}
	}
}

func TestSelectedForkInputReadTerminalStillRequiresExactCurrentFence(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, current := range []bool{false, true} {
			t.Run(fmt.Sprintf("sqlite=%v/current=%v", sqlite, current), func(t *testing.T) {
				f := newSelectedInputReadFixture(t)
				f.attachment.snapshot.State = runlifecycle.StateCompleted
				tx, mock := startSnapshotTransaction(t)
				expectSelectedAttachmentBinding(mock, f.attachment.evidence.binding)
				expectSelectedAttachmentRecord(t, mock, f.attachment, sqlite, false)
				a := f.attachment.authority
				mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM run_fork_selected_contract_runtime_executions`).
					WithArgs(a.SelectedFork.ExecutionID, a.SelectedFork.ForkRunID, a.SelectedFork.Generation,
						a.ExecutionOwner, a.FenceGeneration, a.SelectedFork.AdmissionFingerprint,
						a.SelectedFork.ContainerPlanFingerprint, a.SelectedFork.ActorCensusFingerprint, a.SelectedFork.EffectiveConfigFingerprint).
					WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(current))
				binding, err := requireSelectedForkInputReadBindingTx(context.Background(), tx, f.attachment.snapshot, a, f.schema, sqlite)
				if current {
					if err != nil || binding != f.attachment.evidence.binding {
						t.Fatalf("terminal immutable read failed: %v", err)
					}
				} else if err == nil {
					t.Fatal("stale fence admitted")
				}
			})
		}
	}
}

func TestSelectedForkInputReadRejectsDifferentChildArtifactBeforeLookup(t *testing.T) {
	f := newSelectedInputReadFixture(t)
	f.attachment.snapshot.BundleHash = "bundle-v2:sha256:" + fmt.Sprintf("%064x", 2)
	if _, err := requireSelectedForkInputReadBindingTx(context.Background(), nil, f.attachment.snapshot, f.attachment.authority, f.schema, false); err == nil {
		t.Fatal("different child artifact admitted")
	}
}
