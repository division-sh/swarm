package store_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestContextualRuntimeProducerLineageReadbackParity(t *testing.T) {
	backends := []struct {
		name string
		open func(*testing.T) any
	}{
		{name: "sqlite", open: func(t *testing.T) any { return storetest.StartSQLiteRuntimeStore(t) }},
		{name: "postgres", open: func(t *testing.T) any {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db)
		}},
	}
	manifestations := []struct {
		name        string
		eventType   events.EventType
		class       events.EventAdmissionClass
		producerID  string
		directWrite bool
	}{
		{name: "pipeline_normal_dead_letter", eventType: "platform.dead_letter", class: events.EventAdmissionRuntimeDiagnostic, producerID: "workflow-runtime"},
		{name: "pipeline_direct_dead_letter", eventType: "platform.dead_letter", class: events.EventAdmissionRuntimeDiagnostic, producerID: "workflow-runtime"},
		{name: "pipeline_intercepted_dead_letter", eventType: "platform.dead_letter", class: events.EventAdmissionRuntimeDiagnostic, producerID: "workflow-runtime"},
		{name: "system_node_dead_letter", eventType: "platform.dead_letter", class: events.EventAdmissionRuntimeDiagnostic, producerID: "runtime"},
		{name: "ingress_safety_pause", eventType: "platform.paused", class: events.EventAdmissionRuntimeControl, producerID: "runtime"},
		{name: "ingress_safety_resume", eventType: "platform.resumed", class: events.EventAdmissionRuntimeControl, producerID: "runtime"},
		{name: "active_work_budget_threshold", eventType: "platform.budget_threshold_crossed", class: events.EventAdmissionRuntimeDiagnostic, producerID: "runtime"},
		{name: "agent_panic", eventType: "platform.agent_panic", class: events.EventAdmissionRuntimeDiagnostic, producerID: "runtime"},
		{name: "agent_failure", eventType: "platform.agent_failed", class: events.EventAdmissionRuntimeDiagnostic, producerID: "runtime"},
		{name: "agent_started", eventType: "platform.agent_started", class: events.EventAdmissionRuntimeDiagnostic, producerID: "runtime"},
		{name: "runtime_log", eventType: events.EventTypePlatformRuntimeLog, class: events.EventAdmissionDiagnosticDirect, producerID: "runtime", directWrite: true},
	}

	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			selectedStore := backend.open(t)
			for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
				for _, manifestation := range manifestations {
					t.Run(string(mode)+"/"+manifestation.name, func(t *testing.T) {
						eventID, runID, parentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
						createdAt := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
						parent := eventtest.RunCreatingRootIngressWithMode(parentID, "context.started", "context-proof", "task-contextual", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, createdAt.Add(-time.Second), mode)
						ctx := context.Background()
						storetest.CommitSemanticEvent(t, ctx, selectedStore, parent)
						var event events.Event
						switch manifestation.class {
						case events.EventAdmissionRuntimeControl:
							event = eventtest.RuntimeControl(eventID, manifestation.eventType, manifestation.producerID, "task-contextual", []byte(`{}`), 0, runID, parentID, events.EventEnvelope{}, createdAt)
						case events.EventAdmissionRuntimeDiagnostic:
							event = eventtest.RuntimeDiagnostic(eventID, manifestation.eventType, manifestation.producerID, "task-contextual", []byte(`{}`), 0, runID, parentID, events.EventEnvelope{}, createdAt)
						case events.EventAdmissionDiagnosticDirect:
							event = eventtest.DiagnosticDirect(eventID, manifestation.eventType, manifestation.producerID, "task-contextual", []byte(`{}`), 0, runID, parentID, events.EventEnvelope{}, createdAt)
						default:
							t.Fatalf("unsupported contextual class %s", manifestation.class)
						}
						event = eventtest.InExecutionMode(event, mode)
						if manifestation.directWrite {
							dialect := runtimeauthoractivity.DialectSQLite
							if backend.name == "postgres" {
								dialect = runtimeauthoractivity.DialectPostgres
							}
							storetest.InsertCanonicalEventRecord(t, ctx, selectedDatabase(t, selectedStore), dialect, event)
						} else {
							storetest.CommitSemanticEvent(t, ctx, selectedStore, event)
						}
						got := storetest.LoadCanonicalEventRecord(t, ctx, selectedStore, eventID)
						if got.AdmissionClass() != manifestation.class || got.RunID() != runID || got.ParentEventID() != parentID || got.TaskID() != "task-contextual" || got.ExecutionMode() != mode || !got.Producer().Equal(event.Producer()) {
							t.Fatalf("canonical readback = class:%s run:%s parent:%s task:%s mode:%s producer:%v", got.AdmissionClass(), got.RunID(), got.ParentEventID(), got.TaskID(), got.ExecutionMode(), got.Producer())
						}
					})
				}
			}
		})
	}
}

func selectedDatabase(t *testing.T, selectedStore any) *sql.DB {
	t.Helper()
	switch selected := selectedStore.(type) {
	case *store.PostgresStore:
		return selected.DB
	case *store.SQLiteRuntimeStore:
		return selected.DB
	default:
		t.Fatalf("selected store %T has no canonical database", selectedStore)
		return nil
	}
}
