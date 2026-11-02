package runtimepersistence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestStandingServicePostureCensusRejectsEachLiveWorkFamilyBeforeMutation(t *testing.T) {
	families := []string{
		"event_delivery",
		"workflow_timer",
		"generic_schedule",
		"decision_card",
		"activity_attempt",
		"runtime_readiness",
		"raw_pipeline_event",
		"decision_route",
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		for _, family := range families {
			family := family
			t.Run(backend+"/"+family, func(t *testing.T) {
				fixture := openStandingCensusFixture(t, backend)
				ctx := testAuthorActivityContext()
				ctx = originalStoreFixtureContext(t, ctx, fixture.store)
				runID := uuid.NewString()
				ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				seedStandingCensusFamily(t, fixture, ctx, runID, family, executionmode.Live)
				if got := countStandingCensusFamily(t, fixture, ctx, runID, family, executionmode.Live); got != 1 {
					t.Fatalf("seeded %s count = %d, want 1", family, got)
				}

				admitter := fixture.store.(interface {
					AdmitRunExecutionPosture(context.Context, string, executionposture.Posture) error
				})
				err := admitter.AdmitRunExecutionPosture(ctx, runID, executionposture.MockOnly)
				if err == nil || !strings.Contains(err.Error(), "command-selected mock execution") {
					t.Fatalf("AdmitRunExecutionPosture error = %v, want live %s rejection", err, family)
				}
				if got := countStandingCensusFamily(t, fixture, ctx, runID, family, executionmode.Live); got != 1 {
					t.Fatalf("rejected admission changed %s count to %d", family, got)
				}
			})
		}
	}
}

func TestStandingServicePostureCensusAdmitsExactMockWork(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingCensusFixture(t, backend)
			ctx := testAuthorActivityContext()
			ctx = originalStoreFixtureContext(t, ctx, fixture.store)
			runID := uuid.NewString()
			ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			seedStandingCensusFamily(t, fixture, ctx, runID, "raw_pipeline_event", executionmode.Mock)
			admitter := fixture.store.(interface {
				AdmitRunExecutionPosture(context.Context, string, executionposture.Posture) error
			})
			if err := admitter.AdmitRunExecutionPosture(ctx, runID, executionposture.MockOnly); err != nil {
				t.Fatalf("AdmitRunExecutionPosture exact mock work: %v", err)
			}
			if got := countStandingCensusFamily(t, fixture, ctx, runID, "raw_pipeline_event", executionmode.Mock); got != 1 {
				t.Fatalf("mock admission changed raw event count to %d", got)
			}
		})
	}
}

func openStandingCensusFixture(t *testing.T, backend string) authorActivityReceiptFixture {
	t.Helper()
	if backend == "sqlite" {
		return openSQLiteAuthorActivityReceiptFixture(t)
	}
	return openPostgresAuthorActivityReceiptFixture(t)
}

func seedStandingCensusFamily(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID, family string, mode executionmode.Mode) {
	t.Helper()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	newEvent := func(name string) (events.Event, string) {
		eventID := uuid.NewString()
		entityID := uuid.NewString()
		envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
		event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(
			eventID, events.EventType(name), "standing-census", "", json.RawMessage(`{}`), 0,
			runID, envelope, eventtest.RootRoutingSource(entityID), now, mode,
		)
		return event, eventID
	}
	switch family {
	case "event_delivery":
		event, eventID := newEvent("standing.census.delivery")
		if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("standing-census")}); err != nil {
			t.Fatalf("seed standing delivery: %v", err)
		}
		owner := fixture.store.(pipelineObligationParityStore).PipelineObligations()
		settlePipelineParityEvent(t, ctx, owner, eventID, runtimepipelineobligation.Acknowledged("standing_census_delivery"))
	case "workflow_timer":
		row := newWorkflowTimerDDLProofRow(runID)
		row.executionMode = mode
		if err := insertWorkflowTimerDDLProofRow(ctx, fixture.db, fixture.store.(workflowTestSelectedStore), row); err != nil {
			t.Fatalf("seed standing workflow timer: %v", err)
		}
	case "generic_schedule":
		command := testRootGenericScheduleCommand(
			t, runID, uuid.NewString(), "standing-census-"+uuid.NewString(),
			runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
		)
		command.ExecutionMode = mode
		admitGenericScheduleFixture(t, ctx, fixture.store.(runtimegenericschedule.Store), command)
	case "decision_card":
		seedStandingCensusDecisionCard(t, fixture, ctx, runID, mode, false, "")
	case "activity_attempt":
		seedStandingCensusActivityAttempt(t, fixture, ctx, runID, mode)
	case "runtime_readiness":
		seedStandingCensusReadiness(t, fixture, ctx, runID, mode)
	case "raw_pipeline_event":
		event, _ := newEvent("standing.census.raw")
		if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
			t.Fatalf("seed standing raw event: %v", err)
		}
	case "decision_route":
		event, eventID := newEvent("standing.census.route")
		if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
			t.Fatalf("seed standing route event: %v", err)
		}
		seedStandingCensusDecisionCard(t, fixture, ctx, runID, mode, true, eventID)
	default:
		t.Fatalf("unknown standing census family %q", family)
	}
}

func seedStandingCensusDecisionCard(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID string, mode executionmode.Mode, route bool, eventID string) {
	t.Helper()
	cardID := uuid.NewString()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	status := "pending"
	if route {
		status = "decided"
	}
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		if _, err := fixture.db.ExecContext(ctx, `
			INSERT INTO decision_cards (
				card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
				card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
				provenance, verdict, fields, decided_by, decided_at, decision_event_id,
				created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, 'stage_gate', '{}'::jsonb, $3, $4, '{}'::jsonb,
				'card-hash', 'schema-hash', 'bundle-hash', '{}'::jsonb, '{}'::jsonb,
				CASE WHEN $3 = 'decided' THEN 'approve' ELSE NULL END, '{}'::jsonb,
				CASE WHEN $3 = 'decided' THEN 'test' ELSE NULL END,
				CASE WHEN $3 = 'decided' THEN $5::timestamptz ELSE NULL::timestamptz END,
				CASE WHEN $3 = 'decided' THEN $6::uuid ELSE NULL::uuid END, $5::timestamptz, $5::timestamptz
			)
		`, cardID, runID, status, string(mode), now, nullUUID(eventID)); err != nil {
			t.Fatalf("seed postgres standing decision card: %v", err)
		}
		if route {
			if _, err := fixture.db.ExecContext(ctx, `INSERT INTO decision_card_route_obligations (event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3::uuid, 'pending', 0, $4, $4, $4)`, eventID, cardID, runID, now); err != nil {
				t.Fatalf("seed postgres standing decision route: %v", err)
			}
		}
		return
	}
	if _, err := fixture.db.ExecContext(ctx, `
		INSERT INTO decision_cards (
			card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
			card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
			provenance, verdict, fields, decided_by, decided_at, decision_event_id,
			created_at, updated_at
		) VALUES (?, ?, 'stage_gate', '{}', ?, ?, '{}', 'card-hash', 'schema-hash',
			'bundle-hash', '{}', '{}', CASE WHEN ? = 'decided' THEN 'approve' ELSE NULL END,
			'{}', CASE WHEN ? = 'decided' THEN 'test' ELSE NULL END,
			CASE WHEN ? = 'decided' THEN ? ELSE NULL END,
			CASE WHEN ? = 'decided' THEN ? ELSE NULL END, ?, ?)
	`, cardID, runID, status, string(mode), status, status, status, now, status, nullUUID(eventID), now, now); err != nil {
		t.Fatalf("seed sqlite standing decision card: %v", err)
	}
	if route {
		if _, err := fixture.db.ExecContext(ctx, `INSERT INTO decision_card_route_obligations (event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)`, eventID, cardID, runID, now, now, now); err != nil {
			t.Fatalf("seed sqlite standing decision route: %v", err)
		}
	}
}

func nullUUID(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func seedStandingCensusActivityAttempt(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID string, mode executionmode.Mode) {
	t.Helper()
	query := `INSERT INTO activity_attempts (request_event_id, run_id, execution_mode, node_id, handler_event_key, activity_id, tool, attempt, status, success_event, failure_event, input_hash, loop_generation) VALUES (?, ?, ?, 'node', 'event', 'activity', 'provider.write', 1, 'started', 'activity.succeeded', 'activity.failed', 'input-hash', '{}')`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `INSERT INTO activity_attempts (request_event_id, run_id, execution_mode, node_id, handler_event_key, activity_id, tool, attempt, status, success_event, failure_event, input_hash, loop_generation) VALUES ($1::uuid, $2::uuid, $3, 'node', 'event', 'activity', 'provider.write', 1, 'started', 'activity.succeeded', 'activity.failed', 'input-hash', '{}'::jsonb)`
	}
	if _, err := fixture.db.ExecContext(ctx, query, uuid.NewString(), runID, string(mode)); err != nil {
		t.Fatalf("seed standing activity attempt: %v", err)
	}
}

func seedStandingCensusReadiness(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID string, mode executionmode.Mode) {
	t.Helper()
	instanceID, entityID := uuid.NewString(), uuid.NewString()
	instancePath := "standing-census/" + instanceID
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("standing census requires its exact source artifact")
	}
	readiness, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{TemplateID: "standing-census", ScopeKey: "standing-census", InstanceID: instanceID, InstancePath: instancePath, EntityID: entityID, HasStoredPath: true},
		RunID:    runID, BundleHash: source.BundleHash(), WorkflowVersion: "1", ExecutionMode: mode,
	}).Normalized()
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(readiness)
	if err != nil {
		t.Fatal(err)
	}
	plan := string(wire)
	seedWorkflowHeaderProjectionFixture(t, ctx, fixture.db, runID, entityID, instancePath, "standing-census", "", "ready", "{}", time.Now().UTC())
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		if _, err := fixture.db.ExecContext(ctx, `INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, plan_hash, created_at, updated_at) VALUES ($1::uuid, $2, $3::jsonb, $4, NOW(), NOW())`, runID, instancePath, plan, readinessPlanFixtureHash(t, plan)); err != nil {
			t.Fatalf("seed postgres standing readiness: %v", err)
		}
		return
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, plan_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, runID, instancePath, plan, readinessPlanFixtureHash(t, plan), time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite standing readiness: %v", err)
	}
}

func countStandingCensusFamily(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID, family string, mode executionmode.Mode) int {
	t.Helper()
	placeholder := "?"
	modePlaceholder := "?"
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		placeholder = "$1::uuid"
		modePlaceholder = "$2"
	}
	queries := map[string]string{
		"event_delivery":   `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id WHERE d.run_id = ` + placeholder + ` AND d.status = 'pending' AND e.execution_mode = ` + modePlaceholder,
		"workflow_timer":   `SELECT COUNT(*) FROM timers WHERE run_id = ` + placeholder + ` AND task_type = 'workflow_timer' AND status = 'active' AND execution_mode = ` + modePlaceholder,
		"generic_schedule": `SELECT COUNT(*) FROM timers WHERE run_id = ` + placeholder + ` AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active' AND execution_mode = ` + modePlaceholder,
		"decision_card":    `SELECT COUNT(*) FROM decision_cards WHERE run_id = ` + placeholder + ` AND status = 'pending' AND execution_mode = ` + modePlaceholder,
		"activity_attempt": `SELECT COUNT(*) FROM activity_attempts WHERE run_id = ` + placeholder + ` AND status = 'started' AND execution_mode = ` + modePlaceholder,
		"runtime_readiness": `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id = ` + placeholder + ` AND phase = 'planned' AND ` + func() string {
			if fixture.dialect == authoractivityfixture.DialectPostgres {
				return "plan->>'execution_mode'"
			}
			return "json_extract(plan, '$.execution_mode')"
		}() + ` = ` + modePlaceholder,
		"raw_pipeline_event": `SELECT COUNT(*) FROM events e LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline' WHERE e.run_id = ` + placeholder + ` AND e.execution_mode = ` + modePlaceholder + ` AND receipt.event_id IS NULL AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')`,
		"decision_route":     `SELECT COUNT(*) FROM decision_card_route_obligations route JOIN events e ON e.event_id = route.event_id WHERE route.run_id = ` + placeholder + ` AND route.status = 'pending' AND route.attempt_count = 0 AND e.execution_mode = ` + modePlaceholder,
	}
	query, ok := queries[family]
	if !ok {
		t.Fatalf("unknown standing census family %q", family)
	}
	var count int
	if err := fixture.db.QueryRowContext(ctx, query, runID, string(mode)).Scan(&count); err != nil {
		t.Fatalf("count standing census family %s: %v", family, err)
	}
	return count
}
