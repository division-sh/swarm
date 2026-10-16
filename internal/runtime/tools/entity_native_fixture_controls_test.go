package tools_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func entityToolAliasFaultFixture(t *testing.T) (context.Context, any, string) {
	t.Helper()
	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator", Tools: []string{"get_entity"}}
	ctx, _, selected := newEntityToolTestHarnessWithBundleAndLegacyAccess(t, actor, loadRoleScopedEntityToolBundle(t, actor), false)
	fixture := ctx.Value(entityToolImportFixtureKey{}).(entityToolImportFixture)
	entity := uuid.NewString()
	constructEntityToolFixture(t, ctx, selected, fixture.pipeline, fixture.source, "validation", "validation/inst-1", entity, "queued", map[string]any{"status": "open"}, time.Unix(1700000750, 0).UTC())
	return ctx, selected, entity
}

func TestEntityToolAliasFaultKeepsOriginalWriterHeaderAndHistory(t *testing.T) {
	ctx, selected, entity := entityToolAliasFaultFixture(t)
	before, err := storetest.ReadSelectedForkRecoveredStorageSnapshot(ctx, selected, entityToolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	sibling := uuid.NewString()
	at := time.Unix(1700000760, 0).UTC()
	fields := map[string]any{"status": "open", "business_brief": map[string]any{"summary": "sibling", "confidence": 2}}
	if err := storetest.InsertEntityToolSiblingAliasFault(ctx, selected, entityToolTestRunID, entity, sibling, fields, at); err != nil {
		t.Fatal(err)
	}
	if got := probe.Snapshot(); got.Active != 0 || got.Total.WriteCommits != 1 || got.Total.Revision.Finalizations != 0 {
		t.Fatalf("alias fault escaped the exact original unrevisioned writer: %+v", got)
	}
	after, err := storetest.ReadSelectedForkRecoveredStorageSnapshot(ctx, selected, entityToolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	for table, rows := range before {
		if table != "entity_state" && !reflect.DeepEqual(rows, after[table]) {
			t.Fatalf("alias fault changed non-field history or authority: %s", table)
		}
	}
	assertEntityToolAliasPhysicalClocks(t, after["entity_state"], sibling, at)
	row, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, selected, entityToolTestRunID, sibling)
	if err != nil || row.Revision != 1 || row.EntityType != "validation_case" || row.CurrentState != "queued" || len(row.Mutations) != 0 {
		t.Fatalf("alias fault manufactured construction/history: %+v/%v", row, err)
	}
	raw, err := storetest.ReadEntityToolFieldWire(ctx, selected, entityToolTestRunID, sibling, "business_brief")
	if err != nil || string(raw) != `{"summary": "sibling", "confidence": 2}` {
		t.Fatalf("exact alias payload changed: %s/%v", raw, err)
	}
	if err := storetest.InsertEntityToolSiblingAliasFault(ctx, selected, entityToolTestRunID, entity, sibling, fields, at); err == nil {
		t.Fatal("duplicate alias fault did not refuse")
	}
	stable, err := storetest.ReadSelectedForkRecoveredStorageSnapshot(ctx, selected, entityToolTestRunID)
	if err != nil || !reflect.DeepEqual(after, stable) {
		t.Fatalf("duplicate fault changed committed evidence: %v", err)
	}
}

func assertEntityToolAliasPhysicalClocks(t *testing.T, table storetest.SelectedForkStorageTableSnapshot, sibling string, at time.Time) {
	t.Helper()
	rows, err := storetest.DecodeSelectedForkSnapshotRows(table)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		identity, err := row["entity_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if identity != sibling {
			continue
		}
		if path, err := row["flow_instance"].Text(); err != nil || path != "validation/inst-1" {
			t.Fatalf("same-path negative evidence changed: %s/%v", path, err)
		}
		for _, column := range []string{"entered_state_at", "created_at", "updated_at"} {
			var got time.Time
			if err := json.Unmarshal(row[column].Value, &got); err != nil || !got.Equal(at) {
				t.Fatalf("exact alias clock %s=%s want=%s err=%v", column, got, at, err)
			}
		}
		return
	}
	t.Fatal("exact sibling field row is absent")
}

func TestEntityToolHistoricalImportSecondFailureKeepsCommittedJournal(t *testing.T) {
	ctx, _, selected := newEntityToolTestHarness(t)
	entity := seedReadOnlyImportedEntityForToolTest(t, ctx, map[string]any{"flow_instance": "review/inst-1", "fields": map[string]any{"status": "open"}})
	at := time.Unix(1700000765, 0).UTC()
	event := uuid.NewString()
	storetest.InsertNativeCanonicalEventRecord(t, ctx, selected, eventtest.PersistedProjectionForProducer(
		event, "fork.state_entry", eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0, entityToolTestRunID, "",
		events.EventEnvelope{EntityID: entity, Scope: events.EventScopeEntity}, at,
	))
	before, err := storetest.ReadSelectedForkRecoveredStorageSnapshot(ctx, selected, entityToolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	if err := storetest.SeedEntityToolHistoricalReadOnlyImport(ctx, selected, entityToolTestRunID, entity, event, at); err == nil {
		t.Fatal("existing state did not refuse the second history cut")
	}
	if got := probe.Snapshot(); got.Active != 0 || got.Total.WriteCommits != 1 || got.Total.Revision.Finalizations != 0 {
		t.Fatalf("second failure collapsed committed history or added capture: %+v", got)
	}
	after, err := storetest.ReadSelectedForkRecoveredStorageSnapshot(ctx, selected, entityToolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	for table, rows := range before {
		if table == "entity_mutations" {
			if len(after[table].Rows) != len(rows.Rows)+2 {
				t.Fatal("the two original mutations did not survive the failed state cut")
			}
		} else if !reflect.DeepEqual(rows, after[table]) {
			t.Fatalf("failed second history cut changed %s", table)
		}
	}
}

func TestEntityToolNativePortsRefuseForeignInvalidCancelledAndClosedOwners(t *testing.T) {
	ctx, selected, entity := entityToolAliasFaultFixture(t)
	for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}, storetest.StartSQLiteRuntimeStore(t)} {
		assertEntityToolNativePortsRefuse(t, ctx, owner, entity)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := storetest.InsertEntityToolSiblingAliasFault(cancelled, selected, entityToolTestRunID, entity, uuid.NewString(), nil, time.Now().UTC()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled original writer supplied fault evidence: %v", err)
	}
	for _, identity := range []string{"", "not-a-uuid", uuid.Nil.String()} {
		if err := storetest.InsertEntityToolSiblingAliasFault(ctx, selected, entityToolTestRunID, identity, uuid.NewString(), nil, time.Now().UTC()); err == nil {
			t.Fatalf("invalid owner %q accepted", identity)
		}
	}
	if err := storetest.InsertEntityToolSiblingAliasFault(ctx, selected, entityToolTestRunID, entity, entity, nil, time.Now().UTC()); err == nil {
		t.Fatal("same owner and sibling accepted")
	}
	if err := storetest.InsertEntityToolSiblingAliasFault(ctx, selected, entityToolTestRunID, uuid.NewString(), uuid.NewString(), nil, time.Now().UTC()); err == nil {
		t.Fatal("absent constructed owner accepted")
	}
	if err := selected.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	assertEntityToolNativePortsRefuse(t, ctx, selected, entity)
}

func assertEntityToolNativePortsRefuse(t *testing.T, ctx context.Context, selected any, entity string) {
	t.Helper()
	if err := storetest.InsertEntityToolSiblingAliasFault(ctx, selected, entityToolTestRunID, entity, uuid.NewString(), nil, time.Now().UTC()); err == nil {
		t.Fatalf("unavailable %T accepted the alias fault", selected)
	}
	for _, fault := range []func(context.Context, any, string) error{storetest.SetEntityToolPrivateBookkeepingFault, storetest.SetEntityToolUndeclaredFieldFault, storetest.SetEntityToolMarginalStateFault} {
		if err := fault(ctx, selected, entity); err == nil {
			t.Fatalf("unavailable %T accepted the exact field fault", selected)
		}
	}
	if raw, err := storetest.ReadEntityToolFieldWire(ctx, selected, entityToolTestRunID, entity, "status"); len(raw) != 0 || err == nil {
		t.Fatalf("unavailable %T supplied field evidence: %s/%v", selected, raw, err)
	}
	if count, err := storetest.CountEntityToolAuthoredPathMutations(ctx, selected, entity, "status"); count != 0 || err == nil {
		t.Fatalf("unavailable %T supplied mutation count: %d/%v", selected, count, err)
	}
	if rows, err := storetest.ReadEntityToolInitialMutationAuthorities(ctx, selected, entity); len(rows) != 0 || err == nil {
		t.Fatalf("unavailable %T supplied mutation authority: %+v/%v", selected, rows, err)
	}
	if value, err := storetest.ReadEntityToolStatus(ctx, selected, entityToolTestRunID, entity); value != "" || err == nil {
		t.Fatalf("unavailable %T supplied status: %q/%v", selected, value, err)
	}
	if value, err := storetest.ReadEntityToolStatusRevision(ctx, selected, entityToolTestRunID, entity); value != (storetest.EntityToolStatusRevisionStorage{}) || err == nil {
		t.Fatalf("unavailable %T supplied status/revision: %+v/%v", selected, value, err)
	}
	if count, err := storetest.CountEntityToolAgentStatusMutations(ctx, selected, entityToolTestRunID, entity); count != 0 || err == nil {
		t.Fatalf("unavailable %T supplied agent mutation count: %d/%v", selected, count, err)
	}
	if err := storetest.AdvanceEntityToolForkSourceFault(ctx, selected, entityToolTestRunID, entity, time.Now().UTC()); err == nil {
		t.Fatalf("unavailable %T accepted the exact fork-source advance", selected)
	}
	if err := storetest.SeedEntityToolHistoricalReadOnlyImport(ctx, selected, entityToolTestRunID, entity, uuid.NewString(), time.Now().UTC()); err == nil {
		t.Fatalf("unavailable %T accepted historical import", selected)
	}
}
