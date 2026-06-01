package apiv1

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type fakeEntityReadStore struct {
	listResult    store.OperatorEntityListResult
	listErr       error
	getResult     store.OperatorEntityFull
	getErr        error
	aggregate     store.OperatorEntityAggregateResult
	aggregateErr  error
	lastList      store.OperatorEntityListOptions
	lastEntityID  string
	lastRunID     string
	lastAggregate store.OperatorEntityAggregateOptions
}

func (s *fakeEntityReadStore) ListOperatorEntities(_ context.Context, opts store.OperatorEntityListOptions) (store.OperatorEntityListResult, error) {
	s.lastList = opts
	return s.listResult, s.listErr
}

func (s *fakeEntityReadStore) LoadOperatorEntity(_ context.Context, entityID, runID string) (store.OperatorEntityFull, error) {
	s.lastEntityID = entityID
	s.lastRunID = runID
	return s.getResult, s.getErr
}

func (s *fakeEntityReadStore) AggregateOperatorEntities(_ context.Context, opts store.OperatorEntityAggregateOptions) (store.OperatorEntityAggregateResult, error) {
	s.lastAggregate = opts
	return s.aggregate, s.aggregateErr
}

func TestOperatorEntityHandlersExposeEntityNativeReads(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	entities := &fakeEntityReadStore{
		listResult: store.OperatorEntityListResult{
			Entities: []store.OperatorEntitySummary{{
				EntityID:     "entity-1",
				RunID:        "run-1",
				FlowInstance: "review/primary",
				EntityType:   "mvp_spec",
				CurrentState: "collecting",
				Revision:     2,
				CreatedAt:    now,
				UpdatedAt:    now,
			}},
			NextCursor: "next",
		},
		getResult: store.OperatorEntityFull{
			Entity: store.OperatorEntitySummary{EntityID: "entity-1", RunID: "run-1", EntityType: "mvp_spec", CurrentState: "collecting"},
			Fields: map[string]any{"priority": "high"},
			Gates:  map[string]bool{"approved": true},
			Accumulated: map[string]any{
				"score":       float64(3),
				"accumulator": map[string]any{"count": float64(2)},
				"notes":       []any{"a", map[string]any{"text": "probe"}},
			},
		},
		aggregate: store.OperatorEntityAggregateResult{Counts: map[string]int{"collecting": 1}},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Entities: entities,
		}),
	})

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"entity.list","params":{"run_id":"run-1","flow":"review","type":"mvp_spec","current_state":"collecting","limit":10,"cursor":"abc"}}`)
	if list.Error != nil {
		t.Fatalf("entity.list error = %#v", list.Error)
	}
	listResult := asMap(t, list.Result)
	if rows, _ := listResult["entities"].([]any); len(rows) != 1 || listResult["next_cursor"] != "next" {
		t.Fatalf("entity.list result = %#v", list.Result)
	}
	if entities.lastList.RunID != "run-1" || entities.lastList.Flow != "review" || entities.lastList.Type != "mvp_spec" || entities.lastList.CurrentState != "collecting" || entities.lastList.Limit != 10 || entities.lastList.Cursor != "abc" {
		t.Fatalf("entity.list options = %#v", entities.lastList)
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"entity.get","params":{"entity_id":"entity-1","run_id":"run-1"}}`)
	if get.Error != nil {
		t.Fatalf("entity.get error = %#v", get.Error)
	}
	getResult := asMap(t, get.Result)
	if fields := asMap(t, getResult["fields"]); fields["priority"] != "high" {
		t.Fatalf("entity.get result = %#v", get.Result)
	}
	accumulated := asMap(t, getResult["accumulated"])
	if accumulated["score"] != float64(3) {
		t.Fatalf("entity.get accumulated = %#v, want score", accumulated)
	}
	if bucket := asMap(t, accumulated["accumulator"]); bucket["count"] != float64(2) {
		t.Fatalf("entity.get accumulated bucket = %#v, want count", accumulated["accumulator"])
	}
	if got := asMap(t, getResult["entity"])["entity_type"]; got != "mvp_spec" {
		t.Fatalf("entity.get entity_type = %#v, want mvp_spec", got)
	}
	if entities.lastEntityID != "entity-1" || entities.lastRunID != "run-1" {
		t.Fatalf("entity.get args = entity_id=%q run_id=%q", entities.lastEntityID, entities.lastRunID)
	}

	aggregate := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agg","method":"entity.aggregate","params":{"run_id":"run-1","group_by":"current_state","type":"mvp_spec"}}`)
	if aggregate.Error != nil {
		t.Fatalf("entity.aggregate error = %#v", aggregate.Error)
	}
	counts := asMap(t, asMap(t, aggregate.Result)["counts"])
	if counts["collecting"] != float64(1) {
		t.Fatalf("entity.aggregate result = %#v", aggregate.Result)
	}
	if entities.lastAggregate.RunID != "run-1" || entities.lastAggregate.GroupBy != "current_state" || entities.lastAggregate.Type != "mvp_spec" {
		t.Fatalf("entity.aggregate options = %#v", entities.lastAggregate)
	}
}

func TestOperatorEntityHandlersServeContractEntityTypesFromPostgres(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := &store.PostgresStore{DB: db}
	runID := "11111111-1111-1111-1111-111111111111"
	entityA := "22222222-2222-2222-2222-222222222222"
	entityB := "33333333-3333-3333-3333-333333333333"
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES
			($1::uuid, $2::uuid, 'scoring/vertical-a', 'vertical', 'discovered',
			 '{}'::jsonb, '{"vertical_name":"Healthcare"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($1::uuid, $3::uuid, 'scoring/vertical-b', 'vertical', 'pending',
			 '{}'::jsonb, '{"vertical_name":"Manufacturing"}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, runID, entityA, entityB); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Entities: pg,
		}),
	})

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"entity.list","params":{"run_id":"11111111-1111-1111-1111-111111111111","type":"vertical","limit":10}}`)
	if list.Error != nil {
		t.Fatalf("entity.list error = %#v", list.Error)
	}
	rows, _ := asMap(t, list.Result)["entities"].([]any)
	if len(rows) != 2 {
		t.Fatalf("entity.list rows = %#v", list.Result)
	}
	for _, row := range rows {
		if got := asMap(t, row)["entity_type"]; got != "vertical" {
			t.Fatalf("entity.list entity_type = %#v, want vertical", got)
		}
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"entity.get","params":{"entity_id":"22222222-2222-2222-2222-222222222222","run_id":"11111111-1111-1111-1111-111111111111"}}`)
	if get.Error != nil {
		t.Fatalf("entity.get error = %#v", get.Error)
	}
	getResult := asMap(t, get.Result)
	if got := asMap(t, getResult["entity"])["entity_type"]; got != "vertical" {
		t.Fatalf("entity.get entity_type = %#v, want vertical", got)
	}
	if fields := asMap(t, getResult["fields"]); fields["vertical_name"] != "Healthcare" {
		t.Fatalf("entity.get fields = %#v", fields)
	}

	byType := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agg-type","method":"entity.aggregate","params":{"run_id":"11111111-1111-1111-1111-111111111111","group_by":"entity_type"}}`)
	if byType.Error != nil {
		t.Fatalf("entity.aggregate entity_type error = %#v", byType.Error)
	}
	typeCounts := asMap(t, asMap(t, byType.Result)["counts"])
	if typeCounts["vertical"] != float64(2) || typeCounts["default"] != nil {
		t.Fatalf("entity_type counts = %#v", typeCounts)
	}

	typedState := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agg-state","method":"entity.aggregate","params":{"run_id":"11111111-1111-1111-1111-111111111111","group_by":"current_state","type":"vertical"}}`)
	if typedState.Error != nil {
		t.Fatalf("entity.aggregate typed current_state error = %#v", typedState.Error)
	}
	stateCounts := asMap(t, asMap(t, typedState.Result)["counts"])
	if stateCounts["discovered"] != float64(1) || stateCounts["pending"] != float64(1) {
		t.Fatalf("typed current_state counts = %#v", stateCounts)
	}
}

func TestOperatorEntityHandlersServeContractEntityTypesFromSQLite(t *testing.T) {
	ctx := context.Background()
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	runID := "11111111-1111-1111-1111-111111111111"
	entityA := "22222222-2222-2222-2222-222222222222"
	entityB := "33333333-3333-3333-3333-333333333333"
	now := time.Unix(1700000000, 0).UTC()
	if _, err := sqliteStore.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, now); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES
			(?, ?, 'scoring/vertical-a', 'vertical', 'discovered',
			 '{}', '{"vertical_name":"Healthcare"}', '{}', 1, ?, ?, ?),
			(?, ?, 'scoring/vertical-b', 'vertical', 'pending',
			 '{}', '{"vertical_name":"Manufacturing"}', '{}', 1, ?, ?, ?)
	`, runID, entityA, now, now, now, runID, entityB, now, now, now); err != nil {
		t.Fatalf("seed sqlite entity_state: %v", err)
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Entities: sqliteStore,
		}),
	})

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"entity.list","params":{"run_id":"11111111-1111-1111-1111-111111111111","type":"vertical","limit":10}}`)
	if list.Error != nil {
		t.Fatalf("entity.list error = %#v", list.Error)
	}
	rows, _ := asMap(t, list.Result)["entities"].([]any)
	if len(rows) != 2 {
		t.Fatalf("entity.list rows = %#v", list.Result)
	}
	for _, row := range rows {
		if got := asMap(t, row)["entity_type"]; got != "vertical" {
			t.Fatalf("entity.list entity_type = %#v, want vertical", got)
		}
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"entity.get","params":{"entity_id":"22222222-2222-2222-2222-222222222222","run_id":"11111111-1111-1111-1111-111111111111"}}`)
	if get.Error != nil {
		t.Fatalf("entity.get error = %#v", get.Error)
	}
	getResult := asMap(t, get.Result)
	if got := asMap(t, getResult["entity"])["entity_type"]; got != "vertical" {
		t.Fatalf("entity.get entity_type = %#v, want vertical", got)
	}
	if fields := asMap(t, getResult["fields"]); fields["vertical_name"] != "Healthcare" {
		t.Fatalf("entity.get fields = %#v", fields)
	}

	byType := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"agg-type","method":"entity.aggregate","params":{"run_id":"11111111-1111-1111-1111-111111111111","group_by":"entity_type"}}`)
	if byType.Error != nil {
		t.Fatalf("entity.aggregate entity_type error = %#v", byType.Error)
	}
	typeCounts := asMap(t, asMap(t, byType.Result)["counts"])
	if typeCounts["vertical"] != float64(2) || typeCounts["default"] != nil {
		t.Fatalf("entity_type counts = %#v", typeCounts)
	}

	entityContext := mailboxEntityContext(ctx, store.MailboxV1Item{
		SourceRunID:    runID,
		SourceEntityID: entityA,
	}, sqliteStore)
	if !entityContext.Available || entityContext.Entity == nil || entityContext.Entity.Entity.EntityID != entityA {
		t.Fatalf("mailbox entity context = %#v, want sqlite entity context", entityContext)
	}
}

func TestOperatorEntityHandlersTypedErrors(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		body     string
		store    *fakeEntityReadStore
		wantCode int
		wantApp  string
	}{
		{
			name:    "entity get missing",
			method:  "entity.get",
			body:    `{"jsonrpc":"2.0","id":"get","method":"entity.get","params":{"entity_id":"missing"}}`,
			store:   &fakeEntityReadStore{getErr: store.ErrEntityNotFound},
			wantApp: EntityNotFoundCode,
		},
		{
			name:     "entity get ambiguous",
			method:   "entity.get",
			body:     `{"jsonrpc":"2.0","id":"get","method":"entity.get","params":{"entity_id":"reused"}}`,
			store:    &fakeEntityReadStore{getErr: store.ErrAmbiguousEntityRunID},
			wantCode: codeInvalidParams,
		},
		{
			name:     "entity list bad cursor",
			method:   "entity.list",
			body:     `{"jsonrpc":"2.0","id":"list","method":"entity.list","params":{"cursor":"bad"}}`,
			store:    &fakeEntityReadStore{listErr: store.ErrInvalidEntityCursor},
			wantCode: codeInvalidParams,
		},
		{
			name:     "entity aggregate bad group",
			method:   "entity.aggregate",
			body:     `{"jsonrpc":"2.0","id":"agg","method":"entity.aggregate","params":{"group_by":"bad"}}`,
			store:    &fakeEntityReadStore{aggregateErr: &store.EntityReadParamError{Field: "group_by", Reason: "unsupported entity aggregate group_by"}},
			wantCode: codeInvalidParams,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := testHandler(t, Options{
				AuthTokens: []string{testToken},
				Handlers: OperatorReadHandlers(OperatorReadOptions{
					Entities: tc.store,
				}),
			})
			resp := rpcCall(t, handler, tc.body)
			if resp.Error == nil {
				t.Fatalf("expected rpc error")
			}
			if tc.wantApp != "" {
				data := asMap(t, resp.Error.Data)
				if data["code"] != tc.wantApp {
					t.Fatalf("application code = %#v, want %s", data["code"], tc.wantApp)
				}
				return
			}
			if resp.Error.Code != tc.wantCode {
				t.Fatalf("error code = %d, want %d: %#v", resp.Error.Code, tc.wantCode, resp.Error)
			}
		})
	}
}
