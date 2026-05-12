package apiv1

import (
	"context"
	"testing"
	"time"

	"swarm/internal/store"
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
			Entity:      store.OperatorEntitySummary{EntityID: "entity-1", RunID: "run-1", CurrentState: "collecting"},
			Fields:      map[string]any{"priority": "high"},
			Gates:       map[string]bool{"approved": true},
			Accumulated: map[string]any{"notes": []any{"a"}},
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
