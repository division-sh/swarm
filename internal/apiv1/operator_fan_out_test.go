package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apispec"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const fanOutReadProbeRunID = "4ca1069b-b1e5-450c-8122-eccfb3b790cf"

func fanOutReadProbePage() fanoutobligation.ListPage {
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	return fanoutobligation.ListPage{RunID: fanOutReadProbeRunID, RunStatus: "running", ObservedAt: at, Order: fanoutobligation.ListIdentityOrder, Intents: []fanoutobligation.IntentReadback{{
		Key:        fanoutobligation.IntentKey{RunID: fanOutReadProbeRunID, TriggeringDeliveryID: "dd8200af-3a7d-4a51-bf58-599f963a342a", ElementRef: runtimecontracts.FanOutElementRef{FlowPath: "root", Family: "fan_out", SemanticPath: `handlers["ready"].rules[0]`}},
		BundleHash: readOnlyProbeBundleHash, Status: fanoutobligation.StatusOpen, DurableState: "eligible", Cardinality: 64, Cursor: 32, Owed: 32, NextChunkSize: 32, CreatedAt: at, UpdatedAt: at, Runtime: fanoutobligation.UnavailableRuntimeReadback(),
	}}}
}

func TestFanOutReadAPISchema(t *testing.T) {
	registry := testRegistry(t)
	raw, err := apispec.GenerateOpenRPC(registry.api)
	if err != nil {
		t.Fatal(err)
	}
	var doc apispec.OpenRPCDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	validator := newOpenRPCResultSchemaValidator(t, doc)
	validator.preflightMethodResultSchemas(t, []string{"run.fan_out.list"})
	raw, err = json.Marshal(fanOutReadProbePage())
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	validator.validateMethodResult(t, "run.fan_out.list", result)
	runtime := asMap(t, result["intents"].([]any)[0])["runtime"]
	for _, field := range []string{"observed_at", "eligible", "workers", "active_workers", "last_commit_ms"} {
		value, present := asMap(t, runtime)[field]
		if !present || value != nil {
			t.Fatalf("unavailable runtime field %s must be explicit null: %+v", field, runtime)
		}
	}
	for _, availability := range []string{"available", "available_without_commit", "retired"} {
		t.Run(availability, func(t *testing.T) {
			page := fanOutReadProbePage()
			page.Intents[0].Runtime = fanOutReadProbeRuntime()
			if availability == "available_without_commit" {
				page.Intents[0].Runtime.LastCommitMS = nil
			}
			if availability == "retired" {
				page.Intents[0].Runtime = fanoutobligation.RuntimeReadback{Availability: "retired", Reason: "grant_retired"}
			}
			raw, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]any
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			validator.validateMethodResult(t, "run.fan_out.list", result)
		})
	}
}

func fanOutReadProbeRuntime() fanoutobligation.RuntimeReadback {
	at := fanOutReadProbePage().ObservedAt.Add(time.Second)
	eligible, workers, active, latency := true, 8, 3, 2.5
	return fanoutobligation.RuntimeReadback{ObservedAt: &at, Availability: "available", Reason: "eligible", Eligible: &eligible, Workers: &workers, ActiveWorkers: &active, LastCommitMS: &latency}
}

func TestFanOutReadAPIRuntimeCallback(t *testing.T) {
	for _, mode := range []string{"live", "offline", "unavailable", "no_commit", "changed_durable_fields", "error", "missing_key", "foreign_key", "foreign_run", "duplicate_key", "reordered_keys", "invalid_metrics"} {
		t.Run(mode, func(t *testing.T) {
			page := fanOutReadProbePage()
			if mode == "duplicate_key" || mode == "reordered_keys" {
				second := page.Intents[0]
				second.Key.ElementRef.SemanticPath += ".next"
				page.Intents = append(page.Intents, second)
			}
			store := &fanOutReadTestStore{fakeRunReadStore: &fakeRunReadStore{}, page: page}
			calls := 0
			opts := RunReadHandlerOptions{Runs: store}
			if mode != "offline" {
				opts.FanOutRuntime = func(ctx context.Context, input fanoutobligation.ListPage) (fanoutobligation.ListPage, error) {
					calls++
					if err := ctx.Err(); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(input, page) {
						t.Fatalf("callback input=%+v", input)
					}
					input.Intents[0].Runtime = fanOutReadProbeRuntime()
					switch mode {
					case "unavailable":
						input.Intents[0].Runtime = fanoutobligation.UnavailableRuntimeReadback()
					case "no_commit":
						input.Intents[0].Runtime.LastCommitMS = nil
					case "changed_durable_fields":
						input.Intents[0].Cursor = 1
						input.RunStatus = "paused"
						input.ObservedAt = input.ObservedAt.Add(time.Hour)
					case "error":
						return fanoutobligation.ListPage{}, errors.New("runtime observation failed")
					case "missing_key":
						input.Intents = nil
					case "foreign_key":
						input.Intents[0].Key.ElementRef.FlowPath = "foreign"
					case "foreign_run":
						input.RunID = uuid.NewString()
					case "duplicate_key":
						input.Intents[1].Key = input.Intents[0].Key
					case "reordered_keys":
						input.Intents[0], input.Intents[1] = input.Intents[1], input.Intents[0]
					case "invalid_metrics":
						input.Intents[0].Runtime.ObservedAt = nil
					}
					return input, nil
				}
			}
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorRunReadHandlers(opts)})
			resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"live","method":"run.fan_out.list","params":{"run_id":%q,"limit":2}}`, page.RunID))
			wantCalls := 1
			if mode == "offline" {
				wantCalls = 0
			}
			if calls != wantCalls || store.calls != 1 {
				t.Fatalf("runtime calls=%d store calls=%d", calls, store.calls)
			}
			switch mode {
			case "error", "missing_key", "foreign_key", "foreign_run", "duplicate_key", "reordered_keys", "invalid_metrics":
				if resp.Error == nil {
					t.Fatalf("invalid callback accepted: %+v", resp)
				}
				return
			}
			if resp.Error != nil {
				t.Fatalf("callback response=%+v", resp.Error)
			}
			raw, err := json.Marshal(resp.Result)
			if err != nil {
				t.Fatal(err)
			}
			var got fanoutobligation.ListPage
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			wantRuntime := fanOutReadProbeRuntime()
			if mode == "offline" || mode == "unavailable" {
				wantRuntime = fanoutobligation.UnavailableRuntimeReadback()
			}
			if mode == "no_commit" {
				wantRuntime.LastCommitMS = nil
			}
			if !reflect.DeepEqual(got.Intents[0].Runtime, wantRuntime) {
				t.Fatalf("runtime=%+v want=%+v", got.Intents[0].Runtime, wantRuntime)
			}
			got.Intents[0].Runtime = page.Intents[0].Runtime
			if !reflect.DeepEqual(got, page) {
				t.Fatalf("runtime callback changed durable page: %+v", got)
			}
			if !reflect.DeepEqual(store.page, fanOutReadProbePage()) {
				t.Fatal("runtime callback mutated store-owned page")
			}
		})
	}
}

func TestFanOutReadAPIRuntimeCallbackAdmission(t *testing.T) {
	for _, mode := range []string{"unauthorized", "invalid_query", "store_error", "malformed_page", "empty_page"} {
		t.Run(mode, func(t *testing.T) {
			page := fanOutReadProbePage()
			store := &fanOutReadTestStore{fakeRunReadStore: &fakeRunReadStore{}, page: page}
			params := fmt.Sprintf(`{"run_id":%q}`, page.RunID)
			switch mode {
			case "invalid_query":
				params = fmt.Sprintf(`{"run_id":%q,"limit":501}`, page.RunID)
			case "store_error":
				store.err = errors.New("read failed")
			case "malformed_page":
				store.page.ObservedAt = time.Time{}
			case "empty_page":
				store.page.Intents = []fanoutobligation.IntentReadback{}
			}
			calls := 0
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorRunReadHandlers(RunReadHandlerOptions{
				Runs: store,
				FanOutRuntime: func(_ context.Context, page fanoutobligation.ListPage) (fanoutobligation.ListPage, error) {
					calls++
					return page, nil
				},
			})})
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"admission","method":"run.fan_out.list","params":%s}`, params)
			if mode == "unauthorized" {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body)))
				if recorder.Code != http.StatusUnauthorized {
					t.Fatalf("unauthorized status=%d", recorder.Code)
				}
			} else {
				resp := rpcCall(t, handler, body)
				if (resp.Error == nil) != (mode == "empty_page") {
					t.Fatalf("response=%+v", resp)
				}
			}
			if calls != 0 {
				t.Fatalf("runtime callback reached before admission or for empty page: %d", calls)
			}
		})
	}
}

type fanOutReadTestStore struct {
	*fakeRunReadStore
	query fanoutobligation.ListQuery
	page  fanoutobligation.ListPage
	err   error
	calls int
}

func (s *fanOutReadTestStore) ListFanOutIntents(_ context.Context, q fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	s.query = q
	s.calls++
	return s.page, s.err
}

func TestFanOutReadAPIAuthSchemaAndErrors(t *testing.T) {
	runID := uuid.NewString()
	store := &fanOutReadTestStore{fakeRunReadStore: &fakeRunReadStore{}, page: fanoutobligation.ListPage{RunID: runID, RunStatus: "running", ObservedAt: time.Now().UTC(), Order: fanoutobligation.ListIdentityOrder, Intents: []fanoutobligation.IntentReadback{}}}
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorRunReadHandlers(RunReadHandlerOptions{Runs: store})})
	method, ok := handler.registry.Method("run.fan_out.list")
	if !ok || !reflect.DeepEqual(method.Scope.Required, []string{"read:runs"}) {
		t.Fatalf("missing canonical read:runs registration: %+v", method)
	}
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"page","method":"run.fan_out.list","params":{"run_id":%q,"limit":2,"filter":{"status":"open","flow_path":"root"}}}`, runID)
	for _, token := range []string{"", "wrong-token"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("unauth status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}
	if store.calls != 0 {
		t.Fatal("unauthenticated call reached owner")
	}
	resp := rpcCall(t, handler, body)
	if resp.Error != nil {
		t.Fatalf("read error=%+v", resp.Error)
	}
	if store.query.RunID != runID || store.query.Limit != 2 || store.query.Filter.Status != fanoutobligation.StatusOpen || store.query.Filter.FlowPath != "root" {
		t.Fatalf("query=%+v", store.query)
	}
	resp = rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"root","method":"run.fan_out.list","params":{"run_id":%q,"filter":{"flow_path":"."}}}`, runID))
	if resp.Error != nil || store.query.Filter.FlowPath != "." {
		t.Fatalf("exact selected-root filter lost: query=%+v error=%+v", store.query, resp.Error)
	}
	for _, params := range []string{
		fmt.Sprintf(`{"run_id":%q,"limit":0}`, runID), fmt.Sprintf(`{"run_id":%q,"limit":501}`, runID), fmt.Sprintf(`{"run_id":%q,"limit":1.5}`, runID),
		fmt.Sprintf(`{"run_id":%q,"filter":{"status":"leased"}}`, runID), fmt.Sprintf(`{"run_id":%q,"filter":{"unknown":true}}`, runID), fmt.Sprintf(`{"run_id":%q,"unknown":true}`, runID), `{"run_id":"bad"}`,
		fmt.Sprintf(`{"run_id":%q,"filter":{"flow_path":""}}`, runID), fmt.Sprintf(`{"run_id":%q,"filter":{"flow_path":"./child"}}`, runID),
		fmt.Sprintf(`{"run_id":%q,"filter":null}`, runID), fmt.Sprintf(`{"run_id":%q,"filter":{"semantic_path":null}}`, runID),
		fmt.Sprintf(`{"run_id":%q,"cursor":""}`, runID), fmt.Sprintf(`{"run_id":%q,"cursor":null}`, runID),
	} {
		before := store.calls
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"bad","method":"run.fan_out.list","params":%s}`, params))
		if resp.Error == nil || resp.Error.Code != codeInvalidParams || store.calls != before {
			t.Fatalf("accepted params %s response=%+v", params, resp)
		}
	}
	store.err = fanoutobligation.ErrInvalidListCursor
	resp = rpcCall(t, handler, body)
	if resp.Error == nil || resp.Error.Code != codeInvalidParams {
		t.Fatalf("cursor error=%+v", resp.Error)
	}
	store.err = operatorread.ErrRunNotFound
	resp = rpcCall(t, handler, body)
	if resp.Error == nil || asMap(t, resp.Error.Data)["code"] != RunNotFoundCode {
		t.Fatalf("missing run=%+v", resp.Error)
	}
}

func TestFanOutReadAPISelectedStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			runID := uuid.NewString()
			var runs RunReadStore
			if backend == "sqlite" {
				selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				storetest.RequireSQLiteRun(t, ctx, storetest.DatabaseForTest(selected), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC()})
				runs = selected
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				storetest.RequirePostgresRun(t, ctx, db, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC()})
				runs = selected
			}
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorRunReadHandlers(RunReadHandlerOptions{Runs: runs})})
			resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"read","method":"run.fan_out.list","params":{"run_id":%q}}`, runID))
			if resp.Error != nil {
				t.Fatalf("%s error=%+v", backend, resp.Error)
			}
			result := asMap(t, resp.Result)
			rows, ok := result["intents"].([]any)
			if !ok || len(rows) != 0 || result["observed_at"] == "" || result["run_id"] != runID {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}
