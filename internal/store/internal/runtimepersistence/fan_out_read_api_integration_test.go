package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestFanOutReadAPILiveSelectedStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 64, time.Now().UTC())
			process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			select {
			case <-executor.turns:
			case err := <-executor.errors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			calls := 0
			handler, err := apiv1.NewHandler(apiv1.Options{
				PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRootForRuntimeWriterGuard(t)),
				AuthTokens:       []string{"fan-out-read-token"},
				Handlers: apiv1.OperatorRunReadHandlers(apiv1.RunReadHandlerOptions{
					Runs: selected.(apiv1.RunReadStore),
					FanOutRuntime: func(ctx context.Context, page fanoutobligation.ListPage) (fanoutobligation.ListPage, error) {
						calls++
						return startupownership.ObserveFanOutRuntimePage(ctx, process, page)
					},
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			read := func(eligible bool, reason, runStatus string) {
				t.Helper()
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"read","method":"run.fan_out.list","params":{"run_id":%q,"limit":1,"filter":{"flow_path":%q}}}`, fixture.runID, fixture.flowPath)
				req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body)).WithContext(ctx)
				req.Header.Set("Authorization", "Bearer fan-out-read-token")
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, req)
				var response struct {
					Result fanoutobligation.ListPage `json:"result"`
					Error  json.RawMessage           `json:"error"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != http.StatusOK || len(response.Error) != 0 {
					t.Fatalf("HTTP read: status=%d body=%s", recorder.Code, recorder.Body.String())
				}
				page := response.Result
				query := fanoutobligation.ListQuery{RunID: fixture.runID, Limit: 1, Filter: fanoutobligation.ListFilter{FlowPath: fixture.flowPath}}
				if err := page.Validate(query); err != nil {
					t.Fatal(err)
				}
				if len(page.Intents) != 1 || page.NextCursor != "" || page.RunStatus != runStatus {
					t.Fatalf("page=%+v", page)
				}
				row := page.Intents[0]
				observed := row.Runtime
				if row.DurableState != "eligible" || row.Cursor != 0 || observed.Availability != "available" || observed.Reason != reason || observed.Eligible == nil || *observed.Eligible != eligible {
					t.Fatalf("canonical runtime observation lost: %+v", row)
				}
				if observed.ObservedAt == nil || observed.ObservedAt.IsZero() || observed.Workers == nil || *observed.Workers != 1 || observed.ActiveWorkers == nil || *observed.ActiveWorkers != 1 || observed.LastCommitMS != nil {
					t.Fatalf("shared runtime metrics fabricated or omitted: %+v", observed)
				}
			}
			read(true, "eligible", "running")
			control := selected.(interface {
				PauseRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
			})
			if outcome, err := control.PauseRunControlOutcome(testAuthorActivityContextForBundle(fixture.bundleHash), runcontrol.TransitionRequest{
				RunID: fixture.runID, Now: time.Now().UTC(), Reason: "read-api-proof", ControlledBy: "test",
			}); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause outcome=%+v err=%v", outcome, err)
			}
			read(false, "run_paused", "paused")
			if calls != 2 {
				t.Fatalf("runtime callback count=%d", calls)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}
