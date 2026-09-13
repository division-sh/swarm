package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPublicOperationSemanticCarrierParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			var selected canonicalEventPublishProofStore
			var db *sql.DB
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				selected, db = s, storetest.DatabaseForTest(s)
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			source := semanticview.Wrap(runStartTestBundle("scan.requested"))
			bus, err := newScopedAPITestEventBus(t, selected, runStartTestEventBusOptions(source))
			if err != nil {
				t.Fatal(err)
			}
			handlers := testOperatorHandlers(testOperatorCapabilities{
				Runs: selected, Observability: selected, Idempotency: selected, Events: bus, Source: source,
				RunBundleContext: selected.(RunBundleContextStore), Entities: selected.(EntityReadStore),
				Bundle: runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0", BundleHash: runStartTestBundleHash},
			})
			counts := func() map[string]int {
				t.Helper()
				out := map[string]int{}
				for _, table := range []string{"runs", "events", "event_deliveries", "entity_state", "api_idempotency"} {
					var n int
					if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
						t.Fatal(err)
					}
					out[table] = n
				}
				return out
			}
			for _, method := range []string{"event.publish", "run.start"} {
				t.Run(method, func(t *testing.T) {
					key, run := uuid.NewString(), uuid.NewString()
					request := func(value any) Request {
						params := map[string]any{"event_name": "scan.requested", "bundle_hash": runStartTestBundleHash, "idempotency_key": key,
							"payload": map[string]any{"value": value, "nested": []any{value, 7.5}}}
						if method == "run.start" {
							params["run_id"] = run
						}
						semantic, err := canonicaljson.FromGo(params)
						if err != nil {
							t.Fatal(err)
						}
						return Request{Method: method, Params: params, SemanticParams: semantic, ActorTokenID: actorTokenID(testToken),
							RequestHash: requestBodyHash(method, requestHashParams(method, semantic))}
					}
					var wantResult string
					var after map[string]int
					for i, value := range []any{int(7), int64(7), float64(7), json.Number("7.0"), json.Number("7e0")} {
						result, err := handlers[method](ctx, request(value))
						if err != nil {
							t.Fatalf("%T(%v): %v", value, value, err)
						}
						raw, err := canonicaljson.Bytes(result)
						if err != nil {
							t.Fatal(err)
						}
						if i == 0 {
							wantResult, after = string(raw), counts()
						} else if string(raw) != wantResult || !reflect.DeepEqual(counts(), after) {
							t.Fatalf("carrier replay changed result/domain: %s -> %s; %v -> %v", wantResult, raw, after, counts())
						}
						var ids struct {
							RunID string `json:"run_id"`
						}
						if err := json.Unmarshal(raw, &ids); err != nil {
							t.Fatal(err)
						}
						var payload string
						if err := db.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='scan.requested'`, ids.RunID).Scan(&payload); err != nil {
							t.Fatal(err)
						}
						var decoded map[string]any
						if err := canonicaljson.DecodePreservingNumberLexemes([]byte(payload), &decoded); err != nil {
							t.Fatal(err)
						}
						projected, err := workflowexpr.ProjectCELValue(decoded)
						if err != nil {
							t.Fatal(err)
						}
						p := projected.(map[string]any)
						if p["value"] != int64(7) || !reflect.DeepEqual(p["nested"], []any{int64(7), float64(7.5)}) {
							t.Fatalf("persisted execution payload %#v", p)
						}
					}
					if _, err := handlers[method](ctx, request(8)); err == nil {
						t.Fatal("changed value replay accepted")
					}
					if !reflect.DeepEqual(counts(), after) {
						t.Fatal("conflict mutated domain")
					}
					for _, invalid := range []any{math.NaN(), math.Inf(1), math.Copysign(0, -1), float64(9007199254740992)} {
						// Bypass the transport decoder, not the public operation adapter.
						req := request(7)
						req.Params["payload"] = map[string]any{"value": invalid}
						req.Params["idempotency_key"] = uuid.NewString()
						if _, err := handlers[method](ctx, req); err == nil {
							t.Fatalf("invalid direct carrier %v accepted", invalid)
						}
						if !reflect.DeepEqual(counts(), after) {
							t.Fatal("invalid direct carrier mutated domain")
						}
					}
				})
			}
		})
	}
}
