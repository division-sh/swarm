package pipelinepersistence

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestFanOutReadMixedOriginPaginationBothStores(t *testing.T) {
	if os.Getenv("SWARM_TEST_POSTGRES_DSN") == "" {
		t.Fatal("SWARM_TEST_POSTGRES_DSN required for dual-store readback proof")
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			db := fanOutReadbackTestDB(t, backend)
			handler := seedFanOutReadbackClaim(t, db).Claim.Key
			if _, err := db.ExecContext(ctx, `CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO runs VALUES ($1,'paused')`, handler.RunID); err != nil {
				t.Fatal(err)
			}
			ids := []string{
				"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002",
				"00000000-0000-4000-8000-000000000003", "00000000-0000-4000-8000-000000000004",
			}
			want := make([]fanoutobligation.IntentKey, 0, 5)
			for _, id := range ids {
				want = append(want, fanoutobligation.IntentKey{RunID: handler.RunID, DeploymentFeedID: id})
			}
			want = append(want, handler)
			at := time.Now().UTC().Truncate(time.Microsecond)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// Reverse insertion distinguishes the byte-order contract from row order.
			for n := len(ids) - 1; n >= 0; n-- {
				request := deploymentStoreRequest(2)
				request.Key = want[n]
				if n%2 == 0 {
					request.Cardinality = 0
				}
				if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, at); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			read := func(t *testing.T, q fanoutobligation.ListQuery) fanoutobligation.ListPage {
				t.Helper()
				page, err := listFanOutIntents(ctx, db, backend == "postgres", time.Now, q)
				if err != nil {
					t.Fatal(err)
				}
				if err := page.Validate(q); err != nil || page.RunStatus != "paused" {
					t.Fatalf("paused mixed page invalid: %+v, %v", page, err)
				}
				return page
			}
			for _, limit := range []int{1, 2} {
				t.Run("limit_"+strconv.Itoa(limit), func(t *testing.T) {
					q := fanoutobligation.ListQuery{RunID: handler.RunID, Limit: limit}
					var seen []fanoutobligation.IntentKey
					firstCursor := ""
					for {
						page := read(t, q)
						for _, row := range page.Intents {
							seen = append(seen, row.Key)
							if row.BundleHash == "" || row.Runtime != fanoutobligation.UnavailableRuntimeReadback() {
								t.Fatalf("lost origin or invented runtime: %+v", row)
							}
							if row.Key.DeploymentFeedID != "" {
								if row.Cardinality == 0 && (row.Status != fanoutobligation.StatusClosed || row.DurableState != "closed" || row.Owed != 0) {
									t.Fatalf("closed feed changed: %+v", row)
								}
								if row.Cardinality == 2 && (row.Status != fanoutobligation.StatusOpen || row.DurableState != "eligible" || row.Owed != 2) {
									t.Fatalf("open feed changed: %+v", row)
								}
							}
						}
						if page.NextCursor == "" {
							break
						}
						if firstCursor == "" {
							firstCursor = page.NextCursor
						}
						q.Cursor = page.NextCursor
					}
					if !reflect.DeepEqual(seen, want) {
						t.Fatalf("mixed order/gaps/duplicates: got %+v, want %+v", seen, want)
					}
					for _, wrong := range []fanoutobligation.ListQuery{
						{RunID: uuid.NewString(), Cursor: firstCursor},
						{RunID: handler.RunID, Cursor: firstCursor, Filter: fanoutobligation.ListFilter{Status: fanoutobligation.StatusOpen}},
						{RunID: handler.RunID, Cursor: firstCursor, Filter: fanoutobligation.ListFilter{TriggeringDeliveryID: handler.TriggeringDeliveryID}},
						{RunID: handler.RunID, Cursor: firstCursor, Filter: fanoutobligation.ListFilter{FlowPath: handler.ElementRef.FlowPath}},
					} {
						if _, err := listFanOutIntents(ctx, db, backend == "postgres", time.Now, wrong); !errors.Is(err, fanoutobligation.ErrInvalidListCursor) {
							t.Fatalf("cursor scope mismatch admitted: %+v, %v", wrong, err)
						}
					}
					raw, err := base64.RawURLEncoding.DecodeString(firstCursor)
					if err != nil {
						t.Fatal(err)
					}
					var cursor map[string]any
					if err := json.Unmarshal(raw, &cursor); err != nil {
						t.Fatal(err)
					}
					cursor["after"].(map[string]any)["element_ref"] = map[string]any{}
					raw, err = json.Marshal(cursor)
					if err != nil {
						t.Fatal(err)
					}
					wrong := fanoutobligation.ListQuery{RunID: handler.RunID, Cursor: base64.RawURLEncoding.EncodeToString(raw)}
					if _, err := listFanOutIntents(ctx, db, backend == "postgres", time.Now, wrong); !errors.Is(err, fanoutobligation.ErrInvalidListCursor) {
						t.Fatalf("noncanonical deployment cursor admitted: %v", err)
					}
				})
			}
			for _, filter := range []fanoutobligation.ListFilter{
				{TriggeringDeliveryID: handler.TriggeringDeliveryID},
				{FlowPath: handler.ElementRef.FlowPath},
				{SemanticPath: handler.ElementRef.SemanticPath},
			} {
				page := read(t, fanoutobligation.ListQuery{RunID: handler.RunID, Filter: filter})
				if len(page.Intents) != 1 || page.Intents[0].Key != handler || page.NextCursor != "" {
					t.Fatalf("handler filter leaked deployment: %+v", page)
				}
			}
		})
	}
}
