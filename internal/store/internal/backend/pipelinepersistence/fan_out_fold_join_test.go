package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type foldJoinQueryCount struct {
	pipelineQueryer
	compact, joined, maxArgs int
}

func (q *foldJoinQueryCount) count(query string, args []any) {
	if strings.Contains(query, "FROM fan_out_intents") || strings.Contains(query, "FROM fan_out_outcomes") {
		q.compact++
		q.maxArgs = max(q.maxArgs, len(args))
		if strings.Contains(query, "LEFT JOIN fan_out_outcomes") {
			q.joined++
		}
	}
}

func (q *foldJoinQueryCount) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.count(query, args)
	return q.pipelineQueryer.QueryContext(ctx, query, args...)
}

func (q *foldJoinQueryCount) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.count(query, args)
	return q.pipelineQueryer.QueryRowContext(ctx, query, args...)
}

// The deliberately nullable, unconstrained physical rows admit corruption that
// production DDL ordinarily prevents. Both readers still use native SQL drivers.
func TestFanOutFoldJoinDifferentialAndQueryBoundBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "fold-join.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			coords := "run_id TEXT, triggering_delivery_id TEXT, flow_path TEXT, declaration_family TEXT, semantic_path TEXT"
			for _, ddl := range []string{
				"CREATE TABLE fan_out_intents (" + coords + ", cardinality BIGINT, cursor BIGINT, status TEXT)",
				"CREATE TABLE fan_out_outcomes (" + coords + ", ordinal BIGINT, outcome_kind TEXT, event_id TEXT, source_event_id TEXT, inherited_disposition TEXT)",
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			key := fanoutobligation.IntentKey{RunID: uuid.NewString(), TriggeringDeliveryID: uuid.NewString(),
				ElementRef: runtimecontracts.FanOutElementRef{FlowPath: "root", Family: "handler_rule", SemanticPath: `handlers["items.ready"].rules[0]`}}
			args := []any{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath}
			type outcome struct {
				ordinal                    any
				kind                       any
				event, source, disposition any
			}
			rejected := outcome{ordinal: 0, kind: "semantic_rejected"}
			inherited := func(n int, disposition string) outcome {
				return outcome{ordinal: n, kind: "committed", source: uuid.NewString(), disposition: disposition}
			}
			cases := []struct {
				name                string
				cardinality, cursor any
				status              any
				facts               []outcome
				wantError           string
			}{
				{"missing", nil, nil, nil, nil, "is missing"},
				{"empty_closed", 0, 0, "closed", nil, ""},
				{"empty_open", 4, 0, "open", nil, ""},
				{"empty_canceled", 4, 0, "canceled", nil, ""},
				{"empty_blocked", 4, 0, "blocked", nil, ""},
				{"rejected", 1, 1, "closed", []outcome{rejected}, ""},
				{"mixed_inherited", 4, 4, "closed", []outcome{rejected, inherited(1, "succeeded"), inherited(2, "dead_lettered"), inherited(3, "no_route")}, ""},
				{"canceled_suffix", 5, 1, "canceled", []outcome{rejected}, ""},
				{"null_cardinality", nil, 0, "closed", nil, "converting NULL"},
				{"null_cursor", 1, nil, "closed", nil, "converting NULL"},
				{"null_status", 1, 0, nil, nil, "converting NULL"},
				{"negative_cardinality_before_null_ordinal", -1, 0, "closed", []outcome{{kind: "semantic_rejected"}}, "invalid progress"},
				{"negative_cursor", 1, -1, "closed", nil, "invalid progress"},
				{"cursor_over_cardinality", 1, 2, "closed", nil, "invalid progress"},
				{"invalid_status_before_null_kind", 1, 1, "bogus", []outcome{{ordinal: 0}}, "invalid status"},
				{"missing_outcome", 1, 1, "closed", nil, "ordinal outcomes"},
				{"extra_outcome", 0, 0, "closed", []outcome{rejected}, "ordinal outcomes"},
				{"null_ordinal_is_present", 1, 1, "closed", []outcome{{kind: "semantic_rejected"}}, "converting NULL"},
				{"null_kind", 1, 1, "closed", []outcome{{ordinal: 0}}, "converting NULL"},
				{"wrong_ordinal", 1, 1, "closed", []outcome{{ordinal: 1, kind: "semantic_rejected"}}, "not contiguous"},
				{"duplicate_ordinal", 2, 2, "closed", []outcome{rejected, rejected}, "not contiguous"},
				{"invalid_kind", 1, 1, "closed", []outcome{{ordinal: 0, kind: "bogus"}}, "invalid outcome kind"},
				{"rejected_identity", 1, 1, "closed", []outcome{{ordinal: 0, kind: "semantic_rejected", event: uuid.NewString()}}, "carries settlement identity"},
				{"owned_absent_identity", 1, 1, "closed", []outcome{{ordinal: 0, kind: "committed"}}, "contradictory settlement evidence"},
				{"inherited_both_identities", 1, 1, "closed", []outcome{{ordinal: 0, kind: "committed", event: uuid.NewString(), source: uuid.NewString(), disposition: "succeeded"}}, "contradictory settlement evidence"},
				{"inherited_missing_disposition", 1, 1, "closed", []outcome{{ordinal: 0, kind: "committed", source: uuid.NewString()}}, "contradictory settlement evidence"},
				{"inherited_invalid_disposition", 1, 1, "closed", []outcome{inherited(0, "bogus")}, "invalid terminal disposition"},
			}
			for _, size := range []int{18, 128, 129} {
				facts := make([]outcome, size)
				for i := range facts {
					facts[i] = outcome{ordinal: i, kind: "semantic_rejected"}
				}
				cases = append(cases, struct {
					name                string
					cardinality, cursor any
					status              any
					facts               []outcome
					wantError           string
				}{fmt.Sprintf("query_bound_%d", size), size, size, "closed", facts, ""})
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if tc.name != "missing" {
						values := append(append([]any{}, args...), tc.cardinality, tc.cursor, tc.status)
						if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, values...); err != nil {
							t.Fatal(err)
						}
					}
					for _, fact := range tc.facts {
						values := append(append([]any{}, args...), fact.ordinal, fact.kind, fact.event, fact.source, fact.disposition)
						if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, values...); err != nil {
							t.Fatal(err)
						}
					}
					// A corrupt outcome differing in EACH coordinate must remain invisible.
					for i := range args {
						other := append([]any{}, args...)
						other[i] = uuid.NewString()
						values := append(other, nil, "bogus", nil, nil, nil)
						if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, values...); err != nil {
							t.Fatal(err)
						}
					}
					beforeQ := &foldJoinQueryCount{pipelineQueryer: tx}
					before, beforeErr := foldFanOutIntentTwoQueryBefore(ctx, beforeQ, backend == "postgres", key)
					afterQ := &foldJoinQueryCount{pipelineQueryer: tx}
					after, afterErr := foldFanOutIntentTerminalDispositions(ctx, afterQ, backend == "postgres", key)
					prepared := &fanOutFoldStatement{Tx: tx}
					defer prepared.close()
					preparedQ := &foldJoinQueryCount{pipelineQueryer: prepared}
					for range 2 {
						got, gotErr := foldFanOutIntentTerminalDispositions(ctx, preparedQ, backend == "postgres", key)
						if !reflect.DeepEqual(after, got) || fmt.Sprint(afterErr) != fmt.Sprint(gotErr) || reflect.TypeOf(afterErr) != reflect.TypeOf(gotErr) {
							t.Fatalf("raw=%+v %v; prepared=%+v %v", after, afterErr, got, gotErr)
						}
					}
					if preparedQ.compact != 2 || preparedQ.joined != 2 || preparedQ.maxArgs != 5 {
						t.Fatalf("prepared query bound: %+v", preparedQ)
					}
					// Joined columns change only database/sql's numeric scan index.
					normalize := func(err error) string {
						if err == nil {
							return ""
						}
						return regexp.MustCompile(`column index [0-9]+`).ReplaceAllString(err.Error(), "column index N")
					}
					if !reflect.DeepEqual(before, after) || normalize(beforeErr) != normalize(afterErr) {
						t.Fatalf("before=%+v %v; after=%+v %v", before, beforeErr, after, afterErr)
					}
					if tc.wantError == "" && afterErr != nil || tc.wantError != "" && (afterErr == nil || !strings.Contains(afterErr.Error(), tc.wantError)) {
						t.Fatalf("error=%v, want %q", afterErr, tc.wantError)
					}
					if afterQ.compact != 1 || afterQ.joined != 1 || afterQ.maxArgs != 5 {
						t.Fatalf("joined query bound: %+v", afterQ)
					}
					if tc.wantError == "" && beforeQ.compact != 2 {
						t.Fatalf("frozen oracle query count=%d", beforeQ.compact)
					}
				})
			}
		})
	}
}
