package authoractivityadapter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/google/uuid"
)

func TestActivityEvidenceIsReadOnlyForRuntimeEffects(t *testing.T) {
	if strings.Contains(OccurrenceSelect, "runtime_external_effect") {
		t.Fatal("generic occurrence read depends on an effect table")
	}
	for _, dialect := range []Dialect{DialectSQLite, DialectPostgres} {
		for _, mode := range []string{"authored", "registered", "missing", "contradictory", "scan_error", "close_error"} {
			t.Run(string(rune('0'+dialect))+"/"+mode, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				db.SetMaxOpenConns(1)
				id, attemptID := uuid.NewString(), uuid.NewString()
				parent := channelnative.TransportParent{Kind: "deployment_registration", ID: uuid.NewString(), Revision: 4, BindingOperationID: uuid.NewString(), BindingOperationRevision: 3}
				evidence, _ := json.Marshal(map[string]any{"transport_parent": parent, "effect_operation_id": id, "authority_kind": "channel_confirmation", "authority_id": id, "onboarding_operation_id": parent.ID, "onboarding_revision": parent.Revision})
				attempt := 1
				projection, _ := json.Marshal(authoractivity.Projection{EffectClass: "write_or_unknown", Attempt: &attempt, Adapter: "channel_confirmation_whatsapp", Transport: "in_process", AuthorityKind: "channel_confirmation", AuthorityID: id, ExecutionMode: "live"})
				scope, bundle := "runtime", ""
				if mode == "authored" {
					scope, bundle = "bundle", "exact-authored-bundle"
				}
				columns := strings.Split("id,sequence,kind,version,transition,owner,source,key,run,entity,agent,flow,scope,runtime,bundle,projection,failure,at", ",")
				values := []driver.Value{uuid.NewString(), int64(1), "effect.lifecycle", int64(2), "launched", "runtime_external_effect_attempts", attemptID + ":launched", "effect:test", "", "", "", "", scope, uuid.NewString(), bundle, projection, nil, time.Now().UTC()}
				rows := sqlmock.NewRows(columns).AddRow(values...)
				if mode == "scan_error" {
					rows.RowError(0, errors.New("original scan failure"))
				}
				if mode == "close_error" {
					rows.CloseError(errors.New("original close failure"))
				}
				mock.ExpectQuery("FROM author_activity_occurrences WHERE").WillReturnRows(rows).RowsWillBeClosed()
				if mode != "authored" && mode != "scan_error" && mode != "close_error" {
					query := mock.ExpectQuery("FROM runtime_external_effect_operations o").WithArgs(attemptID+":launched", "launched", "channel_confirmation", id)
					if mode == "missing" {
						query.WillReturnError(sql.ErrNoRows)
					} else {
						if mode == "contradictory" {
							evidence = []byte(`{"transport_parent":null}`)
						}
						query.WillReturnRows(sqlmock.NewRows([]string{"evidence"}).AddRow(string(evidence)))
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				result, err := List(ctx, db, dialect, authoractivity.ListOptions{})
				wantError := mode != "authored" && mode != "registered"
				if (err != nil) != wantError {
					t.Fatal("wrong evidence disposition", mode, err)
				}
				if mode == "authored" && (len(result.Occurrences) != 1 || len(result.Occurrences[0].EffectAuthorityEvidence) != 0 || result.Occurrences[0].Scope.BundleHash != bundle) {
					t.Fatal("authored occurrence lost its exact bundle or acquired auxiliary evidence")
				}
				if mode == "registered" && (len(result.Occurrences) != 1 || len(result.Occurrences[0].EffectAuthorityEvidence) == 0) {
					t.Fatal("registered occurrence lost its original journal authority")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
