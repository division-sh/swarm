package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Exercise the production candidate reader, not a copy of its admission rule.
func TestChannelRecoverySelectedExecutionExclusionBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(fmt.Sprintf("postgres=%t", postgres), func(t *testing.T) {
			var db *sql.DB
			if postgres {
				_, db, _ = testutil.StartPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			jsonType := "TEXT"
			idType := "TEXT"
			if postgres {
				jsonType = "JSONB"
				idType = "UUID"
			}
			for _, ddl := range []string{
				`CREATE TEMP TABLE runs (run_id ` + idType + `, status TEXT)`,
				`CREATE TEMP TABLE runtime_external_effect_operations (
					operation_id TEXT, execution_mode TEXT, authority_evidence ` + jsonType + `,
					lineage ` + jsonType + `, agent_run_id TEXT, authority_kind TEXT,
					effect_kind TEXT, selected_execution_id TEXT)`,
				`CREATE TEMP TABLE runtime_external_effect_attempts (
					operation_id TEXT, attempt_id TEXT, execution_mode TEXT, usage_target_kind TEXT, state TEXT)`,
			} {
				if _, err := tx.ExecContext(ctx, ddl); err != nil {
					t.Fatal(err)
				}
			}
			selectedID := uuid.NewString()
			for _, kind := range []string{"channel_delivery", "channel_native_setting", "selected_contract_fork"} {
				operationID := uuid.NewString()
				if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_external_effect_operations
					VALUES ($1,'live','{}','{}',NULL,$2,$2,$3)`, operationID, kind, selectedID); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_external_effect_attempts
					VALUES ($1,$2,'live',NULL,'launched')`, operationID, uuid.NewString()); err != nil {
					t.Fatal(err)
				}
			}
			for _, check := range []struct {
				name, selected string
				want           map[string]bool
			}{
				{"ordinary", "", map[string]bool{"channel_delivery": true, "channel_native_setting": true}},
				{"selected", selectedID, map[string]bool{"selected_contract_fork": true}},
				{"wrong selected", uuid.NewString(), map[string]bool{}},
			} {
				t.Run(check.name, func(t *testing.T) {
					candidates, err := loadExternalEffectRecoveryCandidates(ctx, tx, postgres, check.selected)
					if err != nil {
						t.Fatal(err)
					}
					if len(candidates) != len(check.want) {
						t.Fatalf("candidates=%#v want=%v", candidates, check.want)
					}
					for _, candidate := range candidates {
						if !check.want[candidate.AuthorityKind] {
							t.Fatalf("crossed recovery authority: %#v", candidate)
						}
					}
				})
			}
		})
	}
}
