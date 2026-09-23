package delivery

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

// Native PostgreSQL AFTER INSERT statements can change the canonical row after
// the RETURNING tuple was formed. Keep the old scalar-read admission boundary.
func TestHandlerSelectionAfterInsertPostgresCanonicalRead(t *testing.T) {
	db, a, _ := selectionWriterFixture(t, "postgres")
	ctx := context.Background()
	fact := selectionFact(t, "requested")
	for _, cut := range []struct {
		name, body string
	}{
		{"equality", "UPDATE event_delivery_handler_rule_selections SET display_label='rewritten' WHERE delivery_id=NEW.delivery_id;"},
		{"hydrate", "UPDATE event_delivery_handler_rule_selections SET flow_path='..' WHERE delivery_id=NEW.delivery_id;"},
		{"absent", "DELETE FROM event_delivery_handler_rule_selections WHERE delivery_id=NEW.delivery_id;"},
	} {
		t.Run(cut.name, func(t *testing.T) {
			for _, query := range []string{
				"CREATE FUNCTION selection_after_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " + cut.body + " RETURN NEW; END $$",
				"CREATE TRIGGER selection_after AFTER INSERT ON event_delivery_handler_rule_selections FOR EACH ROW EXECUTE FUNCTION selection_after_fn()",
			} {
				if _, err := db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				for _, query := range []string{"DROP TRIGGER selection_after ON event_delivery_handler_rule_selections", "DROP FUNCTION selection_after_fn()"} {
					if _, err := db.ExecContext(ctx, query); err != nil {
						t.Error(err)
					}
				}
			})
			assertRejection := func(t *testing.T, err error) {
				t.Helper()
				switch cut.name {
				case "equality":
					if !errors.Is(err, deliverylifecycle.ErrConflict) {
						t.Errorf("canonical equality was not enforced: %v", err)
					}
				case "hydrate":
					if err == nil || !strings.Contains(err.Error(), "hydrate delivery handler rule selection") {
						t.Errorf("canonical hydration was not enforced: %v", err)
					}
				case "absent":
					if !errors.Is(err, sql.ErrNoRows) {
						t.Errorf("canonical absence was not enforced: %v", err)
					}
				}
			}
			for _, path := range []string{"former_scalar_reread", "native_returning_comparison", "actual_writer"} {
				t.Run(path, func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					id := uuid.NewString()
					if path == "actual_writer" {
						err = a.persistHandlerRuleSelectionSQL(ctx, tx, id, fact)
						t.Logf("actual writer admission: %v", err)
						assertRejection(t, err)
						return
					}
					query := `INSERT INTO event_delivery_handler_rule_selections
						(delivery_id, selection_context, disposition, flow_path, declaration_family, semantic_path, display_label)
						VALUES ($1::uuid,$2,$3,$4,$5,$6,$7) ON CONFLICT (delivery_id) DO NOTHING`
					args := []any{id, string(fact.Context()), string(fact.Disposition()), fact.Ref().Flow().String(), fact.Ref().Family(), fact.Ref().SemanticPath(), fact.DisplayLabel()}
					if path == "native_returning_comparison" {
						var contextRaw, dispositionRaw, flowRaw, familyRaw, semanticPathRaw, labelRaw string
						if err := tx.QueryRowContext(ctx, query+` RETURNING selection_context, disposition, COALESCE(flow_path,''), COALESCE(declaration_family,''), COALESCE(semantic_path,''), display_label`, args...).Scan(&contextRaw, &dispositionRaw, &flowRaw, &familyRaw, &semanticPathRaw, &labelRaw); err != nil {
							t.Fatal(err)
						}
						returned, err := handlerselection.Hydrate(contextRaw, dispositionRaw, flowRaw, familyRaw, semanticPathRaw, labelRaw)
						if err != nil || !returned.Equal(fact) {
							t.Fatalf("native RETURNING did not retain pre-AFTER tuple: %+v %v", returned, err)
						}
						t.Log("native RETURNING equals requested fact despite AFTER INSERT mutation")
					} else if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						t.Fatal(err)
					}
					persisted, err := a.handlerRuleSelection(ctx, tx, id)
					if err == nil && !persisted.Equal(fact) {
						err = deliverylifecycle.ErrConflict
					}
					t.Logf("post-statement canonical scalar reread admission: %v", err)
					assertRejection(t, err)
				})
			}
			var count int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM event_delivery_handler_rule_selections`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected transaction leaked row: count=%d err=%v", count, err)
			}
		})
	}
}
