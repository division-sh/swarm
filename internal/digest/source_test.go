package digest

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
)

func TestNewSource_RequiresTerminalStates(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	_, err := NewSource(db, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err == nil || !strings.Contains(err.Error(), "terminal instance states are required") {
		t.Fatalf("NewSource err = %v, want terminal state requirement", err)
	}
}

func TestSource_FiltersTerminalStatesFromDigestReads(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	source, err := NewSource(db, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"done"},
		},
	}))
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}

	activeID := uuid.NewString()
	doneID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO entity_state (
			entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES
			($1::uuid, 'review/inst-1', 'default', 'active-co', 'ActiveCo', 'active',
			 '{}'::jsonb, '{"name":"ActiveCo"}'::jsonb, '{}'::jsonb, 1, now(), now(), now()),
			($2::uuid, 'review/inst-2', 'default', 'done-co', 'DoneCo', 'done',
			 '{}'::jsonb, '{"name":"DoneCo"}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, activeID, doneID); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}

	n, err := source.CountActiveInstances(context.Background())
	if err != nil {
		t.Fatalf("CountActiveInstances: %v", err)
	}
	if n != 1 {
		t.Fatalf("CountActiveInstances = %d, want 1", n)
	}

	rows, err := source.ListInstanceDigestRows(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListInstanceDigestRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("digest rows = %d, want 1", len(rows))
	}
	if rows[0].EntityID != activeID {
		t.Fatalf("digest row entity_id = %q, want %q", rows[0].EntityID, activeID)
	}
	if rows[0].Stage != "active" {
		t.Fatalf("digest row stage = %q, want active", rows[0].Stage)
	}
}
