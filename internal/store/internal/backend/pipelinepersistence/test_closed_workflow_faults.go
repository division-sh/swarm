package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
)

func (s *PipelinePostgresOwner) HideWorkflowMutationTableForTest(ctx context.Context, hidden bool) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return hideWorkflowMutationTableTx(ctx, tx, hidden)
	})
}

func (s *PipelineSQLiteOwner) HideWorkflowMutationTableForTest(ctx context.Context, hidden bool) error {
	return s.backend.RunTransaction(ctx, "workflow projection shape fault fixture", func(ctx context.Context, tx *sql.Tx) error {
		return hideWorkflowMutationTableTx(ctx, tx, hidden)
	})
}

func hideWorkflowMutationTableTx(ctx context.Context, tx *sql.Tx, hidden bool) error {
	statement := "ALTER TABLE entity_mutations RENAME TO pipeline_hidden_entity_mutations"
	if !hidden {
		statement = "ALTER TABLE pipeline_hidden_entity_mutations RENAME TO entity_mutations"
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}

func (s *PipelinePostgresOwner) SetWorkflowGateCardInsertFaultForTest(ctx context.Context, run string, enabled bool) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return setWorkflowGateCardInsertFaultTx(ctx, tx, true, run, enabled)
	})
}

func (s *PipelineSQLiteOwner) SetWorkflowGateCardInsertFaultForTest(ctx context.Context, run string, enabled bool) error {
	return s.backend.RunTransaction(ctx, "workflow projection shape fault fixture", func(ctx context.Context, tx *sql.Tx) error {
		return setWorkflowGateCardInsertFaultTx(ctx, tx, false, run, enabled)
	})
}

func setWorkflowGateCardInsertFaultTx(ctx context.Context, tx *sql.Tx, postgres bool, run string, enabled bool) error {
	literal := "'" + strings.ReplaceAll(run, "'", "''") + "'"
	statements := []string{"DROP TRIGGER workflow_gate_card_insert_cut"}
	if postgres {
		statements = []string{"DROP TRIGGER workflow_gate_card_insert_cut ON decision_cards", "DROP FUNCTION workflow_gate_card_insert_cut()"}
	}
	if enabled {
		statements = []string{"CREATE TRIGGER workflow_gate_card_insert_cut AFTER INSERT ON decision_cards WHEN NEW.run_id=" + literal + " BEGIN SELECT RAISE(ABORT,'workflow_gate_card_insert_cut'); END"}
		if postgres {
			statements = []string{"CREATE FUNCTION workflow_gate_card_insert_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id=" + literal + "::uuid THEN RAISE EXCEPTION 'workflow_gate_card_insert_cut'; END IF; RETURN NEW; END $$", "CREATE TRIGGER workflow_gate_card_insert_cut AFTER INSERT ON decision_cards FOR EACH ROW EXECUTE FUNCTION workflow_gate_card_insert_cut()"}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *PipelinePostgresOwner) SetSelectedBranchDivergenceInsertFaultForTest(ctx context.Context, enabled bool) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return setSelectedBranchDivergenceInsertFaultTx(ctx, tx, true, enabled)
	})
}

func (s *PipelineSQLiteOwner) SetSelectedBranchDivergenceInsertFaultForTest(ctx context.Context, enabled bool) error {
	return s.backend.RunTransaction(ctx, "workflow projection shape fault fixture", func(ctx context.Context, tx *sql.Tx) error {
		return setSelectedBranchDivergenceInsertFaultTx(ctx, tx, false, enabled)
	})
}

func setSelectedBranchDivergenceInsertFaultTx(ctx context.Context, tx *sql.Tx, postgres bool, enabled bool) error {
	statements := []string{`DROP TRIGGER IF EXISTS selected_branch_divergence_insert_fault`}
	if postgres {
		statements = []string{`DROP TRIGGER IF EXISTS selected_branch_divergence_insert_fault ON run_fork_selected_contract_branch_divergences`, `DROP FUNCTION IF EXISTS selected_branch_divergence_insert_fault()`}
	}
	if enabled {
		statements = []string{`CREATE TRIGGER IF NOT EXISTS selected_branch_divergence_insert_fault AFTER INSERT ON run_fork_selected_contract_branch_divergences BEGIN SELECT RAISE(ABORT,'typed_receiver_owner_fault'); END`}
		if postgres {
			statements = []string{
				`CREATE OR REPLACE FUNCTION selected_branch_divergence_insert_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'typed_receiver_owner_fault'; END $$`,
				`DROP TRIGGER IF EXISTS selected_branch_divergence_insert_fault ON run_fork_selected_contract_branch_divergences`,
				`CREATE TRIGGER selected_branch_divergence_insert_fault AFTER INSERT ON run_fork_selected_contract_branch_divergences FOR EACH ROW EXECUTE FUNCTION selected_branch_divergence_insert_fault()`,
			}
		}
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *PipelinePostgresOwner) SetWorkflowTransitionEvidenceWireForTest(ctx context.Context, run, path string, wire json.RawMessage) (int64, error) {
	var changed int64
	err := s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = SetWorkflowTransitionEvidenceWireForTest(ctx, tx, true, run, path, wire)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

func (s *PipelineSQLiteOwner) SetWorkflowTransitionEvidenceWireForTest(ctx context.Context, run, path string, wire json.RawMessage) (int64, error) {
	var changed int64
	err := s.backend.RunTransaction(ctx, "workflow projection shape fault fixture", func(ctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = SetWorkflowTransitionEvidenceWireForTest(ctx, tx, false, run, path, wire)
		return err
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}
