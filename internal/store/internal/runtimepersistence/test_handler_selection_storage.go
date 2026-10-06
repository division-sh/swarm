package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// HandlerSelectionStorageEvidence retains exact stored columns rather than
// normalizing them through the live delivery or operator-trace projection.
type HandlerSelectionStorageEvidence struct {
	DeliveryID   string
	Context      string
	Disposition  string
	FlowPath     string
	Family       string
	SemanticPath string
	DisplayLabel string
}

func ReadHandlerSelectionStorageForTest(ctx context.Context, selected any, eventID string) ([]HandlerSelectionStorageEvidence, error) {
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return nil, fmt.Errorf("handler selection storage requires an exact canonical event identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var evidence []HandlerSelectionStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT s.delivery_id, s.selection_context, s.disposition,
			COALESCE(s.flow_path, ''), COALESCE(s.declaration_family, ''),
			COALESCE(s.semantic_path, ''), s.display_label
			FROM event_delivery_handler_rule_selections s
			JOIN event_deliveries d ON d.delivery_id=s.delivery_id
			WHERE d.event_id=$1 AND d.subscriber_type='node' ORDER BY s.delivery_id`, eventID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row HandlerSelectionStorageEvidence
			if err := rows.Scan(&row.DeliveryID, &row.Context, &row.Disposition, &row.FlowPath, &row.Family, &row.SemanticPath, &row.DisplayLabel); err != nil {
				return err
			}
			evidence = append(evidence, row)
		}
		return rows.Err()
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	return evidence, nil
}
