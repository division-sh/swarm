package workflowheader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

// Projection is a transaction-local view of constructed lifecycle authority.
// The optional field row supplies only declared business fields, never progress.
type Projection struct {
	EntityID, InstancePath, EntityType, FlowTemplate, Stage string
	Revision                                                int64
	Gates, Bookkeeping, Accumulator, Fields                 any
}

func LoadForMutation(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, instancePath string) (Projection, bool, error) {
	if tx == nil || strings.TrimSpace(runID) != runID || (entityID == "" && instancePath == "") {
		return Projection{}, false, fmt.Errorf("constructed header projection requires exact transaction and identity")
	}
	if id, err := uuid.Parse(runID); err != nil || id == uuid.Nil || id.String() != runID {
		return Projection{}, false, fmt.Errorf("constructed header projection requires canonical nonzero run UUID")
	}
	query := `SELECT fi.entity_id, fi.instance_path, fi.entity_type, fi.flow_template, fi.current_state, fi.revision,
		fi.gates, fi.bookkeeping, fi.accumulator,
		es.entity_id, es.flow_instance, es.entity_type, es.fields
		FROM flow_instances fi
		LEFT JOIN entity_state es ON es.run_id = fi.run_id AND (es.entity_id = fi.entity_id OR es.flow_instance = fi.instance_path)
		WHERE fi.run_id = $1 AND ($2 = '' OR CAST(fi.entity_id AS TEXT) = $2)
		AND ($3 = '' OR fi.instance_path = $3)`
	if postgres {
		query += ` FOR UPDATE OF fi`
	}
	var header Projection
	var declaredType, fieldID, fieldPath, fieldType sql.NullString
	rows, err := tx.QueryContext(ctx, query, runID, entityID, instancePath)
	if err != nil {
		return Projection{}, false, err
	}
	if !rows.Next() {
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil {
			return Projection{}, false, readErr
		}
		return Projection{}, false, closeErr
	}
	err = rows.Scan(
		&header.EntityID, &header.InstancePath, &declaredType, &header.FlowTemplate, &header.Stage, &header.Revision,
		&header.Gates, &header.Bookkeeping, &header.Accumulator,
		&fieldID, &fieldPath, &fieldType, &header.Fields,
	)
	if err != nil {
		_ = rows.Close()
		return Projection{}, false, err
	}
	if rows.Next() {
		_ = rows.Close()
		return Projection{}, false, fmt.Errorf("constructed header has conflicting field owners")
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return Projection{}, false, err
	}
	if err := rows.Close(); err != nil {
		return Projection{}, false, err
	}
	if id, err := uuid.Parse(header.EntityID); err != nil || id == uuid.Nil || id.String() != header.EntityID || header.InstancePath == "" || strings.TrimSpace(header.InstancePath) != header.InstancePath || header.FlowTemplate == "" || strings.TrimSpace(header.FlowTemplate) != header.FlowTemplate || header.Stage == "" || header.Revision <= 0 {
		return Projection{}, false, fmt.Errorf("constructed header projection has invalid identity or lifecycle progress")
	}
	if declaredType.Valid {
		if declaredType.String == "" || !fieldID.Valid || fieldID.String != header.EntityID || fieldPath.String != header.InstancePath || fieldType.String != declaredType.String || header.Fields == nil {
			return Projection{}, false, fmt.Errorf("constructed header projection disagrees with its declared field row")
		}
		header.EntityType = declaredType.String
		if postgres {
			var lockedID string
			if err := tx.QueryRowContext(ctx, `SELECT entity_id FROM entity_state WHERE run_id = $1 AND entity_id = $2 AND flow_instance = $3 AND entity_type = $4 FOR UPDATE`, runID, header.EntityID, header.InstancePath, header.EntityType).Scan(&lockedID); err != nil {
				return Projection{}, false, fmt.Errorf("lock constructed header field row: %w", err)
			}
		}
	} else if fieldID.Valid {
		return Projection{}, false, fmt.Errorf("fieldless constructed header acquired a field row")
	}
	for _, object := range []struct {
		name string
		raw  any
	}{{"gates", header.Gates}, {"bookkeeping", header.Bookkeeping}, {"accumulator", header.Accumulator}} {
		if err := requireJSONObject(object.name, object.raw); err != nil {
			return Projection{}, false, err
		}
	}
	if header.EntityType != "" {
		if err := requireJSONObject("fields", header.Fields); err != nil {
			return Projection{}, false, err
		}
	}
	return header, true, nil
}

// Inventory uses the same strict header/optional-field pairing as exact reads.
// Field rows without construction authority never become executable instances.
func InventoryForMutation(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]Projection, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(entity_id AS TEXT), instance_path FROM flow_instances WHERE run_id = $1 ORDER BY entity_id`, runID)
	if err != nil {
		return nil, err
	}
	var coordinates []Projection
	for rows.Next() {
		var coordinate Projection
		if err := rows.Scan(&coordinate.EntityID, &coordinate.InstancePath); err != nil {
			_ = rows.Close()
			return nil, err
		}
		coordinates = append(coordinates, coordinate)
	}
	readErr := rows.Err()
	closeErr := rows.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	for i, coordinate := range coordinates {
		header, found, err := LoadForMutation(ctx, tx, postgres, runID, coordinate.EntityID, coordinate.InstancePath)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("constructed inventory member disappeared")
		}
		coordinates[i] = header
	}
	return coordinates, nil
}

func requireJSONObject(name string, raw any) error {
	var data []byte
	switch value := raw.(type) {
	case []byte:
		data = value
	case string:
		data = []byte(value)
	default:
		return fmt.Errorf("constructed header %s requires persisted JSON object", name)
	}
	value, err := canonicaljson.Decode(data)
	if err != nil {
		return fmt.Errorf("decode constructed header %s: %w", name, err)
	}
	if value.Kind() != semanticvalue.KindObject {
		return fmt.Errorf("constructed header %s requires persisted JSON object", name)
	}
	return nil
}
