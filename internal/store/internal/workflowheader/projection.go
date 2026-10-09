package workflowheader

import (
	"context"
	"database/sql"
	"errors"
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

// ProjectionError distinguishes malformed persisted ownership from storage
// failures. It never turns an operational error into invalid state.
type ProjectionError struct {
	RunID, EntityID, InstancePath, Reason string
}

func (e *ProjectionError) Error() string {
	return fmt.Sprintf("constructed projection run=%s entity=%s instance=%s: %s", e.RunID, e.EntityID, e.InstancePath, e.Reason)
}

// ReadInventory keeps imported state separate from construction authority.
type ReadInventory struct {
	Constructed, StateOnly []Projection
}

func LoadForMutation(ctx context.Context, tx *sql.Tx, postgres bool, runID, entityID, instancePath string) (Projection, bool, error) {
	return load(ctx, tx, postgres, runID, entityID, instancePath)
}

// LoadForRead shares exact pairing and admission without acquiring write locks.
func LoadForRead(ctx context.Context, tx *sql.Tx, runID, entityID, instancePath string) (Projection, bool, error) {
	return load(ctx, tx, false, runID, entityID, instancePath)
}

func load(ctx context.Context, tx *sql.Tx, lockPostgres bool, runID, entityID, instancePath string) (Projection, bool, error) {
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
	if lockPostgres {
		query += ` FOR UPDATE OF fi`
	}
	var header Projection
	var declaredType, fieldID, fieldPath, fieldType sql.NullString
	rows, err := tx.QueryContext(ctx, query, runID, entityID, instancePath)
	if err != nil {
		return Projection{}, false, err
	}
	if !rows.Next() {
		return Projection{}, false, errors.Join(rows.Err(), rows.Close())
	}
	err = rows.Scan(
		&header.EntityID, &header.InstancePath, &declaredType, &header.FlowTemplate, &header.Stage, &header.Revision,
		&header.Gates, &header.Bookkeeping, &header.Accumulator,
		&fieldID, &fieldPath, &fieldType, &header.Fields,
	)
	if err != nil {
		return Projection{}, false, errors.Join(err, rows.Close())
	}
	if rows.Next() {
		return Projection{}, false, errors.Join(&ProjectionError{runID, header.EntityID, header.InstancePath, "constructed header has conflicting field owners"}, rows.Close())
	}
	if err := rows.Err(); err != nil {
		return Projection{}, false, errors.Join(err, rows.Close())
	}
	if err := rows.Close(); err != nil {
		return Projection{}, false, err
	}
	if id, err := uuid.Parse(header.EntityID); err != nil || id == uuid.Nil || id.String() != header.EntityID || header.InstancePath == "" || strings.TrimSpace(header.InstancePath) != header.InstancePath || header.FlowTemplate == "" || strings.TrimSpace(header.FlowTemplate) != header.FlowTemplate || header.Stage == "" || header.Revision <= 0 {
		return Projection{}, false, &ProjectionError{runID, header.EntityID, header.InstancePath, "constructed header projection has invalid identity or lifecycle progress"}
	}
	if declaredType.Valid {
		if declaredType.String == "" || !fieldID.Valid || fieldID.String != header.EntityID || fieldPath.String != header.InstancePath || fieldType.String != declaredType.String || header.Fields == nil {
			return Projection{}, false, &ProjectionError{runID, header.EntityID, header.InstancePath, "constructed header projection disagrees with its declared field row"}
		}
		header.EntityType = declaredType.String
		if lockPostgres {
			var lockedID string
			if err := tx.QueryRowContext(ctx, `SELECT entity_id FROM entity_state WHERE run_id = $1 AND entity_id = $2 AND flow_instance = $3 AND entity_type = $4 FOR UPDATE`, runID, header.EntityID, header.InstancePath, header.EntityType).Scan(&lockedID); err != nil {
				return Projection{}, false, fmt.Errorf("lock constructed header field row: %w", err)
			}
		}
	} else if fieldID.Valid {
		return Projection{}, false, &ProjectionError{runID, header.EntityID, header.InstancePath, "fieldless constructed header acquired a field row"}
	}
	for _, object := range []struct {
		name string
		raw  any
	}{{"gates", header.Gates}, {"bookkeeping", header.Bookkeeping}, {"accumulator", header.Accumulator}} {
		if err := requireJSONObject(object.name, object.raw); err != nil {
			return Projection{}, false, &ProjectionError{runID, header.EntityID, header.InstancePath, err.Error()}
		}
	}
	if header.EntityType != "" {
		if err := requireJSONObject("fields", header.Fields); err != nil {
			return Projection{}, false, &ProjectionError{runID, header.EntityID, header.InstancePath, err.Error()}
		}
	}
	return header, true, nil
}

// Inventory uses the same strict header/optional-field pairing as exact reads.
// Field rows without construction authority never become executable instances.
func InventoryForMutation(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]Projection, error) {
	return inventory(ctx, tx, postgres, runID)
}

// InventoryForRead includes every constructed header, regardless of activity,
// and all state-only members, in the caller's existing snapshot.
func InventoryForRead(ctx context.Context, tx *sql.Tx, runID string) (ReadInventory, error) {
	headers, err := inventory(ctx, tx, false, runID)
	if err != nil {
		return ReadInventory{}, err
	}
	states, err := inventoryStateOnly(ctx, tx, runID)
	if err != nil {
		return ReadInventory{}, err
	}
	return ReadInventory{Constructed: headers, StateOnly: states}, nil
}

func inventory(ctx context.Context, tx *sql.Tx, lockPostgres bool, runID string) ([]Projection, error) {
	if tx == nil {
		return nil, fmt.Errorf("constructed inventory requires a transaction")
	}
	if id, err := uuid.Parse(runID); err != nil || id == uuid.Nil || id.String() != runID {
		return nil, fmt.Errorf("constructed inventory requires canonical nonzero run UUID")
	}
	rows, err := tx.QueryContext(ctx, `SELECT CAST(entity_id AS TEXT), instance_path FROM flow_instances WHERE run_id = $1 ORDER BY entity_id`, runID)
	if err != nil {
		return nil, err
	}
	var coordinates []Projection
	for rows.Next() {
		var coordinate Projection
		if err := rows.Scan(&coordinate.EntityID, &coordinate.InstancePath); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		coordinates = append(coordinates, coordinate)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	for i, coordinate := range coordinates {
		header, found, err := load(ctx, tx, lockPostgres, runID, coordinate.EntityID, coordinate.InstancePath)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, &ProjectionError{runID, coordinate.EntityID, coordinate.InstancePath, "constructed inventory member disappeared"}
		}
		coordinates[i] = header
	}
	return coordinates, nil
}

func inventoryStateOnly(ctx context.Context, tx *sql.Tx, runID string) (_ []Projection, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT es.entity_id, es.flow_instance, es.entity_type, COALESCE(es.current_state,''),
		es.fields, es.bookkeeping, es.gates, es.accumulator FROM entity_state es
		WHERE es.run_id = $1 AND NOT EXISTS (SELECT 1 FROM flow_instances fi WHERE fi.run_id = es.run_id
		AND (fi.entity_id = es.entity_id OR fi.instance_path = es.flow_instance)) ORDER BY es.entity_id`, runID)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	var states []Projection
	for rows.Next() {
		var state Projection
		if err := rows.Scan(&state.EntityID, &state.InstancePath, &state.EntityType, &state.Stage,
			&state.Fields, &state.Bookkeeping, &state.Gates, &state.Accumulator); err != nil {
			return nil, err
		}
		if id, err := uuid.Parse(state.EntityID); err != nil || id == uuid.Nil || id.String() != state.EntityID ||
			state.InstancePath == "" || strings.TrimSpace(state.InstancePath) != state.InstancePath ||
			state.EntityType == "" || strings.TrimSpace(state.EntityType) != state.EntityType || state.Stage == "" {
			return nil, &ProjectionError{runID, state.EntityID, state.InstancePath, "state-only projection requires exact identity, declared fields and lifecycle"}
		}
		for _, object := range []struct {
			name string
			raw  any
		}{{"fields", state.Fields}, {"bookkeeping", state.Bookkeeping}, {"gates", state.Gates}, {"accumulator", state.Accumulator}} {
			if err := requireJSONObject(object.name, object.raw); err != nil {
				return nil, &ProjectionError{runID, state.EntityID, state.InstancePath, err.Error()}
			}
		}
		states = append(states, state)
	}
	return states, rows.Err()
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
