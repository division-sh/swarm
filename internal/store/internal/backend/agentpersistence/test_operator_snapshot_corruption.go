package agentpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// OperatorSnapshotCorruption retains the complete original row, not authority
// inferred from an agent slug. Only the two reviewed defensive-read cuts exist.
type OperatorSnapshotCorruption struct {
	identity      agentidentity.StorageFields
	route         bool
	before, after []byte
	original      string
	constraints   []operatorSnapshotConstraint
}

func CaptureOperatorSnapshotRouteCorruptionTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, postgres bool) (*OperatorSnapshotCorruption, error) {
	return captureOperatorSnapshotCorruptionTx(ctx, tx, identity, postgres, true)
}

func CaptureOperatorSnapshotStatusCorruptionTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, postgres bool) (*OperatorSnapshotCorruption, error) {
	return captureOperatorSnapshotCorruptionTx(ctx, tx, identity, postgres, false)
}

func captureOperatorSnapshotCorruptionTx(ctx context.Context, tx *sql.Tx, identity agentidentity.Identity, postgres, route bool) (*OperatorSnapshotCorruption, error) {
	fields, err := IdentityFields(identity)
	if err != nil {
		return nil, err
	}
	if fields.RoutePresence != "present" || fields.FlowScopeKey == "" {
		return nil, errors.New("operator snapshot corruption requires a complete present-route identity")
	}
	fault := &OperatorSnapshotCorruption{identity: fields, route: route}
	row, original, err := fault.readRow(ctx, tx, postgres, false)
	if err != nil {
		return nil, err
	}
	fault.before, fault.original = row, original
	return fault, nil
}

func (f *OperatorSnapshotCorruption) readRow(ctx context.Context, tx *sql.Tx, postgres, corrupted bool) ([]byte, string, error) {
	if f == nil || tx == nil {
		return nil, "", errors.New("operator snapshot corruption requires a captured native row")
	}
	fields := f.identity
	if corrupted && f.route {
		fields.FlowScopeKey = ""
	}
	query := `SELECT * FROM agents WHERE run_id=$1 AND agent_id=$2 AND agent_name_owner=$3 AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6 AND flow_instance_id=$7 AND flow_instance=$8`
	if postgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, "", err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, "", err
		}
		return nil, "", sql.ErrNoRows
	}
	values, pointers := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		return nil, "", err
	}
	wanted := "status"
	if f.route {
		wanted = "flow_scope_key"
	}
	var original string
	types := make([]string, len(columns))
	for i, column := range columns {
		types[i] = fmt.Sprintf("%T", values[i])
		if column == wanted {
			switch value := values[i].(type) {
			case string:
				original = value
			case []byte:
				original = string(value)
			default:
				return nil, "", fmt.Errorf("operator snapshot %s has unexpected storage type %T", column, value)
			}
		}
	}
	encoded, err := json.Marshal(struct {
		Columns []string
		Types   []string
		Values  []any
	}{columns, types, values})
	if err != nil {
		return nil, "", err
	}
	if rows.Next() {
		return nil, "", errors.New("operator snapshot identity selected multiple rows")
	}
	return encoded, original, rows.Err()
}

func (f *OperatorSnapshotCorruption) InstallTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	if f == nil || len(f.before) == 0 || len(f.after) != 0 {
		return errors.New("operator snapshot fault needs a fresh captured receipt")
	}
	current, _, err := f.readRow(ctx, tx, postgres, false)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, f.before) {
		return errors.New("operator snapshot original row changed before fault installation")
	}
	if postgres {
		f.constraints, err = captureOperatorSnapshotConstraintsTx(ctx, tx, f.route)
		if err != nil {
			return err
		}
		for _, constraint := range f.constraints {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+constraint.table+` DROP CONSTRAINT `+quoteOperatorSnapshotIdentifier(constraint.name)); err != nil {
				return err
			}
		}
	}
	value := "unknown"
	if f.route {
		value = ""
	}
	if err := f.replaceValue(ctx, tx, value, false); err != nil {
		return err
	}
	f.after, _, err = f.readRow(ctx, tx, postgres, true)
	if err != nil {
		return err
	}
	// The fixed assignment must preserve every other column, including clocks.
	return requireOperatorSnapshotSingleFieldDelta(f.before, f.after, f.route)
}

func (f *OperatorSnapshotCorruption) RestoreTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	if f == nil || len(f.before) == 0 {
		return errors.New("operator snapshot restore requires its original receipt")
	}
	current, _, err := f.readRow(ctx, tx, postgres, true)
	if errors.Is(err, sql.ErrNoRows) && f.route {
		current, _, err = f.readRow(ctx, tx, postgres, false)
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(current, f.before) {
		if len(f.after) == 0 || !bytes.Equal(current, f.after) {
			return errors.New("operator snapshot replacement changed before exact restoration")
		}
		if err := f.replaceValue(ctx, tx, f.original, true); err != nil {
			return err
		}
		current, _, err = f.readRow(ctx, tx, postgres, false)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, f.before) {
			return errors.New("operator snapshot restoration did not preserve the full original row")
		}
	}
	if postgres {
		return restoreOperatorSnapshotConstraintsTx(ctx, tx, f.constraints)
	}
	return nil
}

func (f *OperatorSnapshotCorruption) replaceValue(ctx context.Context, tx *sql.Tx, value string, corrupted bool) error {
	fields := f.identity
	if corrupted && f.route {
		fields.FlowScopeKey = ""
	}
	column := "status"
	if f.route {
		column = "flow_scope_key"
	}
	result, err := tx.ExecContext(ctx, `UPDATE agents SET `+column+`=$1 WHERE run_id=$2 AND agent_id=$3 AND agent_name_owner=$4 AND agent_name_source=$5 AND agent_route_presence=$6 AND flow_scope_key=$7 AND flow_instance_id=$8 AND flow_instance=$9`, value, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("operator snapshot fault changed %d rows, want exactly one", count)
	}
	return nil
}

func (f *OperatorSnapshotCorruption) VerifyEnforcementTx(ctx context.Context, tx *sql.Tx) error {
	if f == nil || len(f.before) == 0 {
		return errors.New("operator snapshot enforcement needs its original receipt")
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT operator_snapshot_enforcement`); err != nil {
		return err
	}
	value := "unknown"
	if f.route {
		value = ""
	}
	writeErr := f.replaceValue(ctx, tx, value, false)
	_, rollbackErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT operator_snapshot_enforcement`)
	_, releaseErr := tx.ExecContext(ctx, `RELEASE SAVEPOINT operator_snapshot_enforcement`)
	if err := errors.Join(rollbackErr, releaseErr); err != nil {
		return err
	}
	var pg interface{ SQLState() string }
	var sqlite interface{ Code() int }
	if errors.As(writeErr, &pg) && (pg.SQLState() == "23514" || pg.SQLState() == "23503") {
		return nil
	}
	if errors.As(writeErr, &sqlite) && sqlite.Code()&255 == 19 {
		return nil
	}
	return fmt.Errorf("restored operator snapshot constraints did not refuse the exact violating write: %v", writeErr)
}

func requireOperatorSnapshotSingleFieldDelta(before, after []byte, route bool) error {
	var left, right struct {
		Columns []string
		Types   []string
		Values  []json.RawMessage
	}
	if err := json.Unmarshal(before, &left); err != nil {
		return err
	}
	if err := json.Unmarshal(after, &right); err != nil {
		return err
	}
	if len(left.Columns) != len(right.Columns) || len(left.Columns) != len(left.Types) || len(left.Columns) != len(right.Types) || len(left.Columns) != len(left.Values) || len(left.Columns) != len(right.Values) {
		return errors.New("operator snapshot row shape changed")
	}
	wanted, changed := "status", 0
	if route {
		wanted = "flow_scope_key"
	}
	for i, name := range left.Columns {
		if name != right.Columns[i] || left.Types[i] != right.Types[i] {
			return errors.New("operator snapshot column order changed")
		}
		if bytes.Equal(left.Values[i], right.Values[i]) {
			continue
		}
		if name != wanted {
			return fmt.Errorf("operator snapshot fault changed unrelated column %s", name)
		}
		changed++
	}
	if changed != 1 {
		return fmt.Errorf("operator snapshot fault changed %d columns, want one", changed)
	}
	return nil
}

func quoteOperatorSnapshotIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
