package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type operatorSnapshotConstraint struct {
	table, name, definition string
}

// Only the route CHECK and the fixture's existing session FK need relaxation.
// No other referring-table constraint is removed, even if one is added later.
func captureOperatorSnapshotConstraintsTx(ctx context.Context, tx *sql.Tx, route bool) ([]operatorSnapshotConstraint, error) {
	rows, err := tx.QueryContext(ctx, `SELECT c.conrelid::regclass::text,c.conname,pg_get_constraintdef(c.oid),c.convalidated,
 (SELECT string_agg(a.attname,',' ORDER BY a.attname) FROM unnest(c.conkey) key(attnum) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.attnum)
 FROM pg_constraint c WHERE
 (c.conrelid='agents'::regclass AND c.contype='c' AND (($1 AND pg_get_constraintdef(c.oid) LIKE '%flow_scope_key%') OR (NOT $1 AND pg_get_constraintdef(c.oid) LIKE '%status%')))
 OR ($1 AND c.conrelid='agent_sessions'::regclass AND c.confrelid='agents'::regclass AND c.contype='f')
 ORDER BY c.conrelid::regclass::text,c.conname`, route)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []operatorSnapshotConstraint
	for rows.Next() {
		var constraint operatorSnapshotConstraint
		var validated bool
		var columns string
		if err := rows.Scan(&constraint.table, &constraint.name, &constraint.definition, &validated, &columns); err != nil {
			return nil, err
		}
		if !validated {
			return nil, errors.New("operator snapshot fault refuses an unvalidated original constraint")
		}
		expected := "status"
		if route {
			expected = "agent_route_presence,flow_instance,flow_instance_id,flow_scope_key"
		}
		if constraint.table == "agent_sessions" && route {
			names := []string{"run_id", "agent_id", "agent_name_owner", "agent_name_source", "agent_route_presence", "flow_scope_key", "flow_instance_id", "flow_instance"}
			sort.Strings(names)
			expected = strings.Join(names, ",")
		} else if constraint.table != "agents" {
			return nil, errors.New("unexpected operator snapshot constraint owner")
		}
		if columns != expected {
			return nil, fmt.Errorf("operator snapshot refuses unexpected %s constraint columns %s", constraint.table, columns)
		}
		out = append(out, constraint)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	want := 1
	if route {
		want = 2
	}
	if len(out) != want {
		return nil, fmt.Errorf("operator snapshot found %d required constraints, want%d", len(out), want)
	}
	return out, nil
}

func restoreOperatorSnapshotConstraintsTx(ctx context.Context, tx *sql.Tx, original []operatorSnapshotConstraint) error {
	for _, constraint := range original {
		var definition string
		var validated bool
		err := tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid),convalidated FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2`, constraint.table, constraint.name).Scan(&definition, &validated)
		if errors.Is(err, sql.ErrNoRows) {
			// Restoring without NOT VALID validates all rows before commit.
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+constraint.table+` ADD CONSTRAINT `+quoteOperatorSnapshotIdentifier(constraint.name)+` `+constraint.definition); err != nil {
				return err
			}
			if err := tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid),convalidated FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2`, constraint.table, constraint.name).Scan(&definition, &validated); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if definition != constraint.definition || !validated {
			return errors.New("operator snapshot constraint restoration changed its exact definition or validation state")
		}
	}
	return nil
}
