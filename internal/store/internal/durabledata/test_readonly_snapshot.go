package durabledata

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// The order is the original read-only journey's independent observation order.
// It is deliberately not a caller-supplied relation or query interface.
var readOnlyResourceRelations = [...]string{
	"resource_declarations", "resource_bundle_declarations", "resource_bundle_import_shapes",
	"resource_heads", "resource_head_history", "resource_versions", "resource_version_provenance",
	"resource_version_pins", "resource_source_invocations", "resource_prune_invocations",
	"resource_prune_pin_evidence", "resource_run_creation_operations",
	"resource_run_creation_child_evaluations", "resource_run_creation_child_reservations",
}

func ResourceReadOnlyObservationCount() int { return len(readOnlyResourceRelations) }

func ReadResourcePhysicalObservationTx(ctx context.Context, tx *sql.Tx, part int) (string, []string, error) {
	if part < 0 || part >= len(readOnlyResourceRelations) {
		return "", nil, fmt.Errorf("invalid resource observation ordinal %d", part)
	}
	table := readOnlyResourceRelations[part]
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", nil, err
	}
	var state []string
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return "", nil, err
		}
		state = append(state, fmt.Sprintf("%#v", values))
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	sort.Strings(state)
	return table, state, nil
}
