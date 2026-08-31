package entitystore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimeentity "github.com/division-sh/swarm/internal/runtime/entityruntime"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// ReadRunSummary is the entity owner's selected-store projection of exact
// terminal descriptors. Unknown flow/state combinations are malformed.
func ReadRunSummary(
	ctx context.Context,
	queryer SummaryQueryer,
	dialect SummaryDialect,
	runID string,
	catalog runtimeentity.TerminalCatalog,
) (runtimeentity.RunSummary, error) {
	runID = strings.TrimSpace(runID)
	if queryer == nil || runID == "" || catalog == nil {
		return runtimeentity.RunSummary{}, fmt.Errorf("entity run summary requires selected store, run_id, and terminal catalog")
	}
	query := `
		SELECT LOWER(COALESCE(es.current_state, '')),
		       COALESCE(es.flow_instance, ''),
		       COALESCE(fi.flow_template, '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
		WHERE es.run_id = ?
		ORDER BY es.entity_id`
	args := []any{runID}
	switch dialect {
	case SummaryDialectSQLite:
	case SummaryDialectPostgres:
		query = `
			SELECT LOWER(COALESCE(es.current_state, '')),
			       COALESCE(es.flow_instance, ''),
			       COALESCE(fi.flow_template, '')
			FROM entity_state es
			LEFT JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
			WHERE es.run_id = $1::uuid
			ORDER BY es.entity_id
			FOR SHARE OF es`
	default:
		return runtimeentity.RunSummary{}, fmt.Errorf("entity run summary requires selected store dialect")
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimeentity.RunSummary{}, fmt.Errorf("read entity run summary: %w", err)
	}
	defer rows.Close()
	summary := runtimeentity.RunSummary{RunID: runID}
	for rows.Next() {
		var state, flowInstance, flowTemplate string
		if err := rows.Scan(&state, &flowInstance, &flowTemplate); err != nil {
			return runtimeentity.RunSummary{}, fmt.Errorf("scan entity run summary: %w", err)
		}
		summary.Total++
		terminal, known := catalog.Terminal(flowTemplate, flowInstance, state)
		switch {
		case !known:
			summary.Malformed++
		case terminal:
			summary.Terminal++
		default:
			summary.Nonterminal++
		}
	}
	if err := rows.Err(); err != nil {
		return runtimeentity.RunSummary{}, fmt.Errorf("iterate entity run summary: %w", err)
	}
	return summary, summary.Validate()
}
