package entityruntime

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type TerminalCatalog interface {
	Terminal(flowTemplate, flowInstance, state string) (terminal bool, known bool)
}

// RunSummary is the entity owner's exact terminal-descriptor projection for
// one run. Unknown descriptors are malformed, not nonterminal guesses.
type RunSummary struct {
	RunID       string
	Total       int
	Terminal    int
	Nonterminal int
	Malformed   int
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("entity run summary requires run_id")
	}
	if s.Total < 0 || s.Terminal < 0 || s.Nonterminal < 0 || s.Malformed < 0 {
		return fmt.Errorf("entity run summary counts cannot be negative")
	}
	if s.Terminal+s.Nonterminal+s.Malformed != s.Total {
		return fmt.Errorf("entity run summary counts do not cover total")
	}
	return nil
}

func (s RunSummary) ReadyForCompletion() bool {
	return s.Total > 0 && s.Terminal == s.Total
}

// ReadRunSummary is the entity owner's selected-store projection of exact
// terminal descriptors. Unknown flow/state combinations are malformed.
func ReadRunSummary(
	ctx context.Context,
	queryer SummaryQueryer,
	dialect SummaryDialect,
	runID string,
	catalog TerminalCatalog,
) (RunSummary, error) {
	runID = strings.TrimSpace(runID)
	if queryer == nil || runID == "" || catalog == nil {
		return RunSummary{}, fmt.Errorf("entity run summary requires selected store, run_id, and terminal catalog")
	}
	query := `
		SELECT LOWER(COALESCE(es.current_state, '')),
		       COALESCE(es.flow_instance, ''),
		       COALESCE(fi.flow_template, '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
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
			LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
			WHERE es.run_id = $1::uuid
			ORDER BY es.entity_id
			FOR SHARE OF es`
	default:
		return RunSummary{}, fmt.Errorf("entity run summary requires selected store dialect")
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return RunSummary{}, fmt.Errorf("read entity run summary: %w", err)
	}
	defer rows.Close()
	summary := RunSummary{RunID: runID}
	for rows.Next() {
		var state, flowInstance, flowTemplate string
		if err := rows.Scan(&state, &flowInstance, &flowTemplate); err != nil {
			return RunSummary{}, fmt.Errorf("scan entity run summary: %w", err)
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
		return RunSummary{}, fmt.Errorf("iterate entity run summary: %w", err)
	}
	return summary, summary.Validate()
}
