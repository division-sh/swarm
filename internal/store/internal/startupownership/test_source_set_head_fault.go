package startupownership

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
)

type SourceSetHeadFault string

const (
	SourceSetHeadCorruptPlan      SourceSetHeadFault = "corrupt_plan"
	SourceSetHeadRevisionMismatch SourceSetHeadFault = "revision_mismatch"
)

// The native owner captures both CAS images; callers cannot supply replacement
// SQL, JSON, revisions or a different singleton for restoration.
type SourceSetHeadFaultReceipt struct {
	owner                      any
	beforeRevision, beforePlan string
	afterRevision, afterPlan   string
	operationID                string
}

func ApplySourceSetHeadFaultForTest(ctx context.Context, owner any, expected agenttopology.SourceSetPlan, fault SourceSetHeadFault) (*SourceSetHeadFaultReceipt, error) {
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	if fault != SourceSetHeadCorruptPlan && fault != SourceSetHeadRevisionMismatch {
		return nil, fmt.Errorf("source-set head fault requires a named variant")
	}
	receipt := &SourceSetHeadFaultReceipt{owner: owner}
	err := runSourceSetHeadFault(ctx, owner, func(ctx context.Context, tx *sql.Tx, sqlite bool) error {
		query := `SELECT revision,CAST(plan AS TEXT),CAST(operation_id AS TEXT) FROM agent_topology_source_set_head WHERE singleton_id=1`
		if !sqlite {
			query += ` FOR UPDATE`
		}
		if err := tx.QueryRowContext(ctx, query).Scan(&receipt.beforeRevision, &receipt.beforePlan, &receipt.operationID); err != nil {
			return err
		}
		actual, found, err := loadSourceSetModeTx(ctx, tx, sqlite, false)
		if err != nil || !found || !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("source-set head fault requires its exact valid predecessor: found=%t err=%v", found, err)
		}
		receipt.afterRevision, receipt.afterPlan = receipt.beforeRevision, receipt.beforePlan
		if fault == SourceSetHeadCorruptPlan {
			receipt.afterPlan = `{}`
		} else {
			receipt.afterRevision = strings.Repeat("0", 64)
			if receipt.afterRevision == receipt.beforeRevision {
				receipt.afterRevision = strings.Repeat("1", 64)
			}
		}
		if err := receipt.replaceTx(ctx, tx, receipt.beforeRevision, receipt.beforePlan, receipt.afterRevision, receipt.afterPlan, sqlite); err != nil {
			return err
		}
		// Capture PostgreSQL's persisted JSONB spelling, not a submitted spelling.
		return tx.QueryRowContext(ctx, `SELECT CAST(plan AS TEXT) FROM agent_topology_source_set_head WHERE singleton_id=1`).Scan(&receipt.afterPlan)
	})
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

func RestoreSourceSetHeadFaultForTest(ctx context.Context, owner any, receipt *SourceSetHeadFaultReceipt) error {
	if receipt == nil || receipt.owner != owner || receipt.beforeRevision == "" || receipt.beforePlan == "" || receipt.operationID == "" {
		return fmt.Errorf("source-set head restoration requires its original native owner and receipt")
	}
	return runSourceSetHeadFault(ctx, owner, func(ctx context.Context, tx *sql.Tx, sqlite bool) error {
		return receipt.replaceTx(ctx, tx, receipt.afterRevision, receipt.afterPlan, receipt.beforeRevision, receipt.beforePlan, sqlite)
	})
}

func (r *SourceSetHeadFaultReceipt) replaceTx(ctx context.Context, tx *sql.Tx, priorRevision, priorPlan, nextRevision, nextPlan string, sqlite bool) error {
	query := `UPDATE agent_topology_source_set_head SET revision=$1,plan=$2 WHERE singleton_id=1
		AND revision=$3 AND CAST(plan AS TEXT)=$4 AND CAST(operation_id AS TEXT)=$5`
	if !sqlite {
		query = `UPDATE agent_topology_source_set_head SET revision=$1,plan=$2::jsonb WHERE singleton_id=1
			AND revision=$3 AND CAST(plan AS TEXT)=$4 AND CAST(operation_id AS TEXT)=$5`
	}
	result, err := tx.ExecContext(ctx, query, nextRevision, nextPlan, priorRevision, priorPlan, r.operationID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("source-set head fault lost its exact singleton CAS: rows=%d err=%v", count, err)
	}
	return nil
}

func runSourceSetHeadFault(ctx context.Context, owner any, change func(context.Context, *sql.Tx, bool) error) error {
	if ctx == nil {
		return fmt.Errorf("source-set head fault requires its native context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch s := owner.(type) {
	case *StartupPostgresOwner:
		if s == nil || s.backend == nil || s.schemaGuard == nil {
			return fmt.Errorf("source-set head fault requires initialized PostgreSQL ownership")
		}
		if err := s.schemaGuard(); err != nil {
			return err
		}
		return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return change(ctx, tx, false) })
	case *StartupSQLiteOwner:
		if s == nil || s.backend == nil || s.schemaGuard == nil {
			return fmt.Errorf("source-set head fault requires initialized SQLite ownership")
		}
		if err := s.schemaGuard(); err != nil {
			return err
		}
		return s.backend.RunTransaction(ctx, "source-set head observation fault", func(ctx context.Context, tx *sql.Tx) error { return change(ctx, tx, true) })
	default:
		return fmt.Errorf("source-set head fault requires its native startup owner")
	}
}
