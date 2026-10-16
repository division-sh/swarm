package schemastore

import (
	"context"
	"database/sql"
	"strings"

	"github.com/google/uuid"
)

func CountPublicationCompletionFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID) (int, error) {
	name := "publication_completion_" + strings.ReplaceAll(id.String(), "-", "")
	var count int
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
	if postgres {
		query = `SELECT (SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1) + (SELECT COUNT(*) FROM pg_proc WHERE proname=$1 AND pronamespace=current_schema()::regnamespace)`
	}
	err := tx.QueryRowContext(ctx, query, name).Scan(&count)
	return count, err
}

func CountPublicationMaterializationFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID) (int, error) {
	name := "publication_materialization_" + strings.ReplaceAll(id.String(), "-", "")
	var count int
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
	if postgres {
		query = `SELECT (SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1) + (SELECT COUNT(*) FROM pg_proc WHERE proname=$1 AND pronamespace=current_schema()::regnamespace)`
	}
	err := tx.QueryRowContext(ctx, query, name).Scan(&count)
	return count, err
}

func CountPublicationReplayFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool, id uuid.UUID) (int, error) {
	name := "publication_replay_" + strings.ReplaceAll(id.String(), "-", "")
	var count int
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
	if postgres {
		query = `SELECT (SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1) + (SELECT COUNT(*) FROM pg_proc WHERE proname=$1 AND pronamespace=current_schema()::regnamespace)`
	}
	err := tx.QueryRowContext(ctx, query, name).Scan(&count)
	return count, err
}

type ConformanceFaultObjectsStorage struct{ Functions, Triggers int }

func ReadConformanceDiagnosticFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool) (ConformanceFaultObjectsStorage, error) {
	var out ConformanceFaultObjectsStorage
	query := `SELECT 0,(SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='fail_delivery_dead_letter_insert')`
	if postgres {
		query = `SELECT
	(SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='fail_delivery_dead_letter_insert'),
	(SELECT COUNT(*) FROM pg_trigger WHERE tgname='fail_delivery_dead_letter_insert' AND NOT tgisinternal)`
	}
	err := tx.QueryRowContext(ctx, query).Scan(&out.Functions, &out.Triggers)
	return out, err
}

func CountNodeRecoveryPoisonFaultObjectsTx(ctx context.Context, tx *sql.Tx, postgres bool) (int, error) {
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='fail_node_recovery_poison_update'`
	if postgres {
		query = `SELECT
	(SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='fail_node_recovery_poison_update_fn')+
	(SELECT COUNT(*) FROM pg_trigger WHERE tgname='fail_node_recovery_poison_update' AND NOT tgisinternal)`
	}
	var out int
	err := tx.QueryRowContext(ctx, query).Scan(&out)
	return out, err
}
