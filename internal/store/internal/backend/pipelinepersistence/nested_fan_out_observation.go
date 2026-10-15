package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type NestedFanOutBarrierStorage struct {
	Status  string
	Summary []byte
}

type NestedFanOutCursorClaimStorage struct{ Cursor, Claims int }
type NestedFanOutOwedStorage struct{ Intents, Owed int }

type RepeatedFanOutIntentStorage struct {
	Cardinality int
	MutationID  string
}

func ReadRepeatedFanOutIntentTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey) (RepeatedFanOutIntentStorage, error) {
	var out RepeatedFanOutIntentStorage
	err := tx.QueryRowContext(ctx, `SELECT cardinality,source_mutation_id FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&out.Cardinality, &out.MutationID)
	if err != nil {
		return RepeatedFanOutIntentStorage{}, err
	}
	return out, nil
}

func CountRepeatedFanOutTriggerOutcomesTx(ctx context.Context, tx *sql.Tx, run, delivery string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2`, run, delivery).Scan(&out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadNestedFanOutBarrierStatusTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT status FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func ReadNestedFanOutBarrierTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey) (NestedFanOutBarrierStorage, error) {
	var out NestedFanOutBarrierStorage
	err := tx.QueryRowContext(ctx, `SELECT status,summary FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&out.Status, &out.Summary)
	if err != nil {
		return NestedFanOutBarrierStorage{}, err
	}
	out.Summary = append([]byte(nil), out.Summary...)
	return out, nil
}

func ReadNestedPipelineReceiptOutcomeTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountNestedSuccessfulPipelineReceiptTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline' AND outcome='success'`, event).Scan(&out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadNestedFanOutCursorClaimTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey) (NestedFanOutCursorClaimStorage, error) {
	var out NestedFanOutCursorClaimStorage
	err := tx.QueryRowContext(ctx, `SELECT cursor,CASE WHEN claim_owner IS NULL THEN 0 ELSE 1 END FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&out.Cursor, &out.Claims)
	if err != nil {
		return NestedFanOutCursorClaimStorage{}, err
	}
	return out, nil
}

func CountNestedRunFanOutOutcomesTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, run).Scan(&out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountRunFanOutBarrierStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var out int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_obligation_barriers WHERE run_id=$1`, run).Scan(&out)
	return out, err
}

func ReadNestedRunFanOutOwedTx(ctx context.Context, tx *sql.Tx, run string) (NestedFanOutOwedStorage, error) {
	var out NestedFanOutOwedStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cardinality-cursor),0) FROM fan_out_intents WHERE run_id=$1`, run).Scan(&out.Intents, &out.Owed)
	if err != nil {
		return NestedFanOutOwedStorage{}, err
	}
	return out, nil
}

func ReadNestedCommittedOutcomeEventIDsTx(ctx context.Context, tx *sql.Tx, run string) (out []string, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed' ORDER BY event_id`, run)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func ReadNestedAccountIntentKeysTx(ctx context.Context, tx *sql.Tx, run string) (out []fanoutobligation.IntentKey, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path FROM fan_out_intents WHERE run_id=$1 AND flow_path='account'`, run)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = nil
		}
	}()
	for rows.Next() {
		key := fanoutobligation.IntentKey{RunID: run}
		if err := rows.Scan(&key.TriggeringDeliveryID, &key.ElementRef.FlowPath, &key.ElementRef.Family, &key.ElementRef.SemanticPath); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}
