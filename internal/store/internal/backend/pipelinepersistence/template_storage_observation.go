package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
)

func ReadPostgresExactPipelineReceiptOutcomeStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var outcome string
	err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&outcome)
	if err != nil {
		return "", err
	}
	return outcome, nil
}

type TemplateInstanceFieldsStorage struct{ Path, Fields string }

type TemplateReceiptDiagnosticStorage struct{ SubscriberType, SubscriberID, Outcome, IdempotencyKey, SideEffects string }

func ReadTemplateEventReceiptDiagnosticsTx(ctx context.Context, tx *sql.Tx, event string) (out []TemplateReceiptDiagnosticStorage, err error) {
	rows, err := tx.QueryContext(ctx, `SELECT subscriber_type,subscriber_id,outcome,COALESCE(idempotency_key,''),COALESCE(CAST(side_effects AS TEXT),'') FROM event_receipts WHERE event_id=$1 ORDER BY subscriber_type,subscriber_id,idempotency_key`, event)
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
		var row TemplateReceiptDiagnosticStorage
		if err := rows.Scan(&row.SubscriberType, &row.SubscriberID, &row.Outcome, &row.IdempotencyKey, &row.SideEffects); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func CountTemplateNodeReceiptTx(ctx context.Context, tx *sql.Tx, event, node string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, event, node).Scan(&count)
	return count, err
}

func CountTemplateSubscribedReplayScopeTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id=$1 AND scope='subscribed'`, event).Scan(&count)
	return count, err
}

func CountTemplateEventReplayScopesTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id=$1`, event).Scan(&count)
	return count, err
}

func CountTemplateInstancesTx(ctx context.Context, tx *sql.Tx, template string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE flow_template=$1`, template).Scan(&count)
	return count, err
}

func CountRunOperatingInstancesTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='operating'`, run).Scan(&count)
	return count, err
}

func ReadRunOperatingInstanceFieldsTx(ctx context.Context, tx *sql.Tx, run string) (TemplateInstanceFieldsStorage, error) {
	var out TemplateInstanceFieldsStorage
	err := tx.QueryRowContext(ctx, `SELECT fi.instance_path,CAST(es.fields AS TEXT) FROM flow_instances fi JOIN entity_state es ON es.run_id=fi.run_id AND es.entity_id=fi.entity_id WHERE fi.run_id=$1 AND fi.flow_template='operating'`, run).Scan(&out.Path, &out.Fields)
	if err != nil {
		return TemplateInstanceFieldsStorage{}, err
	}
	return out, nil
}

func CountTemplateActiveAgentRoutesTx(ctx context.Context, tx *sql.Tx, agent, path string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM routing_rules WHERE flow_instance=$2 AND subscriber_type='agent' AND subscriber_id=$1 AND status='active'`, agent, path).Scan(&count)
	return count, err
}

func CountTemplatePlaceholderRoutesTx(ctx context.Context, tx *sql.Tx, path string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM routing_rules WHERE flow_instance=$1 AND subscriber_id='ceo-{product_id}'`, path).Scan(&count)
	return count, err
}

func CountTemplatePrefixRoutingRulesTx(ctx context.Context, tx *sql.Tx, template string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM routing_rules WHERE flow_instance LIKE $1`, template+"/%").Scan(&count)
	return count, err
}

func CountPhysicalRoutingRulesTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM routing_rules`).Scan(&count)
	return count, err
}
