package eventrecord

import (
	"context"
	"database/sql"
)

type ArtifactCallbackEventContextStorage struct {
	EntityID, FlowInstance, SourceRoute, Payload string
}

func ReadPostgresArtifactCallbackEventContextTx(ctx context.Context, tx *sql.Tx, event string) (ArtifactCallbackEventContextStorage, error) {
	var out ArtifactCallbackEventContextStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), source_route::text, payload::text FROM events WHERE event_id = $1::uuid`, event).Scan(&out.EntityID, &out.FlowInstance, &out.SourceRoute, &out.Payload)
	if err != nil {
		return ArtifactCallbackEventContextStorage{}, err
	}
	return out, nil
}

func ReadPostgresArtifactCallbackSourceRouteTx(ctx context.Context, tx *sql.Tx, event string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT source_route::text FROM events WHERE event_id = $1::uuid`, event).Scan(&out)
	if err != nil {
		return "", err
	}
	return out, nil
}
