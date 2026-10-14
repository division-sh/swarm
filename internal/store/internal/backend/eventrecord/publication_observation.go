package eventrecord

import (
	"context"
	"database/sql"
	"encoding/json"
)

type PublicationEventProjection struct {
	EntityID, FlowInstance, ProducedBy, TargetRoute, TargetSet string
	Payload                                                    json.RawMessage
}

func ReadActivityForkRequestPayloadStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) ([]byte, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT payload FROM events WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

type ActivityForkResultStorage struct {
	EventName string
	Payload   []byte
}

func ReadActivityForkResultStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (ActivityForkResultStorage, error) {
	var out ActivityForkResultStorage
	if err := tx.QueryRowContext(ctx, `SELECT event_name,payload FROM events WHERE run_id=$1 AND event_id=$2`, runID, eventID).Scan(&out.EventName, &out.Payload); err != nil {
		return ActivityForkResultStorage{}, err
	}
	return out, nil
}

// This physical projection does not admit or decode a complete event. Preserve
// the original scalar NULL defaults and payload bytes for caller assertions.
func ReadPublicationEventProjectionTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (PublicationEventProjection, error) {
	query := `SELECT COALESCE(entity_id,''), COALESCE(flow_instance,''), COALESCE(produced_by,''), COALESCE(target_route,'{}'), COALESCE(target_set,'[]'), payload FROM events WHERE event_id=$1`
	if postgres {
		query = `SELECT COALESCE(entity_id::text,''), COALESCE(flow_instance,''), produced_by, COALESCE(target_route::text,'{}'), COALESCE(target_set::text,'[]'), payload FROM events WHERE event_id=$1::uuid`
	}
	var out PublicationEventProjection
	var payload []byte
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&out.EntityID, &out.FlowInstance, &out.ProducedBy, &out.TargetRoute, &out.TargetSet, &payload); err != nil {
		return PublicationEventProjection{}, err
	}
	out.Payload = payload
	return out, nil
}

func ReadRunStartEventProjectionTx(ctx context.Context, tx *sql.Tx, runID, eventName string) (PublicationEventProjection, error) {
	var out PublicationEventProjection
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(entity_id::text,''), COALESCE(flow_instance,''), produced_by, COALESCE(target_route::text,'{}'), COALESCE(target_set::text,'[]'), payload FROM events WHERE run_id=$1::uuid AND event_name=$2`, runID, eventName).Scan(&out.EntityID, &out.FlowInstance, &out.ProducedBy, &out.TargetRoute, &out.TargetSet, &out.Payload)
	if err != nil {
		return PublicationEventProjection{}, err
	}
	return out, nil
}

func ReadRunStartEventIdentityTx(ctx context.Context, tx *sql.Tx, runID, eventName string) (string, error) {
	var out string
	err := tx.QueryRowContext(ctx, `SELECT event_id::text FROM events WHERE run_id=$1::uuid AND event_name=$2`, runID, eventName).Scan(&out)
	return out, err
}

type PublicationEventIdentityStorage struct{ RunID, EventName, ProducedBy, EntityID string }

// Diagnostics need only the stored run identity, even when other event columns
// cannot be decoded. Do not turn this physical lookup into event admission.
func ReadEventRunIDStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var runID string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM events WHERE event_id=$1`, eventID).Scan(&runID); err != nil {
		return "", err
	}
	return runID, nil
}

type RootTemplatePublicationStorage struct {
	EventName, EntityID, FlowInstance, TargetRoute, TargetSet string
	Payload                                                   json.RawMessage
}

func ReadPostgresRootTemplatePublicationTx(ctx context.Context, tx *sql.Tx, eventID string) (RootTemplatePublicationStorage, error) {
	var out RootTemplatePublicationStorage
	var payload []byte
	err := tx.QueryRowContext(ctx, `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,''), COALESCE(target_route::text,'{}'), COALESCE(target_set::text,'[]'), payload FROM events WHERE event_id=$1::uuid`, eventID).Scan(&out.EventName, &out.EntityID, &out.FlowInstance, &out.TargetRoute, &out.TargetSet, &payload)
	if err != nil {
		return RootTemplatePublicationStorage{}, err
	}
	out.Payload = payload
	return out, nil
}

func ReadPublicationEventIdentityStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (PublicationEventIdentityStorage, error) {
	var out PublicationEventIdentityStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT), event_name, produced_by, COALESCE(CAST(entity_id AS TEXT),'') FROM events WHERE event_id=$1`, eventID).Scan(&out.RunID, &out.EventName, &out.ProducedBy, &out.EntityID)
	if err != nil {
		return PublicationEventIdentityStorage{}, err
	}
	return out, nil
}

func ReadPhysicalPublicationEntityIdentityStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	var entityID string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(entity_id AS TEXT),'') FROM events WHERE event_id=$1`, eventID).Scan(&entityID)
	if err != nil {
		return "", err
	}
	return entityID, nil
}

type OperatorReferenceStorage struct{ SourceEventID, ReferenceEventID string }

func ReadOperatorReferenceStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (OperatorReferenceStorage, error) {
	var out OperatorReferenceStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(source_event_id AS TEXT),''), COALESCE(CAST(operator_reference_event_id AS TEXT),'') FROM events WHERE event_id=$1`, eventID).Scan(&out.SourceEventID, &out.ReferenceEventID)
	if err != nil {
		return OperatorReferenceStorage{}, err
	}
	return out, nil
}
