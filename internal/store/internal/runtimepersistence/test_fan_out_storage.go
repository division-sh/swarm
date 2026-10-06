package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

type FanOutTriggeredIntentStorageEvidence struct {
	TriggeringDeliveryID, SemanticDigest string
	Capsule                              json.RawMessage
	Source                               fanoutobligation.SourceRef
	Cursor, Cardinality                  int
	Status                               string
}

type FanOutIntentOutcomeStorageEvidence struct {
	Ordinal       int
	EventID, Kind string
}

type FanOutIntentCompletionStorageEvidence struct {
	Outcomes            []FanOutIntentOutcomeStorageEvidence
	Cursor, Cardinality int
	Status              string
}

func ReadFanOutTriggeredIntentStorageForTest(ctx context.Context, selected any, runID, eventID string, node runtimeidentity.ExecutableNode, ref runtimecontracts.FanOutElementRef) ([]FanOutTriggeredIntentStorageEvidence, error) {
	for _, value := range []string{runID, eventID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return nil, fmt.Errorf("fan-out trigger storage requires exact canonical run/event identities")
		}
	}
	if !node.Valid() {
		return nil, fmt.Errorf("fan-out trigger storage requires an admitted node identity")
	}
	if _, err := ref.DeclarationIdentity(); err != nil {
		return nil, err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var evidence []FanOutTriggeredIntentStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT i.triggering_delivery_id,i.semantic_digest,i.capsule,
			i.source_kind,COALESCE(CAST(i.source_event_id AS TEXT),''),COALESCE(CAST(i.source_run_id AS TEXT),''),COALESCE(CAST(i.source_entity_id AS TEXT),''),
			i.source_field,COALESCE(CAST(i.source_mutation_id AS TEXT),''),i.cardinality,i.cursor,i.status
			FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id
			WHERE i.run_id=$1 AND d.event_id=$2 AND d.subscriber_type='node' AND d.subscriber_id=$3
			AND i.flow_path=$4 AND i.declaration_family=$5 AND i.semantic_path=$6`,
			runID, eventID, node.Key(), ref.FlowPath, ref.Family, ref.SemanticPath)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row FanOutTriggeredIntentStorageEvidence
			var capsule []byte
			if err := rows.Scan(&row.TriggeringDeliveryID, &row.SemanticDigest, &capsule,
				&row.Source.Kind, &row.Source.EventID, &row.Source.RunID, &row.Source.EntityID,
				&row.Source.Field, &row.Source.MutationID, &row.Cardinality, &row.Cursor, &row.Status); err != nil {
				return err
			}
			row.Capsule = append(json.RawMessage(nil), capsule...)
			evidence = append(evidence, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	return evidence, nil
}

func ReadFanOutIntentCompletionStorageForTest(ctx context.Context, selected any, key fanoutobligation.IntentKey) (FanOutIntentCompletionStorageEvidence, error) {
	var empty FanOutIntentCompletionStorageEvidence
	if err := key.Validate(); err != nil {
		return empty, err
	}
	if key.DeploymentFeedID != "" {
		return empty, fmt.Errorf("handler fan-out completion storage cannot observe a deployment-feed origin")
	}
	for _, value := range []string{key.RunID, key.TriggeringDeliveryID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return empty, fmt.Errorf("fan-out completion storage requires exact canonical run/delivery identities")
		}
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return empty, err
	}
	var evidence FanOutIntentCompletionStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT ordinal,event_id,outcome_kind FROM fan_out_outcomes
			WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 ORDER BY ordinal`,
			key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row FanOutIntentOutcomeStorageEvidence
			if err := rows.Scan(&row.Ordinal, &row.EventID, &row.Kind); err != nil {
				return err
			}
			evidence.Outcomes = append(evidence.Outcomes, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT cursor,cardinality,status FROM fan_out_intents
			WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`,
			key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).
			Scan(&evidence.Cursor, &evidence.Cardinality, &evidence.Status)
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return empty, err
	}
	return evidence, nil
}
