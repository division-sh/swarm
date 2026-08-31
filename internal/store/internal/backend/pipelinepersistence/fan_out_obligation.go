package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func commitFanOutIntentTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *revisionEffects,
	request fanoutobligation.IntentRequest,
	stateRunID string,
	stateFields json.RawMessage,
	triggerEventID string,
	createdAt time.Time,
) error {
	if tx == nil {
		return fmt.Errorf("fan-out intent requires private transaction")
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Key.RunID != strings.TrimSpace(stateRunID) {
		return fmt.Errorf("fan-out intent run disagrees with engine mutation")
	}
	persistedSource, err := bindFanOutSourceTx(ctx, tx, postgres, effects, request, stateFields, triggerEventID, createdAt)
	if err != nil {
		return err
	}
	capsule, err := fanoutobligation.MarshalCapsule(request.Capsule)
	if err != nil {
		return fmt.Errorf("encode fan-out capsule: %w", err)
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	status := fanoutobligation.StatusOpen
	if request.Cardinality == 0 {
		status = fanoutobligation.StatusClosed
	}
	args := fanOutIntentSQLArgs(request, persistedSource, capsule, status, createdAt)
	query := `
		INSERT INTO fan_out_intents (
			run_id, triggering_delivery_id, package_key, element_id,
			bundle_hash, semantic_digest, source_kind, source_event_id,
			source_run_id, source_entity_id, source_field, source_mutation_id,
			source_resource_package_key, source_resource_event_name, source_resource_version_id,
			cardinality, cursor, status, next_chunk_size, capsule, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, 0, $17, $18, $19, $20, $20
		)`
	if postgres {
		query = strings.ReplaceAll(query, "$19", "$19::jsonb")
	} else {
		query = postgresPlaceholdersToSQLite(query, 20)
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert fan-out intent: %w", err)
	}
	return effects.Add(request.Key.RunID, privaterunforkrevision.FamilyFanOutObligations)
}

func fanOutIntentSQLArgs(request fanoutobligation.IntentRequest, source fanoutobligation.SourceRef, capsule []byte, status fanoutobligation.Status, createdAt time.Time) []any {
	return []any{
		request.Key.RunID,
		request.Key.TriggeringDeliveryID,
		request.Key.ElementRef.PackageKey,
		request.Key.ElementRef.ElementID,
		request.PlanRef.BundleHash,
		request.PlanRef.SemanticDigest,
		string(source.Kind),
		fanOutNullable(source.EventID),
		fanOutNullable(source.RunID),
		fanOutNullable(source.EntityID),
		fanOutNullable(source.Field),
		fanOutNullable(source.MutationID),
		fanOutNullable(source.Declaration.PackageKey),
		fanOutNullable(source.Declaration.EventName),
		fanOutNullable(string(source.VersionID)),
		request.Cardinality,
		string(status),
		fanoutobligation.InitialChunkSize,
		string(capsule),
		createdAt.UTC(),
	}
}

func fanOutNullable(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func bindFanOutSourceTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *revisionEffects,
	request fanoutobligation.IntentRequest,
	stateFields json.RawMessage,
	triggerEventID string,
	createdAt time.Time,
) (fanoutobligation.SourceRef, error) {
	source := request.Source
	switch source.Kind {
	case fanoutobligation.SourceEventPayloadField:
		event, _, err := loadFanOutSourceEvent(ctx, tx, source.EventID, postgres)
		if err != nil {
			return fanoutobligation.SourceRef{}, fmt.Errorf("load fan-out payload source: %w", err)
		}
		if event.RunID() != request.Key.RunID {
			return fanoutobligation.SourceRef{}, fmt.Errorf("fan-out payload source run disagrees with originating intent")
		}
		if err := requireFanOutCollectionCardinality(event.Payload(), source.Field, request.Cardinality); err != nil {
			return fanoutobligation.SourceRef{}, err
		}
	case fanoutobligation.SourceEntityField:
		if source.RunID != request.Key.RunID {
			return fanoutobligation.SourceRef{}, fmt.Errorf("fan-out entity source run disagrees with originating intent")
		}
		var fields map[string]any
		if err := canonicaljson.DecodePreservingNumberLexemes(stateFields, &fields); err != nil {
			return fanoutobligation.SourceRef{}, fmt.Errorf("decode fan-out entity source state: %w", err)
		}
		value, ok := fields[source.Field]
		if !ok {
			return fanoutobligation.SourceRef{}, fmt.Errorf("fan-out entity source field %s is absent after handler mutation", source.Field)
		}
		items, ok := value.([]any)
		if !ok || len(items) != request.Cardinality {
			return fanoutobligation.SourceRef{}, fmt.Errorf("fan-out entity source cardinality changed before persistence: got %d items, want %d", len(items), request.Cardinality)
		}
		mutationID, err := insertFanOutEntitySourceRevisionTx(ctx, tx, postgres, effects, request.Key.RunID, source.EntityID, source.Field, value, triggerEventID, createdAt)
		if err != nil {
			return fanoutobligation.SourceRef{}, err
		}
		source.MutationID = mutationID
	case fanoutobligation.SourceResourceVersion:
		var present int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM resource_version_pins WHERE run_id=$1 AND package_key=$2 AND event_name=$3 AND version_id=$4`, request.Key.RunID, source.Declaration.PackageKey, source.Declaration.EventName, source.VersionID).Scan(&present); err != nil {
			return fanoutobligation.SourceRef{}, fmt.Errorf("fan-out resource source requires exact run pin: %w", err)
		}
	default:
		return fanoutobligation.SourceRef{}, fmt.Errorf("fan-out source kind %q is unsupported", source.Kind)
	}
	if err := source.Validate(true); err != nil {
		return fanoutobligation.SourceRef{}, err
	}
	return source, nil
}

func requireFanOutCollectionCardinality(raw []byte, field string, cardinality int) error {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("decode fan-out payload source: %w", err)
	}
	value, ok := payload[strings.TrimSpace(field)]
	if !ok {
		return fmt.Errorf("fan-out payload source field %s is absent from persisted event", strings.TrimSpace(field))
	}
	items, ok := value.([]any)
	if !ok || len(items) != cardinality {
		return fmt.Errorf("fan-out payload source cardinality disagrees with persisted event: got %d items, want %d", len(items), cardinality)
	}
	return nil
}

func insertFanOutEntitySourceRevisionTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *revisionEffects,
	runID, entityID, field string,
	value any,
	triggerEventID string,
	createdAt time.Time,
) (string, error) {
	encoded, err := canonicaljson.MarshalPreservingNumberKinds(value)
	if err != nil {
		return "", fmt.Errorf("encode fan-out entity source revision: %w", err)
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	mutationID := uuid.NewString()
	query := `
		INSERT INTO entity_mutations (
			mutation_id, run_id, entity_id, domain, path, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step, created_at
		) VALUES ($1, $2, $3, 'authored_field', $4, $5, $5, $6, 'platform', 'fan_out_source_binding', 'fan_out', $7)`
	args := []any{mutationID, runID, entityID, field, string(encoded), triggerEventID, createdAt.UTC()}
	if postgres {
		query = strings.ReplaceAll(query, "$5", "$5::jsonb")
	} else {
		query = postgresPlaceholdersToSQLite(query, 7)
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return "", fmt.Errorf("insert fan-out entity source revision: %w", err)
	}
	if err := effects.Add(runID, privaterunforkrevision.FamilyEntityMutations); err != nil {
		return "", err
	}
	return mutationID, nil
}

func postgresPlaceholdersToSQLite(query string, count int) string {
	for index := count; index > 0; index-- {
		query = strings.ReplaceAll(query, fmt.Sprintf("$%d", index), fmt.Sprintf("?%d", index))
	}
	return query
}
