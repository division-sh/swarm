package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
)

var errInboundPublicationNotFound = errors.New("inbound publication not found")

type inboundPublicationTransactionStore interface {
	appendInboundEvidenceTx(context.Context, *sql.Tx, events.Event) error
	finalizeInboundPublicationTx(context.Context, *sql.Tx, runtimeinbound.Request, runtimeinbound.Finalization) (runtimeinbound.Record, error)
}

type sqlInboundPublicationMutation struct {
	runtimebus.EventMutation
	ctx       context.Context
	tx        *sql.Tx
	store     inboundPublicationTransactionStore
	request   runtimeinbound.Request
	finalized bool
	record    runtimeinbound.Record
}

func newSQLInboundPublicationMutation(ctx context.Context, tx *sql.Tx, txStore runtimebus.TransactionalEventStore, store inboundPublicationTransactionStore) *sqlInboundPublicationMutation {
	eventMutation := newSQLEventMutation(ctx, tx, txStore, store)
	return &sqlInboundPublicationMutation{
		EventMutation: eventMutation,
		ctx:           eventMutation.Context(),
		tx:            tx,
		store:         store,
	}
}

func (m *sqlInboundPublicationMutation) Context() context.Context {
	if m == nil || m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *sqlInboundPublicationMutation) FinalizeInboundPublication(ctx context.Context, finalization runtimeinbound.Finalization) error {
	if m == nil || m.store == nil || m.tx == nil {
		return fmt.Errorf("inbound publication mutation is required")
	}
	if m.finalized {
		return fmt.Errorf("inbound publication mutation is already finalized")
	}
	if strings.TrimSpace(finalization.EvidenceEvent.ID()) != m.request.MarkerEventID {
		return fmt.Errorf("inbound evidence event_id does not match reserved marker_event_id")
	}
	if strings.TrimSpace(finalization.PublicationEvent.ID()) != m.request.PublicationEventID {
		return fmt.Errorf("inbound publication event_id does not match reserved publication_event_id")
	}
	if strings.TrimSpace(finalization.EvidenceEvent.RunID()) != m.request.ResolvedRunID || strings.TrimSpace(finalization.PublicationEvent.RunID()) != m.request.ResolvedRunID {
		return fmt.Errorf("inbound publication events must use the admitted resolved_run_id")
	}
	if strings.TrimSpace(string(finalization.EvidenceEvent.Type())) != "platform.inbound_recorded" {
		return fmt.Errorf("inbound evidence event must be platform.inbound_recorded")
	}
	if err := m.store.appendInboundEvidenceTx(ctx, m.tx, finalization.EvidenceEvent); err != nil {
		return err
	}
	record, err := m.store.finalizeInboundPublicationTx(ctx, m.tx, m.request, finalization)
	if err != nil {
		return err
	}
	m.finalized = true
	m.record = record
	return nil
}

func (s *PostgresStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return runtimeinbound.Record{}, err
	}
	if fn == nil {
		return runtimeinbound.Record{}, fmt.Errorf("inbound publication mutation callback is required")
	}
	var result runtimeinbound.Record
	err := s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		identityKey := inboundEventIdempotencyKey(request.ProviderEventID, request.EntityID, request.Provider)
		if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, identityKey); err != nil {
			return fmt.Errorf("lock inbound publication identity: %w", err)
		}
		existing, found, err := loadPostgresInboundPublicationTx(txctx, tx, request.Provider, request.EntityID, request.ProviderEventID, true)
		if err != nil {
			return err
		}
		if found {
			if err := validateInboundPublicationRetry(request, existing); err != nil {
				return err
			}
			if err := validatePostgresInboundPublicationIntegrityTx(txctx, tx, &existing); err != nil {
				return err
			}
			result = existing
			return nil
		}
		if err := admitPostgresInboundStandingTargetTx(txctx, tx, request); err != nil {
			return err
		}
		if err := insertPostgresInboundPublicationPreparedTx(txctx, tx, request); err != nil {
			return err
		}
		mutation := newSQLInboundPublicationMutation(txctx, tx, s, s)
		mutation.request = request
		if err := fn(mutation); err != nil {
			return err
		}
		if !mutation.finalized {
			return fmt.Errorf("inbound publication callback returned without finalizing the operation")
		}
		result = mutation.record
		result.Created = true
		return nil
	})
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	return result, nil
}

func (s *PostgresStore) LoadInboundPublicationByIdentity(ctx context.Context, provider, entityID, providerEventID string) (runtimeinbound.Record, bool, error) {
	if s == nil || s.DB == nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("postgres store is required")
	}
	record, found, err := loadPostgresInboundPublicationTx(ctx, s.DB, provider, entityID, providerEventID, false)
	if err != nil || !found {
		return record, found, err
	}
	if err := validatePostgresInboundPublicationIntegrityTx(ctx, s.DB, &record); err != nil {
		return runtimeinbound.Record{}, false, err
	}
	return record, true, nil
}

func (s *PostgresStore) ValidateInboundPublicationIntegrity(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT provider, entity_id::text, provider_event_id
		FROM inbound_publications
		ORDER BY provider, entity_id::text, provider_event_id
	`)
	if err != nil {
		return fmt.Errorf("list inbound publications for integrity validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var provider, entityID, providerEventID string
		if err := rows.Scan(&provider, &entityID, &providerEventID); err != nil {
			return fmt.Errorf("scan inbound publication identity: %w", err)
		}
		record, found, err := loadPostgresInboundPublicationTx(ctx, s.DB, provider, entityID, providerEventID, false)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("inbound publication disappeared during integrity validation")
		}
		if err := validatePostgresInboundPublicationIntegrityTx(ctx, s.DB, &record); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read inbound publication identities: %w", err)
	}
	return nil
}

type inboundPublicationQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadPostgresInboundPublicationTx(ctx context.Context, db inboundPublicationQueryer, provider, entityID, providerEventID string, forUpdate bool) (runtimeinbound.Record, bool, error) {
	query := postgresInboundPublicationSelect + `
		WHERE provider = $1
		  AND entity_id = $2::uuid
		  AND provider_event_id = $3`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	record, err := scanPostgresInboundPublication(db.QueryRowContext(ctx, query, strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(entityID), strings.TrimSpace(providerEventID)))
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeinbound.Record{}, false, nil
	}
	if err != nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("load inbound publication: %w", err)
	}
	return record, true, nil
}

const postgresInboundPublicationSelect = `
	SELECT
		p.publication_id::text, p.provider, p.entity_id::text, p.provider_event_id,
		p.semantic_fingerprint, p.semantic_projection_version,
		p.stable_service_id::text, p.package_key, p.flow_id, p.instance_id,
		p.target_alias, p.target_flow_instance, p.expected_publication_sequence,
		p.resolved_run_id::text, COALESCE(p.marker_event_id::text, ''),
		COALESCE(p.publication_event_id::text, ''), p.acknowledgement_mode,
		p.recipient_manifest, p.recipient_manifest_fingerprint, p.recipient_count,
		p.original_received_at, p.original_user_agent, p.original_transport_metadata,
		p.state, p.created_at, p.committed_at
	FROM inbound_publications p
`

func scanPostgresInboundPublication(row *sql.Row) (runtimeinbound.Record, error) {
	var record runtimeinbound.Record
	var ackMode string
	var committedAt sql.NullTime
	err := row.Scan(
		&record.PublicationID, &record.Provider, &record.EntityID, &record.ProviderEventID,
		&record.SemanticFingerprint, &record.SemanticProjectionVersion,
		&record.StableServiceID, &record.PackageKey, &record.FlowID, &record.InstanceID,
		&record.TargetAlias, &record.TargetFlowInstance, &record.ExpectedPublicationSequence,
		&record.ResolvedRunID, &record.MarkerEventID, &record.PublicationEventID, &ackMode,
		&record.RecipientManifest, &record.RecipientFingerprint, &record.RecipientCount,
		&record.OriginalReceivedAt, &record.OriginalUserAgent, &record.OriginalTransportMetadata,
		&record.State, &record.CreatedAt, &committedAt,
	)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	record.AcknowledgementMode = runtimeinbound.AcknowledgementMode(ackMode)
	if committedAt.Valid {
		record.CommittedAt = committedAt.Time.UTC()
	}
	record.Request = record.Request.Normalized()
	return record, nil
}

func admitPostgresInboundStandingTargetTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) error {
	var packageKey, flowID, instanceID, entityID, runID, effectiveState, publicationState string
	var generation, publicationSequence int64
	err := tx.QueryRowContext(ctx, `
		SELECT package_key, flow_id, instance_id, entity_id::text, current_run_id::text,
		       current_generation, publication_sequence, effective_state, publication_state
		FROM standing_services
		WHERE service_id = $1::uuid
		FOR UPDATE
	`, request.StableServiceID).Scan(&packageKey, &flowID, &instanceID, &entityID, &runID, &generation, &publicationSequence, &effectiveState, &publicationState)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("standing service %s is not admitted", request.StableServiceID)
	}
	if err != nil {
		return fmt.Errorf("lock inbound standing service: %w", err)
	}
	if packageKey != request.PackageKey || flowID != request.FlowID || instanceID != request.InstanceID || entityID != request.EntityID || runID != request.ResolvedRunID || publicationSequence != request.ExpectedPublicationSequence {
		return fmt.Errorf("stale or conflicting inbound standing target")
	}
	if effectiveState != "active" || publicationState != "published" {
		return fmt.Errorf("standing service %s is %s/%s and cannot accept inbound publication", request.StableServiceID, effectiveState, publicationState)
	}
	var runStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid FOR UPDATE`, request.ResolvedRunID).Scan(&runStatus); err != nil {
		return fmt.Errorf("lock inbound target run: %w", err)
	}
	if runStatus != "running" && runStatus != "paused" {
		return fmt.Errorf("inbound target run %s has terminal status %s", request.ResolvedRunID, runStatus)
	}
	var generationRunID string
	if err := tx.QueryRowContext(ctx, `
		SELECT run_id::text
		FROM standing_service_generations
		WHERE service_id = $1::uuid AND generation = $2 AND retired_at IS NULL
		FOR UPDATE
	`, request.StableServiceID, generation).Scan(&generationRunID); err != nil {
		return fmt.Errorf("lock inbound standing generation: %w", err)
	}
	if generationRunID != request.ResolvedRunID {
		return fmt.Errorf("standing generation run changed during inbound admission")
	}
	return nil
}

func insertPostgresInboundPublicationPreparedTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_publications (
			publication_id, provider, entity_id, provider_event_id,
			semantic_fingerprint, semantic_projection_version, stable_service_id,
			package_key, flow_id, instance_id, target_alias, target_flow_instance,
			expected_publication_sequence, resolved_run_id, acknowledgement_mode,
			original_received_at, original_user_agent, original_transport_metadata,
			state, created_at
		) VALUES (
			$1::uuid, $2, $3::uuid, $4, $5, $6, $7::uuid,
			$8, $9, $10, $11, $12, $13, $14::uuid, $15,
			$16, $17, $18::jsonb, 'prepared', now()
		)
	`, request.PublicationID, request.Provider, request.EntityID, request.ProviderEventID,
		request.SemanticFingerprint, request.SemanticProjectionVersion, request.StableServiceID,
		request.PackageKey, request.FlowID, request.InstanceID, request.TargetAlias, request.TargetFlowInstance,
		request.ExpectedPublicationSequence, request.ResolvedRunID, string(request.AcknowledgementMode),
		request.OriginalReceivedAt, request.OriginalUserAgent, string(request.OriginalTransportMetadata))
	if err != nil {
		return fmt.Errorf("insert prepared inbound publication: %w", err)
	}
	return nil
}

func (s *PostgresStore) appendInboundEvidenceTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	ctx = withDiagnosticDirectOwner(ctx, diagnosticDirectInboundRecord)
	if err := s.AppendEventTx(ctx, tx, evt); err != nil {
		return fmt.Errorf("append inbound evidence: %w", err)
	}
	return recordInboundAuthorActivity(ctx, evt, evt.SourceAgent())
}

func (s *PostgresStore) finalizeInboundPublicationTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request, finalization runtimeinbound.Finalization) (runtimeinbound.Record, error) {
	manifest, fingerprint, count, err := canonicalInboundRecipientManifest(finalization.RecipientManifest)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE inbound_publications
		SET marker_event_id = $2::uuid,
		    publication_event_id = $3::uuid,
		    recipient_manifest = $4::jsonb,
		    recipient_manifest_fingerprint = $5,
		    recipient_count = $6,
		    state = 'committed',
		    committed_at = now()
		WHERE publication_id = $1::uuid AND state = 'prepared'
	`, request.PublicationID, request.MarkerEventID, request.PublicationEventID, string(manifest), fingerprint, count)
	if err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("finalize inbound publication: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return runtimeinbound.Record{}, fmt.Errorf("prepared inbound publication %s was not finalized", request.PublicationID)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET event_count = (SELECT COUNT(*) FROM events WHERE run_id = $1::uuid)
		WHERE run_id = $1::uuid
	`, request.ResolvedRunID); err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("synchronize inbound publication event count: %w", err)
	}
	record, found, err := loadPostgresInboundPublicationTx(ctx, tx, request.Provider, request.EntityID, request.ProviderEventID, false)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	if !found {
		return runtimeinbound.Record{}, errInboundPublicationNotFound
	}
	record.PublicationEvent = finalization.PublicationEvent
	return record, nil
}

func validateInboundPublicationRetry(request runtimeinbound.Request, existing runtimeinbound.Record) error {
	if existing.State != "committed" {
		return fmt.Errorf("inbound publication %s is durably %s; store is corrupt and requires fresh-store remediation", existing.PublicationID, existing.State)
	}
	if existing.SemanticProjectionVersion != request.SemanticProjectionVersion || existing.SemanticFingerprint != request.SemanticFingerprint {
		return fmt.Errorf("inbound provider identity conflicts with the committed semantic publication")
	}
	return nil
}

func validatePostgresInboundPublicationIntegrityTx(ctx context.Context, db inboundPublicationQueryer, record *runtimeinbound.Record) error {
	if record == nil {
		return fmt.Errorf("inbound publication record is required")
	}
	if record.State != "committed" || record.MarkerEventID == "" || record.PublicationEventID == "" || record.CommittedAt.IsZero() {
		return fmt.Errorf("inbound publication %s has incomplete committed coupling", record.PublicationID)
	}
	_, fingerprint, count, err := canonicalInboundRecipientManifest(record.RecipientManifest)
	if err != nil {
		return fmt.Errorf("inbound publication %s manifest: %w", record.PublicationID, err)
	}
	if fingerprint != record.RecipientFingerprint || count != record.RecipientCount {
		return fmt.Errorf("inbound publication %s recipient manifest is incoherent", record.PublicationID)
	}
	var markerCount, eventCount, scopeCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM events WHERE event_id = $1::uuid AND event_name = 'platform.inbound_recorded'),
			(SELECT COUNT(*) FROM events WHERE event_id = $2::uuid),
			(SELECT COUNT(*) FROM event_deliveries WHERE event_id = $2::uuid AND subscriber_type = $3 AND subscriber_id = $4)
	`, record.MarkerEventID, record.PublicationEventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&markerCount, &eventCount, &scopeCount); err != nil {
		return fmt.Errorf("validate inbound publication coupling: %w", err)
	}
	if markerCount != 1 || eventCount != 1 || scopeCount != 1 {
		return fmt.Errorf("inbound publication %s is missing coupled marker/event/replay scope", record.PublicationID)
	}
	evt, err := loadPostgresInboundPublicationEvent(ctx, db, record.PublicationEventID)
	if err != nil {
		return err
	}
	record.PublicationEvent = evt
	return nil
}

func loadPostgresInboundPublicationEvent(ctx context.Context, db inboundPublicationQueryer, eventID string) (events.Event, error) {
	var eventType, sourceAgent, taskID, payload, runID, parentID, entityID, flowInstance, scope string
	var sourceRouteRaw, targetRouteRaw, targetSetRaw []byte
	var chainDepth int
	var createdAt time.Time
	err := db.QueryRowContext(ctx, `
		SELECT event_name, COALESCE(produced_by, ''), '', payload::text, COALESCE(run_id::text, ''),
		       COALESCE(source_event_id::text, ''), COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''),
		       scope, source_route, target_route, target_set, chain_depth, created_at
		FROM events WHERE event_id = $1::uuid
	`, eventID).Scan(&eventType, &sourceAgent, &taskID, &payload, &runID, &parentID, &entityID, &flowInstance, &scope, &sourceRouteRaw, &targetRouteRaw, &targetSetRaw, &chainDepth, &createdAt)
	if err != nil {
		return events.EmptyEvent(), fmt.Errorf("load inbound publication event: %w", err)
	}
	envelope, err := decodeInboundEventEnvelope(entityID, flowInstance, scope, sourceRouteRaw, targetRouteRaw, targetSetRaw)
	if err != nil {
		return events.EmptyEvent(), err
	}
	return events.NewProjectionEvent(eventID, events.EventType(eventType), sourceAgent, taskID, json.RawMessage(payload), chainDepth, runID, parentID, envelope, createdAt), nil
}

func canonicalInboundRecipientManifest(raw json.RawMessage) (json.RawMessage, string, int, error) {
	var routes []events.DeliveryRoute
	if len(raw) == 0 {
		raw = json.RawMessage(`[]`)
	}
	if err := json.Unmarshal(raw, &routes); err != nil {
		return nil, "", 0, fmt.Errorf("decode inbound recipient manifest: %w", err)
	}
	return runtimeinbound.CanonicalRecipientManifest(routes)
}

func decodeInboundEventEnvelope(entityID, flowInstance, scope string, sourceRouteRaw, targetRouteRaw, targetSetRaw []byte) (events.EventEnvelope, error) {
	envelope := events.EventEnvelope{EntityID: strings.TrimSpace(entityID), FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"), Scope: events.EventScope(strings.TrimSpace(scope))}
	if len(sourceRouteRaw) > 0 {
		if err := json.Unmarshal(sourceRouteRaw, &envelope.Source); err != nil {
			return events.EventEnvelope{}, fmt.Errorf("decode inbound event source route: %w", err)
		}
	}
	if len(targetRouteRaw) > 0 {
		if err := json.Unmarshal(targetRouteRaw, &envelope.Target); err != nil {
			return events.EventEnvelope{}, fmt.Errorf("decode inbound event target route: %w", err)
		}
	}
	if len(targetSetRaw) > 0 {
		if err := json.Unmarshal(targetSetRaw, &envelope.TargetSet); err != nil {
			return events.EventEnvelope{}, fmt.Errorf("decode inbound event target set: %w", err)
		}
	}
	return envelope.Normalized(), nil
}

var _ runtimeinbound.Runner = (*PostgresStore)(nil)

func inboundEventIdempotencyKey(providerEventID, entityID, provider string) string {
	return strings.Join([]string{
		"inbound-publication",
		strings.TrimSpace(strings.ToLower(provider)),
		strings.TrimSpace(entityID),
		strings.TrimSpace(providerEventID),
	}, ":")
}
