package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// Family is the closed registry of facts that may affect supported fork
// planning at a selected event revision.
type Family string

// Change declares the complete fork-reader projection families changed for a
// run by one persistence transaction.
type Change struct {
	RunID    string
	Families []Family
}

const (
	FamilyEvents                  Family = "events"
	FamilyEntityMutations         Family = "entity_mutations"
	FamilyEntityMetadata          Family = "entity_metadata"
	FamilyEventDeliveries         Family = "event_deliveries"
	FamilyEventReceipts           Family = "event_receipts"
	FamilyDeadLetters             Family = "dead_letters"
	FamilyTimers                  Family = "timers"
	FamilyAgentSessions           Family = "agent_sessions"
	FamilyAgentTurns              Family = "agent_turns"
	FamilyAgentConversationAudits Family = "agent_conversation_audits"
	FamilyReplyContexts           Family = "reply_contexts"
)

var allFamilies = []Family{
	FamilyEvents,
	FamilyEntityMutations,
	FamilyEntityMetadata,
	FamilyEventDeliveries,
	FamilyEventReceipts,
	FamilyDeadLetters,
	FamilyTimers,
	FamilyAgentSessions,
	FamilyAgentTurns,
	FamilyAgentConversationAudits,
	FamilyReplyContexts,
}

func AllFamilies() []Family {
	return append([]Family(nil), allFamilies...)
}

func ValidFamily(family Family) bool {
	for _, candidate := range allFamilies {
		if family == candidate {
			return true
		}
	}
	return false
}

// Capture records the current narrow projection for each changed family. It
// must run after the corresponding writes and inside their existing SQL
// transaction. Repeated calls in one transaction reuse the same run revision.
func Capture(ctx context.Context, tx *sql.Tx, runID string, families ...Family) (int64, error) {
	revisions, err := CaptureChanges(ctx, tx, Change{RunID: runID, Families: families})
	if err != nil {
		return 0, err
	}
	return revisions[strings.TrimSpace(runID)], nil
}

// CaptureForEvent derives the authoritative run through the immutable event
// identity before recording the changed families.
func CaptureForEvent(ctx context.Context, tx *sql.Tx, eventID string, families ...Family) (int64, error) {
	runID, err := RunIDForEvent(ctx, tx, eventID)
	if err != nil {
		return 0, err
	}
	if runID == "" {
		return 0, nil
	}
	return Capture(ctx, tx, runID, families...)
}

// CaptureChanges allocates revisions in deterministic run-ID order. This is
// the only supported path for transactions that affect more than one run.
func CaptureChanges(ctx context.Context, tx *sql.Tx, changes ...Change) (map[string]int64, error) {
	if tx == nil {
		return nil, fmt.Errorf("run fork revision capture requires an existing postgres transaction")
	}
	byRun := make(map[string][]Family, len(changes))
	for _, change := range changes {
		runID := strings.TrimSpace(change.RunID)
		if _, err := uuid.Parse(runID); err != nil {
			return nil, fmt.Errorf("run fork revision capture requires a UUID run_id: %w", err)
		}
		byRun[runID] = append(byRun[runID], change.Families...)
	}
	runIDs := make([]string, 0, len(byRun))
	for runID := range byRun {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	revisions := make(map[string]int64, len(runIDs))
	for _, runID := range runIDs {
		families, err := normalizeFamilies(byRun[runID])
		if err != nil {
			return nil, err
		}
		if len(families) == 0 {
			return nil, fmt.Errorf("run fork revision capture requires at least one fact family")
		}
		revision, err := allocate(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		for _, family := range families {
			query := captureQuery(family)
			if query == "" {
				return nil, fmt.Errorf("run fork revision capture has no writer for family %q", family)
			}
			if _, err := tx.ExecContext(ctx, query, runID, revision); err != nil {
				return nil, fmt.Errorf("capture run fork %s facts at revision %d: %w", family, revision, err)
			}
			if err := captureRemovedFacts(ctx, tx, runID, revision, family); err != nil {
				return nil, err
			}
		}
		revisions[runID] = revision
	}
	return revisions, nil
}

// CaptureCurrentTransaction discovers the bounded fact rows changed by the
// current PostgreSQL transaction and records their families per affected run.
// It keeps domain writers declarative while preserving one revision for every
// same-run fact written by the transaction.
func CaptureCurrentTransaction(ctx context.Context, tx *sql.Tx) (map[string]int64, error) {
	if tx == nil {
		return nil, fmt.Errorf("run fork revision capture requires an existing postgres transaction")
	}
	changeQueries, err := availableCurrentTransactionChangeQueries(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(changeQueries) == 0 {
		return map[string]int64{}, nil
	}
	query := `
		SELECT DISTINCT run_id, family
		FROM (` + strings.Join(changeQueries, "\nUNION ALL\n") + `) changed
		WHERE run_id <> ''
		ORDER BY run_id, family
	`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("discover current run fork revision changes: %w", err)
	}
	defer rows.Close()
	byRun := map[string][]Family{}
	for rows.Next() {
		var runID string
		var family Family
		if err := rows.Scan(&runID, &family); err != nil {
			return nil, fmt.Errorf("scan current run fork revision change: %w", err)
		}
		byRun[runID] = append(byRun[runID], family)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read current run fork revision changes: %w", err)
	}
	changes := make([]Change, 0, len(byRun))
	for runID, families := range byRun {
		changes = append(changes, Change{RunID: runID, Families: families})
	}
	if len(changes) == 0 {
		return map[string]int64{}, nil
	}
	return CaptureChanges(ctx, tx, changes...)
}

type currentTransactionFamilySchema struct {
	required map[string][]string
	query    string
}

var currentTransactionFamilySchemas = map[Family]currentTransactionFamilySchema{
	FamilyEvents: {
		required: map[string][]string{"events": {"event_id", "run_id", "event_name", "entity_id", "flow_instance", "source_route", "target_route", "target_set", "scope", "payload", "chain_depth", "produced_by", "produced_by_type", "handler_node", "idempotency_key", "source_event_id", "created_at"}},
		query:    `SELECT run_id::text AS run_id, 'events'::text AS family FROM events WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
	FamilyEntityMutations: {
		required: map[string][]string{"entity_mutations": {"mutation_id", "run_id", "entity_id", "field", "new_value", "caused_by_event", "created_at"}},
		query:    `SELECT run_id::text AS run_id, 'entity_mutations'::text AS family FROM entity_mutations WHERE xmin::text::bigint = txid_current()`,
	},
	FamilyEntityMetadata: {
		required: map[string][]string{"entity_state": {"run_id", "entity_id", "flow_instance", "entity_type", "created_at"}},
		query:    `SELECT run_id::text AS run_id, 'entity_metadata'::text AS family FROM entity_state WHERE xmin::text::bigint = txid_current()`,
	},
	FamilyEventDeliveries: {
		required: map[string][]string{"event_deliveries": {"delivery_id", "run_id", "event_id", "subscriber_type", "subscriber_id", "delivery_target_route", "delivery_context", "status", "retry_count", "reason_code", "active_session_id", "started_at", "delivered_at", "created_at"}},
		query:    `SELECT run_id::text AS run_id, 'event_deliveries'::text AS family FROM event_deliveries WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
	FamilyEventReceipts: {
		required: map[string][]string{
			"event_receipts": {"receipt_id", "event_id", "subscriber_type", "subscriber_id", "outcome", "reason_code", "processed_at"},
			"events":         {"event_id", "run_id"},
		},
		query: `SELECT e.run_id::text AS run_id, 'event_receipts'::text AS family FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id IS NOT NULL AND r.xmin::text::bigint = txid_current()`,
	},
	FamilyDeadLetters: {
		required: map[string][]string{
			"dead_letters": {"dead_letter_id", "original_event_id", "handler_node", "created_at"},
			"events":       {"event_id", "run_id"},
		},
		query: `SELECT e.run_id::text AS run_id, 'dead_letters'::text AS family FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id IS NOT NULL AND d.xmin::text::bigint = txid_current()`,
	},
	FamilyTimers: {
		required: map[string][]string{"timers": {"timer_id", "run_id", "timer_name", "entity_id", "flow_instance", "fire_event", "fire_payload", "fire_at", "recurring", "recurrence_cron", "recurrence_interval", "owner_node", "owner_agent", "task_type", "status", "fired_at", "created_at"}},
		query:    `SELECT run_id::text AS run_id, 'timers'::text AS family FROM timers WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
	FamilyAgentSessions: {
		required: map[string][]string{"agent_sessions": {"session_id", "run_id", "status", "created_at", "terminated_at"}},
		query:    `SELECT run_id::text AS run_id, 'agent_sessions'::text AS family FROM agent_sessions WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
	FamilyAgentTurns: {
		required: map[string][]string{"agent_turns": {"turn_id", "run_id", "session_id", "created_at"}},
		query:    `SELECT run_id::text AS run_id, 'agent_turns'::text AS family FROM agent_turns WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
	FamilyAgentConversationAudits: {
		required: map[string][]string{"agent_conversation_audits": {"session_id", "run_id", "status", "created_at", "updated_at"}},
		query:    `SELECT run_id::text AS run_id, 'agent_conversation_audits'::text AS family FROM agent_conversation_audits WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
	FamilyReplyContexts: {
		required: map[string][]string{"reply_contexts": {"reply_context_id", "run_id", "request_event_id", "state", "created_at", "updated_at", "terminal_at"}},
		query:    `SELECT run_id::text AS run_id, 'reply_contexts'::text AS family FROM reply_contexts WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	},
}

var revisionOwnerSchema = map[string][]string{
	"run_fork_revision_heads": {"run_id", "last_revision", "updated_at"},
	"run_fork_revisions":      {"run_id", "revision", "transaction_id"},
	"run_fork_fact_revisions": {"run_id", "revision", "family", "fact_key", "fact", "present", "source_transaction_id"},
}

func availableCurrentTransactionChangeQueries(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name IN (
			'events', 'entity_mutations', 'entity_state', 'event_deliveries',
			'event_receipts', 'dead_letters', 'timers', 'agent_sessions',
			'agent_turns', 'agent_conversation_audits', 'reply_contexts',
			'run_fork_revision_heads', 'run_fork_revisions', 'run_fork_fact_revisions'
		  )
	`)
	if err != nil {
		return nil, fmt.Errorf("inspect run fork revision capture schema: %w", err)
	}
	defer rows.Close()
	columns := map[string]map[string]struct{}{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return nil, fmt.Errorf("scan run fork revision capture schema: %w", err)
		}
		if columns[table] == nil {
			columns[table] = map[string]struct{}{}
		}
		columns[table][column] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run fork revision capture schema: %w", err)
	}
	if !schemaContainsRequirements(columns, revisionOwnerSchema) {
		return nil, nil
	}
	queries := make([]string, 0, len(allFamilies))
	for _, family := range allFamilies {
		schema := currentTransactionFamilySchemas[family]
		if schemaContainsRequirements(columns, schema.required) {
			queries = append(queries, schema.query)
		}
	}
	return queries, nil
}

func schemaContainsRequirements(columns map[string]map[string]struct{}, required map[string][]string) bool {
	for table, names := range required {
		for _, name := range names {
			if _, ok := columns[table][name]; !ok {
				return false
			}
		}
	}
	return true
}

func captureRemovedFacts(ctx context.Context, tx *sql.Tx, runID string, revision int64, family Family) error {
	if _, err := tx.ExecContext(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (fact_key) fact_key, present
			FROM run_fork_fact_revisions
			WHERE run_id = $1::uuid AND family = $3 AND revision < $2
			ORDER BY fact_key, revision DESC
		)
		INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, present, source_transaction_id)
		SELECT $1::uuid, $2, $3, latest.fact_key, '{}'::jsonb, FALSE, 0
		FROM latest
		WHERE latest.present
		  AND NOT EXISTS (
			SELECT 1
			FROM run_fork_fact_revisions current
			WHERE current.run_id = $1::uuid
			  AND current.revision = $2
			  AND current.family = $3
			  AND current.fact_key = latest.fact_key
			  AND current.present
		  )
		ON CONFLICT (run_id, family, fact_key, revision)
		DO UPDATE SET fact = EXCLUDED.fact, present = FALSE, source_transaction_id = 0
	`, runID, revision, family); err != nil {
		return fmt.Errorf("capture removed run fork %s facts at revision %d: %w", family, revision, err)
	}
	return nil
}

func RunIDForEvent(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("event run lookup requires an existing postgres transaction")
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return "", fmt.Errorf("event run lookup requires a UUID event_id: %w", err)
	}
	var runID string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`, eventID).Scan(&runID); err != nil {
		return "", fmt.Errorf("resolve run_id for event %s: %w", eventID, err)
	}
	if strings.TrimSpace(runID) == "" {
		return "", nil
	}
	return strings.TrimSpace(runID), nil
}

// ValidateComplete rejects old, bypassed, or partially deleted projections.
// PostgreSQL xmin is only a same-row writer stamp; it is never exposed as a
// fork revision or used to order transactions.
func ValidateComplete(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return fmt.Errorf("run fork revision validation requires an existing postgres transaction")
	}
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("run fork revision validation requires a UUID run_id: %w", err)
	}
	for _, family := range allFamilies {
		current := currentIdentityQuery(family)
		if current == "" {
			return fmt.Errorf("run fork revision validation has no projection for family %q", family)
		}
		var drifted bool
		query := `
			WITH current_facts AS (` + current + `),
			latest AS (
				SELECT DISTINCT ON (fact_key) fact_key, present, source_transaction_id
				FROM run_fork_fact_revisions
				WHERE run_id = $1::uuid AND family = $2
				ORDER BY fact_key, revision DESC
			)
			SELECT EXISTS (
				SELECT 1
				FROM current_facts current
				LEFT JOIN latest USING (fact_key)
				WHERE latest.fact_key IS NULL
				   OR NOT latest.present
				   OR latest.source_transaction_id <> current.source_transaction_id
				UNION ALL
				SELECT 1
				FROM latest
				LEFT JOIN current_facts current USING (fact_key)
				WHERE latest.present AND current.fact_key IS NULL
			)
		`
		if err := tx.QueryRowContext(ctx, query, runID, family).Scan(&drifted); err != nil {
			return fmt.Errorf("validate run fork %s projection: %w", family, err)
		}
		if drifted {
			return fmt.Errorf("run %s has unsupported unrevisioned %s facts; recreate the store and retry", runID, family)
		}
	}
	return nil
}

func allocate(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, `
		SELECT revision
		FROM run_fork_revisions
		WHERE run_id = $1::uuid
		  AND transaction_id = txid_current()
	`, runID).Scan(&revision)
	if err == nil {
		return revision, nil
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("read current run fork transaction revision: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_revision_heads (run_id)
		VALUES ($1::uuid)
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		return 0, fmt.Errorf("ensure run fork revision head: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
		UPDATE run_fork_revision_heads
		SET last_revision = last_revision + 1,
		    updated_at = now()
		WHERE run_id = $1::uuid
		RETURNING last_revision
	`, runID).Scan(&revision); err != nil {
		return 0, fmt.Errorf("allocate run fork revision: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_revisions (run_id, revision, transaction_id)
		VALUES ($1::uuid, $2, txid_current())
	`, runID, revision); err != nil {
		return 0, fmt.Errorf("record run fork revision: %w", err)
	}
	return revision, nil
}

func normalizeFamilies(families []Family) ([]Family, error) {
	seen := make(map[Family]struct{}, len(families))
	out := make([]Family, 0, len(families))
	for _, family := range families {
		family = Family(strings.TrimSpace(string(family)))
		if !ValidFamily(family) {
			return nil, fmt.Errorf("unsupported run fork revision fact family %q", family)
		}
		if _, ok := seen[family]; ok {
			continue
		}
		seen[family] = struct{}{}
		out = append(out, family)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func captureQuery(family Family) string {
	switch family {
	case FamilyEvents:
		return captureEventsSQL
	case FamilyEntityMutations:
		return captureEntityMutationsSQL
	case FamilyEntityMetadata:
		return captureEntityMetadataSQL
	case FamilyEventDeliveries:
		return captureEventDeliveriesSQL
	case FamilyEventReceipts:
		return captureEventReceiptsSQL
	case FamilyDeadLetters:
		return captureDeadLettersSQL
	case FamilyTimers:
		return captureTimersSQL
	case FamilyAgentSessions:
		return captureAgentSessionsSQL
	case FamilyAgentTurns:
		return captureAgentTurnsSQL
	case FamilyAgentConversationAudits:
		return captureAgentConversationAuditsSQL
	case FamilyReplyContexts:
		return captureReplyContextsSQL
	default:
		return ""
	}
}

func currentIdentityQuery(family Family) string {
	switch family {
	case FamilyEvents:
		return `SELECT event_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM events WHERE run_id = $1::uuid`
	case FamilyEntityMutations:
		return `SELECT mutation_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM entity_mutations WHERE run_id = $1::uuid`
	case FamilyEntityMetadata:
		return `SELECT entity_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM entity_state WHERE run_id = $1::uuid`
	case FamilyEventDeliveries:
		return `SELECT delivery_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM event_deliveries WHERE run_id = $1::uuid`
	case FamilyEventReceipts:
		return `SELECT r.receipt_id::text AS fact_key, r.xmin::text::bigint AS source_transaction_id FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid`
	case FamilyDeadLetters:
		return `SELECT d.dead_letter_id::text AS fact_key, d.xmin::text::bigint AS source_transaction_id FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid`
	case FamilyTimers:
		return `SELECT timer_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM timers WHERE run_id = $1::uuid`
	case FamilyAgentSessions:
		return `SELECT session_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM agent_sessions WHERE run_id = $1::uuid`
	case FamilyAgentTurns:
		return `SELECT turn_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM agent_turns WHERE run_id = $1::uuid`
	case FamilyAgentConversationAudits:
		return `SELECT session_id::text AS fact_key, xmin::text::bigint AS source_transaction_id FROM agent_conversation_audits WHERE run_id = $1::uuid`
	case FamilyReplyContexts:
		return `SELECT reply_context_id AS fact_key, xmin::text::bigint AS source_transaction_id FROM reply_contexts WHERE run_id = $1::uuid`
	default:
		return ""
	}
}

const captureUpsertSuffix = `
	ON CONFLICT (run_id, family, fact_key, revision)
	DO UPDATE SET fact = EXCLUDED.fact, present = TRUE, source_transaction_id = EXCLUDED.source_transaction_id
`

const captureEventsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT e.run_id, $2, 'events', e.event_id::text,
	       jsonb_build_object(
	           'event_id', e.event_id, 'event_name', e.event_name,
	           'entity_id', e.entity_id, 'flow_instance', e.flow_instance,
	           'source_route', e.source_route, 'target_route', e.target_route,
	           'target_set', e.target_set, 'scope', e.scope, 'payload', e.payload,
	           'chain_depth', e.chain_depth, 'produced_by', e.produced_by,
	           'produced_by_type', e.produced_by_type, 'handler_node', e.handler_node,
	           'idempotency_key', e.idempotency_key, 'source_event_id', e.source_event_id,
	           'created_at', e.created_at), e.xmin::text::bigint
	FROM events e
	WHERE e.run_id = $1::uuid
` + captureUpsertSuffix

const captureEntityMutationsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT m.run_id, $2, 'entity_mutations', m.mutation_id::text,
	       jsonb_build_object(
	           'mutation_id', m.mutation_id, 'entity_id', m.entity_id,
	           'field', m.field, 'new_value', m.new_value,
	           'caused_by_event', m.caused_by_event, 'created_at', m.created_at), m.xmin::text::bigint
	FROM entity_mutations m
	WHERE m.run_id = $1::uuid
` + captureUpsertSuffix

const captureEntityMetadataSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT e.run_id, $2, 'entity_metadata', e.entity_id::text,
	       jsonb_build_object(
	           'entity_id', e.entity_id, 'flow_instance', e.flow_instance,
	           'entity_type', e.entity_type, 'created_at', e.created_at), e.xmin::text::bigint
	FROM entity_state e
	WHERE e.run_id = $1::uuid
` + captureUpsertSuffix

const captureEventDeliveriesSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT d.run_id, $2, 'event_deliveries', d.delivery_id::text,
	       jsonb_build_object(
	           'delivery_id', d.delivery_id, 'event_id', d.event_id,
	           'subscriber_type', d.subscriber_type, 'subscriber_id', d.subscriber_id,
	           'delivery_target_route', d.delivery_target_route,
	           'delivery_context', d.delivery_context, 'status', d.status,
	           'retry_count', d.retry_count, 'reason_code', d.reason_code,
	           'active_session_id', d.active_session_id, 'started_at', d.started_at,
	           'delivered_at', d.delivered_at, 'created_at', d.created_at), d.xmin::text::bigint
	FROM event_deliveries d
	WHERE d.run_id = $1::uuid
` + captureUpsertSuffix

const captureEventReceiptsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT e.run_id, $2, 'event_receipts', r.receipt_id::text,
	       jsonb_build_object(
	           'receipt_id', r.receipt_id, 'event_id', r.event_id,
	           'subscriber_type', r.subscriber_type, 'subscriber_id', r.subscriber_id,
	           'outcome', r.outcome, 'reason_code', r.reason_code,
	           'processed_at', r.processed_at), r.xmin::text::bigint
	FROM event_receipts r
	JOIN events e ON e.event_id = r.event_id
	WHERE e.run_id = $1::uuid
` + captureUpsertSuffix

const captureDeadLettersSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT e.run_id, $2, 'dead_letters', d.dead_letter_id::text,
	       jsonb_build_object(
	           'dead_letter_id', d.dead_letter_id, 'original_event_id', d.original_event_id,
	           'handler_node', d.handler_node, 'created_at', d.created_at), d.xmin::text::bigint
	FROM dead_letters d
	JOIN events e ON e.event_id = d.original_event_id
	WHERE e.run_id = $1::uuid
` + captureUpsertSuffix

const captureTimersSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT t.run_id, $2, 'timers', t.timer_id::text,
	       jsonb_build_object(
	           'timer_id', t.timer_id, 'timer_name', t.timer_name,
	           'entity_id', t.entity_id, 'flow_instance', t.flow_instance,
	           'fire_event', t.fire_event, 'fire_payload', t.fire_payload,
	           'fire_at', t.fire_at, 'recurring', t.recurring,
	           'recurrence_cron', t.recurrence_cron,
	           'recurrence_interval', t.recurrence_interval,
	           'owner_node', t.owner_node, 'owner_agent', t.owner_agent,
	           'task_type', t.task_type, 'status', t.status,
	           'fired_at', t.fired_at, 'created_at', t.created_at), t.xmin::text::bigint
	FROM timers t
	WHERE t.run_id = $1::uuid
` + captureUpsertSuffix

const captureAgentSessionsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT s.run_id, $2, 'agent_sessions', s.session_id::text,
	       jsonb_build_object(
	           'session_id', s.session_id, 'status', s.status,
	           'created_at', s.created_at, 'terminated_at', s.terminated_at), s.xmin::text::bigint
	FROM agent_sessions s
	WHERE s.run_id = $1::uuid
` + captureUpsertSuffix

const captureAgentTurnsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT t.run_id, $2, 'agent_turns', t.turn_id::text,
	       jsonb_build_object(
	           'turn_id', t.turn_id, 'session_id', t.session_id,
	           'created_at', t.created_at), t.xmin::text::bigint
	FROM agent_turns t
	WHERE t.run_id = $1::uuid
` + captureUpsertSuffix

const captureAgentConversationAuditsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT a.run_id, $2, 'agent_conversation_audits', a.session_id::text,
	       jsonb_build_object(
	           'session_id', a.session_id, 'status', a.status,
	           'created_at', a.created_at, 'updated_at', a.updated_at), a.xmin::text::bigint
	FROM agent_conversation_audits a
	WHERE a.run_id = $1::uuid
` + captureUpsertSuffix

const captureReplyContextsSQL = `
	INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, source_transaction_id)
	SELECT r.run_id, $2, 'reply_contexts', r.reply_context_id,
	       jsonb_build_object(
	           'reply_context_id', r.reply_context_id,
	           'request_event_id', r.request_event_id, 'state', r.state,
	           'created_at', r.created_at, 'updated_at', r.updated_at,
	           'terminal_at', r.terminal_at), r.xmin::text::bigint
	FROM reply_contexts r
	WHERE r.run_id = $1::uuid
` + captureUpsertSuffix
