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
	FamilyCommittedReplayScopes   Family = "committed_replay_scopes"
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
	FamilyCommittedReplayScopes,
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
	normalizedByRun := make(map[string][]Family, len(runIDs))
	for _, runID := range runIDs {
		families, err := normalizeFamilies(byRun[runID])
		if err != nil {
			return nil, err
		}
		if len(families) == 0 {
			return nil, fmt.Errorf("run fork revision capture requires at least one fact family")
		}
		normalizedByRun[runID] = families
	}
	if err := lockParentRuns(ctx, tx, runIDs); err != nil {
		return nil, err
	}
	revisions := make(map[string]int64, len(runIDs))
	for _, runID := range runIDs {
		families := normalizedByRun[runID]
		changedFamilies := make([]Family, 0, len(families))
		for _, family := range families {
			changed, err := canonicalProjectionDiffersFromLatest(ctx, tx, runID, family)
			if err != nil {
				return nil, fmt.Errorf("compare run fork %s projection: %w", family, err)
			}
			if changed {
				changedFamilies = append(changedFamilies, family)
			}
		}
		if len(changedFamilies) == 0 {
			revision, ok, err := currentTransactionRevision(ctx, tx, runID)
			if err != nil {
				return nil, err
			}
			if !ok {
				revision, ok, err = latestRevision(ctx, tx, runID)
				if err != nil {
					return nil, err
				}
			}
			if ok {
				revisions[runID] = revision
			}
			continue
		}
		revision, err := allocate(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		for _, family := range changedFamilies {
			if err := captureCanonicalProjection(ctx, tx, runID, revision, family); err != nil {
				return nil, err
			}
		}
		revisions[runID] = revision
	}
	return revisions, nil
}

func lockParentRuns(ctx context.Context, tx *sql.Tx, runIDs []string) error {
	for _, runID := range runIDs {
		var lockedRunID string
		err := tx.QueryRowContext(ctx, `
			SELECT run_id::text
			FROM runs
			WHERE run_id = $1::uuid
			FOR KEY SHARE
		`, runID).Scan(&lockedRunID)
		if err == sql.ErrNoRows {
			return fmt.Errorf("lock run fork revision parent run %s: run does not exist", runID)
		}
		if err != nil {
			return fmt.Errorf("lock run fork revision parent run %s: %w", runID, err)
		}
	}
	return nil
}

// CaptureCurrentTransaction discovers the bounded fact rows changed by the
// current PostgreSQL transaction and records their families per affected run.
// It keeps domain writers declarative while preserving one revision for every
// same-run fact written by the transaction.
func CaptureCurrentTransaction(ctx context.Context, tx *sql.Tx) (map[string]int64, error) {
	if tx == nil {
		return nil, fmt.Errorf("run fork revision capture requires an existing postgres transaction")
	}
	changeQueries := currentTransactionChangeQueries()
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

var currentTransactionFamilyQueries = map[Family]string{
	FamilyEvents:                  `SELECT run_id::text AS run_id, 'events'::text AS family FROM events WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyEntityMutations:         `SELECT run_id::text AS run_id, 'entity_mutations'::text AS family FROM entity_mutations WHERE xmin::text::bigint = txid_current()`,
	FamilyEntityMetadata:          `SELECT run_id::text AS run_id, 'entity_metadata'::text AS family FROM entity_state WHERE xmin::text::bigint = txid_current()`,
	FamilyEventDeliveries:         `SELECT run_id::text AS run_id, 'event_deliveries'::text AS family FROM event_deliveries WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyCommittedReplayScopes:   `SELECT run_id::text AS run_id, 'committed_replay_scopes'::text AS family FROM committed_replay_scopes WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyEventReceipts:           `SELECT e.run_id::text AS run_id, 'event_receipts'::text AS family FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id IS NOT NULL AND r.xmin::text::bigint = txid_current()`,
	FamilyDeadLetters:             `SELECT e.run_id::text AS run_id, 'dead_letters'::text AS family FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id IS NOT NULL AND d.xmin::text::bigint = txid_current()`,
	FamilyTimers:                  `SELECT run_id::text AS run_id, 'timers'::text AS family FROM timers WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyAgentSessions:           `SELECT run_id::text AS run_id, 'agent_sessions'::text AS family FROM agent_sessions WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyAgentTurns:              `SELECT run_id::text AS run_id, 'agent_turns'::text AS family FROM agent_turns WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyAgentConversationAudits: `SELECT run_id::text AS run_id, 'agent_conversation_audits'::text AS family FROM agent_conversation_audits WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
	FamilyReplyContexts:           `SELECT run_id::text AS run_id, 'reply_contexts'::text AS family FROM reply_contexts WHERE run_id IS NOT NULL AND xmin::text::bigint = txid_current()`,
}

func currentTransactionChangeQueries() []string {
	queries := make([]string, 0, len(allFamilies))
	for _, family := range allFamilies {
		queries = append(queries, currentTransactionFamilyQueries[family])
	}
	return queries
}

func captureCanonicalProjection(ctx context.Context, tx *sql.Tx, runID string, revision int64, family Family) error {
	projection := canonicalProjectionQuery(family)
	if projection == "" {
		return fmt.Errorf("run fork revision capture has no projection for family %q", family)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_fact_revisions (
			run_id, revision, family, fact_key, fact, present, source_transaction_id
		)
		SELECT $1::uuid, $2, $3, current.fact_key, current.fact, TRUE, current.source_transaction_id
		FROM (`+projection+`) current
		ON CONFLICT (run_id, family, fact_key, revision)
		DO UPDATE SET
			fact = EXCLUDED.fact,
			present = TRUE,
			source_transaction_id = EXCLUDED.source_transaction_id
	`, runID, revision, family); err != nil {
		return fmt.Errorf("capture run fork %s facts at revision %d: %w", family, revision, err)
	}
	return captureRemovedFacts(ctx, tx, runID, revision, family, projection)
}

func captureRemovedFacts(ctx context.Context, tx *sql.Tx, runID string, revision int64, family Family, projection string) error {
	if _, err := tx.ExecContext(ctx, `
			WITH current_facts AS (`+projection+`),
			latest AS (
				SELECT DISTINCT ON (fact_key) fact_key, present
				FROM run_fork_fact_revisions
				WHERE run_id = $1::uuid AND family = $3 AND revision <= $2
				ORDER BY fact_key, revision DESC
			)
			INSERT INTO run_fork_fact_revisions (run_id, revision, family, fact_key, fact, present, source_transaction_id)
			SELECT $1::uuid, $2, $3, latest.fact_key, '{}'::jsonb, FALSE, 0
			FROM latest
			WHERE latest.present
			  AND NOT EXISTS (
				SELECT 1
				FROM current_facts current
				WHERE current.fact_key = latest.fact_key
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
// PostgreSQL xmin is only used to discover candidate writes in the current
// transaction; canonical projected fact and presence equality own semantics.
func ValidateComplete(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return fmt.Errorf("run fork revision validation requires an existing postgres transaction")
	}
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("run fork revision validation requires a UUID run_id: %w", err)
	}
	for _, family := range allFamilies {
		drifted, err := canonicalProjectionDiffersFromLatest(ctx, tx, runID, family)
		if err != nil {
			return fmt.Errorf("validate run fork %s projection: %w", family, err)
		}
		if drifted {
			return fmt.Errorf("run %s has unsupported unrevisioned %s facts; recreate the store and retry", runID, family)
		}
	}
	return nil
}

func canonicalProjectionDiffersFromLatest(ctx context.Context, tx *sql.Tx, runID string, family Family) (bool, error) {
	projection := canonicalProjectionQuery(family)
	if projection == "" {
		return false, fmt.Errorf("run fork revision owner has no canonical projection for family %q", family)
	}
	var differs bool
	query := `
		WITH current_facts AS (` + projection + `),
		latest AS (
			SELECT DISTINCT ON (fact_key) fact_key, fact, present
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
			   OR latest.fact IS DISTINCT FROM current.fact
			UNION ALL
			SELECT 1
			FROM latest
			LEFT JOIN current_facts current USING (fact_key)
			WHERE latest.present AND current.fact_key IS NULL
		)
	`
	if err := tx.QueryRowContext(ctx, query, runID, family).Scan(&differs); err != nil {
		return false, err
	}
	return differs, nil
}

func allocate(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	if revision, ok, err := currentTransactionRevision(ctx, tx, runID); err != nil {
		return 0, err
	} else if ok {
		return revision, nil
	}
	var revision int64
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

func currentTransactionRevision(ctx context.Context, tx *sql.Tx, runID string) (int64, bool, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, `
			SELECT revision
			FROM run_fork_revisions
			WHERE run_id = $1::uuid
			  AND transaction_id = txid_current()
		`, runID).Scan(&revision)
	if err == nil {
		return revision, true, nil
	}
	if err != sql.ErrNoRows {
		return 0, false, fmt.Errorf("read current run fork transaction revision: %w", err)
	}
	return 0, false, nil
}

func latestRevision(ctx context.Context, tx *sql.Tx, runID string) (int64, bool, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, `
		SELECT last_revision
		FROM run_fork_revision_heads
		WHERE run_id = $1::uuid
	`, runID).Scan(&revision)
	if err == nil {
		return revision, true, nil
	}
	if err != sql.ErrNoRows {
		return 0, false, fmt.Errorf("read latest run fork revision: %w", err)
	}
	return 0, false, nil
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

func canonicalProjectionQuery(family Family) string {
	switch family {
	case FamilyEvents:
		return canonicalEventsProjectionSQL
	case FamilyEntityMutations:
		return canonicalEntityMutationsProjectionSQL
	case FamilyEntityMetadata:
		return canonicalEntityMetadataProjectionSQL
	case FamilyEventDeliveries:
		return canonicalEventDeliveriesProjectionSQL
	case FamilyCommittedReplayScopes:
		return canonicalCommittedReplayScopesProjectionSQL
	case FamilyEventReceipts:
		return canonicalEventReceiptsProjectionSQL
	case FamilyDeadLetters:
		return canonicalDeadLettersProjectionSQL
	case FamilyTimers:
		return canonicalTimersProjectionSQL
	case FamilyAgentSessions:
		return canonicalAgentSessionsProjectionSQL
	case FamilyAgentTurns:
		return canonicalAgentTurnsProjectionSQL
	case FamilyAgentConversationAudits:
		return canonicalAgentConversationAuditsProjectionSQL
	case FamilyReplyContexts:
		return canonicalReplyContextsProjectionSQL
	default:
		return ""
	}
}

const canonicalEventsProjectionSQL = `
	SELECT e.event_id::text AS fact_key,
	       jsonb_build_object(
	           'event_id', e.event_id, 'event_name', e.event_name,
	           'entity_id', e.entity_id, 'flow_instance', e.flow_instance,
	           'source_route', e.source_route, 'target_route', e.target_route,
	           'target_set', e.target_set, 'scope', e.scope, 'payload', e.payload,
	           'chain_depth', e.chain_depth, 'produced_by', e.produced_by,
	           'produced_by_type', e.produced_by_type, 'handler_node', e.handler_node,
	           'idempotency_key', e.idempotency_key, 'source_event_id', e.source_event_id,
	           'created_at', e.created_at) AS fact, e.xmin::text::bigint AS source_transaction_id
	FROM events e
	WHERE e.run_id = $1::uuid
`

const canonicalEntityMutationsProjectionSQL = `
	SELECT m.mutation_id::text AS fact_key,
	       jsonb_build_object(
	           'mutation_id', m.mutation_id, 'entity_id', m.entity_id,
	           'field', m.field, 'new_value', m.new_value,
	           'caused_by_event', m.caused_by_event, 'created_at', m.created_at) AS fact, m.xmin::text::bigint AS source_transaction_id
	FROM entity_mutations m
	WHERE m.run_id = $1::uuid
`

const canonicalEntityMetadataProjectionSQL = `
	SELECT e.entity_id::text AS fact_key,
	       jsonb_build_object(
	           'entity_id', e.entity_id, 'flow_instance', e.flow_instance,
	           'entity_type', e.entity_type, 'created_at', e.created_at) AS fact, e.xmin::text::bigint AS source_transaction_id
	FROM entity_state e
	WHERE e.run_id = $1::uuid
`

const canonicalEventDeliveriesProjectionSQL = `
	SELECT d.delivery_id::text AS fact_key,
	       jsonb_build_object(
	           'delivery_id', d.delivery_id, 'event_id', d.event_id,
	           'run_id', d.run_id, 'route_identity', d.route_identity,
	           'subscriber_type', d.subscriber_type, 'subscriber_id', d.subscriber_id,
	           'delivery_target_route', d.delivery_target_route,
	           'delivery_context', d.delivery_context,
	           'delivery_payload_projection', d.delivery_payload_projection, 'status', d.status,
	           'retry_count', d.retry_count, 'max_retries', d.max_retries,
	           'next_eligible_at', d.next_eligible_at,
	           'claim_version', d.claim_version, 'claim_expires_at', d.claim_expires_at,
	           'reason_code', d.reason_code, 'failure', d.failure,
	           'active_session_id', d.active_session_id, 'started_at', d.started_at,
	           'settled_at', d.settled_at, 'created_at', d.created_at,
	           'updated_at', d.updated_at) AS fact, d.xmin::text::bigint AS source_transaction_id
	FROM event_deliveries d
	WHERE d.run_id = $1::uuid
`

const canonicalCommittedReplayScopesProjectionSQL = `
	SELECT s.event_id::text AS fact_key,
	       jsonb_build_object(
	           'event_id', s.event_id, 'run_id', s.run_id, 'scope', s.scope,
	           'created_at', s.created_at, 'updated_at', s.updated_at) AS fact,
	       s.xmin::text::bigint AS source_transaction_id
	FROM committed_replay_scopes s
	WHERE s.run_id = $1::uuid
`

const canonicalEventReceiptsProjectionSQL = `
	SELECT r.receipt_id::text AS fact_key,
	       jsonb_build_object(
	           'receipt_id', r.receipt_id, 'event_id', r.event_id,
	           'subscriber_type', r.subscriber_type, 'subscriber_id', r.subscriber_id,
	           'outcome', r.outcome, 'reason_code', r.reason_code,
	           'processed_at', r.processed_at) AS fact, r.xmin::text::bigint AS source_transaction_id
	FROM event_receipts r
	JOIN events e ON e.event_id = r.event_id
	WHERE e.run_id = $1::uuid
`

const canonicalDeadLettersProjectionSQL = `
	SELECT d.dead_letter_id::text AS fact_key,
	       jsonb_build_object(
	           'dead_letter_id', d.dead_letter_id, 'original_event_id', d.original_event_id,
	           'handler_node', d.handler_node, 'created_at', d.created_at) AS fact, d.xmin::text::bigint AS source_transaction_id
	FROM dead_letters d
	JOIN events e ON e.event_id = d.original_event_id
	WHERE e.run_id = $1::uuid
`

const canonicalTimersProjectionSQL = `
	SELECT t.timer_id::text AS fact_key,
	       jsonb_build_object(
	           'timer_id', t.timer_id, 'timer_name', t.timer_name,
	           'entity_id', t.entity_id, 'flow_instance', t.flow_instance,
	           'fire_event', t.fire_event, 'fire_payload', t.fire_payload,
	           'fire_at', t.fire_at, 'recurring', t.recurring,
	           'recurrence_cron', t.recurrence_cron,
	           'recurrence_interval', t.recurrence_interval,
	           'owner_node', t.owner_node, 'owner_agent', t.owner_agent,
	           'task_type', t.task_type, 'status', t.status,
	           'fired_at', t.fired_at, 'created_at', t.created_at) AS fact, t.xmin::text::bigint AS source_transaction_id
	FROM timers t
	WHERE t.run_id = $1::uuid
`

const canonicalAgentSessionsProjectionSQL = `
	SELECT s.session_id::text AS fact_key,
	       jsonb_build_object(
	           'session_id', s.session_id, 'status', s.status,
	           'created_at', s.created_at, 'terminated_at', s.terminated_at) AS fact, s.xmin::text::bigint AS source_transaction_id
	FROM agent_sessions s
	WHERE s.run_id = $1::uuid
`

const canonicalAgentTurnsProjectionSQL = `
	SELECT t.turn_id::text AS fact_key,
	       jsonb_build_object(
	           'turn_id', t.turn_id, 'session_id', t.session_id,
	           'created_at', t.created_at) AS fact, t.xmin::text::bigint AS source_transaction_id
	FROM agent_turns t
	WHERE t.run_id = $1::uuid
`

const canonicalAgentConversationAuditsProjectionSQL = `
	SELECT a.session_id::text AS fact_key,
	       jsonb_build_object(
	           'session_id', a.session_id, 'status', a.status,
	           'created_at', a.created_at, 'updated_at', a.updated_at) AS fact, a.xmin::text::bigint AS source_transaction_id
	FROM agent_conversation_audits a
	WHERE a.run_id = $1::uuid
`

const canonicalReplyContextsProjectionSQL = `
	SELECT r.reply_context_id AS fact_key,
	       jsonb_build_object(
	           'reply_context_id', r.reply_context_id,
	           'request_event_id', r.request_event_id, 'state', r.state,
	           'created_at', r.created_at, 'updated_at', r.updated_at,
	           'terminal_at', r.terminal_at) AS fact, r.xmin::text::bigint AS source_transaction_id
	FROM reply_contexts r
	WHERE r.run_id = $1::uuid
`
