package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

type ShardBus interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
}

type ShardManager interface {
	SpawnEphemeralClone(baseAgentID, cloneAgentID string) error
	TeardownAgent(agentID string) error
	ReplayAgentBacklog(ctx context.Context, agentID string) error
	GetAgentConfig(agentID string) (models.AgentConfig, bool)
}

type ShardDispatcher struct {
	db      *sql.DB
	bus     ShardBus
	manager ShardManager
	cfg     config.ShardingConfig

	pollInterval       time.Duration
	startupGracePeriod time.Duration
	receiptGracePeriod time.Duration
}

type shardRuntimeRow struct {
	ID         string
	RootTaskID string
	ScanID     string
	Stage      string
	ShardIndex int
	ShardCount int
	ShardKey   string
	Scope      []byte
	AgentID    string
	Status     string
	DeadlineAt time.Time
	BudgetCts  int
	RetryCount int
}

type shardStageMeta struct {
	BaseAgentID string
	AssignEvent events.EventType
}

func NewShardDispatcher(
	db *sql.DB,
	bus ShardBus,
	manager ShardManager,
	cfg config.ShardingConfig,
) *ShardDispatcher {
	if db == nil || bus == nil || manager == nil {
		return nil
	}
	return &ShardDispatcher{
		db:                 db,
		bus:                bus,
		manager:            manager,
		cfg:                cfg,
		pollInterval:       2 * time.Second,
		startupGracePeriod: startupGraceOrDefault(cfg.StartupGracePeriod),
		receiptGracePeriod: 30 * time.Second,
	}
}

func (d *ShardDispatcher) SetPollInterval(interval time.Duration) {
	if d == nil || interval <= 0 {
		return
	}
	d.pollInterval = interval
}

func (d *ShardDispatcher) SetReceiptGracePeriod(interval time.Duration) {
	if d == nil || interval <= 0 {
		return
	}
	d.receiptGracePeriod = interval
}

func (d *ShardDispatcher) SetStartupGracePeriod(interval time.Duration) {
	if d == nil || interval <= 0 {
		return
	}
	d.startupGracePeriod = interval
}

func (d *ShardDispatcher) Run(ctx context.Context) {
	if d == nil || d.db == nil || d.bus == nil || d.manager == nil {
		return
	}
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	waitingForSchema := false
	runTick := func() {
		if !d.shardsTableAvailable(ctx) {
			if !waitingForSchema {
				waitingForSchema = true
				log.Printf("shard-dispatcher: shards table not ready; waiting and retrying")
			}
			return
		}
		if waitingForSchema {
			waitingForSchema = false
			log.Printf("shard-dispatcher: shards table detected; resuming dispatch")
		}
		d.tick(ctx)
	}

	// Kick once on startup to process recovered pending shards immediately.
	runTick()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runTick()
		}
	}
}

func (d *ShardDispatcher) tick(ctx context.Context) {
	d.cleanupTerminalAgents(ctx)
	d.handleBudgetExceededAssigned(ctx)
	d.handleAssignedStartupStalls(ctx)
	d.handleAssignedTimeouts(ctx)
	d.dispatchPendingShards(ctx)
}

func (d *ShardDispatcher) shardsTableAvailable(ctx context.Context) bool {
	var ok bool
	if err := dbQueryRowContext(ctx, d.db, `SELECT to_regclass('public.shards') IS NOT NULL`).Scan(&ok); err != nil {
		return false
	}
	return ok
}

func (d *ShardDispatcher) dispatchPendingShards(ctx context.Context) {
	if d.isCircuitBreakerOpen(ctx) {
		return
	}
	available := d.availableAssignmentSlots(ctx)
	if available <= 0 {
		return
	}
	rows := make([]shardRuntimeRow, 0, available)
	dbRows, err := dbQueryContext(ctx, d.db, `
		SELECT
			id::text,
			root_task_id::text,
			COALESCE(scan_id::text, ''),
			stage,
			shard_index,
			shard_count,
			shard_key,
			COALESCE(scope, '{}'::jsonb),
			COALESCE(agent_id, ''),
			status,
			deadline_at,
			budget_cents,
			retry_count
		FROM shards
		WHERE status = 'pending'
		ORDER BY created_at ASC, shard_index ASC
		LIMIT $1
	`, available)
	if err != nil {
		log.Printf("shard-dispatcher: query pending failed: %v", err)
		return
	}
	for dbRows.Next() {
		var row shardRuntimeRow
		if scanErr := dbRows.Scan(
			&row.ID,
			&row.RootTaskID,
			&row.ScanID,
			&row.Stage,
			&row.ShardIndex,
			&row.ShardCount,
			&row.ShardKey,
			&row.Scope,
			&row.AgentID,
			&row.Status,
			&row.DeadlineAt,
			&row.BudgetCts,
			&row.RetryCount,
		); scanErr == nil {
			rows = append(rows, row)
		}
	}
	_ = dbRows.Close()
	for _, row := range rows {
		d.dispatchShard(ctx, row)
	}
}

func (d *ShardDispatcher) dispatchShard(ctx context.Context, row shardRuntimeRow) {
	meta, ok := stageMeta(row.Stage)
	if !ok {
		d.markPendingRetryOrTerminal(ctx, row, "unsupported shard stage")
		return
	}
	cloneID := shardCloneAgentID(meta.BaseAgentID, row.ShardIndex, row.ScanID)
	deadline := time.Now().UTC().Add(d.cfg.PerShardTimeout)
	if d.cfg.PerShardTimeout <= 0 {
		deadline = time.Now().UTC().Add(30 * time.Minute)
	}

	if err := d.manager.SpawnEphemeralClone(meta.BaseAgentID, cloneID); err != nil {
		d.markPendingRetryOrTerminal(ctx, row, "spawn ephemeral clone failed: "+err.Error())
		return
	}

	res, err := dbExecContext(ctx, d.db, `
		UPDATE shards
		SET status = 'assigned',
		    agent_id = $2,
		    assigned_at = now(),
		    deadline_at = $3,
		    error = NULL
		WHERE id = $1::uuid
		  AND status = 'pending'
	`, row.ID, cloneID, deadline)
	if err != nil {
		log.Printf("shard-dispatcher: claim shard failed id=%s err=%v", row.ID, err)
		_ = d.manager.TeardownAgent(cloneID)
		return
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		_ = d.manager.TeardownAgent(cloneID)
		return
	}

	scope := map[string]any{}
	_ = json.Unmarshal(row.Scope, &scope)
	if scope == nil {
		scope = map[string]any{}
	}
	if strings.TrimSpace(asString(scope["scan_id"])) == "" {
		scope["scan_id"] = row.ScanID
	}
	shardScope := clonePayloadMap(scope)
	scope["shard"] = map[string]any{
		"root_task_id": row.RootTaskID,
		"scan_id":      row.ScanID,
		"shard_id":     row.ID,
		"shard_index":  row.ShardIndex,
		"shard_count":  row.ShardCount,
		"shard_key":    row.ShardKey,
		"scope":        shardScope,
		"deadline_at":  deadline.UTC().Format(time.RFC3339),
		"budget_cents": row.BudgetCts,
	}

	assignEvt := events.Event{
		ID:          uuid.NewString(),
		Type:        meta.AssignEvent,
		SourceAgent: "shard-dispatcher",
		Payload:     mustJSON(scope),
		CreatedAt:   time.Now(),
	}
	if err := d.bus.PublishDirect(ctx, assignEvt, []string{cloneID}); err != nil {
		d.markRetryOrTerminal(ctx, row, cloneID, "publish shard assignment failed: "+err.Error())
		return
	}
	_ = d.manager.ReplayAgentBacklog(ctx, cloneID)
}

func (d *ShardDispatcher) handleAssignedTimeouts(ctx context.Context) {
	rows := make([]shardRuntimeRow, 0, 32)
	dbRows, err := dbQueryContext(ctx, d.db, `
		SELECT
			id::text,
			root_task_id::text,
			COALESCE(scan_id::text, ''),
			stage,
			shard_index,
			shard_count,
			shard_key,
			COALESCE(scope, '{}'::jsonb),
			COALESCE(agent_id, ''),
			status,
			deadline_at,
			budget_cents,
			retry_count
		FROM shards
		WHERE status = 'assigned'
		  AND deadline_at <= now()
		ORDER BY deadline_at ASC
		LIMIT 64
	`)
	if err != nil {
		log.Printf("shard-dispatcher: query timed out shards failed: %v", err)
		return
	}
	for dbRows.Next() {
		var row shardRuntimeRow
		if scanErr := dbRows.Scan(
			&row.ID,
			&row.RootTaskID,
			&row.ScanID,
			&row.Stage,
			&row.ShardIndex,
			&row.ShardCount,
			&row.ShardKey,
			&row.Scope,
			&row.AgentID,
			&row.Status,
			&row.DeadlineAt,
			&row.BudgetCts,
			&row.RetryCount,
		); scanErr == nil {
			rows = append(rows, row)
		}
	}
	_ = dbRows.Close()

	for _, row := range rows {
		if strings.TrimSpace(row.AgentID) != "" {
			_ = d.manager.TeardownAgent(row.AgentID)
		}
		if row.RetryCount < d.cfg.MaxRetriesPerShard {
			if _, err := dbExecContext(ctx, d.db, `
				UPDATE shards
				SET status = 'pending',
				    retry_count = retry_count + 1,
				    agent_id = NULL,
				    assigned_at = NULL,
				    error = $2
				WHERE id = $1::uuid
				  AND status = 'assigned'
			`, row.ID, "shard timeout; requeueing"); err != nil {
				log.Printf("shard-dispatcher: requeue timeout shard failed id=%s err=%v", row.ID, err)
			}
			continue
		}
		if _, err := dbExecContext(ctx, d.db, `
			UPDATE shards
			SET status = 'timed_out',
			    retry_count = retry_count + 1,
			    completed_at = now(),
			    error = $2
			WHERE id = $1::uuid
			  AND status = 'assigned'
		`, row.ID, "shard timeout; retries exhausted"); err != nil {
			log.Printf("shard-dispatcher: mark timed_out failed id=%s err=%v", row.ID, err)
			continue
		}
		d.emitTerminalShardCompletion(ctx, row, "timed_out", "shard timeout; retries exhausted")
	}
}

func (d *ShardDispatcher) handleAssignedStartupStalls(ctx context.Context) {
	if d == nil || d.db == nil {
		return
	}
	grace := d.startupGracePeriod
	if grace <= 0 {
		grace = 20 * time.Minute
	}
	rows := make([]shardRuntimeRow, 0, 32)
	dbRows, err := dbQueryContext(ctx, d.db, `
		SELECT
			id::text,
			root_task_id::text,
			COALESCE(scan_id::text, ''),
			stage,
			shard_index,
			shard_count,
			shard_key,
			COALESCE(scope, '{}'::jsonb),
			COALESCE(agent_id, ''),
			status,
			deadline_at,
			budget_cents,
			retry_count
		FROM shards
		WHERE status = 'assigned'
		  AND assigned_at IS NOT NULL
		  AND assigned_at <= now() - ($1::text)::interval
		ORDER BY assigned_at ASC
		LIMIT 64
	`, fmt.Sprintf("%d seconds", int(grace.Seconds())))
	if err != nil {
		log.Printf("shard-dispatcher: query startup stalls failed: %v", err)
		return
	}
	for dbRows.Next() {
		var row shardRuntimeRow
		if scanErr := dbRows.Scan(
			&row.ID,
			&row.RootTaskID,
			&row.ScanID,
			&row.Stage,
			&row.ShardIndex,
			&row.ShardCount,
			&row.ShardKey,
			&row.Scope,
			&row.AgentID,
			&row.Status,
			&row.DeadlineAt,
			&row.BudgetCts,
			&row.RetryCount,
		); scanErr == nil {
			rows = append(rows, row)
		}
	}
	_ = dbRows.Close()

	for _, row := range rows {
		agentID := strings.TrimSpace(row.AgentID)
		if agentID == "" {
			continue
		}
		pendingEventID, pending := d.pendingAssignmentReceipt(ctx, agentID)
		if !pending {
			continue
		}
		if d.agentHasRecordedTurns(ctx, agentID) {
			continue
		}
		if d.agentSessionLeaseActive(ctx, agentID) {
			// The shard currently holds a live session lease, so it is still actively
			// running even if no turn has been persisted yet.
			continue
		}
		_ = d.reconcileAssignmentReceipt(ctx, pendingEventID, agentID)
		d.markRetryOrTerminal(ctx, row, agentID, "startup stall: assigned shard recorded no turns before grace window")
	}
}

func (d *ShardDispatcher) handleBudgetExceededAssigned(ctx context.Context) {
	rows := make([]shardRuntimeRow, 0, 32)
	dbRows, err := dbQueryContext(ctx, d.db, `
		SELECT
			id::text,
			root_task_id::text,
			COALESCE(scan_id::text, ''),
			stage,
			shard_index,
			shard_count,
			shard_key,
			COALESCE(scope, '{}'::jsonb),
			COALESCE(agent_id, ''),
			status,
			deadline_at,
			budget_cents,
			retry_count
		FROM shards
		WHERE status = 'assigned'
		  AND spend_cents >= budget_cents
		ORDER BY assigned_at ASC
		LIMIT 32
	`)
	if err != nil {
		return
	}
	for dbRows.Next() {
		var row shardRuntimeRow
		if scanErr := dbRows.Scan(
			&row.ID,
			&row.RootTaskID,
			&row.ScanID,
			&row.Stage,
			&row.ShardIndex,
			&row.ShardCount,
			&row.ShardKey,
			&row.Scope,
			&row.AgentID,
			&row.Status,
			&row.DeadlineAt,
			&row.BudgetCts,
			&row.RetryCount,
		); scanErr == nil {
			rows = append(rows, row)
		}
	}
	_ = dbRows.Close()
	for _, row := range rows {
		if strings.TrimSpace(row.AgentID) != "" {
			_ = d.manager.TeardownAgent(row.AgentID)
		}
		if _, err := dbExecContext(ctx, d.db, `
			UPDATE shards
			SET status = 'failed',
			    completed_at = now(),
			    error = $2
			WHERE id = $1::uuid
			  AND status = 'assigned'
		`, row.ID, "per-shard budget exceeded"); err != nil {
			continue
		}
		d.emitTerminalShardCompletion(ctx, row, "failed", "per-shard budget exceeded")
	}
}

func (d *ShardDispatcher) cleanupTerminalAgents(ctx context.Context) {
	rows, err := dbQueryContext(ctx, d.db, `
		SELECT COALESCE(agent_id, ''), COALESCE(completed_at, now())
		FROM shards
		WHERE status IN ('completed', 'failed', 'timed_out')
		  AND agent_id IS NOT NULL
		ORDER BY completed_at DESC NULLS LAST, created_at DESC
		LIMIT 200
	`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			agentID     string
			completedAt time.Time
		)
		if err := rows.Scan(&agentID, &completedAt); err != nil {
			continue
		}
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if pendingEventID, pending := d.pendingAssignmentReceipt(ctx, agentID); pending {
			if time.Since(completedAt) < d.receiptGracePeriod {
				continue
			}
			if err := d.reconcileAssignmentReceipt(ctx, pendingEventID, agentID); err != nil {
				log.Printf("shard-dispatcher: reconcile receipt failed agent=%s event=%s err=%v", agentID, pendingEventID, err)
				continue
			}
		}
		if _, ok := d.manager.GetAgentConfig(agentID); !ok {
			continue
		}
		if err := d.manager.TeardownAgent(agentID); err != nil && !strings.Contains(err.Error(), "agent not found") {
			log.Printf("shard-dispatcher: teardown terminal agent failed id=%s err=%v", agentID, err)
		}
	}
}

func (d *ShardDispatcher) pendingAssignmentReceipt(ctx context.Context, agentID string) (string, bool) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || d == nil || d.db == nil {
		return "", false
	}
	var eventID string
	err := dbQueryRowContext(ctx, d.db, `
		SELECT d.event_id::text
		FROM event_deliveries d
		JOIN events e ON e.id = d.event_id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		WHERE d.agent_id = $1
		  AND e.type IN ('market_research.scan_assigned', 'trend_research.scan_assigned')
		  AND r.event_id IS NULL
		ORDER BY d.created_at DESC
		LIMIT 1
	`, agentID).Scan(&eventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false
		}
		log.Printf("shard-dispatcher: pending receipt query failed agent=%s err=%v", agentID, err)
		return "", false
	}
	eventID = strings.TrimSpace(eventID)
	return eventID, eventID != ""
}

func (d *ShardDispatcher) reconcileAssignmentReceipt(ctx context.Context, eventID, agentID string) error {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return nil
	}
	_, err := dbExecContext(ctx, d.db, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, $2, now(), 'processed', 0, 'reconciled terminal shard teardown')
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`, eventID, agentID)
	return err
}

func (d *ShardDispatcher) agentHasRecordedTurns(ctx context.Context, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || d == nil || d.db == nil {
		return false
	}
	var turns int
	if err := dbQueryRowContext(ctx, d.db, `
		SELECT COUNT(*)
		FROM agent_turns t
		INNER JOIN agent_sessions s ON s.id = t.session_row_id
		WHERE s.agent_id = $1
	`, agentID).Scan(&turns); err != nil {
		return false
	}
	return turns > 0
}

func (d *ShardDispatcher) agentSessionLeaseActive(ctx context.Context, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || d == nil || d.db == nil {
		return false
	}
	var active bool
	if err := dbQueryRowContext(ctx, d.db, `
		SELECT EXISTS (
			SELECT 1
			FROM agent_sessions
			WHERE agent_id = $1
			  AND status = 'active'
			  AND lock_owner IS NOT NULL
			  AND lock_expires_at IS NOT NULL
			  AND lock_expires_at > now()
		)
	`, agentID).Scan(&active); err != nil {
		return false
	}
	return active
}

func (d *ShardDispatcher) markRetryOrTerminal(ctx context.Context, row shardRuntimeRow, agentID, reason string) {
	if strings.TrimSpace(agentID) != "" {
		_ = d.manager.TeardownAgent(agentID)
	}
	if row.RetryCount < d.cfg.MaxRetriesPerShard {
		if _, err := dbExecContext(ctx, d.db, `
			UPDATE shards
			SET status = 'pending',
			    retry_count = retry_count + 1,
			    agent_id = NULL,
			    assigned_at = NULL,
			    error = $2
			WHERE id = $1::uuid
			  AND status = 'assigned'
		`, row.ID, truncateForDB(reason, 400)); err != nil {
			log.Printf("shard-dispatcher: mark retry failed id=%s err=%v", row.ID, err)
		}
		return
	}
	d.markTerminalFailure(ctx, row, reason)
}

func (d *ShardDispatcher) markPendingRetryOrTerminal(ctx context.Context, row shardRuntimeRow, reason string) {
	if row.RetryCount < d.cfg.MaxRetriesPerShard {
		if _, err := dbExecContext(ctx, d.db, `
			UPDATE shards
			SET retry_count = retry_count + 1,
			    error = $2
			WHERE id = $1::uuid
			  AND status = 'pending'
		`, row.ID, truncateForDB(reason, 400)); err != nil {
			log.Printf("shard-dispatcher: mark pending retry failed id=%s err=%v", row.ID, err)
		}
		return
	}
	if _, err := dbExecContext(ctx, d.db, `
		UPDATE shards
		SET status = 'failed',
		    retry_count = retry_count + 1,
		    completed_at = now(),
		    error = $2
		WHERE id = $1::uuid
		  AND status = 'pending'
	`, row.ID, truncateForDB(reason, 400)); err != nil {
		log.Printf("shard-dispatcher: mark pending terminal failed id=%s err=%v", row.ID, err)
		return
	}
	d.emitTerminalShardCompletion(ctx, row, "failed", reason)
}

func (d *ShardDispatcher) markTerminalFailure(ctx context.Context, row shardRuntimeRow, reason string) {
	if strings.TrimSpace(row.AgentID) != "" {
		_ = d.manager.TeardownAgent(row.AgentID)
	}
	if _, err := dbExecContext(ctx, d.db, `
		UPDATE shards
		SET status = 'failed',
		    retry_count = retry_count + 1,
		    completed_at = now(),
		    error = $2
		WHERE id = $1::uuid
		  AND status IN ('pending', 'assigned')
	`, row.ID, truncateForDB(reason, 400)); err != nil {
		log.Printf("shard-dispatcher: mark terminal failed id=%s err=%v", row.ID, err)
		return
	}
	d.emitTerminalShardCompletion(ctx, row, "failed", reason)
}

func (d *ShardDispatcher) emitTerminalShardCompletion(ctx context.Context, row shardRuntimeRow, status, reason string) {
	meta, ok := stageMeta(row.Stage)
	if !ok {
		return
	}
	source := strings.TrimSpace(row.AgentID)
	if source == "" {
		source = "shard-dispatcher-" + shortID(row.ID)
	}
	payload := map[string]any{
		"scan_id": row.ScanID,
		"shard": map[string]any{
			"shard_id":    row.ID,
			"shard_index": row.ShardIndex,
			"shard_count": row.ShardCount,
			"shard_key":   row.ShardKey,
			"status":      status,
			"terminal":    true,
			"error":       truncateForDB(reason, 400),
		},
	}
	_ = d.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        completionEventFor(meta.AssignEvent),
		SourceAgent: source,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	})
}

func (d *ShardDispatcher) availableAssignmentSlots(ctx context.Context) int {
	maxConcurrent := d.cfg.MaxConcurrentShards
	if maxConcurrent <= 0 {
		maxConcurrent = 12
	}
	var assigned int
	_ = dbQueryRowContext(ctx, d.db, `SELECT COUNT(*) FROM shards WHERE status = 'assigned'`).Scan(&assigned)
	available := maxConcurrent - assigned
	if available < 0 {
		return 0
	}
	return available
}

func (d *ShardDispatcher) isCircuitBreakerOpen(ctx context.Context) bool {
	threshold := d.cfg.CircuitBreakerThreshold
	if threshold <= 0 || threshold > 1 {
		threshold = 0.5
	}
	var failed, terminal int
	_ = dbQueryRowContext(ctx, d.db, `
		SELECT
			COUNT(*) FILTER (WHERE status IN ('failed', 'timed_out')) AS failed,
			COUNT(*) FILTER (WHERE status IN ('completed', 'failed', 'timed_out')) AS terminal
		FROM shards
		WHERE COALESCE(completed_at, created_at) >= now() - interval '1 hour'
	`).Scan(&failed, &terminal)
	if terminal == 0 {
		return false
	}
	return float64(failed)/float64(terminal) > threshold
}

func stageMeta(stage string) (shardStageMeta, bool) {
	switch strings.TrimSpace(stage) {
	case ShardStageMarketResearch:
		return shardStageMeta{
			BaseAgentID: "market-research-agent",
			AssignEvent: events.EventType("market_research.scan_assigned"),
		}, true
	case ShardStageTrendResearch:
		return shardStageMeta{
			BaseAgentID: "trend-research-agent",
			AssignEvent: events.EventType("trend_research.scan_assigned"),
		}, true
	default:
		return shardStageMeta{}, false
	}
}

func completionEventFor(assignEvent events.EventType) events.EventType {
	switch assignEvent {
	case events.EventType("market_research.scan_assigned"):
		return events.EventType("market_research.scan_complete")
	case events.EventType("trend_research.scan_assigned"):
		return events.EventType("trend_research.scan_complete")
	default:
		return events.EventType("scan.completed")
	}
}

func shardCloneAgentID(base string, shardIndex int, scanID string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "agent"
	}
	return fmt.Sprintf("%s-shard-%d-%s", base, shardIndex, shortID(scanID))
}

func shortID(raw string) string {
	raw = strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if len(raw) >= 8 {
		return raw[:8]
	}
	if raw == "" {
		return "unknown"
	}
	return raw
}

func clonePayloadMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	raw, err := json.Marshal(in)
	if err != nil || len(raw) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func truncateForDB(s string, max int) string {
	s = strings.TrimSpace(s)
	if max <= 0 || len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func startupGraceOrDefault(v time.Duration) time.Duration {
	if v <= 0 {
		return 20 * time.Minute
	}
	return v
}
