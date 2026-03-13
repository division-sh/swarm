package pipeline

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func (sc *ScanCoordinator) handleScanRequested(ctx context.Context, evt events.Event) {
	if pc, ok := sc.runtime.(*FactoryPipelineCoordinator); ok {
		pc.handleScanRequested(ctx, evt)
	}
}

func (pc *FactoryPipelineCoordinator) planAndPersistShards(
	ctx context.Context,
	evt events.Event,
	scanID, mode string,
	payload map[string]any,
) int {
	if pc == nil || pc.db == nil || evt.ID == "" {
		return 0
	}
	pc.mu.Lock()
	planner := pc.shardPlanner
	pc.mu.Unlock()
	if planner == nil {
		return 0
	}
	if !pc.isShardsTableEnabled(ctx) {
		return 0
	}
	stage := shardStageForScanMode(mode)
	if stage == "" {
		return 0
	}
	assignments, err := planner.Plan(stage, payload)
	if err != nil || len(assignments) == 0 {
		return 0
	}

	rootTaskID := stableUUID(evt.ID)
	scanUUID := stableUUID(scanID)
	now := time.Now().UTC()
	for _, assignment := range assignments {
		deadline := now.Add(assignment.Timeout)
		if assignment.Timeout <= 0 {
			deadline = now.Add(30 * time.Minute)
		}
		shardID := uuid.NewSHA1(rootTaskID, []byte(assignment.Stage+":"+assignment.ShardKey)).String()
		scope := assignment.Scope
		if scope == nil {
			scope = map[string]any{}
		}
		scope["scan_id"] = scanID
		scope["mode"] = mode
		if v := strings.TrimSpace(asString(payload["campaign_id"])); v != "" {
			scope["campaign_id"] = v
		}
		if v := strings.TrimSpace(asString(payload["geography"])); v != "" {
			scope["geography"] = v
		}
		if v := strings.TrimSpace(asString(payload["geography_id"])); v != "" {
			scope["geography_id"] = v
		}
		if v := strings.TrimSpace(asString(payload["priority"])); v != "" {
			scope["priority"] = v
		}
		if v := strings.TrimSpace(asString(payload["directive_id"])); v != "" {
			scope["directive_id"] = v
		}
		if campaignContext := payload["campaign_context"]; campaignContext != nil {
			scope["campaign_context"] = campaignContext
		}
		if strategicContext := payload["strategic_context"]; strategicContext != nil {
			scope["strategic_context"] = strategicContext
		}
		if _, err := dbExecContext(ctx, pc.db, `
			INSERT INTO shards (
				id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
				scope, status, deadline_at, budget_cents, created_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7,
				$8::jsonb, 'pending', $9, $10, now()
			)
			ON CONFLICT (root_task_id, shard_key) DO NOTHING
		`,
			shardID,
			rootTaskID.String(),
			scanUUID.String(),
			assignment.Stage,
			assignment.ShardIndex,
			assignment.ShardCount,
			assignment.ShardKey,
			string(mustJSON(scope)),
			deadline,
			assignment.BudgetCents,
		); err != nil {
			log.Printf("pipeline: shard persist failed scan=%s stage=%s key=%s err=%v", scanID, stage, assignment.ShardKey, err)
			return 0
		}
	}

	var count int
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM shards
		WHERE root_task_id = $1::uuid
	`, rootTaskID.String()).Scan(&count); err != nil {
		log.Printf("pipeline: shard count failed scan=%s stage=%s err=%v", scanID, stage, err)
		return len(assignments)
	}
	return count
}

func (pc *FactoryPipelineCoordinator) isShardsTableEnabled(ctx context.Context) bool {
	if pc == nil || pc.db == nil {
		return false
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.shardsTableEnabled
}

func detectShardsTable(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	var ok bool
	_ = dbQueryRowContext(ctx, db, `SELECT to_regclass('public.shards') IS NOT NULL`).Scan(&ok)
	return ok
}

func shardStageForScanMode(mode string) string {
	switch normalizeCampaignScanMode(mode) {
	case pipelineModeName("saas", "gap"):
		return ShardStageMarketResearch
	case pipelineModeName("saas", "trend"):
		return ShardStageTrendResearch
	default:
		return ""
	}
}

func stableUUID(raw string) uuid.UUID {
	raw = strings.TrimSpace(raw)
	if parsed, err := uuid.Parse(raw); err == nil {
		return parsed
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(raw))
}

func (sc *ScanCoordinator) handleScanCompletion(ctx context.Context, evt events.Event) {
	if pc, ok := sc.runtime.(*FactoryPipelineCoordinator); ok {
		pc.handleScanCompletion(ctx, evt)
	}
}

func (sc *ScanCoordinator) handleScanTimeout(ctx context.Context, _ events.Event) {
	if sc == nil {
		return
	}
	sc.checkTimeouts(ctx, time.Now().UTC())
}

func (sc *ScanCoordinator) handleCampaignDeadline(context.Context, events.Event) {
	// 2.2.0 assigns this timer to scan-orchestrator. Campaign completion still
	// runs through the campaign manager, so this remains an explicit scan-owned
	// compatibility no-op until the remaining campaign state is moved over.
}

func (pc *FactoryPipelineCoordinator) markShardCompletedByAgent(ctx context.Context, agentID string) string {
	if pc == nil || pc.db == nil || strings.TrimSpace(agentID) == "" {
		return ""
	}
	if !pc.isShardsTableEnabled(ctx) {
		return ""
	}
	var shardID string
	if err := dbQueryRowContext(ctx, pc.db, `
		UPDATE shards
		SET status = 'completed',
		    completed_at = COALESCE(completed_at, now())
		WHERE agent_id = $1
		  AND status = 'assigned'
		RETURNING id::text
	`, strings.TrimSpace(agentID)).Scan(&shardID); err != nil {
		return ""
	}
	return strings.TrimSpace(shardID)
}

func (pc *FactoryPipelineCoordinator) shardTerminalProgress(ctx context.Context, scanID string) (total, completed, failed int, ok bool) {
	if pc == nil || pc.db == nil || strings.TrimSpace(scanID) == "" {
		return 0, 0, 0, false
	}
	if !pc.isShardsTableEnabled(ctx) {
		return 0, 0, 0, false
	}
	scanUUID := stableUUID(scanID).String()
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status IN ('failed', 'timed_out')) AS failed
		FROM shards
		WHERE scan_id = $1::uuid
	`, scanUUID).Scan(&total, &completed, &failed); err != nil {
		return 0, 0, 0, false
	}
	return total, completed, failed, total > 0
}

func (sc *ScanCoordinator) checkTimeouts(ctx context.Context, now time.Time) {
	if sc == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	type timedOutScan struct {
		scanID       string
		campaignID   string
		mode         string
		geography    string
		reports      int
		expected     int
		completed    int
		discovered   int
		skipped      int
		pendingDedup int
		shardScanID  string
	}
	expired := make([]timedOutScan, 0, 8)
	sc.mu.Lock()
	for scanID, acc := range sc.scans {
		if acc == nil {
			continue
		}
		createdAt := acc.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		if now.Before(createdAt.Add(scanTimeout)) {
			continue
		}
		expired = append(expired, timedOutScan{
			scanID:       acc.ScanID,
			campaignID:   acc.CampaignID,
			mode:         acc.Mode,
			geography:    acc.Geography,
			reports:      acc.Reports,
			expected:     maxInt(acc.Expected, 1),
			completed:    len(acc.CompletedBy),
			discovered:   acc.Discovered,
			skipped:      acc.Skipped,
			pendingDedup: sc.pendingDedupCountForScan(scanID),
			shardScanID:  scanID,
		})
		delete(sc.scans, scanID)
	}
	sc.mu.Unlock()

	for _, scan := range expired {
		stats := sc.payloadFactory.BuildScanCompletedPayload(scanCompletedBuildInput{
			ScanID:          scan.scanID,
			CampaignID:      scan.campaignID,
			Mode:            scan.mode,
			Geography:       scan.geography,
			ReportsReceived: scan.reports,
			Expected:        scan.expected,
			Complete:        scan.completed,
			Discovered:      scan.discovered,
			Skipped:         scan.skipped,
			PendingDedup:    scan.pendingDedup,
			TimedOut:        true,
		})
		shardTotal, shardCompleted, shardFailed, hasShardProgress := sc.runtime.shardTerminalProgress(ctx, scan.shardScanID)
		if hasShardProgress {
			terminal := shardCompleted + shardFailed
			stats.Expected = shardTotal
			stats.Complete = terminal
			stats.ShardsTotal = shardTotal
			stats.ShardsCompleted = shardCompleted
			stats.ShardsFailed = shardFailed
		}
		sc.runtime.publish(ctx, "scan.completed", "", payloadMap(stats))
	}
}
