package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func (pc *FactoryPipelineCoordinator) handleScanRequested(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		scanID = evt.ID
	}
	mode := normalizeScanMode(asString(payload["mode"]))
	if mode == "" {
		mode = "saas_gap"
	}
	campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
	if campaignID == "" {
		// v2.0.35 canonical schema requires non-null campaign_id in scan_accumulators.
		// When legacy events omit campaign_id, use scan_id as a stable surrogate.
		campaignID = scanID
	}
	geography := strings.TrimSpace(asString(payload["geography"]))
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_label"]))
	}
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_id"]))
	}

	plannedShardCount := pc.planAndPersistShards(ctx, evt, scanID, mode, payload)

	acc := &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  campaignID,
		Mode:        mode,
		Geography:   geography,
		Expected:    expectedAgents(mode),
		CompletedBy: make(map[string]struct{}),
		ReportData:  make([]map[string]any, 0),
		CreatedAt:   time.Now(),
	}
	if plannedShardCount > 0 {
		acc.Expected = plannedShardCount
	}
	pc.mu.Lock()
	pc.scans[scanID] = acc
	pc.mu.Unlock()

	assigned := pc.buildScanAssignedPayload(scanID, campaignID, mode, geography, payload, plannedShardCount)
	if plannedShardCount > 0 && (mode == "saas_gap" || mode == "saas_trend") {
		// Assignment dispatch is owned by the shard dispatcher loop.
		return
	}

	switch mode {
	case "saas_gap":
		pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
	case "saas_trend":
		pc.publish(ctx, "trend_research.scan_assigned", "", payloadMap(assigned))
	case "corpus":
		corpusPath := strings.TrimSpace(asString(payload["corpus_path"]))
		assigned.CorpusPath = corpusPath
		batches, err := readJSONLFile(corpusPath, corpusBatchSize)
		if err != nil {
			runtimeWarn("pipeline-coordinator", "corpus mode read failed path=%q err=%v", corpusPath, err)
			assigned.CorpusSignals = []map[string]any{}
			pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
			return
		}
		if len(batches) == 0 {
			assigned.CorpusSignals = []map[string]any{}
			pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
			return
		}
		for _, batch := range batches {
			perBatch := assigned
			perBatch.CorpusSignals = batch
			pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(perBatch))
		}
	case "local_services":
		pc.publish(ctx, "scanner.google_maps.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.instagram.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.reviews.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.directories.scan_assigned", "", payloadMap(assigned))
		pc.publish(ctx, "scanner.yelp.scan_assigned", "", payloadMap(assigned))
	default:
		pc.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
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
	if pc.shardsTableChecked {
		return pc.shardsTableEnabled
	}
	var ok bool
	_ = dbQueryRowContext(ctx, pc.db, `SELECT to_regclass('public.shards') IS NOT NULL`).Scan(&ok)
	pc.shardsTableChecked = true
	pc.shardsTableEnabled = ok
	return pc.shardsTableEnabled
}

func shardStageForScanMode(mode string) string {
	switch normalizeScanMode(mode) {
	case "saas_gap":
		return ShardStageMarketResearch
	case "saas_trend":
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

func readJSONLFile(path string, batchSize int) ([][]map[string]any, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("corpus_path is required for corpus mode")
	}
	if batchSize <= 0 {
		batchSize = corpusBatchSize
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make([][]map[string]any, 0, 8)
	current := make([]map[string]any, 0, batchSize)
	sc := bufio.NewScanner(f)
	// Allow reasonably large lines for corpus entries.
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 2*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		row := map[string]any{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("invalid jsonl row: %w", err)
		}
		current = append(current, row)
		if len(current) >= batchSize {
			out = append(out, current)
			current = make([]map[string]any, 0, batchSize)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out, nil
}

func (pc *FactoryPipelineCoordinator) handleScanCompletion(ctx context.Context, evt events.Event) {
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		runtimeWarn(
			"pipeline-coordinator",
			"dropping scan completion missing scan_id event_id=%s type=%s source=%s",
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}
	completionKey := strings.TrimSpace(evt.SourceAgent)
	if completionKey == "" {
		completionKey = strings.TrimSpace(string(evt.Type))
	}
	// local_services fanout uses one scanner agent role handling multiple scanner
	// event types; completion accounting must key by scanner completion event type.
	if strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "scanner.") &&
		strings.HasSuffix(strings.TrimSpace(string(evt.Type)), ".scan_complete") {
		completionKey = strings.TrimSpace(string(evt.Type))
	}
	if shardID := pc.markShardCompletedByAgent(ctx, strings.TrimSpace(evt.SourceAgent)); shardID != "" {
		completionKey = "shard:" + shardID
	}
	shardTotal, shardCompleted, shardFailed, hasShardProgress := pc.shardTerminalProgress(ctx, scanID)

	pc.mu.Lock()
	acc := pc.scans[scanID]
	if acc == nil {
		pc.mu.Unlock()
		runtimeWarn(
			"pipeline-coordinator",
			"received scan completion for unknown accumulator scan_id=%s event_id=%s source=%s",
			scanID,
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}
	acc.CompletedBy[completionKey] = struct{}{}
	done := len(acc.CompletedBy) >= maxInt(acc.Expected, 1)
	stats := pc.buildScanCompletedPayload(scanCompletedBuildInput{
		ScanID:          acc.ScanID,
		CampaignID:      acc.CampaignID,
		Mode:            acc.Mode,
		Geography:       acc.Geography,
		ReportsReceived: acc.Reports,
		Expected:        maxInt(acc.Expected, 1),
		Complete:        len(acc.CompletedBy),
		Discovered:      acc.Discovered,
		Skipped:         acc.Skipped,
		PendingDedup:    pc.pendingDedupCountForScan(acc.ScanID),
		TimedOut:        false,
	})
	if hasShardProgress {
		terminal := shardCompleted + shardFailed
		stats.Expected = shardTotal
		stats.Complete = terminal
		stats.ShardsTotal = shardTotal
		stats.ShardsCompleted = shardCompleted
		stats.ShardsFailed = shardFailed
		done = terminal >= shardTotal && shardTotal > 0
	}
	if done {
		delete(pc.scans, scanID)
	}
	pc.mu.Unlock()

	if done {
		pc.publish(ctx, "scan.completed", "", payloadMap(stats))
	}
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

func (pc *FactoryPipelineCoordinator) checkScanTimeouts(ctx context.Context, now time.Time) {
	if pc == nil {
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
	pc.mu.Lock()
	for scanID, acc := range pc.scans {
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
			pendingDedup: pc.pendingDedupCountForScan(scanID),
			shardScanID:  scanID,
		})
		delete(pc.scans, scanID)
	}
	pc.mu.Unlock()

	for _, scan := range expired {
		stats := pc.buildScanCompletedPayload(scanCompletedBuildInput{
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
		shardTotal, shardCompleted, shardFailed, hasShardProgress := pc.shardTerminalProgress(ctx, scan.shardScanID)
		if hasShardProgress {
			terminal := shardCompleted + shardFailed
			stats.Expected = shardTotal
			stats.Complete = terminal
			stats.ShardsTotal = shardTotal
			stats.ShardsCompleted = shardCompleted
			stats.ShardsFailed = shardFailed
		}
		pc.publish(ctx, "scan.completed", "", payloadMap(stats))
	}
}
