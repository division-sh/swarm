package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
)

func (n *ScanOrchestrator) handleScanRequested(ctx context.Context, evt events.Event) {
	sc := n.scanCoordinator()
	if sc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		scanID = evt.ID
	}
	mode := normalizeScanMode(asString(payload["mode"]))
	if mode == "" {
		mode = runtimeproductpolicy.DiscoveryFallbackMode()
	}
	campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
	if campaignID == "" {
		campaignID = scanID
	}
	geography := strings.TrimSpace(asString(payload["geography"]))
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_label"]))
	}
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_id"]))
	}

	plannedShardCount := sc.runtime.planAndPersistShards(ctx, evt, scanID, mode, payload)

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
	sc.mu.Lock()
	sc.scans[scanID] = acc
	sc.mu.Unlock()

	assigned := sc.payloadFactory.BuildScanAssignedPayload(scanID, campaignID, mode, geography, payload, plannedShardCount)
	if plannedShardCount > 0 && runtimeproductpolicy.ScanShardStage(mode) != "" {
		return
	}
	assignments, err := defaultWorkflowModule().ScanPolicy().ExpandScanAssignments(mode, payload, assigned, corpusBatchSize)
	if err != nil {
		runtimeWarn(n.NodeID(), "scan assignment expansion failed mode=%s err=%v", mode, err)
	}
	if len(assignments) == 0 {
		assignments = []ScanAssignedPayload{assigned}
	}

	switch runtimeproductpolicy.ScanDispatchKind(mode) {
	case "market":
		for _, expanded := range assignments {
			sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(expanded))
		}
	case "trend":
		for _, expanded := range assignments {
			sc.runtime.publish(ctx, "trend_research.scan_assigned", "", payloadMap(expanded))
		}
	case "local":
		for _, expanded := range assignments {
			sc.runtime.publish(ctx, "scanner.google_maps.scan_assigned", "", payloadMap(expanded))
			sc.runtime.publish(ctx, "scanner.instagram.scan_assigned", "", payloadMap(expanded))
			sc.runtime.publish(ctx, "scanner.reviews.scan_assigned", "", payloadMap(expanded))
			sc.runtime.publish(ctx, "scanner.directories.scan_assigned", "", payloadMap(expanded))
			sc.runtime.publish(ctx, "scanner.yelp.scan_assigned", "", payloadMap(expanded))
		}
	default:
		for _, expanded := range assignments {
			sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(expanded))
		}
	}
}

func (n *ScanOrchestrator) handleScanCompletion(ctx context.Context, evt events.Event) {
	sc := n.scanCoordinator()
	if sc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		runtimeWarn(
			n.NodeID(),
			"dropping scan completion missing scan_id event_id=%s type=%s source=%s",
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}
	sc.ensureScanProjectionLoaded(ctx, scanID)
	completionKey := strings.TrimSpace(evt.SourceAgent)
	if completionKey == "" {
		completionKey = strings.TrimSpace(string(evt.Type))
	}
	if strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "scanner.") &&
		strings.HasSuffix(strings.TrimSpace(string(evt.Type)), ".scan_complete") {
		completionKey = strings.TrimSpace(string(evt.Type))
	}
	if shardID := sc.runtime.markShardCompletedByAgent(ctx, strings.TrimSpace(evt.SourceAgent)); shardID != "" {
		completionKey = "shard:" + shardID
	}
	shardTotal, shardCompleted, shardFailed, hasShardProgress := sc.runtime.shardTerminalProgress(ctx, scanID)

	sc.mu.Lock()
	acc := sc.scans[scanID]
	if acc == nil {
		sc.mu.Unlock()
		runtimeWarn(
			n.NodeID(),
			"received scan completion for unknown accumulator scan_id=%s event_id=%s source=%s",
			scanID,
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}
	acc.CompletedBy[completionKey] = struct{}{}
	done := len(acc.CompletedBy) >= maxInt(acc.Expected, 1)
	stats := sc.payloadFactory.BuildScanCompletedPayload(scanCompletedBuildInput{
		ScanID:          acc.ScanID,
		CampaignID:      acc.CampaignID,
		Mode:            acc.Mode,
		Geography:       acc.Geography,
		ReportsReceived: acc.Reports,
		Expected:        maxInt(acc.Expected, 1),
		Complete:        len(acc.CompletedBy),
		Discovered:      acc.Discovered,
		Skipped:         acc.Skipped,
		PendingDedup:    sc.pendingDedupCountForScan(acc.ScanID),
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
		delete(sc.scans, scanID)
	}
	sc.mu.Unlock()

	if done {
		sc.runtime.publish(ctx, "scan.completed", "", payloadMap(stats))
	}
}

func (n *ScanOrchestrator) scanCoordinator() *ScanCoordinator {
	if n == nil {
		return nil
	}
	return n.coordinator
}

func readJSONLFile(path string, batchSize int) ([][]map[string]any, error) {
	return defaultWorkflowModule().ScanPolicy().ReadJSONLBatches(path, batchSize)
}
