package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"empireai/internal/events"
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
		mode = "saas_gap"
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
	if plannedShardCount > 0 && (mode == "saas_gap" || mode == "saas_trend") {
		return
	}

	switch mode {
	case "saas_gap":
		sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
	case "saas_trend":
		sc.runtime.publish(ctx, "trend_research.scan_assigned", "", payloadMap(assigned))
	case "corpus":
		corpusPath := strings.TrimSpace(asString(payload["corpus_path"]))
		assigned.CorpusPath = corpusPath
		batches, err := readJSONLFile(corpusPath, corpusBatchSize)
		if err != nil {
			runtimeWarn(n.NodeID(), "corpus mode read failed path=%q err=%v", corpusPath, err)
			assigned.CorpusSignals = []map[string]any{}
			sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
			return
		}
		if len(batches) == 0 {
			assigned.CorpusSignals = []map[string]any{}
			sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
			return
		}
		for _, batch := range batches {
			perBatch := assigned
			perBatch.CorpusSignals = batch
			sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(perBatch))
		}
	case "local_services":
		sc.runtime.publish(ctx, "scanner.google_maps.scan_assigned", "", payloadMap(assigned))
		sc.runtime.publish(ctx, "scanner.instagram.scan_assigned", "", payloadMap(assigned))
		sc.runtime.publish(ctx, "scanner.reviews.scan_assigned", "", payloadMap(assigned))
		sc.runtime.publish(ctx, "scanner.directories.scan_assigned", "", payloadMap(assigned))
		sc.runtime.publish(ctx, "scanner.yelp.scan_assigned", "", payloadMap(assigned))
	default:
		sc.runtime.publish(ctx, "market_research.scan_assigned", "", payloadMap(assigned))
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
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("corpus_path is required for corpus mode")
	}
	if batchSize <= 0 {
		batchSize = corpusBatchSize
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxCapacity := max(batchSize*8*1024, 1024*1024)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	batches := make([][]map[string]any, 0)
	current := make([]map[string]any, 0, batchSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		entry, ok := parseJSONLine(line)
		if !ok {
			continue
		}
		current = append(current, entry)
		if batchSize > 0 && len(current) >= batchSize {
			batches = append(batches, current)
			current = make([]map[string]any, 0, batchSize)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
}

func parseJSONLine(line string) (map[string]any, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, false
	}
	row := map[string]any{}
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return nil, false
	}
	return row, true
}
