package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
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
		mode = scanOrchestratorFallbackMode()
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
	assignmentEvents := scanOrchestratorAssignmentEvents(mode)
	if plannedShardCount > 0 && scanOrchestratorUsesShardedDispatch(assignmentEvents) {
		return
	}
	assignments, err := defaultWorkflowModule().ScanPolicy().ExpandScanAssignments(mode, payload, assigned, corpusBatchSize)
	if err != nil {
		runtimeWarn(n.NodeID(), "scan assignment expansion failed mode=%s err=%v", mode, err)
	}
	if len(assignments) == 0 {
		assignments = []ScanAssignedPayload{assigned}
	}

	for _, expanded := range assignments {
		for _, eventType := range assignmentEvents {
			sc.runtime.publish(ctx, eventType, "", payloadMap(expanded))
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

func scanOrchestratorFallbackMode() string {
	if bundle := scanOrchestratorContractBundle(); bundle != nil {
		if pv, ok := bundle.MergedPolicy.Values["default_scan_mode"]; ok {
			if mode := strings.TrimSpace(asString(pv.Value)); mode != "" {
				return normalizeScanMode(mode)
			}
		}
		if pv, ok := bundle.Policy.Values["default_scan_mode"]; ok {
			if mode := strings.TrimSpace(asString(pv.Value)); mode != "" {
				return normalizeScanMode(mode)
			}
		}
	}
	return bundleDefaultScanMode(nil)
}

func scanOrchestratorAssignmentEvents(mode string) []string {
	mode = normalizeScanMode(mode)
	if mode == "" {
		mode = scanOrchestratorFallbackMode()
	}
	if bundle := scanOrchestratorContractBundle(); bundle != nil {
		if events := scanOrchestratorAssignmentEventsFromBundle(bundle, mode); len(events) > 0 {
			return events
		}
	}
	return scanOrchestratorAssignmentEventsFallback(mode)
}

func scanOrchestratorUsesShardedDispatch(assignmentEvents []string) bool {
	for _, eventType := range assignmentEvents {
		switch strings.TrimSpace(eventType) {
		case "market_research.scan_assigned", "trend_research.scan_assigned":
			return true
		}
	}
	return false
}

func scanOrchestratorContractBundle() *runtimecontracts.WorkflowContractBundle {
	module := defaultWorkflowModuleOrNil()
	if module == nil {
		return nil
	}
	return module.ContractBundle()
}

func scanOrchestratorAssignmentEventsFromBundle(bundle *runtimecontracts.WorkflowContractBundle, mode string) []string {
	handler, ok := bundle.NodeEventHandler("scan-orchestrator", "scan.requested")
	if !ok {
		return nil
	}
	dispatchEvents := scanOrchestratorDispatchEventsForMode(handler, mode)
	if len(dispatchEvents) == 0 {
		dispatchEvents = handler.Emits.Values()
	}
	assignmentEvents := make([]string, 0, len(dispatchEvents))
	for _, dispatchEvent := range dispatchEvents {
		dispatchEvent = strings.TrimSpace(dispatchEvent)
		if dispatchEvent == "" {
			continue
		}
		dispatchHandler, ok := bundle.NodeEventHandler("scan-orchestrator", dispatchEvent)
		if !ok {
			continue
		}
		assignmentEvents = append(assignmentEvents, scanOrchestratorFanOutTargets(dispatchHandler.FanOut)...)
	}
	return uniqueStrings(assignmentEvents)
}

func scanOrchestratorDispatchEventsForMode(handler runtimecontracts.SystemNodeEventHandler, mode string) []string {
	if len(handler.Rules) == 0 {
		return nil
	}
	mode = normalizeScanMode(mode)
	if mode == "" {
		return nil
	}
	// Look for a rule matching the mode ID
	for _, rule := range handler.Rules {
		if strings.EqualFold(strings.TrimSpace(rule.ID), mode) {
			return rule.Emits.Values()
		}
	}
	// Fall back to the "else" rule
	for _, rule := range handler.Rules {
		if strings.EqualFold(strings.TrimSpace(rule.Condition), "else") {
			return rule.Emits.Values()
		}
	}
	return nil
}

func scanOrchestratorFanOutTargets(fanOut *runtimecontracts.FanOutSpec) []string {
	if fanOut == nil {
		return nil
	}
	emitPerItem := strings.TrimSpace(fanOut.EmitPerItem)
	switch emitPerItem {
	case "":
		return nil
	case "scanner.scan_assigned":
		return []string{
			"scanner.google_maps.scan_assigned",
			"scanner.instagram.scan_assigned",
			"scanner.reviews.scan_assigned",
			"scanner.directories.scan_assigned",
			"scanner.yelp.scan_assigned",
		}
	default:
		return []string{emitPerItem}
	}
}

func scanOrchestratorAssignmentEventsFallback(mode string) []string {
	return []string{"market_research.scan_assigned"}
}
