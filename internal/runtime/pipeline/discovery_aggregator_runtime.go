package pipeline

import (
	"context"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func discoveryRuntimeDefaultScanMode(runtime scanWorkflowRuntime) string {
	if pc, ok := runtime.(*FactoryPipelineCoordinator); ok {
		return defaultPipelineScanMode(pc.SemanticSource())
	}
	return defaultPipelineScanMode(nil)
}

func discoveryRuntimeResolveScanMode(runtime scanWorkflowRuntime, raw string) string {
	if pc, ok := runtime.(*FactoryPipelineCoordinator); ok {
		return resolvePipelineScanMode(pc.SemanticSource(), raw)
	}
	return resolvePipelineScanMode(nil, raw)
}

func (pc *FactoryPipelineCoordinator) handleDiscoveryReport(ctx context.Context, evt events.Event) {
	if pc == nil || pc.scanCoordinator == nil {
		return
	}
	sc := pc.scanCoordinator
	payload := parsePayloadMap(evt.Payload)
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	if scanID == "" {
		runtimeWarn(
			"discovery-aggregator",
			"dropping discovery report missing scan_id event_id=%s type=%s source=%s",
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(string(evt.Type)),
			strings.TrimSpace(evt.SourceAgent),
		)
		return
	}

	sc.mu.Lock()
	acc := sc.scans[scanID]
	if acc == nil {
		acc = &scanAccumulator{
			ScanID:      scanID,
			Mode:        discoveryRuntimeResolveScanMode(sc.runtime, asString(payload["mode"])),
			Geography:   strings.TrimSpace(asString(payload["geography"])),
			Expected:    1,
			CompletedBy: make(map[string]struct{}),
			ReportData:  make([]map[string]any, 0),
			CreatedAt:   time.Now(),
		}
		if acc.Mode == "" {
			acc.Mode = discoveryRuntimeDefaultScanMode(sc.runtime)
		}
		sc.scans[scanID] = acc
	}
	acc.ReportData = append(acc.ReportData, cloneMap(payload))
	acc.Reports++
	sc.mu.Unlock()

	if payloadIndicatesSynthesisNeeded(payload) {
		sc.runtime.publish(ctx, "synthesis.needed", "", payloadMap(sc.payloadFactory.BuildSynthesisNeededPayload(scanID, acc, payload)))
		return
	}

	candidates := buildDiscoveryCandidatesForReport(acc.Mode, payload)
	for _, cand := range candidates {
			pc.processDiscoveryCandidate(ctx, evt, scanID, acc, cand)
	}
}

func (pc *FactoryPipelineCoordinator) processDiscoveryCandidate(
	ctx context.Context,
	evt events.Event,
	scanID string,
	acc *scanAccumulator,
	candidate discoveryCandidate,
) {
	if pc == nil || pc.scanCoordinator == nil || acc == nil {
		return
	}
	sc := pc.scanCoordinator
	signal := candidate.Signal
	allowed, adjustedSignal, reason := sc.discovery.EvaluateDiscoveryPreFilter(candidate.Payload, signal)
	if !allowed {
		sc.runtime.logPrefilterSkip(ctx, evt, scanID, acc.CampaignID, reason, candidate.Mode, candidate.Payload, signal, adjustedSignal)
		sc.mu.Lock()
		acc.Skipped++
		sc.mu.Unlock()
		return
	}
	signal = adjustedSignal
	candidate.Payload["signal_strength"] = adjustedSignal

	payload := candidate.Payload
	name := deriveDiscoveryCandidateName(payload)
	if name == "" {
		runtimeWarn(
			"discovery-aggregator",
			"skipping discovery candidate with missing name scan_id=%s event_id=%s source=%s mode=%s",
			scanID,
			strings.TrimSpace(evt.ID),
			strings.TrimSpace(evt.SourceAgent),
			candidate.Mode,
		)
		sc.mu.Lock()
		acc.Skipped++
		sc.mu.Unlock()
		return
	}

	geography := strings.TrimSpace(firstNonEmptyString(asString(payload["geography"]), acc.Geography))
	if geography == "" {
		geography = "unknown"
	}

	existing, err := sc.runtime.loadVerticalsByGeography(ctx, geography)
	if err != nil {
		log.Printf("pipeline: dedup lookup failed scan=%s geo=%s err=%v", scanID, geography, err)
		existing = nil
	}
	for _, v := range existing {
		if normalizeName(v.Name) == normalizeName(name) {
			sc.mu.Lock()
			acc.Skipped++
			sc.mu.Unlock()
			return
		}
	}

	if best, score := fuzzyBestMatch(name, existing); best.ID != "" && score >= 0.70 {
		dedupEventID := uuid.NewString()
		cand := pendingCandidate{
			DedupEventID: dedupEventID,
			ExistingID:   strings.TrimSpace(best.ID),
			ScanID:       scanID,
			CampaignID:   acc.CampaignID,
			Mode:         candidate.Mode,
			Geography:    geography,
			Name:         name,
			Signal:       signal,
			Payload:      payload,
		}
		sc.mu.Lock()
		sc.pendingDedup[dedupEventID] = cand
		sc.mu.Unlock()
		sc.runtime.publish(ctx, "dedup.ambiguous", "", payloadMap(sc.payloadFactory.BuildDedupAmbiguousPayload(scanID, dedupEventID, score, name, geography, signal, best.ID, best.Name)))
		return
	}

	verticalID, err := sc.runtime.ensureVerticalDiscovered(ctx, name, geography, candidate.Mode, payload)
	if err != nil {
		log.Printf("pipeline: ensure discovered vertical failed name=%s geo=%s mode=%s err=%v", name, geography, candidate.Mode, err)
		return
	}
	sc.mu.Lock()
	acc.Discovered++
	sc.mu.Unlock()
	discoveredPayload := payloadMap(sc.payloadFactory.BuildVerticalDiscoveredPayload(verticalID, name, geography, candidate.Mode, scanID, acc.CampaignID, signal, evt.SourceAgent, payload))
	sc.runtime.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}

func (pc *FactoryPipelineCoordinator) handleDedupResolved(ctx context.Context, evt events.Event) {
	if pc == nil || pc.scanCoordinator == nil {
		return
	}
	sc := pc.scanCoordinator
	payload := parsePayloadMap(evt.Payload)
	dedupEventID := strings.TrimSpace(asString(payload["dedup_event_id"]))
	if dedupEventID == "" {
		return
	}

	sc.mu.Lock()
	cand, ok := sc.pendingDedup[dedupEventID]
	if ok {
		delete(sc.pendingDedup, dedupEventID)
	}
	sc.mu.Unlock()
	if !ok {
		sc.ensureScanProjectionLoaded(ctx, strings.TrimSpace(asString(payload["scan_id"])))
		sc.mu.Lock()
		cand, ok = sc.pendingDedup[dedupEventID]
		if ok {
			delete(sc.pendingDedup, dedupEventID)
		}
		sc.mu.Unlock()
	}
	if !ok {
		return
	}

	action := strings.ToLower(strings.TrimSpace(asString(payload["action"])))
	if action == "merge" {
		sc.mu.Lock()
		if acc := sc.scans[cand.ScanID]; acc != nil {
			acc.Skipped++
		}
		sc.mu.Unlock()
		return
	}

	verticalID, err := sc.runtime.ensureVerticalDiscovered(ctx, cand.Name, cand.Geography, cand.Mode, cand.Payload)
	if err != nil {
		log.Printf("pipeline: dedup keep_both insert failed err=%v", err)
		return
	}
	sc.mu.Lock()
	if acc := sc.scans[cand.ScanID]; acc != nil {
		acc.Discovered++
	}
	sc.mu.Unlock()
	discoveredPayload := payloadMap(sc.payloadFactory.BuildVerticalDiscoveredPayload(verticalID, cand.Name, cand.Geography, cand.Mode, cand.ScanID, cand.CampaignID, cand.Signal, "discovery-aggregator", cand.Payload))
	sc.runtime.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}

func (pc *FactoryPipelineCoordinator) handleSynthesisResolved(ctx context.Context, evt events.Event) {
	if pc == nil || pc.scanCoordinator == nil || pc.scanCoordinator.runtime == nil {
		return
	}
	sc := pc.scanCoordinator
	payload := parsePayloadMap(evt.Payload)
	resolution := strings.ToLower(strings.TrimSpace(firstNonEmptyString(
		asString(payload["resolution"]),
		asString(payload["action"]),
		asString(payload["resolved_assessment"]),
	)))
	if resolution == "" {
		resolution = "conflict_resolved"
	}
	if resolution == "irreconcilable" || resolution == "discard" || resolution == "discard_both" {
		return
	}

	name := deriveDiscoveryCandidateName(payload)
	if name == "" {
		return
	}
	mode := discoveryRuntimeResolveScanMode(sc.runtime, asString(payload["mode"]))
	if mode == "" {
		mode = discoveryRuntimeDefaultScanMode(sc.runtime)
	}
	geography := strings.TrimSpace(asString(payload["geography"]))
	if geography == "" {
		geography = "unknown"
	}
	scanID := strings.TrimSpace(asString(payload["scan_id"]))
	campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
	signal := asFloat(payload["signal_strength"])

	verticalID, err := sc.runtime.ensureVerticalDiscovered(ctx, name, geography, mode, payload)
	if err != nil {
		log.Printf("pipeline: synthesized discovery insert failed name=%s geo=%s mode=%s err=%v", name, geography, mode, err)
		return
	}
	if scanID != "" {
		sc.ensureScanProjectionLoaded(ctx, scanID)
		sc.mu.Lock()
		if acc := sc.scans[scanID]; acc != nil {
			acc.Discovered++
			if campaignID == "" {
				campaignID = acc.CampaignID
			}
			if geography == "unknown" && strings.TrimSpace(acc.Geography) != "" {
				geography = strings.TrimSpace(acc.Geography)
			}
		}
		sc.mu.Unlock()
	}

	discoverySource := strings.TrimSpace(evt.SourceAgent)
	if discoverySource == "" {
		discoverySource = "discovery-aggregator"
	}
	discoveredPayload := payloadMap(sc.payloadFactory.BuildVerticalDiscoveredPayload(
		verticalID,
		name,
		geography,
		mode,
		scanID,
		campaignID,
		signal,
		discoverySource,
		payload,
	))
	sc.runtime.publish(ctx, "vertical.discovered", verticalID, discoveredPayload)
}
