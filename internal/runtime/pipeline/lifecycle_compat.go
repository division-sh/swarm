package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
)

func (pc *FactoryPipelineCoordinator) forwardSystemDirective(ctx context.Context, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	recipients := []string{"scan-campaign-manager"}
	pc.publishDirect(ctx, string(evt.Type), workflowEventEntityID(evt), parsePayloadMap(evt.Payload), recipients)
}

func (pc *FactoryPipelineCoordinator) handlePortfolioSystemDirective(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if _, ok := asObject(payload["directive"]); !ok {
		pc.forwardSystemDirective(ctx, evt)
		return
	}
	source := pc.SemanticSource()
	if source == nil {
		return
	}
	handler, ok := source.NodeEventHandler("portfolio-node", string(evt.Type))
	if !ok || len(handler.Rules) == 0 {
		return
	}
	triggerCtx := workflowTriggerContext{
		Event: evt,
		State: WorkflowState{},
	}
	match, ok := pc.matchWorkflowRulesWithVars(triggerCtx, handler.Rules, nil)
	if !ok {
		return
	}
	for _, emitEvent := range match.Emits {
		emitEvent = strings.TrimSpace(emitEvent)
		if emitEvent == "" {
			continue
		}
		verticalID, emitPayload := portfolioDirectiveEmitPayload(evt, emitEvent)
		pc.publish(ctx, emitEvent, verticalID, emitPayload)
	}
}

func portfolioDirectiveEmitPayload(evt events.Event, emitEvent string) (string, map[string]any) {
	payload := parsePayloadMap(evt.Payload)
	directive, _ := asObject(payload["directive"])
	params, _ := asObject(directive["parameters"])
	out := cloneStringAnyMap(params)
	if out == nil {
		out = map[string]any{}
	}
	directiveType := strings.TrimSpace(asString(directive["type"]))
	directiveID := strings.TrimSpace(evt.ID)
	switch strings.TrimSpace(emitEvent) {
	case "scan.requested":
		if strings.TrimSpace(asString(out["campaign_id"])) == "" {
			out["campaign_id"] = firstNonEmptyString(directiveID, evt.ID)
		}
		if _, ok := out["campaign_context"]; !ok {
			mode := strings.TrimSpace(asString(out["mode"]))
			modes := []string{}
			if mode != "" {
				modes = append(modes, mode)
			}
			out["campaign_context"] = map[string]any{
				"directive_id":      directiveID,
				"modes":             modes,
				"strategic_context": portfolioDirectiveContextString(directive),
			}
		}
		return "", out
	case "budget.adjustment_requested":
		return strings.TrimSpace(asString(out["vertical_id"])), out
	case "policy.change_requested":
		if strings.TrimSpace(asString(out["requested_by"])) == "" {
			out["requested_by"] = strings.TrimSpace(evt.SourceAgent)
		}
		return "", out
	case "vertical.resumed":
		verticalID := strings.TrimSpace(firstNonEmptyString(asString(out["vertical_id"]), workflowEventEntityID(evt)))
		if verticalID != "" && strings.TrimSpace(asString(out["vertical_id"])) == "" {
			out["vertical_id"] = verticalID
		}
		if strings.TrimSpace(asString(out["reason"])) == "" {
			out["reason"] = "system.directive"
		}
		return verticalID, out
	case "directive.unhandled":
		return "", map[string]any{
			"directive_text": portfolioDirectiveContextString(directive),
			"directive_type": directiveType,
			"reason":         "no_matching_rule",
		}
	default:
		return strings.TrimSpace(asString(out["vertical_id"])), out
	}
}

func portfolioDirectiveContextString(directive map[string]any) string {
	if len(directive) == 0 {
		return ""
	}
	if data, err := json.Marshal(directive); err == nil {
		return string(data)
	}
	return fmt.Sprintf("%v", directive)
}

func (pc *FactoryPipelineCoordinator) resetWorkflowRuntimeState(ctx context.Context) {
	if pc == nil {
		return
	}
	pc.resetInMemoryState()
	pc.clearPersistentState(ctx)
}

func (pc *FactoryPipelineCoordinator) forwardPortfolioDigestTimer(ctx context.Context, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	recipients := pc.bus.ResolveSubscribedRecipients("timer.portfolio_digest")
	if len(recipients) == 0 {
		return
	}
	raw := parsePayloadMap(evt.Payload)
	if boolFromAny(raw["scoring_rejections_injected"]) {
		return
	}
	pc.mu.Lock()
	since := pc.lastScoringDigestReadAt
	pc.mu.Unlock()
	entries, newest := pc.consumeScoringDigestEntries(ctx, 100, since)
	now := time.Now().UTC()
	if !newest.IsZero() {
		now = newest
	}
	pc.mu.Lock()
	pc.lastScoringDigestReadAt = now
	pc.mu.Unlock()

	snapshot, _ := raw["snapshot"].(map[string]any)
	metadata, _ := raw["metadata"].(map[string]any)
	payload := PortfolioDigestTimerPayload{
		Message:                   strings.TrimSpace(asString(raw["message"])),
		DigestText:                strings.TrimSpace(asString(raw["digest_text"])),
		TriggerReason:             strings.TrimSpace(asString(raw["trigger_reason"])),
		Snapshot:                  snapshot,
		Metadata:                  metadata,
		VerticalID:                strings.TrimSpace(asString(raw["vertical_id"])),
		TaskID:                    strings.TrimSpace(asString(raw["task_id"])),
		RecentRejections:          entries,
		RejectionCount:            len(entries),
		ScoringRejectionsInjected: true,
		ScoringRejectionsCount:    len(entries),
		ScoringRejectionSummaries: entries,
	}
	pc.publishDirect(ctx, "timer.portfolio_digest", workflowEventEntityID(evt), payloadMap(payload), recipients)
}

func (pc *FactoryPipelineCoordinator) applyMarginalKillTimer(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
	if verticalID == "" {
		return
	}
	if pc.currentWorkflowState(ctx, verticalID).Stage != NormalizeWorkflowStateID("marginal_review") {
		return
	}
	if strings.TrimSpace(asString(payload["reason"])) == "" {
		payload["reason"] = "marginal_kill_timer"
	}
	payload["timer_id"] = firstNonEmptyString(asString(payload["timer_id"]), "marginal_kill_timer")
	pc.publish(ctx, "vertical.killed", verticalID, payload)
}
