package pipeline

import (
	"context"
	"strings"
)

func validationDeclarativeHandlerSupported(pc *FactoryPipelineCoordinator, eventType string) bool {
	if pc == nil || pc.ContractBundle() == nil {
		return false
	}
	_, ok := pc.ContractBundle().NodeEventHandler("validation-orchestrator", strings.TrimSpace(eventType))
	return ok
}

func (pc *FactoryPipelineCoordinator) specVersionMatches(verticalID string, payload map[string]any) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.validationGate.getStateLocked(verticalID)
	if st.SpecVersion <= 0 {
		return true
	}
	got := asInt(payload["spec_version"])
	if got == 0 {
		return true
	}
	return got == st.SpecVersion
}

func (pc *FactoryPipelineCoordinator) parkVerticalWithMailbox(ctx context.Context, verticalID, summary string, details map[string]any) {
	if pc == nil || pc.db == nil {
		return
	}
	if details == nil {
		details = map[string]any{}
	}
	contextPayload := map[string]any{
		"vertical_id": verticalID,
		"source":      "validation-orchestrator",
		"details":     details,
	}
	_, _ = dbExecContext(ctx, pc.db, `
		INSERT INTO mailbox (event_id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES (NULL, NULLIF($1,'')::uuid, $2, 'vertical_approval', 'high', 'pending', $3::jsonb, $4, now())
	`, strings.TrimSpace(verticalID), "validation-orchestrator", string(mustJSON(contextPayload)), strings.TrimSpace(summary))
	pc.updateVerticalStage(ctx, verticalID, "ready_for_review", "")
}
