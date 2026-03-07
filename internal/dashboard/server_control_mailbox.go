package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"empireai/internal/events"
	mailboxsvc "empireai/internal/mailbox"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
	"github.com/google/uuid"
)

func (s *Server) handleControlMailboxDecide(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.mailboxStore == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("mailbox store unavailable"))
		return
	}
	var req struct {
		MailboxID string `json:"mailbox_id"`
		Action    string `json:"action"`
		Notes     string `json:"notes"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.MailboxID = strings.TrimSpace(req.MailboxID)
	req.Action = strings.TrimSpace(req.Action)
	if req.MailboxID == "" || req.Action == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("mailbox_id and action are required"))
		return
	}

	item, err := s.mailboxStore.GetMailboxItem(r.Context(), req.MailboxID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	outcome, err := mailboxsvc.Decide(r.Context(), s.mailboxStore, req.MailboxID, req.Action, req.Notes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.emitMailboxDecisionSideEffects(r.Context(), item, outcome, req.Notes); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"id":       req.MailboxID,
		"status":   outcome.Status,
		"decision": outcome.Decision,
	})
}


func (s *Server) emitMailboxDecisionSideEffects(
	ctx context.Context,
	item runtimetools.MailboxItem,
	outcome mailboxsvc.DecisionOutcome,
	notes string,
) error {
	if s.eventStore == nil {
		return nil
	}
	basePayload := map[string]any{
		"mailbox_id": item.ID,
		"type":       item.Type,
		"status":     outcome.Status,
		"decision":   outcome.Decision,
		"notes":      notes,
		"context":    json.RawMessage(item.Context),
	}
	if err := s.appendTargetedEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.decision"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   s.now(),
	}, nil); err != nil {
		return err
	}
	if err := s.appendTargetedEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.item_decided"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   s.now(),
	}, nil); err != nil {
		return err
	}
	if outcome.Status == "more_data" && item.VerticalID != "" {
		if err := s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.needs_more_data"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   s.now(),
		}, []string{"empire-coordinator"}); err != nil {
			return err
		}
	}
	if item.Type == "vertical_approval" && item.VerticalID != "" {
		var evtType events.EventType
		switch outcome.Status {
		case "approved":
			evtType = events.EventType("vertical.approved")
		case "rejected":
			evtType = events.EventType("vertical.killed")
		}
		if evtType != "" {
			if err := s.appendTargetedEvent(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   s.now(),
			}, []string{"empire-coordinator"}); err != nil {
				return err
			}
		}
	}
	if item.Type == "spend_request" || item.Type == "budget_increase" || item.Type == "devops.capacity_warning" {
		var evtType events.EventType
		if outcome.Status == "approved" {
			evtType = events.EventType("spend.approved")
		}
		if outcome.Status == "rejected" {
			evtType = events.EventType("spend.rejected")
		}
		if evtType != "" {
			recipients := []string{}
			if item.VerticalID != "" {
				recipients = []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}
			} else if strings.TrimSpace(item.FromAgent) != "" {
				recipients = []string{strings.TrimSpace(item.FromAgent)}
			}
			if err := s.appendTargetedEvent(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   s.now(),
			}, recipients); err != nil {
				return err
			}
		}
	}
	if isFounderInputMailbox(item) && item.VerticalID != "" {
		if err := s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("founder_input.response"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   s.now(),
		}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
			return err
		}
	}

	// Spec v2.0 GAP 1: escalation responses are open-ended directives back to the OpCo CEO.
	if item.VerticalID != "" && strings.Contains(strings.ToLower(item.Type), "escalation") && outcome.Status == "approved" {
		directive := strings.TrimSpace(notes)
		if directive != "" {
			if err := s.appendTargetedEvent(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("opco.escalation_response"),
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload: mustJSON(map[string]any{
					"mailbox_id":     item.ID,
					"directive_text": directive,
					"action_items":   []any{},
					"context":        json.RawMessage(item.Context),
				}),
				CreatedAt: s.now(),
			}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
				return err
			}
		}
	}

	// Spec v2.0 §7.6: approved geography expansion recommendations must queue
	// a validation scan campaign.
	if outcome.Status == "approved" && isGeographyExpansionMailbox(item) {
		geoID, req, campaignID, err := s.queueGeographyExpansionValidation(ctx, item)
		if err != nil {
			return err
		}
		if err := s.appendTargetedEvent(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("geography.expansion_queued"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload: mustJSON(map[string]any{
				"mailbox_id":   item.ID,
				"vertical_id":  item.VerticalID,
				"geography_id": geoID,
				"geography":    req.Geography,
				"country":      req.Country,
				"region":       req.Region,
				"mode":         req.Mode,
				"categories":   req.Categories,
				"priority":     req.Priority,
				"campaign_id":  campaignID,
				"context":      json.RawMessage(item.Context),
			}),
			CreatedAt: s.now(),
		}, []string{"empire-coordinator"}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) queueGeographyExpansionValidation(
	ctx context.Context,
	item runtimetools.MailboxItem,
) (string, geographyExpansionRequest, string, error) {
	req := parseGeographyExpansionRequest(item.Context)
	if strings.TrimSpace(req.Geography) == "" {
		return "", req, "", fmt.Errorf("geography expansion requires context.geography")
	}
	if s.db == nil {
		return "", req, "", fmt.Errorf("geography expansion requires postgres db")
	}
	geoID, err := ensureGeographyRecord(ctx, s.db, req)
	if err != nil {
		return "", req, "", err
	}
	pg := &store.PostgresStore{DB: s.db}
	campaign, err := pg.CreateScanCampaign(ctx, runtimepipeline.CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        req.Mode,
		Categories:  req.Categories,
		Priority:    req.Priority,
		Status:      "queued",
	})
	if err != nil {
		return "", req, "", fmt.Errorf("queue geography expansion scan campaign: %w", err)
	}
	return geoID, req, campaign.ID, nil
}

func (s *Server) appendTargetedEvent(ctx context.Context, evt events.Event, recipients []string) error {
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = s.now()
	}
	if len(evt.Payload) == 0 {
		evt.Payload = []byte("{}")
	}
	if err := s.eventStore.AppendEvent(ctx, evt); err != nil {
		return err
	}
	if len(recipients) == 0 {
		return nil
	}
	recipients = s.filterExistingRecipients(ctx, recipients)
	if len(recipients) == 0 {
		return nil
	}
	return s.eventStore.InsertEventDeliveries(ctx, evt.ID, recipients)
}

func (s *Server) filterExistingRecipients(ctx context.Context, recipients []string) []string {
	if s.db == nil || len(recipients) == 0 {
		return recipients
	}
	exists := map[string]struct{}{}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM agents`)
	if err != nil {
		return recipients
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			exists[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(recipients))
	for _, id := range recipients {
		if _, ok := exists[id]; ok {
			out = append(out, id)
		}
	}
	return out
}
