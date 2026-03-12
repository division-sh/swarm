package dashboard

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func (s *Server) handleControlDirective(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
		Message string `json:"message"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.Message = strings.TrimSpace(req.Message)
	if req.AgentID == "" || req.Message == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id and message are required"))
		return
	}
	target, err := s.lookupControlTarget(r.Context(), req.AgentID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var eventID string
	if strings.EqualFold(strings.TrimSpace(target.AgentID), "empire-coordinator") {
		started, serr := s.hasSystemStarted(r.Context())
		if serr != nil {
			writeErr(w, http.StatusInternalServerError, serr)
			return
		}
		if !started {
			writeErr(w, http.StatusConflict, fmt.Errorf("system is not initialized yet (missing system.started); run `empire init` first"))
			return
		}
		eventID, err = s.queueSystemDirective(r.Context(), req.Message, "dashboard")
	} else {
		eventID, err = s.queueBoardMessage(r.Context(), target, events.EventType("board.directive"), req.Message)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"event_id": eventID,
		"target":   target,
	})
}

func (s *Server) handleControlChat(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
		Message string `json:"message"`
		Mode    string `json:"mode"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.Message = strings.TrimSpace(req.Message)
	req.Mode = strings.TrimSpace(strings.ToLower(req.Mode))
	if req.AgentID == "" || req.Message == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id and message are required"))
		return
	}
	if req.Mode == "" {
		req.Mode = "live"
	}
	target, err := s.lookupControlTarget(r.Context(), req.AgentID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	eventID, err := s.queueBoardMessage(r.Context(), target, events.EventType("board.chat"), req.Message)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	resp := map[string]any{
		"ok":       true,
		"mode":     req.Mode,
		"event_id": eventID,
		"target":   target,
	}
	if req.Mode != "async" {
		if s.manager == nil {
			resp["warning"] = "manager unavailable; message queued async"
		} else {
			reply, chatErr := s.manager.ChatWithAgent(r.Context(), req.AgentID, req.Message)
			if chatErr != nil {
				resp["chat_error"] = chatErr.Error()
			} else {
				resp["response"] = strings.TrimSpace(reply)
				_ = s.upsertEventReceipt(r.Context(), eventID, req.AgentID, "processed", "")
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) upsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error {
	if s.db == nil {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	status = strings.TrimSpace(status)
	if eventID == "" || agentID == "" || status == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, $2, now(), $3, 0, NULLIF($4,''))
		ON CONFLICT (event_id, agent_id) DO UPDATE
			SET processed_at = now(),
				status = EXCLUDED.status,
				error = EXCLUDED.error
	`, eventID, agentID, status, strings.TrimSpace(errText))
	return err
}

func (s *Server) lookupControlTarget(ctx context.Context, agentID string) (controlTarget, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return controlTarget{}, fmt.Errorf("agent_id is required")
	}
	var t controlTarget
	err := s.db.QueryRowContext(ctx, `
		SELECT
			a.id,
			COALESCE(a.role, ''),
			COALESCE(a.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(a.status, '')
		FROM agents a
		LEFT JOIN verticals v ON v.id = a.vertical_id
		WHERE a.id = $1
	`, agentID).Scan(&t.AgentID, &t.Role, &t.VerticalID, &t.VerticalSlug, &t.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return controlTarget{}, fmt.Errorf("agent not found: %s", agentID)
		}
		return controlTarget{}, err
	}
	if strings.EqualFold(t.Status, "terminated") {
		return controlTarget{}, fmt.Errorf("agent is terminated: %s", agentID)
	}
	return t, nil
}

func (s *Server) queueBoardMessage(ctx context.Context, target controlTarget, eventType events.EventType, message string) (string, error) {
	if s.eventStore == nil {
		return "", fmt.Errorf("event store unavailable")
	}
	payload := map[string]any{
		"target_agent_id": target.AgentID,
		"role":            target.Role,
		"vertical_id":     target.VerticalID,
		"vertical_key":    target.VerticalSlug,
		"message":         strings.TrimSpace(message),
		"sent_by":         "dashboard",
		"sent_at":         s.now().UTC().Format(time.RFC3339),
	}
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "dashboard",
		Payload:     mustJSON(payload),
		CreatedAt:   s.now(),
	}).WithEntityID(target.VerticalID)
	if err := s.eventStore.AppendEvent(ctx, evt); err != nil {
		return "", err
	}
	if err := s.eventStore.InsertEventDeliveries(ctx, evt.ID, []string{target.AgentID}); err != nil {
		return "", err
	}
	return evt.ID, nil
}

func (s *Server) queueSystemDirective(ctx context.Context, message, sentBy string) (string, error) {
	if s.eventStore == nil {
		return "", fmt.Errorf("event store unavailable")
	}
	if s.manager == nil {
		return "", fmt.Errorf("runtime manager unavailable")
	}
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "", fmt.Errorf("directive_text is required")
	}
	payload := mustJSON(map[string]any{
		"directive_text": msg,
		"timestamp":      s.now().UTC().Format(time.RFC3339),
		"sent_by":        strings.TrimSpace(coalesce(sentBy, "human")),
	})
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     payload,
		CreatedAt:   s.now(),
	}
	if err := s.manager.PublishEvent(ctx, evt); err != nil {
		return "", err
	}
	return evt.ID, nil
}

func (s *Server) hasSystemStarted(ctx context.Context) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database unavailable")
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE type = 'system.started')`).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
