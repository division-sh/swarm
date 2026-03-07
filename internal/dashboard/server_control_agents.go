package dashboard

import (
	"fmt"
	"net/http"
	"strings"
)

func (s *Server) handleControlAgentRestart(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.AgentID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id is required"))
		return
	}
	if err := s.manager.RestartAgent(req.AgentID); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "agent_id": req.AgentID, "action": "restart"})
}

func (s *Server) handleControlAgentReplay(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	if s.manager == nil {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("agent manager unavailable"))
		return
	}
	var req struct {
		AgentID string `json:"agent_id"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.AgentID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("agent_id is required"))
		return
	}
	if err := s.manager.ReplayAgentBacklog(r.Context(), req.AgentID); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "agent_id": req.AgentID, "action": "replay"})
}

func (s *Server) handleControlEventRequeue(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		EventID string `json:"event_id"`
		AgentID string `json:"agent_id"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.EventID == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("event_id is required"))
		return
	}
	if req.AgentID == "" {
		rows, err := s.db.QueryContext(r.Context(), `SELECT agent_id FROM event_deliveries WHERE event_id = $1::uuid`, req.EventID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		defer rows.Close()
		recipients := make([]string, 0, 16)
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				recipients = append(recipients, id)
			}
		}
		if len(recipients) == 0 {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("no deliveries found for event_id %s", req.EventID))
			return
		}
		if _, err := s.db.ExecContext(r.Context(), `DELETE FROM event_receipts WHERE event_id = $1::uuid`, req.EventID); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if s.manager != nil {
			for _, id := range recipients {
				_ = s.manager.ReplayAgentBacklog(r.Context(), id)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":        true,
			"event_id":  req.EventID,
			"agent_ids": recipients,
			"requeued":  len(recipients),
			"action":    "requeue_event_all",
		})
		return
	}

	if _, err := s.db.ExecContext(r.Context(), `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`, req.EventID, req.AgentID); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if _, err := s.db.ExecContext(r.Context(), `
		DELETE FROM event_receipts WHERE event_id = $1::uuid AND agent_id = $2
	`, req.EventID, req.AgentID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.manager != nil {
		_ = s.manager.ReplayAgentBacklog(r.Context(), req.AgentID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"event_id": req.EventID,
		"agent_id": req.AgentID,
		"action":   "requeue_event_single",
	})
}
