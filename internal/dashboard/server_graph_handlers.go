package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type graphNode struct {
	ID            string         `json:"id"`
	Kind          string         `json:"kind"` // agent | event | system | human | mailbox
	Label         string         `json:"label"`
	Group         string         `json:"group"` // holding | template | opco
	Role          string         `json:"role,omitempty"`
	Mode          string         `json:"mode,omitempty"`
	Status        string         `json:"status,omitempty"`
	VerticalID    string         `json:"vertical_id,omitempty"`
	VerticalSlug  string         `json:"vertical_slug,omitempty"`
	ParentID      string         `json:"parent_id,omitempty"`
	SystemPrompt  string         `json:"system_prompt,omitempty"`
	Tools         []string       `json:"tools,omitempty"`
	Subscriptions []string       `json:"subscriptions,omitempty"`
	Constraints   map[string]any `json:"constraints,omitempty"`
}

type graphEdge struct {
	From              string   `json:"from"`
	To                string   `json:"to"`
	Kind              string   `json:"kind"`   // routing | management | subscription | producer | message | mailbox
	Label             string   `json:"label"`  // e.g. event_pattern or "manages"
	Status            string   `json:"status"` // active | proposed | deactivated
	Source            string   `json:"source"` // bootstrap | seeded | discovered | template
	Reason            string   `json:"reason,omitempty"`
	EventType         string   `json:"event_type,omitempty"`
	Stages            []string `json:"stages,omitempty"`
	Rubrics           []string `json:"rubrics,omitempty"`
	Producers         []string `json:"producers,omitempty"`
	Consumers         []string `json:"consumers,omitempty"`
	SchemaRequired    []string `json:"schema_required,omitempty"`
	SchemaProperties  []string `json:"schema_properties,omitempty"`
	InterceptorHandle string   `json:"interceptor_handler,omitempty"`
	Intercepted       bool     `json:"intercepted,omitempty"`
	Passthrough       bool     `json:"passthrough,omitempty"`
}

type flowEventView struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	SourceNode  string    `json:"source_node"`
	TargetNodes []string  `json:"target_nodes"`
	Intercepted bool      `json:"intercepted"`
	Passthrough bool      `json:"passthrough"`
	Timestamp   time.Time `json:"timestamp"`
	VerticalID  string    `json:"vertical_id,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
}

func (s *Server) handleGraph(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	mode := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = "holding"
	}

	switch mode {
	case "holding":
		nodes, edges, err := s.buildHoldingGraph(ctx)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at": s.now().UTC(),
			"mode":         mode,
			"nodes":        nodes,
			"edges":        edges,
		})
		return
	case "template":
		version := strings.TrimSpace(r.URL.Query().Get("version"))
		nodes, edges, ver, err := s.buildTemplateGraph(ctx, version)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at":     s.now().UTC(),
			"mode":             mode,
			"template_version": ver,
			"nodes":            nodes,
			"edges":            edges,
		})
		return
	case "opco":
		vertical := strings.TrimSpace(r.URL.Query().Get("vertical"))
		if vertical == "" {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("vertical is required"))
			return
		}
		nodes, edges, resolved, err := s.buildOpCoGraph(ctx, vertical)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at": s.now().UTC(),
			"mode":         mode,
			"vertical":     resolved,
			"nodes":        nodes,
			"edges":        edges,
		})
		return
	default:
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid mode: %s (expected holding|template|opco)", mode))
		return
	}
}

func (s *Server) handlePipelineGraph(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	view := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("view")))
	if view == "" {
		view = "design"
	}
	if view != "design" && view != "runtime" && view != "replay" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid view: %s (expected design|runtime|replay)", view))
		return
	}
	vertical := strings.TrimSpace(r.URL.Query().Get("vertical"))
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 250), 20, 2000)
	ctx := r.Context()

	nodes, edges, meta, err := s.buildPipelineDesignGraph(ctx, vertical)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	resp := map[string]any{
		"generated_at": s.now().UTC(),
		"view":         view,
		"vertical":     vertical,
		"nodes":        nodes,
		"edges":        edges,
		"meta":         meta,
	}

	if view == "runtime" || view == "replay" {
		start, end := parseFlowRange(r.URL.Query().Get("start"), r.URL.Query().Get("end"))
		if view == "runtime" && start.IsZero() {
			start = s.now().UTC().Add(-15 * time.Minute)
		}
		if view == "replay" && start.IsZero() {
			start = s.now().UTC().Add(-2 * time.Hour)
		}
		flows, qErr := s.queryFlowEvents(ctx, start, end, vertical, limit, true)
		if qErr != nil {
			writeErr(w, http.StatusInternalServerError, qErr)
			return
		}
		resp["flow_events"] = flows
		resp["flow_count"] = len(flows)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) buildPipelineDesignGraph(ctx context.Context, vertical string) ([]graphNode, []graphEdge, map[string]any, error) {
	return s.buildPipelineDesignGraphFromSources(ctx, vertical)
}

func (s *Server) handleFlowEvents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	vertical := strings.TrimSpace(r.URL.Query().Get("vertical"))
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 250), 1, 2000)
	start, end := parseFlowRange(r.URL.Query().Get("start"), r.URL.Query().Get("end"))
	stream := parseBoolQuery(r.URL.Query().Get("stream"), false)

	if !stream {
		items, err := s.queryFlowEvents(r.Context(), start, end, vertical, limit, false)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at": s.now().UTC(),
			"count":        len(items),
			"flow_events":  items,
		})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")

	since := start
	if since.IsZero() {
		since = s.now().UTC().Add(-30 * time.Second)
	}
	ctx := r.Context()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		items, err := s.queryFlowEvents(ctx, since, end, vertical, limit, true)
		if err == nil {
			for _, item := range items {
				raw, _ := json.Marshal(item)
				_, _ = fmt.Fprintf(w, "event: flow\ndata: %s\n\n", raw)
				if item.Timestamp.After(since) {
					since = item.Timestamp
				}
			}
		}
		flusher.Flush()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func parseFlowRange(startRaw, endRaw string) (time.Time, time.Time) {
	return parseFlowTime(startRaw), parseFlowTime(endRaw)
}

func parseFlowTime(raw string) time.Time {
	v := strings.TrimSpace(raw)
	if v == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC()
	}
	// datetime-local input from the dashboard (no timezone)
	if t, err := time.ParseInLocation("2006-01-02T15:04", v, time.Local); err == nil {
		return t.UTC()
	}
	if t, err := time.ParseInLocation("2006-01-02T15:04:05", v, time.Local); err == nil {
		return t.UTC()
	}
	return time.Time{}
}
