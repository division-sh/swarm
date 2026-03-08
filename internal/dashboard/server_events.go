package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type eventView struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	SourceAgent    string    `json:"source_agent"`
	TaskID         string    `json:"task_id"`
	VerticalID     string    `json:"vertical_id"`
	VerticalSlug   string    `json:"vertical_slug"`
	CreatedAt      time.Time `json:"created_at"`
	DeliveryCount  int       `json:"delivery_count"`
	ProcessedCount int       `json:"processed_count"`
	ErrorCount     int       `json:"error_count"`
	DeadCount      int       `json:"dead_count"`
	PendingCount   int       `json:"pending_count"`
	AvgProcessMS   int64     `json:"avg_processing_ms"`
}

type runtimeLogView struct {
	ID             int64     `json:"id"`
	TS             time.Time `json:"ts"`
	Level          string    `json:"level"`
	Component      string    `json:"component"`
	Action         string    `json:"action"`
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	AgentID        string    `json:"agent_id"`
	VerticalID     string    `json:"vertical_id"`
	CampaignID     string    `json:"campaign_id"`
	ScanID         string    `json:"scan_id"`
	SessionID      string    `json:"session_id"`
	Detail         any       `json:"detail"`
	Error          string    `json:"error"`
	ErrorCode      string    `json:"error_code,omitempty"`
	ErrorComponent string    `json:"error_component,omitempty"`
	ErrorOperation string    `json:"error_operation,omitempty"`
	ErrorRetryable *bool     `json:"error_retryable,omitempty"`
	DurationUS     int       `json:"duration_us"`
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 200), 1, 1000)
	filter := eventFilter{
		EventType:  strings.TrimSpace(r.URL.Query().Get("type")),
		Source:     strings.TrimSpace(r.URL.Query().Get("source")),
		Vertical:   strings.TrimSpace(r.URL.Query().Get("vertical")),
		Subscriber: strings.TrimSpace(r.URL.Query().Get("subscriber")),
	}

	items, err := s.queryEvents(ctx, filter, time.Time{}, limit, false)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"events":       items,
	})
}

func (s *Server) handleEventDetail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	prefix := "/dashboard/api/events/"
	if strings.HasPrefix(r.URL.Path, "/api/events/") {
		prefix = "/api/events/"
	}
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, prefix))
	if id == "" || id == "stream" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()

	var evt eventView
	var payloadRaw []byte
	if err := s.db.QueryRowContext(ctx, `
		SELECT
			e.id::text,
			e.type,
			e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(e.created_at, now()),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		WHERE e.id::text = $1
	`, id).Scan(
		&evt.ID,
		&evt.Type,
		&evt.SourceAgent,
		&evt.TaskID,
		&evt.VerticalID,
		&evt.VerticalSlug,
		&evt.CreatedAt,
		&payloadRaw,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	type deliveryView struct {
		AgentID        string     `json:"agent_id"`
		AgentRole      string     `json:"agent_role"`
		CreatedAt      time.Time  `json:"delivery_created_at"`
		Status         string     `json:"status"`
		RetryCount     int        `json:"retry_count"`
		Error          string     `json:"error,omitempty"`
		ErrorCode      string     `json:"error_code,omitempty"`
		ErrorComponent string     `json:"error_component,omitempty"`
		ErrorOperation string     `json:"error_operation,omitempty"`
		ErrorRetryable *bool      `json:"error_retryable,omitempty"`
		ProcessedAt    *time.Time `json:"processed_at,omitempty"`
		ProcessingMS   int64      `json:"processing_ms"`
	}

	deliveries := make([]deliveryView, 0, 16)
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			d.agent_id,
			COALESCE(a.role, ''),
			COALESCE(d.created_at, now()),
			COALESCE(r.status, 'pending'),
			COALESCE(r.retry_count, 0),
			COALESCE(r.error, ''),
			r.processed_at,
			COALESCE((extract(epoch from (r.processed_at - e.created_at)) * 1000)::bigint, 0)
		FROM event_deliveries d
		LEFT JOIN agents a ON a.id = d.agent_id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		JOIN events e ON e.id = d.event_id
		WHERE d.event_id::text = $1
		ORDER BY d.created_at ASC, d.agent_id ASC
	`, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var d deliveryView
		var processed sql.NullTime
		if err := rows.Scan(&d.AgentID, &d.AgentRole, &d.CreatedAt, &d.Status, &d.RetryCount, &d.Error, &processed, &d.ProcessingMS); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		if processed.Valid {
			d.ProcessedAt = &processed.Time
		}
		errMeta := parseRuntimeErrorMetadata(d.Error)
		d.ErrorCode = errMeta.Code
		d.ErrorComponent = errMeta.Component
		d.ErrorOperation = errMeta.Operation
		d.ErrorRetryable = errMeta.Retryable
		deliveries = append(deliveries, d)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	var payload any
	_ = json.Unmarshal(payloadRaw, &payload)
	writeJSON(w, http.StatusOK, map[string]any{
		"event":      evt,
		"payload":    payload,
		"deliveries": deliveries,
	})
}

func (s *Server) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
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

	filter := eventFilter{
		EventType:  strings.TrimSpace(r.URL.Query().Get("type")),
		Source:     strings.TrimSpace(r.URL.Query().Get("source")),
		Vertical:   strings.TrimSpace(r.URL.Query().Get("vertical")),
		Subscriber: strings.TrimSpace(r.URL.Query().Get("subscriber")),
		Component:  strings.TrimSpace(r.URL.Query().Get("component")),
		Level:      strings.TrimSpace(r.URL.Query().Get("level")),
	}
	includeRuntime := parseBoolQuery(r.URL.Query().Get("include_runtime"), true)
	since := s.now().Add(-30 * time.Second)
	if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			since = t
		}
	}

	ctx := r.Context()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		items, err := s.queryEvents(ctx, filter, since, 200, true)
		if err == nil {
			for _, item := range items {
				raw, _ := json.Marshal(item)
				_, _ = fmt.Fprintf(w, "event: event\ndata: %s\n\n", raw)
				if item.CreatedAt.After(since) {
					since = item.CreatedAt
				}
			}
		}
		if includeRuntime {
			logItems, logErr := s.queryRuntimeLogs(ctx, filter, since, 200, true)
			if logErr == nil {
				for _, item := range logItems {
					raw, _ := json.Marshal(item)
					_, _ = fmt.Fprintf(w, "event: runtime_log\ndata: %s\n\n", raw)
					if item.TS.After(since) {
						since = item.TS
					}
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

func (s *Server) handleRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	filter := eventFilter{
		EventType: strings.TrimSpace(r.URL.Query().Get("type")),
		Source:    strings.TrimSpace(r.URL.Query().Get("source")),
		Vertical:  strings.TrimSpace(r.URL.Query().Get("vertical")),
		Component: strings.TrimSpace(r.URL.Query().Get("component")),
		Level:     strings.TrimSpace(r.URL.Query().Get("level")),
		ErrorCode: strings.TrimSpace(r.URL.Query().Get("error_code")),
	}
	since := time.Time{}
	if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			since = t
		}
	}
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 100), 1, 500)
	asc := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("order")), "asc")
	items, err := s.queryRuntimeLogs(r.Context(), filter, since, limit, asc)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"count":        len(items),
		"runtime_logs": items,
	})
}

func (s *Server) handleRuntimeIncidents(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	hours := clamp(parseInt(r.URL.Query().Get("since_hours"), 24), 1, 24*14)
	limit := clamp(parseInt(r.URL.Query().Get("limit"), 1200), 100, 5000)
	mcpOnly := parseBoolQuery(r.URL.Query().Get("mcp_only"), true)
	level := strings.TrimSpace(r.URL.Query().Get("level"))
	if level == "" {
		level = "warn"
	}
	filter := eventFilter{
		Component: strings.TrimSpace(r.URL.Query().Get("component")),
		Level:     level,
	}
	since := s.now().UTC().Add(-time.Duration(hours) * time.Hour)
	logs, err := s.queryRuntimeLogs(r.Context(), filter, since, limit, false)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	type incidentAgg struct {
		Code       string
		Count      int
		FirstSeen  time.Time
		LastSeen   time.Time
		Agents     map[string]struct{}
		Components map[string]struct{}
		Actions    map[string]struct{}
		LastError  string
	}
	agg := make(map[string]*incidentAgg)
	for _, item := range logs {
		code := strings.TrimSpace(item.ErrorCode)
		if code == "" {
			continue
		}
		if mcpOnly && !strings.HasPrefix(code, "mcp_") {
			continue
		}
		entry, ok := agg[code]
		if !ok {
			entry = &incidentAgg{
				Code:       code,
				Count:      0,
				FirstSeen:  item.TS,
				LastSeen:   item.TS,
				Agents:     map[string]struct{}{},
				Components: map[string]struct{}{},
				Actions:    map[string]struct{}{},
				LastError:  item.Error,
			}
			agg[code] = entry
		}
		entry.Count++
		if item.TS.Before(entry.FirstSeen) {
			entry.FirstSeen = item.TS
		}
		if item.TS.After(entry.LastSeen) {
			entry.LastSeen = item.TS
			entry.LastError = item.Error
		}
		if v := strings.TrimSpace(item.AgentID); v != "" {
			entry.Agents[v] = struct{}{}
		}
		if v := strings.TrimSpace(item.Component); v != "" {
			entry.Components[v] = struct{}{}
		}
		if v := strings.TrimSpace(item.Action); v != "" {
			entry.Actions[v] = struct{}{}
		}
	}

	incidents := make([]map[string]any, 0, len(agg))
	for _, v := range agg {
		incidents = append(incidents, map[string]any{
			"code":        v.Code,
			"count":       v.Count,
			"first_seen":  v.FirstSeen.UTC(),
			"last_seen":   v.LastSeen.UTC(),
			"agents":      mapKeys(v.Agents),
			"components":  mapKeys(v.Components),
			"actions":     mapKeys(v.Actions),
			"last_error":  truncate(v.LastError, 500),
			"root_cause":  classifyIncidentRootCause(v.Code),
			"is_mcp_code": strings.HasPrefix(v.Code, "mcp_"),
		})
	}
	sort.SliceStable(incidents, func(i, j int) bool {
		ci := asInt(incidents[i]["count"])
		cj := asInt(incidents[j]["count"])
		if ci == cj {
			ti, _ := incidents[i]["last_seen"].(time.Time)
			tj, _ := incidents[j]["last_seen"].(time.Time)
			return tj.Before(ti)
		}
		return ci > cj
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"since_hours":  hours,
		"mcp_only":     mcpOnly,
		"count":        len(incidents),
		"incidents":    incidents,
	})
}

type eventFilter struct {
	EventType  string
	Source     string
	Vertical   string
	Subscriber string
	Component  string
	Level      string
	ErrorCode  string
}

func (s *Server) queryEvents(ctx context.Context, filter eventFilter, since time.Time, limit int, asc bool) ([]eventView, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clause = strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(args)))
		where = append(where, clause)
	}
	add2 := func(clause string, v1, v2 any) {
		args = append(args, v1, v2)
		first := "$" + strconv.Itoa(len(args)-1)
		second := "$" + strconv.Itoa(len(args))
		clause = strings.Replace(clause, "?", first, 1)
		clause = strings.Replace(clause, "?", second, 1)
		where = append(where, clause)
	}

	if !since.IsZero() {
		add("e.created_at > ?", since)
	}
	if filter.EventType != "" {
		if strings.HasSuffix(filter.EventType, "*") {
			add("e.type LIKE ?", strings.TrimSuffix(filter.EventType, "*")+"%")
		} else {
			add("e.type = ?", filter.EventType)
		}
	}
	if filter.Source != "" {
		add("e.source_agent = ?", filter.Source)
	}
	if filter.Vertical != "" {
		add2("(COALESCE(e.vertical_id::text, '') = ? OR COALESCE(v.slug, '') = ?)", filter.Vertical, filter.Vertical)
	}
	if filter.Subscriber != "" {
		add("EXISTS (SELECT 1 FROM event_deliveries d2 WHERE d2.event_id = e.id AND d2.agent_id = ?)", filter.Subscriber)
	}
	args = append(args, limit)
	order := "DESC"
	if asc {
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT
			e.id::text,
			e.type,
			e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			COALESCE(e.created_at, now()),
			count(d.agent_id) AS delivery_count,
			count(r.agent_id) FILTER (WHERE r.status = 'processed') AS processed_count,
			count(r.agent_id) FILTER (WHERE r.status = 'error') AS error_count,
			count(r.agent_id) FILTER (WHERE r.status = 'dead_letter') AS dead_count,
			(count(d.agent_id) - count(r.agent_id)) AS pending_count,
			COALESCE((avg(extract(epoch from (r.processed_at - e.created_at)) * 1000) FILTER (WHERE r.processed_at IS NOT NULL))::bigint, 0) AS avg_ms
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		LEFT JOIN event_deliveries d ON d.event_id = e.id
		LEFT JOIN event_receipts r ON r.event_id = d.event_id AND r.agent_id = d.agent_id
		WHERE %s
		GROUP BY e.id, v.slug
		ORDER BY e.created_at %s
		LIMIT $%d
	`, strings.Join(where, " AND "), order, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]eventView, 0, limit)
	for rows.Next() {
		var ev eventView
		if err := rows.Scan(
			&ev.ID,
			&ev.Type,
			&ev.SourceAgent,
			&ev.TaskID,
			&ev.VerticalID,
			&ev.VerticalSlug,
			&ev.CreatedAt,
			&ev.DeliveryCount,
			&ev.ProcessedCount,
			&ev.ErrorCount,
			&ev.DeadCount,
			&ev.PendingCount,
			&ev.AvgProcessMS,
		); err != nil {
			return nil, err
		}
		items = append(items, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) queryFlowEvents(ctx context.Context, since, until time.Time, vertical string, limit int, asc bool) ([]flowEventView, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clause = strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(args)))
		where = append(where, clause)
	}
	add2 := func(clause string, v1, v2 any) {
		args = append(args, v1, v2)
		first := "$" + strconv.Itoa(len(args)-1)
		second := "$" + strconv.Itoa(len(args))
		clause = strings.Replace(clause, "?", first, 1)
		clause = strings.Replace(clause, "?", second, 1)
		where = append(where, clause)
	}

	if !since.IsZero() {
		add("e.created_at > ?", since)
	}
	if !until.IsZero() {
		add("e.created_at <= ?", until)
	}
	if v := strings.TrimSpace(vertical); v != "" {
		add2("(COALESCE(e.vertical_id::text, '') = ? OR COALESCE(v.slug, '') = ?)", v, v)
	}

	args = append(args, limit)
	order := "DESC"
	if asc {
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT
			e.id::text,
			e.type,
			COALESCE(e.source_agent, ''),
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			COALESCE(e.created_at, now()),
			COALESCE(e.payload, '{}'::jsonb),
			COALESCE((
				SELECT jsonb_agg(d.agent_id ORDER BY d.agent_id)
				FROM event_deliveries d
				WHERE d.event_id = e.id
			), '[]'::jsonb)
		FROM events e
		LEFT JOIN verticals v ON v.id = e.vertical_id
		WHERE %s
		ORDER BY e.created_at %s
		LIMIT $%d
	`, strings.Join(where, " AND "), order, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]flowEventView, 0, limit)
	for rows.Next() {
		var (
			id, eventType, sourceAgent, taskID, verticalID string
			createdAt                                      time.Time
			payloadRaw                                     []byte
			targetsRaw                                     []byte
		)
		if err := rows.Scan(
			&id,
			&eventType,
			&sourceAgent,
			&taskID,
			&verticalID,
			&createdAt,
			&payloadRaw,
			&targetsRaw,
		); err != nil {
			return nil, err
		}
		targets := make([]string, 0, 8)
		if len(targetsRaw) > 0 && json.Valid(targetsRaw) {
			var rawTargets []any
			if err := json.Unmarshal(targetsRaw, &rawTargets); err == nil {
				for _, item := range rawTargets {
					v := strings.TrimSpace(asString(item))
					if v != "" {
						targets = append(targets, v)
					}
				}
			}
		}

		intercepted, passthrough := flowInterceptPolicy(eventType, payloadRaw)
		if intercepted && len(targets) == 0 {
			targets = append(targets, defaultFlowTargetNodes(eventType)...)
			if len(targets) == 0 {
				targets = append(targets, "pipeline-coordinator")
			}
		}
		sourceNode := strings.TrimSpace(sourceAgent)
		if sourceNode == "" {
			sourceNode = "runtime"
		}
		items = append(items, flowEventView{
			EventID:     id,
			EventType:   eventType,
			SourceNode:  sourceNode,
			TargetNodes: targets,
			Intercepted: intercepted,
			Passthrough: passthrough,
			Timestamp:   createdAt.UTC(),
			VerticalID:  strings.TrimSpace(verticalID),
			TaskID:      strings.TrimSpace(taskID),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) queryRuntimeLogs(ctx context.Context, filter eventFilter, since time.Time, limit int, asc bool) ([]runtimeLogView, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		clause = strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(args)))
		where = append(where, clause)
	}
	add2 := func(clause string, v1, v2 any) {
		args = append(args, v1, v2)
		first := "$" + strconv.Itoa(len(args)-1)
		second := "$" + strconv.Itoa(len(args))
		clause = strings.Replace(clause, "?", first, 1)
		clause = strings.Replace(clause, "?", second, 1)
		where = append(where, clause)
	}

	if !since.IsZero() {
		add("rl.ts > ?", since)
	}
	if filter.EventType != "" {
		if strings.HasSuffix(filter.EventType, "*") {
			add("COALESCE(rl.event_type, '') LIKE ?", strings.TrimSuffix(filter.EventType, "*")+"%")
		} else {
			add("COALESCE(rl.event_type, '') = ?", filter.EventType)
		}
	}
	if filter.Source != "" {
		add("COALESCE(rl.agent_id, '') = ?", filter.Source)
	}
	if filter.Vertical != "" {
		add2("(COALESCE(rl.vertical_id::text, '') = ? OR COALESCE(v.slug, '') = ?)", filter.Vertical, filter.Vertical)
	}
	if filter.Component != "" {
		add("COALESCE(rl.component, '') = ?", filter.Component)
	}
	if filter.Level != "" {
		add("COALESCE(rl.level, '') = ?", strings.ToLower(filter.Level))
	}
	if filter.ErrorCode != "" {
		add("COALESCE(rl.error, '') LIKE ?", "%code="+strings.TrimSpace(filter.ErrorCode)+"%")
	}
	args = append(args, limit)
	order := "DESC"
	if asc {
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT
			rl.id,
			rl.ts,
			COALESCE(rl.level, ''),
			COALESCE(rl.component, ''),
			COALESCE(rl.action, ''),
			COALESCE(rl.event_id::text, ''),
			COALESCE(rl.event_type, ''),
			COALESCE(rl.agent_id, ''),
			COALESCE(rl.vertical_id::text, ''),
			COALESCE(rl.campaign_id::text, ''),
			COALESCE(rl.scan_id::text, ''),
			COALESCE(rl.session_id::text, ''),
			COALESCE(rl.detail, '{}'::jsonb),
			COALESCE(rl.error, ''),
			COALESCE(rl.duration_us, 0)
		FROM runtime_log rl
		LEFT JOIN verticals v ON v.id = rl.vertical_id
		WHERE %s
		ORDER BY rl.ts %s
		LIMIT $%d
	`, strings.Join(where, " AND "), order, len(args))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		if isMissingRuntimeLogTable(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]runtimeLogView, 0, limit)
	for rows.Next() {
		var item runtimeLogView
		var detailRaw []byte
		if err := rows.Scan(
			&item.ID,
			&item.TS,
			&item.Level,
			&item.Component,
			&item.Action,
			&item.EventID,
			&item.EventType,
			&item.AgentID,
			&item.VerticalID,
			&item.CampaignID,
			&item.ScanID,
			&item.SessionID,
			&detailRaw,
			&item.Error,
			&item.DurationUS,
		); err != nil {
			return nil, err
		}
		var detail any
		_ = json.Unmarshal(detailRaw, &detail)
		item.Detail = detail
		errMeta := parseRuntimeErrorMetadata(item.Error)
		item.ErrorCode = errMeta.Code
		item.ErrorComponent = errMeta.Component
		item.ErrorOperation = errMeta.Operation
		item.ErrorRetryable = errMeta.Retryable
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
