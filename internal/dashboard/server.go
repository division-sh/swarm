package dashboard

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/digest"
	runtimebus "empireai/internal/runtime/bus"
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
)

//go:embed assets/*
var dashboardAssets embed.FS

var (
	dashboardPage   []byte
	dashboardStatic fs.FS
)

func init() {
	var err error
	dashboardPage, err = dashboardAssets.ReadFile("assets/dashboard.html")
	if err != nil {
		panic(fmt.Sprintf("load embedded dashboard.html: %v", err))
	}
	dashboardStatic, err = fs.Sub(dashboardAssets, "assets")
	if err != nil {
		panic(fmt.Sprintf("prepare embedded dashboard static fs: %v", err))
	}
}

type Server struct {
	db           *sql.DB
	cfg          *config.Config
	now          func() time.Time
	eventStore   runtimebus.EventStore
	mailboxStore runtimetools.MailboxPersistence
	manager      *runtimemanager.AgentManager
}

func NewServer(
	db *sql.DB,
	cfg *config.Config,
	eventStore runtimebus.EventStore,
	mailboxStore runtimetools.MailboxPersistence,
	manager *runtimemanager.AgentManager,
) *Server {
	return &Server{
		db:           db,
		cfg:          cfg,
		now:          time.Now,
		eventStore:   eventStore,
		mailboxStore: mailboxStore,
		manager:      manager,
	}
}

func (s *Server) handlePage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/dashboard" && r.URL.Path != "/dashboard/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("content-type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardPage)
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	resp := map[string]any{"generated_at": s.now().UTC()}

	var agentsTotal, agentsActive, events24h, pendingMailbox, verticalsTotal int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM agents`).Scan(&agentsTotal)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM agents WHERE status <> 'terminated'`).Scan(&agentsActive)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE created_at >= now() - interval '24 hours'`).Scan(&events24h)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM mailbox WHERE status = 'pending'`).Scan(&pendingMailbox)
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM verticals`).Scan(&verticalsTotal)

	resp["agents_total"] = agentsTotal
	resp["agents_active"] = agentsActive
	resp["events_24h"] = events24h
	resp["mailbox_pending"] = pendingMailbox
	resp["verticals_total"] = verticalsTotal
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleFunnel(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	stageCounts := map[string]int{}
	rows, err := s.db.QueryContext(ctx, `SELECT stage, count(*) FROM verticals GROUP BY stage`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for rows.Next() {
		var stage string
		var n int
		if err := rows.Scan(&stage, &n); err != nil {
			rows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		stageCounts[stage] = n
	}
	rows.Close()

	stuck := make([]map[string]any, 0, 32)
	rows, err = s.db.QueryContext(ctx, `
		SELECT id::text, name, COALESCE(slug, ''), stage, mode, COALESCE(created_at, now()), COALESCE(updated_at, now()),
			(extract(epoch from (now() - COALESCE(updated_at, now())) ) / 3600)::bigint AS idle_hours
		FROM verticals
		WHERE stage NOT IN ('operating', 'expanding', 'killed', 'winding_down')
		  AND updated_at < now() - interval '24 hours'
		ORDER BY updated_at ASC
		LIMIT 50
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for rows.Next() {
		var id, name, slug, stage, mode string
		var created, updated time.Time
		var idleHours int64
		if err := rows.Scan(&id, &name, &slug, &stage, &mode, &created, &updated, &idleHours); err != nil {
			rows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		stuck = append(stuck, map[string]any{
			"id":         id,
			"name":       name,
			"slug":       slug,
			"stage":      stage,
			"mode":       mode,
			"created_at": created,
			"updated_at": updated,
			"idle_hours": idleHours,
		})
	}
	rows.Close()

	byDay := make([]map[string]any, 0, 14)
	rows, err = s.db.QueryContext(ctx, `
		SELECT
			date(created_at) AS day,
			count(*) FILTER (WHERE mode = 'factory') AS discoveries,
			count(*) FILTER (WHERE stage NOT IN ('discovered', 'scoring')) AS progressed,
			count(*) FILTER (WHERE stage = 'killed') AS killed
		FROM verticals
		WHERE created_at >= now() - interval '14 days'
		GROUP BY 1
		ORDER BY 1 ASC
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	var totalDiscoveries, totalProgressed, totalKilled int
	for rows.Next() {
		var day time.Time
		var d, p, k int
		if err := rows.Scan(&day, &d, &p, &k); err != nil {
			rows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		totalDiscoveries += d
		totalProgressed += p
		totalKilled += k
		byDay = append(byDay, map[string]any{
			"day":         day.Format("2006-01-02"),
			"discoveries": d,
			"progressed":  p,
			"killed":      k,
		})
	}
	rows.Close()

	var approvedOrLive int
	_ = s.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM verticals
		WHERE stage IN ('approved', 'full_speccing', 'building', 'pre_launch', 'launched', 'operating', 'expanding')
	`).Scan(&approvedOrLive)
	var killedTotal int
	_ = s.db.QueryRowContext(ctx, `SELECT count(*) FROM verticals WHERE stage = 'killed'`).Scan(&killedTotal)

	scoringRate := 0.0
	if totalDiscoveries > 0 {
		scoringRate = float64(totalProgressed) / float64(totalDiscoveries)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"stage_counts": stageCounts,
		"stuck":        stuck,
		"throughput": map[string]any{
			"daily":                   byDay,
			"discoveries_14d":         totalDiscoveries,
			"progressed_14d":          totalProgressed,
			"killed_14d":              totalKilled,
			"scoring_completion_rate": scoringRate,
			"specs_approved_or_live":  approvedOrLive,
			"specs_killed_total":      killedTotal,
		},
	})
}

func (s *Server) handleDigest(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	if s.db == nil || s.mailboxStore == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("digest requires persistent store mode"))
		return
	}
	ctx := r.Context()
	topN := clamp(parseInt(r.URL.Query().Get("top"), 10), 1, 100)

	pg := &store.PostgresStore{DB: s.db}
	snap, err := digest.BuildSnapshot(ctx, pg, s.mailboxStore, topN)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	currentText := digest.RenderText(snap)

	// Best-effort: load most recent compiled digest event.
	var last map[string]any
	var lastAt sql.NullTime
	var lastPayloadRaw []byte
	if err := s.db.QueryRowContext(ctx, `
		SELECT created_at, COALESCE(payload, '{}'::jsonb)
		FROM events
		WHERE type = 'portfolio.digest_compiled'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&lastAt, &lastPayloadRaw); err == nil && lastAt.Valid {
		_ = json.Unmarshal(lastPayloadRaw, &last)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"current": map[string]any{
			"top_n": topN,
			"text":  currentText,
			"snap":  snap,
		},
		"last_compiled": map[string]any{
			"at":      lastAt.Time.UTC(),
			"payload": last,
		},
	})
}

func allowMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("allow", method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func decodeJSONBody(r *http.Request, out any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid json body: %w", err)
	}
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func parseInt(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func parseBoolQuery(raw string, fallback bool) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func isMissingRuntimeLogTable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") && strings.Contains(msg, "runtime_log")
}

type runtimeErrorMetadata struct {
	Code      string
	Component string
	Operation string
	Retryable *bool
}

func parseRuntimeErrorMetadata(raw string) runtimeErrorMetadata {
	text := strings.TrimSpace(raw)
	if text == "" || !strings.HasPrefix(text, "runtime_error") {
		return runtimeErrorMetadata{}
	}
	metadata := text
	if idx := strings.Index(metadata, ":"); idx >= 0 {
		metadata = strings.TrimSpace(metadata[:idx])
	}
	parts := strings.Fields(metadata)
	if len(parts) == 0 || parts[0] != "runtime_error" {
		return runtimeErrorMetadata{}
	}
	out := runtimeErrorMetadata{}
	for _, token := range parts[1:] {
		kv := strings.SplitN(token, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key == "" || val == "" {
			continue
		}
		switch key {
		case "code":
			out.Code = val
		case "component":
			out.Component = val
		case "operation":
			out.Operation = val
		case "retryable":
			if parsed, err := strconv.ParseBool(strings.ToLower(val)); err == nil {
				parsedBool := parsed
				out.Retryable = &parsedBool
			}
		}
	}
	return out
}

func classifyIncidentRootCause(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	switch code {
	case "mcp_context_token_missing", "mcp_context_token_not_found", "mcp_context_token_stale_epoch":
		return "mcp_context_lifecycle"
	case "mcp_auth_missing_bearer", "mcp_auth_invalid_bearer":
		return "mcp_gateway_auth"
	case "mcp_tool_not_allowed":
		return "mcp_tool_allowlist"
	case "mcp_tool_execution_failed":
		return "tool_execution_failure"
	case "mcp_stall_detected":
		return "agent_stall_detected"
	default:
		if strings.HasPrefix(code, "mcp_") {
			return "mcp_unknown"
		}
		return "runtime_unknown"
	}
}

func mapKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func truncate(v string, max int) string {
	if max <= 0 || len(v) <= max {
		return v
	}
	return v[:max] + "..."
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	case nil:
		return ""
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func asFloatAny(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case int32:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	default:
		return 0
	}
}

func boolFromAny(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(t)))
		return err == nil && parsed
	case int:
		return t != 0
	case int32:
		return t != 0
	case int64:
		return t != 0
	case float32:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}

func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
