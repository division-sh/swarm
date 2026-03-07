package dashboard

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

func (s *Server) handleHolding(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	// Q1 — Campaigns
	campaigns := make([]map[string]any, 0, 50)
	campRows, err := s.db.QueryContext(ctx, `
		SELECT sc.id::text, sc.mode, g.name, g.country, sc.status, sc.priority,
		       COALESCE(sc.discoveries,0), COALESCE(array_to_string(sc.categories,','), ''),
		       sc.created_at, sc.started_at, sc.completed_at
		FROM scan_campaigns sc JOIN geographies g ON g.id = sc.geography_id
		ORDER BY CASE sc.status WHEN 'active' THEN 0 WHEN 'queued' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END,
		         sc.created_at DESC
		LIMIT 50
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for campRows.Next() {
		var id, mode, geoName, country, status, priority, catStr string
		var discoveries int
		var createdAt time.Time
		var startedAt, completedAt sql.NullTime
		if err := campRows.Scan(&id, &mode, &geoName, &country, &status, &priority,
			&discoveries, &catStr, &createdAt, &startedAt, &completedAt); err != nil {
			campRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		var cats []string
		if catStr != "" {
			cats = strings.Split(catStr, ",")
		}
		c := map[string]any{
			"id":          strings.TrimSpace(id),
			"mode":        mode,
			"geography":   geoName,
			"country":     country,
			"status":      status,
			"priority":    priority,
			"discoveries": discoveries,
			"categories":  cats,
			"created_at":  createdAt,
		}
		if startedAt.Valid {
			c["started_at"] = startedAt.Time
		}
		if completedAt.Valid {
			c["completed_at"] = completedAt.Time
		}
		campaigns = append(campaigns, c)
	}
	campRows.Close()

	// Q2 — Verticals with scores + kill info
	verts := make([]map[string]any, 0, 500)
	vertRows, err := s.db.QueryContext(ctx, `
		SELECT v.id::text, COALESCE(v.slug,''), v.name, COALESCE(v.geography,''),
		       v.stage, COALESCE(v.mode,'factory'),
		       COALESCE((v.scores->>'composite_score')::text,''),
		       COALESCE(v.kill_reason,''), COALESCE(v.killed_at_stage,''),
		       COALESCE(v.created_at, now()), COALESCE(v.updated_at, now()), v.approved_at, v.parked_at, v.launched_at
		FROM verticals v ORDER BY v.updated_at DESC LIMIT 500
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for vertRows.Next() {
		var id, slug, name, geo, stage, mode, composite, killReason, killedAtStage string
		var createdAt, updatedAt time.Time
		var approvedAt, parkedAt, launchedAt sql.NullTime
		if err := vertRows.Scan(&id, &slug, &name, &geo, &stage, &mode,
			&composite, &killReason, &killedAtStage,
			&createdAt, &updatedAt, &approvedAt, &parkedAt, &launchedAt); err != nil {
			vertRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		v := map[string]any{
			"id":              strings.TrimSpace(id),
			"slug":            slug,
			"name":            name,
			"geography":       geo,
			"stage":           stage,
			"mode":            mode,
			"composite_score": composite,
			"kill_reason":     killReason,
			"killed_at_stage": killedAtStage,
			"created_at":      createdAt,
			"updated_at":      updatedAt,
		}
		if approvedAt.Valid {
			v["approved_at"] = approvedAt.Time
		}
		if parkedAt.Valid {
			v["parked_at"] = parkedAt.Time
		}
		if launchedAt.Valid {
			v["launched_at"] = launchedAt.Time
		}
		verts = append(verts, v)
	}
	vertRows.Close()

	// Q3 — Active agent counts per vertical
	agentCounts := map[string]map[string]int{}
	acRows, err := s.db.QueryContext(ctx, `
		SELECT vertical_id::text, COUNT(*),
		       COUNT(*) FILTER (WHERE status IN ('working','running','busy'))
		FROM agents WHERE vertical_id IS NOT NULL GROUP BY vertical_id
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for acRows.Next() {
		var vid string
		var total, active int
		if err := acRows.Scan(&vid, &total, &active); err != nil {
			acRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		agentCounts[strings.TrimSpace(vid)] = map[string]int{"total": total, "active": active}
	}
	acRows.Close()

	// Q4 — Summary counts
	var sTotal, sInPipeline, sKilled, sDiscovered int
	_ = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE stage NOT IN ('killed','winding_down')),
		       COUNT(*) FILTER (WHERE stage = 'killed'),
		       COUNT(*) FILTER (WHERE stage = 'discovered')
		FROM verticals
	`).Scan(&sTotal, &sInPipeline, &sKilled, &sDiscovered)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"campaigns":    campaigns,
		"verticals":    verts,
		"agent_counts": agentCounts,
		"summary": map[string]int{
			"total":       sTotal,
			"in_pipeline": sInPipeline,
			"killed":      sKilled,
			"discovered":  sDiscovered,
		},
	})
}

func (s *Server) handleHoldingVerticalDetail(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()
	target := strings.TrimSpace(r.URL.Query().Get("id"))
	if target == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("id is required"))
		return
	}

	parseJSONDoc := func(raw []byte) any {
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" || trimmed == "null" || trimmed == "{}" || trimmed == "[]" {
			return nil
		}
		var out any
		if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
			return trimmed
		}
		return out
	}

	var (
		verticalID      string
		slug            string
		name            string
		geography       string
		stage           string
		mode            string
		templateVersion string
		liveURL         string
		humanNotes      string
		killReason      string
		killedAtStage   string
		compositeScore  string
		rawSignalsRaw   []byte
		scoresRaw       []byte
		businessBrief   []byte
		mvpSpec         []byte
		specReview      []byte
		ctoFeasibility  []byte
		brandRaw        []byte
		validationKit   []byte
		fullSpec        []byte
		deployConfig    []byte
		launchTargets   []byte
		credsRaw        []byte
		createdAt       time.Time
		updatedAt       time.Time
		approvedAt      sql.NullTime
		parkedAt        sql.NullTime
		launchedAt      sql.NullTime
	)

	if err := s.db.QueryRowContext(ctx, `
		SELECT
			v.id::text,
			COALESCE(v.slug,''),
			COALESCE(v.name,''),
			COALESCE(v.geography,''),
			COALESCE(v.stage,''),
			COALESCE(v.mode,'factory'),
			COALESCE(v.template_version,''),
			COALESCE(v.live_url,''),
			COALESCE(v.human_notes,''),
			COALESCE(v.kill_reason,''),
			COALESCE(v.killed_at_stage,''),
			COALESCE((v.scores->>'composite_score')::text,''),
			COALESCE(v.raw_signals,'{}'::jsonb),
			COALESCE(v.scores,'{}'::jsonb),
			COALESCE(v.business_brief,'{}'::jsonb),
			COALESCE(v.mvp_spec,'{}'::jsonb),
			COALESCE(v.spec_review,'{}'::jsonb),
			COALESCE(v.cto_feasibility,'{}'::jsonb),
			COALESCE(v.brand,'{}'::jsonb),
			COALESCE(v.validation_kit,'{}'::jsonb),
			COALESCE(v.full_spec,'{}'::jsonb),
			COALESCE(v.deploy_config,'{}'::jsonb),
			COALESCE(v.launch_targets,'{}'::jsonb),
			COALESCE(v.credentials,'{}'::jsonb),
			COALESCE(v.created_at, now()),
			COALESCE(v.updated_at, now()),
			v.approved_at,
			v.parked_at,
			v.launched_at
		FROM verticals v
		WHERE v.id::text = $1 OR COALESCE(v.slug,'') = $1
		LIMIT 1
	`, target).Scan(
		&verticalID,
		&slug,
		&name,
		&geography,
		&stage,
		&mode,
		&templateVersion,
		&liveURL,
		&humanNotes,
		&killReason,
		&killedAtStage,
		&compositeScore,
		&rawSignalsRaw,
		&scoresRaw,
		&businessBrief,
		&mvpSpec,
		&specReview,
		&ctoFeasibility,
		&brandRaw,
		&validationKit,
		&fullSpec,
		&deployConfig,
		&launchTargets,
		&credsRaw,
		&createdAt,
		&updatedAt,
		&approvedAt,
		&parkedAt,
		&launchedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, fmt.Errorf("vertical not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	vertical := map[string]any{
		"id":               strings.TrimSpace(verticalID),
		"slug":             slug,
		"name":             name,
		"geography":        geography,
		"stage":            stage,
		"mode":             mode,
		"template_version": templateVersion,
		"live_url":         liveURL,
		"human_notes":      humanNotes,
		"kill_reason":      killReason,
		"killed_at_stage":  killedAtStage,
		"composite_score":  compositeScore,
		"raw_signals":      parseJSONDoc(rawSignalsRaw),
		"scores":           parseJSONDoc(scoresRaw),
		"business_brief":   parseJSONDoc(businessBrief),
		"mvp_spec":         parseJSONDoc(mvpSpec),
		"spec_review":      parseJSONDoc(specReview),
		"cto_feasibility":  parseJSONDoc(ctoFeasibility),
		"brand":            parseJSONDoc(brandRaw),
		"validation_kit":   parseJSONDoc(validationKit),
		"full_spec":        parseJSONDoc(fullSpec),
		"deploy_config":    parseJSONDoc(deployConfig),
		"launch_targets":   parseJSONDoc(launchTargets),
		"credentials":      parseJSONDoc(credsRaw),
		"created_at":       createdAt,
		"updated_at":       updatedAt,
	}
	if approvedAt.Valid {
		vertical["approved_at"] = approvedAt.Time
	}
	if parkedAt.Valid {
		vertical["parked_at"] = parkedAt.Time
	}
	if launchedAt.Valid {
		vertical["launched_at"] = launchedAt.Time
	}

	agents := make([]map[string]any, 0, 24)
	agentRows, err := s.db.QueryContext(ctx, `
		SELECT
			id,
			COALESCE(role,''),
			COALESCE(mode,''),
			COALESCE(status,''),
			COALESCE(current_task_id::text,''),
			last_active_at
		FROM agents
		WHERE COALESCE(vertical_id::text,'') = $1
		ORDER BY role ASC, id ASC
	`, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for agentRows.Next() {
		var id, role, agentMode, status, taskID string
		var lastActive sql.NullTime
		if err := agentRows.Scan(&id, &role, &agentMode, &status, &taskID, &lastActive); err != nil {
			agentRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		item := map[string]any{
			"id":              id,
			"role":            role,
			"mode":            agentMode,
			"status":          status,
			"current_task_id": taskID,
		}
		if lastActive.Valid {
			item["last_active_at"] = lastActive.Time
		}
		agents = append(agents, item)
	}
	agentRows.Close()

	recentEvents := make([]map[string]any, 0, 40)
	eventRows, err := s.db.QueryContext(ctx, `
		SELECT id::text, type, source_agent, payload, created_at
		FROM events
		WHERE COALESCE(vertical_id::text,'') = $1
		ORDER BY created_at DESC
		LIMIT 40
	`, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for eventRows.Next() {
		var eventID, typ, source string
		var payloadRaw []byte
		var created time.Time
		if err := eventRows.Scan(&eventID, &typ, &source, &payloadRaw, &created); err != nil {
			eventRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		recentEvents = append(recentEvents, map[string]any{
			"id":           eventID,
			"type":         typ,
			"source_agent": source,
			"payload":      parseJSONDoc(payloadRaw),
			"created_at":   created,
		})
	}
	eventRows.Close()
	enrichHoldingVerticalArtifacts(vertical, recentEvents)

	mailboxItems := make([]map[string]any, 0, 25)
	mailRows, err := s.db.QueryContext(ctx, `
		SELECT
			id::text,
			COALESCE(from_agent,''),
			COALESCE(type,''),
			COALESCE(priority,''),
			COALESCE(status,''),
			COALESCE(summary,''),
			COALESCE(decision,''),
			COALESCE(decision_notes,''),
			COALESCE(context,'{}'::jsonb),
			created_at,
			decided_at
		FROM mailbox
		WHERE COALESCE(vertical_id::text,'') = $1
		ORDER BY created_at DESC
		LIMIT 25
	`, verticalID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	for mailRows.Next() {
		var id, from, typ, priority, status, summary, decision, decisionNotes string
		var ctxRaw []byte
		var created time.Time
		var decided sql.NullTime
		if err := mailRows.Scan(&id, &from, &typ, &priority, &status, &summary, &decision, &decisionNotes, &ctxRaw, &created, &decided); err != nil {
			mailRows.Close()
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		item := map[string]any{
			"id":             id,
			"from_agent":     from,
			"type":           typ,
			"priority":       priority,
			"status":         status,
			"summary":        summary,
			"decision":       decision,
			"decision_notes": decisionNotes,
			"context":        parseJSONDoc(ctxRaw),
			"created_at":     created,
		}
		if decided.Valid {
			item["decided_at"] = decided.Time
		}
		mailboxItems = append(mailboxItems, item)
	}
	mailRows.Close()

	var spendAll, spendLast30 int64
	_ = s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(amount_cents),0),
			COALESCE(SUM(amount_cents) FILTER (WHERE created_at >= now() - interval '30 days'),0)
		FROM spend_ledger
		WHERE COALESCE(vertical_id::text,'') = $1
	`, verticalID).Scan(&spendAll, &spendLast30)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": s.now().UTC(),
		"vertical":     vertical,
		"agents":       agents,
		"events":       recentEvents,
		"mailbox":      mailboxItems,
		"spend": map[string]any{
			"all_time_cents": spendAll,
			"last_30d_cents": spendLast30,
		},
	})
}

func enrichHoldingVerticalArtifacts(vertical map[string]any, recentEvents []map[string]any) {
	if len(vertical) == 0 || len(recentEvents) == 0 {
		return
	}
	setFromPayload := func(key string, value any) {
		if holdingArtifactEmpty(value) {
			return
		}
		if holdingArtifactEmpty(vertical[key]) {
			vertical[key] = value
			return
		}
		dst, dstOK := vertical[key].(map[string]any)
		src, srcOK := value.(map[string]any)
		if dstOK && srcOK {
			vertical[key] = holdingMergeMapMissing(dst, src)
			return
		}
		if holdingArtifactEmpty(vertical[key]) {
			vertical[key] = value
		}
	}
	for _, evt := range recentEvents {
		typ := strings.TrimSpace(asString(evt["type"]))
		payload, _ := evt["payload"].(map[string]any)
		if len(payload) == 0 {
			continue
		}
		switch typ {
		case "vertical.discovered":
			setFromPayload("raw_signals", payload)
		case "vertical.scored":
			setFromPayload("scores", holdingPickMap(payload, "scores", "scoring", "result"))
			setFromPayload("scores", payload)
		case "research.completed":
			setFromPayload("business_brief", holdingPickNestedMap(payload, []string{"business_brief"}, []string{"brief"}, []string{"research", "business_brief"}, []string{"research"}))
		case "spec.draft_ready":
			setFromPayload("mvp_spec", holdingPickNestedMap(payload, []string{"mvp_spec"}, []string{"spec", "mvp_spec"}, []string{"spec"}, []string{"draft"}))
			setFromPayload("mvp_spec", payload)
		case "spec.approved":
			setFromPayload("full_spec", holdingPickNestedMap(payload, []string{"full_spec"}, []string{"spec"}))
			setFromPayload("mvp_spec", holdingPickNestedMap(payload, []string{"mvp_spec"}, []string{"spec", "mvp_spec"}, []string{"spec"}))
		case "spec_review.requested", "spec_review.passed", "spec_review.issues_found":
			setFromPayload("spec_review", payload)
		case "cto.spec_review_requested", "cto.spec_approved", "cto.spec_revision_needed":
			setFromPayload("cto_feasibility", holdingPickNestedMap(payload, []string{"cto_feasibility"}, []string{"cto_notes"}, []string{"feasibility"}))
			setFromPayload("cto_feasibility", payload)
		case "brand.requested", "brand.candidates_ready", "brand.revision_needed":
			setFromPayload("brand", holdingPickMap(payload, "brand"))
			setFromPayload("brand", payload)
		case "validation.package_ready", "vertical.ready_for_review":
			setFromPayload("validation_kit", payload)
			setFromPayload("business_brief", holdingPickNestedMap(payload, []string{"business_brief"}, []string{"research", "business_brief"}, []string{"research"}))
			setFromPayload("mvp_spec", holdingPickNestedMap(payload, []string{"mvp_spec"}, []string{"spec", "mvp_spec"}, []string{"spec"}))
			setFromPayload("full_spec", holdingPickNestedMap(payload, []string{"full_spec"}, []string{"spec"}))
			setFromPayload("cto_feasibility", holdingPickNestedMap(payload, []string{"cto_feasibility"}, []string{"cto_notes"}))
			setFromPayload("brand", holdingPickMap(payload, "brand"))
		}
	}
}

func holdingPickMap(payload map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		raw, ok := payload[key]
		if !ok || raw == nil {
			continue
		}
		if m, ok := raw.(map[string]any); ok && len(m) > 0 {
			return m
		}
	}
	return nil
}

func holdingPickNestedMap(payload map[string]any, paths ...[]string) map[string]any {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		var cursor any = payload
		ok := true
		for _, key := range path {
			m, isMap := cursor.(map[string]any)
			if !isMap {
				ok = false
				break
			}
			next, exists := m[key]
			if !exists || next == nil {
				ok = false
				break
			}
			cursor = next
		}
		if !ok {
			continue
		}
		if out, isMap := cursor.(map[string]any); isMap && len(out) > 0 {
			return out
		}
	}
	return nil
}

func holdingArtifactEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		s := strings.TrimSpace(t)
		return s == "" || s == "{}" || s == "[]"
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

func holdingMergeMapMissing(dst map[string]any, src map[string]any) map[string]any {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]any{}
	}
	for key, srcVal := range src {
		cur, exists := dst[key]
		if !exists || holdingArtifactEmpty(cur) {
			dst[key] = srcVal
			continue
		}
		curMap, curOK := cur.(map[string]any)
		srcMap, srcOK := srcVal.(map[string]any)
		if curOK && srcOK {
			dst[key] = holdingMergeMapMissing(curMap, srcMap)
		}
	}
	return dst
}

func compactAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func dockerContainers(parent context.Context) ([]map[string]string, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "ps", "--format", "{{.Names}}|{{.Image}}|{{.Status}}")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	containers := make([]map[string]string, 0, 16)
	s := bufio.NewScanner(strings.NewReader(string(out)))
	s.Buffer(make([]byte, 1024), 1024*1024)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 3)
		if len(parts) != 3 {
			continue
		}
		containers = append(containers, map[string]string{
			"name":   strings.TrimSpace(parts[0]),
			"image":  strings.TrimSpace(parts[1]),
			"status": strings.TrimSpace(parts[2]),
		})
	}
	if err := s.Err(); err != nil {
		return containers, err
	}
	return containers, nil
}

func (s *Server) handleVerticalTrace(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, http.MethodGet) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/dashboard/api/verticals/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "trace" {
		http.NotFound(w, r)
		return
	}
	vertical := strings.TrimSpace(parts[0])
	if vertical == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("vertical id or slug is required"))
		return
	}

	ctx := r.Context()
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.id::text, e.type, e.source_agent, COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''), COALESCE(v.slug, ''), e.created_at,
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
		WHERE (COALESCE(e.vertical_id::text, '') = $1 OR COALESCE(v.slug, '') = $1)
		GROUP BY e.id, v.slug
		ORDER BY e.created_at ASC
		LIMIT 1000
	`, vertical)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	trace := make([]eventView, 0, 200)
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
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		trace = append(trace, ev)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	last := map[string]any{}
	if len(trace) > 0 {
		l := trace[len(trace)-1]
		last["id"] = l.ID
		last["type"] = l.Type
		last["source_agent"] = l.SourceAgent
		last["created_at"] = l.CreatedAt
		last["pending_count"] = l.PendingCount
		last["dead_count"] = l.DeadCount
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vertical":    vertical,
		"event_count": len(trace),
		"last_event":  last,
		"trace":       trace,
	})
}
