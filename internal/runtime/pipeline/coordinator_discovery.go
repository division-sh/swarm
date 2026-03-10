package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"strings"

	"empireai/internal/events"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

func (sc *ScanCoordinator) handleDiscoveryReport(ctx context.Context, evt events.Event) {
	if pc, ok := sc.runtime.(*FactoryPipelineCoordinator); ok {
		(&DiscoveryAggregator{coordinator: pc}).handleDiscoveryReport(ctx, evt)
	}
}

type discoveryCandidate = DiscoveryCandidate

func buildDiscoveryCandidatesForReport(scanMode string, payload map[string]any) []DiscoveryCandidate {
	module := defaultWorkflowModule()
	return module.DiscoveryPolicy().BuildDiscoveryCandidatesForReport(scanMode, payload)
}

func (pc *FactoryPipelineCoordinator) logPrefilterSkip(ctx context.Context, evt events.Event, scanID, campaignID, reason, mode string, payload map[string]any, rawSignal, adjustedSignal float64) {
	if pc == nil || pc.bus == nil {
		return
	}
	pc.bus.LogRuntime(ctx, RuntimeLogEntry{
		Level:      "warn",
		Component:  "prefilter",
		Action:     "skipped",
		EventID:    strings.TrimSpace(evt.ID),
		EventType:  strings.TrimSpace(string(evt.Type)),
		AgentID:    strings.TrimSpace(evt.SourceAgent),
		CampaignID: strings.TrimSpace(campaignID),
		ScanID:     strings.TrimSpace(scanID),
		Detail:     pc.discoveryPolicy.BuildPrefilterSkipDetail(payload, rawSignal, adjustedSignal, reason, mode),
	})
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func EvaluateDiscoveryPreFilterForTest(payload map[string]any, rawSignal float64) (bool, float64, string) {
	module := defaultWorkflowModule()
	return module.DiscoveryPolicy().EvaluateDiscoveryPreFilter(payload, rawSignal)
}

func BuildPrefilterSkipDetailForTest(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	module := defaultWorkflowModule()
	return module.DiscoveryPolicy().BuildPrefilterSkipDetail(payload, rawSignal, adjustedSignal, reason, mode)
}

func CloneMapForTest(in map[string]any) map[string]any {
	return cloneMap(in)
}

func (sc *ScanCoordinator) handleDedupResolved(ctx context.Context, evt events.Event) {
	if pc, ok := sc.runtime.(*FactoryPipelineCoordinator); ok {
		(&DiscoveryAggregator{coordinator: pc}).handleDedupResolved(ctx, evt)
	}
}

func (sc *ScanCoordinator) handleSynthesisResolved(ctx context.Context, evt events.Event) {
	if pc, ok := sc.runtime.(*FactoryPipelineCoordinator); ok {
		(&DiscoveryAggregator{coordinator: pc}).handleSynthesisResolved(ctx, evt)
	}
}

func (sc *ScanCoordinator) ensureScanProjectionLoaded(ctx context.Context, scanID string) {
	if sc == nil || sc.runtime == nil {
		return
	}
	scanID = strings.TrimSpace(scanID)
	if scanID == "" {
		return
	}
	sc.mu.Lock()
	_, hasScan := sc.scans[scanID]
	sc.mu.Unlock()
	if hasScan {
		return
	}
	acc, pending, ok := sc.runtime.loadWorkflowScanProjection(ctx, scanID)
	if !ok || acc == nil {
		return
	}
	sc.mu.Lock()
	if _, exists := sc.scans[scanID]; !exists {
		sc.scans[scanID] = acc
	}
	for dedupID, cand := range pending {
		if _, exists := sc.pendingDedup[dedupID]; !exists {
			sc.pendingDedup[dedupID] = cand
		}
	}
	sc.mu.Unlock()
}

func (sc *ScanCoordinator) pendingDedupCountForScan(scanID string) int {
	count := 0
	for _, cand := range sc.pendingDedup {
		if cand.ScanID == scanID {
			count++
		}
	}
	return count
}

type verticalCandidate struct {
	ID   string
	Name string
}

func (pc *FactoryPipelineCoordinator) loadVerticalsByGeography(ctx context.Context, geography string) ([]verticalCandidate, error) {
	if pc == nil || pc.db == nil || strings.TrimSpace(geography) == "" {
		return nil, nil
	}
	rows, err := dbQueryContext(ctx, pc.db, `
		SELECT id::text, name
		FROM verticals
		WHERE lower(geography) = lower($1)
		ORDER BY created_at DESC
		LIMIT 500
	`, geography)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]verticalCandidate, 0, 32)
	for rows.Next() {
		var v verticalCandidate
		if err := rows.Scan(&v.ID, &v.Name); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (pc *FactoryPipelineCoordinator) loadVerticalIdentity(ctx context.Context, verticalID string) (string, string, error) {
	if pc == nil || pc.db == nil || strings.TrimSpace(verticalID) == "" {
		return "", "", nil
	}
	var name, geography string
	err := dbQueryRowContext(ctx, pc.db, `
		SELECT COALESCE(name, ''), COALESCE(geography, '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&name, &geography)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(name), strings.TrimSpace(geography), nil
}

func (pc *FactoryPipelineCoordinator) ensureVerticalDiscovered(ctx context.Context, name, geography, mode string, payload map[string]any) (string, error) {
	existing, err := pc.loadVerticalsByGeography(ctx, geography)
	if err != nil {
		return "", err
	}
	norm := normalizeName(name)
	for _, v := range existing {
		if normalizeName(v.Name) == norm {
			return v.ID, nil
		}
	}
	verticalID := uuid.NewString()
	if pc == nil || pc.db == nil {
		return verticalID, nil
	}
	slug := buildVerticalSlug(name, verticalID)
	if _, err := dbExecContext(ctx, pc.db, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals, created_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, 'discovered', 'factory', $5::jsonb, now(), now())
	`, verticalID, name, slug, geography, string(mustJSON(payload))); err != nil {
		return "", err
	}
	_ = pc.updateVerticalDiscoveryMetadata(ctx, verticalID, mode, payload)
	return verticalID, nil
}

func (pc *FactoryPipelineCoordinator) updateVerticalDiscoveryMetadata(ctx context.Context, verticalID, mode string, payload map[string]any) error {
	if pc == nil || pc.db == nil || strings.TrimSpace(verticalID) == "" {
		return nil
	}
	if payload == nil {
		payload = map[string]any{}
	}
	discoveryMode := normalizeScanMode(mode)
	if discoveryMode == "" {
		discoveryMode = strings.ToLower(strings.TrimSpace(mode))
	}
	if discoveryMode == "" {
		discoveryMode = strings.ToLower(strings.TrimSpace(asString(payload["mode"])))
	}
	if discoveryMode == "" {
		discoveryMode = runtimeproductpolicy.DiscoveryFallbackMode()
	}
	opportunityPattern := pc.discoveryPolicy.NormalizeOpportunityPattern(asString(payload["opportunity_pattern"]))
	if opportunityPattern == "" {
		opportunityPattern = "unknown"
	}
	signalSources := payload["signal_sources"]
	if signalSources == nil {
		signalSources = []any{}
	}
	requiredCapabilities := payload["required_capabilities"]
	if requiredCapabilities == nil {
		requiredCapabilities = map[string]any{}
	}
	parentID := strings.TrimSpace(asString(payload["parent_id"]))
	generationDepth := intFromAny(payload["generation_depth"])
	if generationDepth < 0 {
		generationDepth = 0
	}
	if generationDepth > 2 {
		generationDepth = 2
	}
	generatorAgentID := strings.TrimSpace(asString(payload["generator_agent_id"]))
	derivationRationale := payload["derivation_rationale"]
	if derivationRationale == nil {
		derivationRationale = map[string]any{}
	}
	_, err := dbExecContext(ctx, pc.db, `
		UPDATE verticals
		SET
			discovery_mode = $2,
			opportunity_pattern = COALESCE(NULLIF($3, ''), opportunity_pattern),
			signal_sources = COALESCE($4::jsonb, signal_sources),
			required_capabilities = COALESCE($5::jsonb, required_capabilities),
			parent_id = COALESCE(NULLIF($6, '')::uuid, parent_id),
			generation_depth = CASE WHEN $7 > 0 THEN $7 ELSE generation_depth END,
			generator_agent_id = COALESCE(NULLIF($8, ''), generator_agent_id),
			derivation_rationale = COALESCE($9::jsonb, derivation_rationale),
			updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, discoveryMode, opportunityPattern, string(mustJSON(signalSources)), string(mustJSON(requiredCapabilities)), parentID, generationDepth, generatorAgentID, string(mustJSON(derivationRationale)))
	if err != nil {
		// Older test fixtures may not include newer columns; ignore metadata enrichment failures.
		return err
	}
	return nil
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)
var punctuationHeavyName = regexp.MustCompile(`[.!?;:]`)

var knownVerticalAcronyms = map[string]string{
	"ai":    "AI",
	"api":   "API",
	"b2b":   "B2B",
	"b2c":   "B2C",
	"crm":   "CRM",
	"erp":   "ERP",
	"hr":    "HR",
	"kpi":   "KPI",
	"ppc":   "PPC",
	"pos":   "POS",
	"saas":  "SaaS",
	"seo":   "SEO",
	"spi":   "SPI",
	"smb":   "SMB",
	"smes":  "SMEs",
	"whats": "Whats",
}

func normalizeName(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	raw = nonAlnum.ReplaceAllString(raw, " ")
	return strings.Join(strings.Fields(raw), " ")
}

func deriveDiscoveryCandidateName(payload map[string]any) string {
	if len(payload) == 0 {
		return ""
	}
	for _, key := range []string{"opportunity_name", "vertical_name", "name", "title"} {
		if v := normalizeProvidedVerticalName(asString(payload[key]), false); v != "" {
			return v
		}
	}
	if v := humanizeTaxonomyLabel(firstNonEmptyString(
		asString(payload["subcategory"]),
		asString(payload["trend_category"]),
	)); v != "" {
		return v
	}
	if v := humanizeTaxonomyLabel(asString(payload["category"])); v != "" {
		return v
	}
	for _, key := range []string{"trend_description", "opportunity_hypothesis"} {
		if v := normalizeProvidedVerticalName(asString(payload[key]), true); v != "" {
			return v
		}
	}
	return ""
}

func normalizeProvidedVerticalName(raw string, strictNarrative bool) string {
	name := strings.TrimSpace(raw)
	if name == "" {
		return ""
	}
	name = strings.Join(strings.Fields(name), " ")
	if strictNarrative {
		if len(name) > maxNarrativeFallbackNameLen {
			return ""
		}
		if len(strings.Fields(name)) > maxNarrativeFallbackNameWords {
			return ""
		}
		if punctuationHeavyName.MatchString(name) {
			return ""
		}
	}
	if len(name) > maxVerticalNameLen {
		name = strings.TrimSpace(truncateRunes(name, maxVerticalNameLen))
	}
	// If the candidate looks like a taxonomy token, present a readable label.
	if !strings.Contains(name, " ") && (strings.Contains(name, "_") || strings.Contains(name, "-") || strings.Contains(name, "/")) {
		if humanized := humanizeTaxonomyLabel(name); humanized != "" {
			return humanized
		}
	}
	return name
}

func humanizeTaxonomyLabel(raw string) string {
	norm := normalizeName(raw)
	if norm == "" {
		return ""
	}
	parts := strings.Fields(norm)
	for i, part := range parts {
		if acronym, ok := knownVerticalAcronyms[part]; ok {
			parts[i] = acronym
			continue
		}
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	name := strings.Join(parts, " ")
	if len(name) > maxVerticalNameLen {
		name = strings.TrimSpace(truncateRunes(name, maxVerticalNameLen))
	}
	return name
}

func buildVerticalSlug(name, id string) string {
	base := normalizeName(name)
	base = strings.ReplaceAll(base, " ", "-")
	base = strings.Trim(base, "-")
	if base == "" {
		base = "vertical"
	}
	if len(base) > maxVerticalSlugLen {
		base = strings.Trim(base[:maxVerticalSlugLen], "-")
	}
	if base == "" {
		base = "vertical"
	}
	suffix := id
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return base + "-" + suffix
}

func fuzzyBestMatch(name string, existing []verticalCandidate) (verticalCandidate, float64) {
	cand := tokenSet(normalizeName(name))
	best := verticalCandidate{}
	bestScore := 0.0
	for _, v := range existing {
		score := jaccard(cand, tokenSet(normalizeName(v.Name)))
		if score > bestScore {
			bestScore = score
			best = v
		}
	}
	return best, bestScore
}

func tokenSet(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, t := range strings.Fields(strings.TrimSpace(s)) {
		if t == "" {
			continue
		}
		out[t] = struct{}{}
	}
	return out
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	union := len(a)
	for k := range b {
		if _, ok := a[k]; ok {
			inter++
		} else {
			union++
		}
	}
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

func parsePayloadMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := runtimesharedjson.ParsePayloadMap(raw)
	if len(out) == 0 {
		var probe map[string]any
		if err := json.Unmarshal(raw, &probe); err != nil {
			runtimeWarn(
				"payload-parse",
				"failed to parse JSON payload bytes=%d error=%v snippet=%q",
				len(raw),
				err,
				snippetForLog(string(raw), 220),
			)
		}
		return map[string]any{}
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func payloadMap(v any) map[string]any {
	return parsePayloadMap(mustJSON(v))
}

func cloneRaw(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return cp
}

func mergeRawPayload(current, incoming []byte) json.RawMessage {
	base := parsePayloadMap(current)
	next := parsePayloadMap(incoming)
	for k, v := range next {
		base[k] = v
	}
	return mustJSON(base)
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		s := strings.TrimSpace(asString(v))
		if s == "" {
			return 0
		}
		var num json.Number = json.Number(s)
		f, _ := num.Float64()
		return f
	}
}

func payloadIndicatesSynthesisNeeded(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	for _, key := range []string{"requires_synthesis", "needs_synthesis", "conflict_detected", "conflicting_signals"} {
		if parseBool(payload[key]) {
			return true
		}
	}
	if notes := strings.TrimSpace(asString(payload["conflict_notes"])); notes != "" {
		return true
	}
	return false
}

func parseBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "y", "on":
			return true
		default:
			return false
		}
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}
