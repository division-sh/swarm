package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type fieldSet struct {
	guaranteed map[string]struct{}
	dynamic    bool
}

type emitSite struct {
	eventType string
	file      string
	line      int
	source    string
	fields    fieldSet
}

type eventContract struct {
	eventType        string
	guaranteed       map[string]struct{}
	union            map[string]struct{}
	dynamicAny       bool
	sites            []emitSite
	guaranteedInited bool
}

type agentSpec struct {
	id            string
	role          string
	file          string
	subscriptions []string
	systemPrompt  string
}

type finding struct {
	eventType      string
	agentID        string
	agentRole      string
	agentFile      string
	subscription   string
	expectedFields []string
	guaranteed     []string
	missing        []string
}

var (
	fieldAliasRegex = map[string][]*regexp.Regexp{
		"entity_id": {
			regexp.MustCompile(`(?i)\bentity_id\b`),
			regexp.MustCompile(`(?i)\bentity\s+id\b`),
		},
		"entity_name": {
			regexp.MustCompile(`(?i)\bentity_name\b`),
			regexp.MustCompile(`(?i)\bentity\s+name\b`),
		},
		"geography": {
			regexp.MustCompile(`(?i)\bgeography\b`),
		},
		"business_brief": {
			regexp.MustCompile(`(?i)\bbusiness_brief\b`),
			regexp.MustCompile(`(?i)\bbusiness\s+brief\b`),
		},
		"scoring": {
			regexp.MustCompile(`(?i)\bscoring\b`),
		},
		"taxonomy_categories": {
			regexp.MustCompile(`(?i)\btaxonomy_categories\b`),
			regexp.MustCompile(`(?i)\btaxonomy\s+categories\b`),
		},
	}
	coreFieldCandidates = []string{
		"entity_id",
		"entity_name",
		"geography",
		"mode",
		"scan_id",
		"campaign_id",
		"priority",
		"taxonomy_categories",
	}
	typedPayloadBuilderFields = map[string][]string{
		"buildValidationStartedPayload":       {"entity_id", "scoring", "entity_name", "name", "geography"},
		"buildBrandRequestedPayload":          {"entity_id", "entity_name", "name", "geography", "scoring", "business_brief"},
		"buildValidationPackageReadyPayload":  {"entity_id", "entity_name", "geography", "research", "spec", "cto_notes", "brand", "scoring", "spec_version"},
		"buildSpecValidationRequestedPayload": {"entity_id", "entity_name", "geography", "spec", "spec_version", "validation_tier"},
		"buildCTOSpecReviewRequestedPayload":  {"entity_id", "entity_name", "geography", "spec_validation", "spec_version", "research", "spec", "scoring"},
		"buildSpecRevisionRequestedPayload":   {"entity_id", "entity_name", "geography", "source", "feedback", "research", "spec", "scoring"},
		"buildValidationMoreDataPayload":      {"entity_id", "entity_name", "geography", "request", "research", "spec", "scoring"},
		"buildBrandRevisionNeededPayload":     {"entity_id", "entity_name", "geography", "feedback", "brand"},
		"buildEntityKilledPayload":            {"entity_id", "entity_name", "geography", "source_event", "priority", "reason"},
		"buildScanAssignedPayload":            {"scan_id", "campaign_id", "mode", "geography", "geography_id", "taxonomy_categories", "priority", "campaign_context", "directive_id", "strategic_context", "requested_at", "planned_shards"},
		"buildSynthesisNeededPayload":         {"scan_id", "campaign_id", "mode", "geography", "category", "subcategory", "conflict_notes", "raw_report"},
		"buildDedupAmbiguousPayload":          {"scan_id", "dedup_event_id", "similarity", "new_candidate", "existing_entity"},
		"buildEntityDiscoveredPayload":        {"entity_id", "name", "geography", "mode", "scan_id", "campaign_id", "signal_strength", "discovery_source", "raw_signals"},
		"buildScanCompletedPayload":           {"scan_id", "campaign_id", "mode", "geography", "reports_received", "agents_expected", "agents_complete", "entities_discovered", "entities_skipped", "pending_dedup", "timed_out", "shards_total", "shards_completed", "shards_failed"},
		"buildScoringRequestedPayload":        {"entity_id", "entity_name", "geography", "mode", "rubric", "dimensions_requested"},
		"buildScoringContestedPayload":        {"entity_id", "dimension", "scores", "evidence", "spread", "rubric", "mode"},
		"buildEntityScoredPayload":            {"entity_id", "result", "reason", "composite_score", "viability_score", "market_score", "dimensions", "rubric", "partial", "mode", "entity_name", "geography"},
		"buildEntityShortlistedPayload":       {"entity_id", "composite_score", "viability_score", "scoring_payload"},
		"buildEntityMarginalPayload":          {"entity_id", "composite_score", "viability_score", "dimensions", "promotion_eligible"},
		"buildEntityRejectedPayload":          {"entity_id", "reason"},
		"buildPortfolioDigestTimerPayload":    {"source", "timestamp", "recent_rejections", "rejection_count", "scoring_rejections_injected", "scoring_rejections_count", "scoring_rejection_summaries"},
	}
)

func main() {
	runtimeDir := flag.String("runtime", "internal/runtime", "path to runtime package directory")
	agentsDir := flag.String("agents", "configs/agents", "path to agent YAML directory")
	templatesDir := flag.String("templates", "configs/agents/templates", "path to template agent YAML directory")
	outPath := flag.String("out", "docs/reports/runtime-payload-audit.md", "output markdown report path")
	flag.Parse()

	sites, err := collectEmitSites(*runtimeDir)
	if err != nil {
		fail(err)
	}
	if len(sites) == 0 {
		fail(errors.New("no runtime emit sites discovered"))
	}
	contracts := buildContracts(sites)

	agents, err := loadAgentSpecs(*agentsDir, *templatesDir)
	if err != nil {
		fail(err)
	}
	findings := auditContractsAgainstPrompts(contracts, agents)

	report := buildReport(contracts, findings, *runtimeDir, []string{*agentsDir, *templatesDir})
	if err := writeFile(*outPath, []byte(report)); err != nil {
		fail(err)
	}

	fmt.Printf("runtime payload audit complete\n")
	fmt.Printf("report: %s\n", *outPath)
	fmt.Printf("runtime events: %d\n", len(contracts))
	fmt.Printf("findings: %d\n", len(findings))
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "runtime payload audit failed: %v\n", err)
	os.Exit(1)
}

func collectEmitSites(runtimeDir string) ([]emitSite, error) {
	var files []string
	err := filepath.WalkDir(runtimeDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	fset := token.NewFileSet()
	out := make([]emitSite, 0, 128)
	for _, path := range files {
		fileNode, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil, fmt.Errorf("parse %s: %w", path, parseErr)
		}
		for _, decl := range fileNode.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			varMap := collectFunctionMapVars(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if site, ok := extractPCPublishSite(call, path, fset, varMap); ok {
					out = append(out, site)
				}
				if site, ok := extractBusPublishSite(call, path, fset, varMap); ok {
					out = append(out, site)
				}
				return true
			})
		}
	}
	return out, nil
}

func collectFunctionMapVars(fn *ast.FuncDecl) map[string]fieldSet {
	vars := map[string]fieldSet{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.AssignStmt:
			for i, lhs := range t.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || id == nil || id.Name == "_" {
					continue
				}
				if i >= len(t.Rhs) {
					continue
				}
				vars[id.Name] = resolveFieldSet(t.Rhs[i], vars)
			}
		case *ast.DeclStmt:
			gen, ok := t.Decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				return true
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if name == nil || name.Name == "_" {
						continue
					}
					if i < len(vs.Values) {
						vars[name.Name] = resolveFieldSet(vs.Values[i], vars)
					}
				}
			}
		}
		return true
	})
	return vars
}

func extractPCPublishSite(call *ast.CallExpr, path string, fset *token.FileSet, varMap map[string]fieldSet) (emitSite, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel == nil || sel.Sel == nil || sel.Sel.Name != "publish" {
		return emitSite{}, false
	}
	xid, ok := sel.X.(*ast.Ident)
	if !ok || xid == nil || xid.Name != "pc" {
		return emitSite{}, false
	}
	if len(call.Args) < 4 {
		return emitSite{}, false
	}
	eventType, ok := stringLiteral(call.Args[1])
	if !ok || strings.TrimSpace(eventType) == "" {
		return emitSite{}, false
	}
	fields := resolveFieldSet(call.Args[3], varMap)
	pos := fset.Position(call.Pos())
	return emitSite{
		eventType: strings.TrimSpace(eventType),
		file:      path,
		line:      pos.Line,
		source:    "pipeline-coordinator",
		fields:    fields,
	}, true
}

func extractBusPublishSite(call *ast.CallExpr, path string, fset *token.FileSet, varMap map[string]fieldSet) (emitSite, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel == nil || sel.Sel == nil || sel.Sel.Name != "Publish" {
		return emitSite{}, false
	}
	if len(call.Args) < 2 {
		return emitSite{}, false
	}
	ev, ok := call.Args[1].(*ast.CompositeLit)
	if !ok {
		return emitSite{}, false
	}
	eventType := ""
	source := ""
	fields := fieldSet{guaranteed: map[string]struct{}{}, dynamic: true}
	for _, elt := range ev.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		keyIdent, ok := kv.Key.(*ast.Ident)
		if !ok || keyIdent == nil {
			continue
		}
		switch keyIdent.Name {
		case "Type":
			eventType = parseEventTypeExpr(kv.Value)
		case "SourceAgent":
			source, _ = stringLiteral(kv.Value)
		case "Payload":
			fields = resolvePayloadFieldSet(kv.Value, varMap)
		}
	}
	if strings.TrimSpace(eventType) == "" {
		return emitSite{}, false
	}
	// Narrow to runtime-emitted sources or runtime package publish helpers.
	if strings.TrimSpace(source) == "" {
		// Unknown source from literal; include for visibility as runtime-managed.
		source = "runtime"
	}
	pos := fset.Position(call.Pos())
	return emitSite{
		eventType: strings.TrimSpace(eventType),
		file:      path,
		line:      pos.Line,
		source:    source,
		fields:    fields,
	}, true
}

func resolvePayloadFieldSet(expr ast.Expr, varMap map[string]fieldSet) fieldSet {
	if call, ok := expr.(*ast.CallExpr); ok {
		if callName(call.Fun) == "mustJSON" && len(call.Args) > 0 {
			return resolveFieldSet(call.Args[0], varMap)
		}
	}
	return resolveFieldSet(expr, varMap)
}

func parseEventTypeExpr(expr ast.Expr) string {
	if call, ok := expr.(*ast.CallExpr); ok {
		if callName(call.Fun) == "events.EventType" && len(call.Args) == 1 {
			if s, ok := stringLiteral(call.Args[0]); ok {
				return strings.TrimSpace(s)
			}
		}
	}
	if s, ok := stringLiteral(expr); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func resolveFieldSet(expr ast.Expr, varMap map[string]fieldSet) fieldSet {
	switch t := expr.(type) {
	case *ast.CompositeLit:
		return parseMapCompositeLiteral(t)
	case *ast.Ident:
		if fs, ok := varMap[t.Name]; ok {
			return cloneFieldSet(fs)
		}
		return fieldSet{guaranteed: map[string]struct{}{}, dynamic: true}
	case *ast.CallExpr:
		name := callName(t.Fun)
		switch {
		case name == "mustJSON" && len(t.Args) > 0:
			return resolveFieldSet(t.Args[0], varMap)
		case name == "json.Marshal" && len(t.Args) > 0:
			return resolveFieldSet(t.Args[0], varMap)
		case name == "payloadMap" && len(t.Args) > 0:
			return resolveFieldSet(t.Args[0], varMap)
		case name == "parsePayloadMap":
			return fieldSet{guaranteed: map[string]struct{}{}, dynamic: true}
		default:
			for builder, fields := range typedPayloadBuilderFields {
				if strings.HasSuffix(name, builder) {
					return newFieldSet(false, fields...)
				}
			}
			// Unknown constructor: dynamic payload.
			return fieldSet{guaranteed: map[string]struct{}{}, dynamic: true}
		}
	default:
		return fieldSet{guaranteed: map[string]struct{}{}, dynamic: true}
	}
}

func parseMapCompositeLiteral(lit *ast.CompositeLit) fieldSet {
	// Accept map literals: map[string]any{...}
	_, isMap := lit.Type.(*ast.MapType)
	if !isMap {
		// Accept typed struct literals and normalize their key names to snake_case
		// so payload structs can be audited similarly to map payloads.
		fs := newFieldSet(false)
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			keyID, ok := kv.Key.(*ast.Ident)
			if !ok || keyID == nil {
				continue
			}
			key := strings.TrimSpace(camelToSnake(keyID.Name))
			if key == "" {
				continue
			}
			fs.guaranteed[key] = struct{}{}
		}
		if len(fs.guaranteed) == 0 {
			return fieldSet{guaranteed: map[string]struct{}{}, dynamic: true}
		}
		return fs
	}
	fs := newFieldSet(false)
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := stringLiteral(kv.Key)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		fs.guaranteed[key] = struct{}{}
	}
	return fs
}

func callName(fun ast.Expr) string {
	switch t := fun.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		left := callName(t.X)
		if left == "" {
			return t.Sel.Name
		}
		return left + "." + t.Sel.Name
	default:
		return ""
	}
}

func stringLiteral(expr ast.Expr) (string, bool) {
	bl, ok := expr.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	raw := strings.TrimSpace(bl.Value)
	if raw == "" {
		return "", false
	}
	if unq, err := unquote(raw); err == nil {
		return unq, true
	}
	return strings.Trim(raw, `"`), true
}

func unquote(s string) (string, error) {
	if strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		return strings.Trim(s, "`"), nil
	}
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return strings.Trim(s, `"`), nil
	}
	return "", fmt.Errorf("not a quoted string: %s", s)
}

func buildContracts(sites []emitSite) map[string]*eventContract {
	out := map[string]*eventContract{}
	for _, site := range sites {
		evt := strings.TrimSpace(site.eventType)
		if evt == "" {
			continue
		}
		c := out[evt]
		if c == nil {
			c = &eventContract{
				eventType:  evt,
				guaranteed: map[string]struct{}{},
				union:      map[string]struct{}{},
				dynamicAny: site.fields.dynamic,
				sites:      []emitSite{site},
			}
			for k := range site.fields.guaranteed {
				c.guaranteed[k] = struct{}{}
				c.union[k] = struct{}{}
			}
			c.guaranteedInited = true
			out[evt] = c
			continue
		}
		c.sites = append(c.sites, site)
		c.dynamicAny = c.dynamicAny || site.fields.dynamic
		for k := range site.fields.guaranteed {
			c.union[k] = struct{}{}
		}
		if !c.guaranteedInited {
			for k := range site.fields.guaranteed {
				c.guaranteed[k] = struct{}{}
			}
			c.guaranteedInited = true
		} else {
			c.guaranteed = intersect(c.guaranteed, site.fields.guaranteed)
		}
	}
	return out
}

func intersect(a, b map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	if len(a) == 0 || len(b) == 0 {
		return out
	}
	for k := range a {
		if _, ok := b[k]; ok {
			out[k] = struct{}{}
		}
	}
	return out
}

func cloneFieldSet(in fieldSet) fieldSet {
	out := fieldSet{
		guaranteed: map[string]struct{}{},
		dynamic:    in.dynamic,
	}
	for k := range in.guaranteed {
		out.guaranteed[k] = struct{}{}
	}
	return out
}

func newFieldSet(dynamic bool, keys ...string) fieldSet {
	out := fieldSet{
		guaranteed: map[string]struct{}{},
		dynamic:    dynamic,
	}
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k != "" {
			out.guaranteed[k] = struct{}{}
		}
	}
	return out
}

func loadAgentSpecs(paths ...string) ([]agentSpec, error) {
	var specs []agentSpec
	for _, root := range paths {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".yaml") {
				return nil
			}
			base := filepath.Base(path)
			if base == "roster.yaml" || base == "routes.yaml" {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			raw := map[string]any{}
			if err := yaml.Unmarshal(content, &raw); err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			subscriptions := toStringSlice(raw["subscriptions"])
			if len(subscriptions) == 0 {
				return nil
			}
			id := strings.TrimSpace(asString(raw["id"]))
			role := strings.TrimSpace(asString(raw["role"]))
			if id == "" {
				id = strings.TrimSuffix(base, filepath.Ext(base))
			}
			specs = append(specs, agentSpec{
				id:            id,
				role:          role,
				file:          path,
				subscriptions: subscriptions,
				systemPrompt:  asString(raw["system_prompt"]),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].id < specs[j].id
	})
	return specs, nil
}

func auditContractsAgainstPrompts(contracts map[string]*eventContract, agents []agentSpec) []finding {
	events := make([]string, 0, len(contracts))
	for evt := range contracts {
		events = append(events, evt)
	}
	sort.Strings(events)

	findings := make([]finding, 0, 64)
	for _, agent := range agents {
		for _, sub := range agent.subscriptions {
			for _, evt := range events {
				if !matchesSubscription(sub, evt) {
					continue
				}
				contract := contracts[evt]
				candidates := candidateFieldsForEvent(contract)
				expected := expectedFieldsForEvent(agent.systemPrompt, evt, candidates)
				if len(expected) == 0 {
					continue
				}
				guaranteed := keysSorted(contract.guaranteed)
				missing := diff(expected, guaranteed)
				if len(missing) == 0 {
					continue
				}
				findings = append(findings, finding{
					eventType:      evt,
					agentID:        agent.id,
					agentRole:      agent.role,
					agentFile:      agent.file,
					subscription:   sub,
					expectedFields: expected,
					guaranteed:     guaranteed,
					missing:        missing,
				})
			}
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].eventType != findings[j].eventType {
			return findings[i].eventType < findings[j].eventType
		}
		return findings[i].agentID < findings[j].agentID
	})
	return findings
}

func candidateFieldsForEvent(c *eventContract) []string {
	set := map[string]struct{}{}
	for _, k := range coreFieldCandidates {
		set[k] = struct{}{}
	}
	for k := range c.union {
		set[k] = struct{}{}
	}
	return keysSorted(set)
}

func expectedFieldsForEvent(prompt, eventType string, candidates []string) []string {
	sections := extractEventSections(prompt, eventType)
	if len(sections) == 0 {
		sections = []string{prompt}
	}
	found := map[string]struct{}{}
	for _, section := range sections {
		lines := strings.Split(section, "\n")
		inputScoped := make([]string, 0, len(lines))
		for _, line := range lines {
			if isInputHintLine(line) {
				inputScoped = append(inputScoped, line)
			}
		}
		targetTexts := []string{section}
		if len(inputScoped) > 0 {
			targetTexts = inputScoped
		}
		for _, txt := range targetTexts {
			for _, field := range candidates {
				if mentionsField(txt, field) {
					found[field] = struct{}{}
				}
			}
		}
	}
	return keysSorted(found)
}

func extractEventSections(prompt, eventType string) []string {
	lines := strings.Split(prompt, "\n")
	eventLower := strings.ToLower(strings.TrimSpace(eventType))
	sections := make([]string, 0, 2)
	for i := 0; i < len(lines); i++ {
		line := strings.ToLower(strings.TrimSpace(lines[i]))
		if !strings.Contains(line, eventLower) {
			continue
		}
		var buf bytes.Buffer
		buf.WriteString(lines[i])
		buf.WriteByte('\n')
		for j := i + 1; j < len(lines); j++ {
			cur := lines[j]
			trim := strings.TrimSpace(cur)
			if trim == "" {
				buf.WriteByte('\n')
				continue
			}
			if isEventHeaderLine(trim) {
				break
			}
			buf.WriteString(cur)
			buf.WriteByte('\n')
		}
		sections = append(sections, buf.String())
	}
	return sections
}

func isEventHeaderLine(line string) bool {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "-")
	line = strings.TrimPrefix(line, "*")
	line = strings.TrimSpace(line)
	if !strings.Contains(line, ":") {
		return false
	}
	head := strings.SplitN(line, ":", 2)[0]
	head = strings.TrimSpace(head)
	if head == "" {
		return false
	}
	for _, r := range head {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' || r == '*' {
			continue
		}
		return false
	}
	// looks like "event.name:" style.
	return strings.Contains(head, ".")
}

func isInputHintLine(line string) bool {
	low := strings.ToLower(strings.TrimSpace(line))
	if low == "" {
		return false
	}
	if strings.HasPrefix(low, "input contract") {
		return true
	}
	if strings.HasPrefix(low, "contains ") || strings.HasPrefix(low, "contains:") {
		return true
	}
	if strings.HasPrefix(low, "you will receive") || strings.HasPrefix(low, "payload contains") || strings.Contains(low, " payload contains ") {
		return true
	}
	if strings.HasPrefix(low, "payload:") || strings.HasPrefix(low, "input:") {
		return true
	}
	if strings.Contains(low, "receive assignment") {
		return true
	}
	if strings.HasPrefix(low, "read payload") || strings.HasPrefix(low, "read the payload") || strings.HasPrefix(low, "read from payload") {
		return true
	}
	return false
}

func camelToSnake(in string) string {
	in = strings.TrimSpace(in)
	if in == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(in) + 4)
	for i, r := range in {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				prev := rune(in[i-1])
				if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') {
					b.WriteByte('_')
				}
			}
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func mentionsField(text, field string) bool {
	text = strings.ToLower(text)
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "" {
		return false
	}
	if regexes, ok := fieldAliasRegex[field]; ok {
		for _, rx := range regexes {
			if rx.MatchString(text) {
				return true
			}
		}
	}
	alias := strings.ReplaceAll(field, "_", " ")
	if containsWord(text, field) {
		return true
	}
	if alias != field && containsWord(text, alias) {
		return true
	}
	return false
}

func containsWord(text, token string) bool {
	token = strings.TrimSpace(strings.ToLower(token))
	if token == "" {
		return false
	}
	rx := regexp.MustCompile(`\b` + regexp.QuoteMeta(token) + `\b`)
	return rx.MatchString(strings.ToLower(text))
}

func matchesSubscription(subscription, eventType string) bool {
	subscription = strings.TrimSpace(subscription)
	eventType = strings.TrimSpace(eventType)
	if subscription == "" || eventType == "" {
		return false
	}
	if subscription == eventType {
		return true
	}
	if !strings.Contains(subscription, "*") {
		return false
	}
	pat := regexp.QuoteMeta(subscription)
	pat = strings.ReplaceAll(pat, `\*`, ".*")
	rx := regexp.MustCompile("^" + pat + "$")
	return rx.MatchString(eventType)
}

func buildReport(contracts map[string]*eventContract, findings []finding, runtimeDir string, configDirs []string) string {
	var b strings.Builder
	now := time.Now().UTC().Format(time.RFC3339)
	b.WriteString("# Runtime Payload Completeness Audit\n\n")
	b.WriteString("- generated_at: " + now + "\n")
	b.WriteString("- runtime_dir: `" + runtimeDir + "`\n")
	b.WriteString("- config_dirs: `" + strings.Join(configDirs, "`, `") + "`\n")
	b.WriteString("- scope: runtime-emitted events (Go-side publish paths) vs subscribed agent prompt field expectations\n\n")

	eventNames := make([]string, 0, len(contracts))
	for evt := range contracts {
		eventNames = append(eventNames, evt)
	}
	sort.Strings(eventNames)

	b.WriteString("## Runtime Event Contracts\n\n")
	b.WriteString("| Event | Guaranteed Fields | Any Dynamic Path | Sites |\n")
	b.WriteString("|---|---|---:|---|\n")
	for _, evt := range eventNames {
		c := contracts[evt]
		b.WriteString("| `" + evt + "` | `" + strings.Join(keysSorted(c.guaranteed), ", ") + "` | ")
		if c.dynamicAny {
			b.WriteString("yes")
		} else {
			b.WriteString("no")
		}
		b.WriteString(" | ")
		siteParts := make([]string, 0, len(c.sites))
		for _, s := range c.sites {
			siteParts = append(siteParts, fmt.Sprintf("`%s:%d`", s.file, s.line))
		}
		sort.Strings(siteParts)
		b.WriteString(strings.Join(siteParts, ", "))
		b.WriteString(" |\n")
	}

	b.WriteString("\n## Findings\n\n")
	if len(findings) == 0 {
		b.WriteString("No prompt-to-payload gaps detected for runtime-emitted events.\n")
		return b.String()
	}
	b.WriteString("| Event | Agent | Subscription | Prompt-Expected Fields | Guaranteed Fields | Missing |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, f := range findings {
		agentLabel := f.agentID
		if strings.TrimSpace(f.agentRole) != "" {
			agentLabel += " (" + f.agentRole + ")"
		}
		b.WriteString("| `" + f.eventType + "` | `" + agentLabel + "` | `" + f.subscription + "` | `" + strings.Join(f.expectedFields, ", ") + "` | `" + strings.Join(f.guaranteed, ", ") + "` | `" + strings.Join(f.missing, ", ") + "` |\n")
	}

	b.WriteString("\n## Suggested Next Step\n\n")
	b.WriteString("Define typed payload structs for each runtime-emitted event and route all `Publish` payload construction through them to enforce compile-time field contracts.\n")
	return b.String()
}

func writeFile(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, content, 0o644)
}

func keysSorted(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func diff(expected, guaranteed []string) []string {
	gset := map[string]struct{}{}
	for _, g := range guaranteed {
		gset[g] = struct{}{}
	}
	out := make([]string, 0, len(expected))
	for _, e := range expected {
		if _, ok := gset[e]; !ok {
			out = append(out, e)
		}
	}
	sort.Strings(out)
	return out
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		out := make([]string, 0, len(t))
		for _, s := range t {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			s := strings.TrimSpace(asString(item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
