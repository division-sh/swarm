package runtime

import (
	"archive/tar"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"regexp"
	goruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"empireai/internal/commgraph"
	"empireai/internal/promptcontracts"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimetools "empireai/internal/runtime/tools"
	"gopkg.in/yaml.v3"
)

type wiringSeverity string

const (
	wiringPass wiringSeverity = "PASS"
	wiringFail wiringSeverity = "FAIL"
	wiringWarn wiringSeverity = "WARN"
)

type wiringResult struct {
	Severity wiringSeverity
	Message  string
}

type wiringAgent struct {
	ID            string
	Role          string
	Path          string
	Subscriptions []string
	SystemPrompt  string
}

type wiringSchema struct {
	EventType string
	Required  map[string]struct{}
	Props     map[string]struct{}
}

type wiringEmitSite struct {
	EventType    string
	File         string
	Line         int
	FuncName     string
	Fields       map[string]struct{}
	Dynamic      bool
	TypedPayload bool
	Source       string
}

type producerRef struct {
	Kind string // "agent" | "runtime"
	ID   string // role or runtime component label
}

type rosterYAML struct {
	Agents map[string]struct {
		ConfigPath string `yaml:"config_path"`
	} `yaml:"agents"`
}

type wiringEventContract struct {
	EventType       string
	Emitter         any    `yaml:"emitter"`
	Consumer        any    `yaml:"consumer"`
	Intercepted     bool   `yaml:"intercepted"`
	Passthrough     any    `yaml:"passthrough"`
	Routing         string `yaml:"routing"`
	DeliveryChannel string `yaml:"delivery_channel"`
}

var (
	emitRefRe             = regexp.MustCompile(`\bemit_[a-zA-Z0-9_]+\b`)
	legacyEmitEventsRe    = regexp.MustCompile(`(?i)\bemit_events\b`)
	legacyEmitEventNameRe = regexp.MustCompile("(?i)\\bemit\\s+`[a-z0-9_.-]+`")
	eventTypeCallRe       = regexp.MustCompile(`events\.EventType\("([a-zA-Z0-9._*-]+)"\)`)
	fieldAliasRegexWiring = map[string][]*regexp.Regexp{
		"vertical_id": {
			regexp.MustCompile(`(?i)\bvertical_id\b`),
			regexp.MustCompile(`(?i)\bvertical\s+id\b`),
		},
		"vertical_name": {
			regexp.MustCompile(`(?i)\bvertical_name\b`),
			regexp.MustCompile(`(?i)\bvertical\s+name\b`),
		},
		"geography": {
			regexp.MustCompile(`(?i)\bgeography\b`),
		},
		"scoring_payload": {
			regexp.MustCompile(`(?i)\bscoring_payload\b`),
			regexp.MustCompile(`(?i)\bscoring\s+payload\b`),
		},
		"business_brief": {
			regexp.MustCompile(`(?i)\bbusiness_brief\b`),
			regexp.MustCompile(`(?i)\bbusiness\s+brief\b`),
		},
		"spec": {
			regexp.MustCompile(`(?i)\bspec\b`),
		},
		"cto_notes": {
			regexp.MustCompile(`(?i)\bcto_notes\b`),
			regexp.MustCompile(`(?i)\bcto\s+notes\b`),
			regexp.MustCompile(`(?i)\bfeasibility\b`),
		},
		"brand": {
			regexp.MustCompile(`(?i)\bbrand\b`),
		},
		"dimensions_requested": {
			regexp.MustCompile(`(?i)\bdimensions_requested\b`),
			regexp.MustCompile(`(?i)\bdimensions\s+requested\b`),
		},
	}
)

func TestSpecRuntimeWiringVerification(t *testing.T) {
	_ = runtimetools.EventSchemaSnapshot()
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatalf("resolve current file path for repo root")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	agentsDir := filepath.Join(repoRoot, "configs", "agents")
	runtimeDir := filepath.Join(repoRoot, "internal", "runtime")
	pipelinePath := filepath.Join(runtimeDir, "pipeline", "coordinator.go")

	agents, err := loadWiringAgentsFromRoster(agentsDir)
	if err != nil {
		t.Fatalf("load agents from roster: %v", err)
	}
	schemas := loadWiringSchemasFromRegistry()
	toolToEvent := buildToolToEventMap()
	contracts := loadWiringEventContracts(repoRoot)
	runtimeManagedEvents := loadRuntimeManagedEvents(repoRoot)

	sites, err := collectRuntimeEmitSites(runtimeDir)
	if err != nil {
		t.Fatalf("collect runtime emit sites: %v", err)
	}
	runtimeEmitted := map[string][]wiringEmitSite{}
	for _, s := range sites {
		runtimeEmitted[s.EventType] = append(runtimeEmitted[s.EventType], s)
	}

	producersByEvent := map[string][]producerRef{}
	for _, role := range commgraph.ProducerRoles() {
		for _, evt := range commgraph.ProducerEventsForRole(role) {
			evt = strings.TrimSpace(evt)
			if evt == "" {
				continue
			}
			producersByEvent[evt] = append(producersByEvent[evt], producerRef{Kind: "agent", ID: role})
		}
	}
	for _, evt := range commgraph.RuntimeEvents() {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		producersByEvent[evt] = append(producersByEvent[evt], producerRef{Kind: "runtime", ID: "runtime"})
	}
	for _, evt := range commgraph.HumanEvents() {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		producersByEvent[evt] = append(producersByEvent[evt], producerRef{Kind: "human", ID: "human"})
	}
	for evt := range runtimeEmitted {
		producersByEvent[evt] = append(producersByEvent[evt], producerRef{Kind: "runtime", ID: "runtime"})
	}

	interceptEvents, handleEvents, handlerCases, err := parsePipelineInterceptorCoverage(pipelinePath)
	if err != nil {
		t.Fatalf("parse pipeline coordinator switch coverage: %v", err)
	}

	results := make([]wiringResult, 0, 512)
	results = append(results, verifyEmitToolCompleteness(agents, schemas, toolToEvent)...)
	results = append(results, verifySubscriptionCompleteness(agents, producersByEvent, contracts)...)
	results = append(results, verifyPayloadContracts(agents, schemas, runtimeEmitted)...)
	nonLocalIntercepts := map[string]struct{}{}
	for id, node := range loadWiringSystemNodes(repoRoot) {
		if strings.TrimSpace(id) == "pipeline-coordinator" {
			continue
		}
		for _, evt := range node.SubscribesTo {
			evt = strings.TrimSpace(evt)
			if evt == "" {
				continue
			}
			nonLocalIntercepts[evt] = struct{}{}
		}
	}
	results = append(results, verifyInterceptorCoverage(interceptEvents, handleEvents, handlerCases, runtimeEmitted, nonLocalIntercepts)...)
	results = append(results, verifyPipelinePathTracing(agents, producersByEvent, interceptEvents, runtimeManagedEvents)...)
	results = append(results, verifyOrphanEmissions(agents, producersByEvent, interceptEvents, runtimeManagedEvents, contracts)...)
	results = append(results, verifySchemaCatalogConsistency(agents, schemas, producersByEvent, interceptEvents, contracts)...)

	failCount := 0
	warnCount := 0
	for _, r := range results {
		t.Logf("%s: %s", r.Severity, r.Message)
		if r.Severity == wiringFail {
			failCount++
		}
		if r.Severity == wiringWarn {
			warnCount++
		}
	}
	t.Logf("summary: pass=%d fail=%d warn=%d", len(results)-failCount-warnCount, failCount, warnCount)
	if failCount > 0 && isWiringStrictMode() {
		t.Fatalf("wiring verification failed with %d FAIL items", failCount)
	}
	if failCount > 0 {
		t.Logf("strict mode disabled; set EMPIRE_WIRING_STRICT=1 to fail on wiring FAIL items (default is strict)")
	}
}

func verifyEmitToolCompleteness(agents []wiringAgent, schemas map[string]wiringSchema, toolToEvent map[string]string) []wiringResult {
	out := make([]wiringResult, 0, 128)
	promptLintStrict := isWiringPromptLintStrict()
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := schemas[eventType]; ok {
				continue
			}
			out = append(out, wiringResult{
				Severity: wiringFail,
				Message:  fmt.Sprintf("producer role %s emits %s but EventSchemaRegistry has no explicit entry", role, eventType),
			})
		}
	}
	for _, a := range agents {
		if !promptLintStrict {
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  fmt.Sprintf("%s prompt lint checks skipped (set EMPIRE_WIRING_PROMPT_LINT_STRICT=1 to enforce)", a.ID),
			})
			continue
		}
		if legacyEmitEventsRe.MatchString(a.SystemPrompt) {
			out = append(out, wiringResult{
				Severity: wiringFail,
				Message:  fmt.Sprintf("%s prompt references legacy emit_events envelope", a.ID),
			})
		}
		if legacyEmitEventNameRe.MatchString(a.SystemPrompt) {
			out = append(out, wiringResult{
				Severity: wiringFail,
				Message:  fmt.Sprintf("%s prompt uses legacy 'Emit `event.name`' wording instead of emit_* tool calls", a.ID),
			})
		}
		refs := extractEmitToolRefs(a.SystemPrompt)
		if len(refs) == 0 {
			continue
		}
		for _, tool := range refs {
			eventType, ok := toolToEvent[tool]
			if !ok {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s prompt references %s but no EventSchemaRegistry entry exists", a.ID, tool),
				})
				continue
			}
			if _, ok := schemas[eventType]; !ok {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s prompt references %s -> %s but schema lookup failed", a.ID, tool, eventType),
				})
				continue
			}
			if !runtimetools.IsEmitToolAllowedForRole(a.Role, tool) {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s prompt references %s but role %s is not allowed to emit %s", a.ID, tool, a.Role, eventType),
				})
				continue
			}
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  fmt.Sprintf("%s prompt references %s and it is schema-backed + allowed", a.ID, tool),
			})
		}
	}
	return out
}

func verifySubscriptionCompleteness(agents []wiringAgent, producersByEvent map[string][]producerRef, contracts map[string]wiringEventContract) []wiringResult {
	out := make([]wiringResult, 0, 192)
	promptLintStrict := isWiringPromptLintStrict()
	allEvents := make([]string, 0, len(producersByEvent))
	for evt := range producersByEvent {
		allEvents = append(allEvents, evt)
	}
	sort.Strings(allEvents)

	for _, a := range agents {
		for _, sub := range a.Subscriptions {
			if !subscriptionNeedsChecks(sub, contracts) {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("%s subscription %s skipped by delivery_channel policy", a.ID, sub),
				})
				continue
			}
			matches := matchingEvents(sub, allEvents)
			if len(matches) == 0 {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s subscribes to %s but no producer (agent/runtime) emits it", a.ID, sub),
				})
				continue
			}

			hasExternalProducer := false
			for _, evt := range matches {
				for _, p := range producersByEvent[evt] {
					if p.Kind == "runtime" || !strings.EqualFold(strings.TrimSpace(p.ID), strings.TrimSpace(a.Role)) {
						hasExternalProducer = true
						break
					}
				}
				if hasExternalProducer {
					break
				}
			}
			if !hasExternalProducer {
				if allowSelfSubscription(a.ID, sub) {
					out = append(out, wiringResult{
						Severity: wiringPass,
						Message:  fmt.Sprintf("%s subscription %s is an allowed self-loop by design", a.ID, sub),
					})
					continue
				}
				out = append(out, wiringResult{
					Severity: wiringWarn,
					Message:  fmt.Sprintf("%s subscribes to %s but only self-emitted events were found", a.ID, sub),
				})
			} else {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("%s subscription %s has upstream producer coverage", a.ID, sub),
				})
			}

			if !promptLintStrict {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("%s prompt subscription lint skipped for %s (set EMPIRE_WIRING_PROMPT_LINT_STRICT=1 to enforce)", a.ID, sub),
				})
				continue
			}

			if !promptMentionsSubscription(a.SystemPrompt, sub, matches) {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s subscribes to %s but prompt has no handling instructions", a.ID, sub),
				})
			} else {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("%s prompt includes handling guidance for %s", a.ID, sub),
				})
			}
		}
	}

	return out
}

func verifyPayloadContracts(agents []wiringAgent, schemas map[string]wiringSchema, runtimeEmitted map[string][]wiringEmitSite) []wiringResult {
	out := make([]wiringResult, 0, 256)

	// Required-field enforcement for runtime-emitted events.
	events := make([]string, 0, len(schemas))
	for evt := range schemas {
		events = append(events, evt)
	}
	sort.Strings(events)
	for _, evt := range events {
		schema := schemas[evt]
		required := sortedKeys(schema.Required)
		out = append(out, wiringResult{
			Severity: wiringPass,
			Message:  fmt.Sprintf("%s schema required fields: [%s]", evt, strings.Join(required, ", ")),
		})
		for _, site := range runtimeEmitted[evt] {
			if len(required) == 0 {
				continue
			}
			if site.Dynamic {
				out = append(out, wiringResult{
					Severity: wiringWarn,
					Message:  fmt.Sprintf("%s runtime emit at %s:%d is dynamic; cannot prove required-field completeness", evt, site.File, site.Line),
				})
				continue
			}
			missing := missingFromSet(required, site.Fields)
			if len(missing) > 0 {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s runtime emit at %s:%d missing required fields: %s", evt, site.File, site.Line, strings.Join(missing, ", ")),
				})
			} else {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("%s runtime emit at %s:%d includes all required fields", evt, site.File, site.Line),
				})
			}
		}
	}

	// Prompt field expectations vs schema + runtime payloads.
	for _, a := range agents {
		for _, sub := range a.Subscriptions {
			for evt, schema := range schemas {
				if !matchesSubscription(sub, evt) {
					continue
				}
				candidates := makeFieldCandidates(schema, runtimeEmitted[evt])
				expected := expectedFieldsForEvent(a.SystemPrompt, evt, candidates)
				if len(expected) == 0 {
					continue
				}
				for _, field := range expected {
					if _, ok := schema.Props[field]; !ok {
						out = append(out, wiringResult{
							Severity: wiringFail,
							Message:  fmt.Sprintf("%s expects field %s from %s but schema has no such property", a.ID, field, evt),
						})
					}
				}
				for _, site := range runtimeEmitted[evt] {
					if site.Dynamic {
						continue
					}
					missing := missingFromSet(expected, site.Fields)
					if len(missing) > 0 {
						out = append(out, wiringResult{
							Severity: wiringFail,
							Message:  fmt.Sprintf("%s expects [%s] from %s but runtime payload at %s:%d lacks [%s]", a.ID, strings.Join(expected, ", "), evt, site.File, site.Line, strings.Join(missing, ", ")),
						})
					}
				}
			}
		}
	}

	return out
}

func verifyInterceptorCoverage(interceptEvents, handleEvents map[string]struct{}, handlerCases map[string]string, runtimeEmitted map[string][]wiringEmitSite, nonLocalIntercepts map[string]struct{}) []wiringResult {
	out := make([]wiringResult, 0, 128)

	for evt := range interceptEvents {
		if _, ok := nonLocalIntercepts[evt]; ok {
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  fmt.Sprintf("interceptor event %s is owned by another system node", evt),
			})
			continue
		}
		if _, ok := handleEvents[evt]; ok {
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  fmt.Sprintf("interceptor event %s has a handleEvent switch case", evt),
			})
			continue
		}
		// spec.revision_needed is handled as a special branch in Intercept()
		if evt == "spec.revision_needed" {
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  "interceptor event spec.revision_needed is handled in Intercept special-case branch",
			})
			continue
		}
		out = append(out, wiringResult{
			Severity: wiringFail,
			Message:  fmt.Sprintf("interceptor event %s exists in interceptPolicy but has no handleEvent case", evt),
		})
	}

	for evt, handler := range handlerCases {
		if strings.TrimSpace(handler) == "" {
			continue
		}
		emits := emittedEventsByHandler(runtimeEmitted, handler)
		if len(emits) == 0 {
			if allowNoEmitHandler(handler, evt) {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("interceptor handler %s (from %s) is state-only by design", handler, evt),
				})
				continue
			}
			out = append(out, wiringResult{
				Severity: wiringWarn,
				Message:  fmt.Sprintf("interceptor handler %s (from %s) emits no runtime events", handler, evt),
			})
			continue
		}
		sort.Strings(emits)
		out = append(out, wiringResult{
			Severity: wiringPass,
			Message:  fmt.Sprintf("interceptor handler %s (from %s) emits: %s", handler, evt, strings.Join(emits, ", ")),
		})
	}

	for evt, emits := range runtimeEmitted {
		for _, s := range emits {
			if !strings.Contains(s.File, "pipeline_coordinator.go") && !strings.Contains(s.File, filepath.Join("pipeline", "coordinator.go")) {
				continue
			}
			if s.TypedPayload {
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("%s runtime payload at %s:%d is typed-struct derived", evt, s.File, s.Line),
				})
				continue
			}
			out = append(out, wiringResult{
				Severity: wiringWarn,
				Message:  fmt.Sprintf("%s runtime payload at %s:%d is ad-hoc map (not typed struct)", evt, s.File, s.Line),
			})
		}
	}
	return out
}

func allowSelfSubscription(agentID, subscription string) bool {
	agentID = strings.TrimSpace(strings.ToLower(agentID))
	subscription = strings.TrimSpace(strings.ToLower(subscription))
	if agentID == "" || subscription == "" {
		return false
	}
	switch agentID {
	case "empire-coordinator":
		return subscription == "template.migration_completed" || subscription == "template.migration_failed"
	case "holding-devops":
		return subscription == "devops.health_check_failed" || subscription == "devops.rollback_failed"
	default:
		return false
	}
}

func allowNoEmitHandler(handler, eventType string) bool {
	handler = strings.TrimSpace(handler)
	eventType = strings.TrimSpace(eventType)
	if handler == "" || eventType == "" {
		return false
	}
	switch handler {
	case "handleScoringContestResolved":
		return eventType == "scoring.contest_resolved"
	case "handleValidationPackaged":
		return eventType == "vertical.ready_for_review"
	case "resetInMemoryState":
		return eventType == "runtime.reset"
	case "handleCTOApproved":
		return eventType == "cto.spec_approved"
	case "handleSpecRevisionRequested":
		return eventType == "spec.revision_requested"
	case "handleVerticalApproved":
		return eventType == "vertical.approved"
	case "handleVerticalKilled":
		return eventType == "vertical.killed"
	default:
		return false
	}
}

func verifyPipelinePathTracing(agents []wiringAgent, producersByEvent map[string][]producerRef, interceptEvents map[string]struct{}, runtimeManagedEvents map[string]struct{}) []wiringResult {
	type pathEdge struct {
		Event        string
		ConsumerKind string // "agent" | "runtime"
		ConsumerID   string // role
	}
	edges := []pathEdge{
		{Event: "vertical.discovered", ConsumerKind: "runtime", ConsumerID: "scoring-node"},
		{Event: "scoring.requested", ConsumerKind: "agent", ConsumerID: "analysis-agent"},
		{Event: "score.dimension_complete", ConsumerKind: "runtime", ConsumerID: "scoring-node"},
		{Event: "scoring.contested", ConsumerKind: "agent", ConsumerID: "empire-coordinator"},
		{Event: "scoring.contest_resolved", ConsumerKind: "runtime", ConsumerID: "scoring-node"},
		{Event: "vertical.scored", ConsumerKind: "agent", ConsumerID: "empire-coordinator"},
		{Event: "vertical.shortlisted", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
		{Event: "validation.started", ConsumerKind: "agent", ConsumerID: "business-research-agent"},
		{Event: "research.completed", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
		{Event: "spec.requested", ConsumerKind: "agent", ConsumerID: "lightweight-spec-agent"},
		{Event: "spec.draft_ready", ConsumerKind: "agent", ConsumerID: "business-research-agent"},
		{Event: "spec_review.requested", ConsumerKind: "agent", ConsumerID: "spec-reviewer"},
		{Event: "spec_review.passed", ConsumerKind: "agent", ConsumerID: "business-research-agent"},
		{Event: "spec.approved", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
		{Event: "spec.validation_requested", ConsumerKind: "agent", ConsumerID: "spec-auditor"},
		{Event: "spec.validation_passed", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
		{Event: "cto.spec_review_requested", ConsumerKind: "agent", ConsumerID: "factory-cto"},
		{Event: "cto.spec_approved", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
		{Event: "brand.candidates_ready", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
		{Event: "validation.package_ready", ConsumerKind: "agent", ConsumerID: "validation-coordinator"},
		{Event: "vertical.ready_for_review", ConsumerKind: "runtime", ConsumerID: "pipeline-coordinator"},
	}

	subIndex := map[string]map[string]struct{}{}
	for _, a := range agents {
		set := map[string]struct{}{}
		for _, s := range a.Subscriptions {
			set[s] = struct{}{}
		}
		subIndex[a.Role] = set
	}

	out := make([]wiringResult, 0, len(edges))
	for _, e := range edges {
		if len(producersByEvent[e.Event]) == 0 {
			out = append(out, wiringResult{
				Severity: wiringFail,
				Message:  fmt.Sprintf("%s has no producer", e.Event),
			})
			continue
		}

		switch e.ConsumerKind {
		case "runtime":
			if _, ok := interceptEvents[e.Event]; !ok {
				if _, managed := runtimeManagedEvents[e.Event]; managed {
					out = append(out, wiringResult{
						Severity: wiringPass,
						Message:  fmt.Sprintf("%s -> runtime managed chain link is wired", e.Event),
					})
					continue
				}
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s should be consumed by runtime interceptor but no interceptPolicy case exists", e.Event),
				})
				continue
			}
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  fmt.Sprintf("%s -> runtime interceptor chain link is wired", e.Event),
			})
		case "agent":
			subs := subIndex[e.ConsumerID]
			if len(subs) == 0 {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s -> %s chain broken: consumer role not found in roster", e.Event, e.ConsumerID),
				})
				continue
			}
			matched := false
			for sub := range subs {
				if matchesSubscription(sub, e.Event) {
					matched = true
					break
				}
			}
			if !matched {
				out = append(out, wiringResult{
					Severity: wiringFail,
					Message:  fmt.Sprintf("%s -> %s chain broken: consumer not subscribed", e.Event, e.ConsumerID),
				})
				continue
			}
			out = append(out, wiringResult{
				Severity: wiringPass,
				Message:  fmt.Sprintf("%s -> %s chain link is wired", e.Event, e.ConsumerID),
			})
		}
	}

	return out
}

func verifyOrphanEmissions(agents []wiringAgent, producersByEvent map[string][]producerRef, interceptEvents map[string]struct{}, runtimeManagedEvents map[string]struct{}, contracts map[string]wiringEventContract) []wiringResult {
	out := make([]wiringResult, 0, 64)
	events := make([]string, 0, len(producersByEvent))
	for evt := range producersByEvent {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		events = append(events, evt)
	}
	sort.Strings(events)

	for _, evt := range events {
		if c, ok := contracts[evt]; ok {
			switch normalizeDeliveryChannel(c.DeliveryChannel) {
			case "audit", "mailbox", "agent_message":
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("ORPHAN_EMISSION: %s exempt via delivery_channel=%s", evt, normalizeDeliveryChannel(c.DeliveryChannel)),
				})
				continue
			case "eventbus_routing_table":
				out = append(out, wiringResult{
					Severity: wiringPass,
					Message:  fmt.Sprintf("ORPHAN_EMISSION: %s routing-table event (consumer sub check skipped)", evt),
				})
				continue
			case "runtime":
				if _, ok := interceptEvents[evt]; ok {
					out = append(out, wiringResult{
						Severity: wiringPass,
						Message:  fmt.Sprintf("ORPHAN_EMISSION: %s runtime-channel event is intercepted", evt),
					})
				} else if _, ok := runtimeManagedEvents[evt]; ok {
					out = append(out, wiringResult{
						Severity: wiringPass,
						Message:  fmt.Sprintf("ORPHAN_EMISSION: %s runtime-channel event is consumed by runtime manager loop", evt),
					})
				} else {
					out = append(out, wiringResult{
						Severity: wiringFail,
						Message:  fmt.Sprintf("ORPHAN_EMISSION: %s delivery_channel=runtime but no interceptor handler exists", evt),
					})
				}
				continue
			case "eventbus_static":
				if contractHasConsumer(c) {
					out = append(out, wiringResult{
						Severity: wiringPass,
						Message:  fmt.Sprintf("ORPHAN_EMISSION: %s has static consumer in event-catalog contract", evt),
					})
					continue
				}
			}
		}
		if !isWiringCriticalEvent(evt) {
			continue
		}
		if eventHasSubscriber(agents, evt) {
			continue
		}
		if _, ok := interceptEvents[evt]; ok {
			continue
		}
		producers := dedupeProducerIDs(producersByEvent[evt])
		out = append(out, wiringResult{
			Severity: wiringWarn,
			Message:  fmt.Sprintf("ORPHAN_EMISSION: %s emitted by [%s] has no subscriber and no runtime interceptor", evt, strings.Join(producers, ", ")),
		})
	}
	return out
}

func verifySchemaCatalogConsistency(agents []wiringAgent, schemas map[string]wiringSchema, producersByEvent map[string][]producerRef, interceptEvents map[string]struct{}, contracts map[string]wiringEventContract) []wiringResult {
	catalog := map[string]struct{}{}
	if len(contracts) > 0 {
		for evt := range contracts {
			evt = strings.TrimSpace(evt)
			if evt != "" {
				catalog[evt] = struct{}{}
			}
		}
	} else {
		for evt := range producersByEvent {
			evt = strings.TrimSpace(evt)
			if evt != "" {
				catalog[evt] = struct{}{}
			}
		}
		for _, a := range agents {
			for _, sub := range a.Subscriptions {
				sub = strings.TrimSpace(sub)
				if sub == "" || strings.Contains(sub, "*") {
					continue
				}
				catalog[sub] = struct{}{}
			}
		}
		for evt := range interceptEvents {
			evt = strings.TrimSpace(evt)
			if evt != "" {
				catalog[evt] = struct{}{}
			}
		}
	}

	out := make([]wiringResult, 0, 32)
	events := make([]string, 0, len(schemas))
	for evt := range schemas {
		evt = strings.TrimSpace(evt)
		if evt != "" {
			events = append(events, evt)
		}
	}
	sort.Strings(events)
	for _, evt := range events {
		if _, ok := catalog[evt]; ok {
			continue
		}
		out = append(out, wiringResult{
			Severity: wiringWarn,
			Message:  fmt.Sprintf("SCHEMA_NO_CATALOG: %s has EventSchemaRegistry entry but no producer/subscriber/interceptor reference", evt),
		})
	}
	return out
}

func eventHasSubscriber(agents []wiringAgent, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	for _, a := range agents {
		for _, sub := range a.Subscriptions {
			if matchesSubscription(sub, eventType) {
				return true
			}
		}
	}
	return false
}

func dedupeProducerIDs(in []producerRef) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, p := range in {
		id := strings.TrimSpace(p.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func isWiringCriticalEvent(eventType string) bool {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	if eventType == "" {
		return false
	}
	criticalPrefixes := []string{
		"scan.",
		"market_research.",
		"trend_research.",
		"scanner.",
		"category.",
		"trend.",
		"source.",
		"campaign.",
		"vertical.",
		"score.",
		"scoring.",
		"validation.",
		"research.",
		"spec.",
		"brand.",
		"cto.",
		"dedup.",
		"synthesis.",
		"timer.portfolio_digest",
	}
	for _, p := range criticalPrefixes {
		if strings.HasPrefix(eventType, p) {
			return true
		}
	}
	return false
}

func normalizeDeliveryChannel(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	switch v {
	case "eventbus_static", "eventbus-routing-static":
		return "eventbus_static"
	case "eventbus_routing_table", "eventbus-routing-table":
		return "eventbus_routing_table"
	case "runtime":
		return "runtime"
	case "audit":
		return "audit"
	case "mailbox":
		return "mailbox"
	case "agent_message":
		return "agent_message"
	default:
		return v
	}
}

func subscriptionNeedsChecks(subscription string, contracts map[string]wiringEventContract) bool {
	subscription = strings.TrimSpace(subscription)
	if subscription == "" || len(contracts) == 0 {
		return true
	}
	for evt, c := range contracts {
		if !matchesSubscription(subscription, evt) {
			continue
		}
		switch normalizeDeliveryChannel(c.DeliveryChannel) {
		case "audit", "mailbox", "agent_message", "eventbus_routing_table":
			return false
		}
	}
	return true
}

func contractHasConsumer(c wiringEventContract) bool {
	switch t := c.Consumer.(type) {
	case string:
		trim := strings.TrimSpace(strings.ToLower(t))
		return trim != "" && trim != "-" && trim != "none"
	case []any:
		for _, item := range t {
			if s := strings.TrimSpace(strings.ToLower(asString(item))); s != "" && s != "-" && s != "none" {
				return true
			}
		}
	case []string:
		for _, item := range t {
			if s := strings.TrimSpace(strings.ToLower(item)); s != "" && s != "-" && s != "none" {
				return true
			}
		}
	}
	return false
}

func loadWiringEventContracts(repoRoot string) map[string]wiringEventContract {
	if c, ok := loadWiringEventContractsFromRoot(repoRoot); ok {
		return c
	}
	bestContracts := map[string]wiringEventContract{}
	bestPatch := -1
	if c, patch, ok := loadWiringEventContractsFromExtracted(repoRoot); ok {
		bestContracts = c
		bestPatch = patch
	}
	if c, patch, ok := loadWiringEventContractsFromTar(repoRoot); ok && patch > bestPatch {
		bestContracts = c
		bestPatch = patch
	}
	if bestPatch < 0 {
		return map[string]wiringEventContract{}
	}
	return bestContracts
}

func loadWiringSystemNodes(repoRoot string) map[string]contractComplianceSystemNode {
	path := filepath.Join(repoRoot, "contracts", "system-nodes.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("read %s: %v", path, err))
	}
	var nodes map[string]contractComplianceSystemNode
	if err := yaml.Unmarshal(raw, &nodes); err != nil {
		panic(fmt.Sprintf("parse %s: %v", path, err))
	}
	return nodes
}

func loadWiringEventContractsFromRoot(repoRoot string) (map[string]wiringEventContract, bool) {
	contractsPath := filepath.Join(repoRoot, "contracts", "event-catalog.yaml")
	raw, err := os.ReadFile(contractsPath)
	if err != nil {
		return nil, false
	}
	c := parseWiringEventCatalogYAML(raw)
	return c, len(c) > 0
}

func loadWiringEventContractsFromExtracted(repoRoot string) (map[string]wiringEventContract, int, bool) {
	base := filepath.Join(repoRoot, "docs", "specs")
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, -1, false
	}
	bestPatch := -1
	bestPath := ""
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := strings.TrimSpace(e.Name())
		if !strings.HasPrefix(name, "empireai-v2.0.") {
			continue
		}
		patch, ok := parseWiringPatchVersion(strings.TrimPrefix(name, "empireai-v2.0."))
		if !ok || patch > 9999 {
			continue
		}
		contractsPath := filepath.Join(base, name, "contracts", "event-catalog.yaml")
		if _, statErr := os.Stat(contractsPath); statErr != nil {
			continue
		}
		if patch > bestPatch {
			bestPatch = patch
			bestPath = contractsPath
		}
	}
	if bestPath == "" {
		return nil, -1, false
	}
	raw, err := os.ReadFile(bestPath)
	if err != nil {
		return nil, -1, false
	}
	c := parseWiringEventCatalogYAML(raw)
	return c, bestPatch, len(c) > 0
}

func loadWiringEventContractsFromTar(repoRoot string) (map[string]wiringEventContract, int, bool) {
	base := filepath.Join(repoRoot, "docs", "specs")
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, -1, false
	}
	bestPatch := -1
	bestTar := ""
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.TrimSpace(e.Name())
		if !strings.HasPrefix(name, "empireai-v2.0.") || !strings.HasSuffix(name, ".tar") {
			continue
		}
		patchPart := strings.TrimSuffix(strings.TrimPrefix(name, "empireai-v2.0."), ".tar")
		patch, ok := parseWiringPatchVersion(patchPart)
		if !ok || patch > 9999 {
			continue
		}
		if patch > bestPatch {
			bestPatch = patch
			bestTar = filepath.Join(base, name)
		}
	}
	if bestTar == "" {
		return nil, -1, false
	}
	f, err := os.Open(bestTar)
	if err != nil {
		return nil, -1, false
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil || hdr == nil {
			return nil, -1, false
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		if !strings.HasSuffix(hdr.Name, "/contracts/event-catalog.yaml") {
			continue
		}
		raw, readErr := io.ReadAll(tr)
		if readErr != nil {
			return nil, -1, false
		}
		c := parseWiringEventCatalogYAML(raw)
		return c, bestPatch, len(c) > 0
	}
	return nil, -1, false
}

func parseWiringPatchVersion(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if strings.Contains(raw, ".") {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseWiringEventCatalogYAML(raw []byte) map[string]wiringEventContract {
	obj := map[string]wiringEventContract{}
	if len(raw) == 0 {
		return obj
	}

	root := map[string]any{}
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return map[string]wiringEventContract{}
	}
	for evt, node := range root {
		eventType := strings.TrimSpace(evt)
		if eventType == "" || strings.HasPrefix(eventType, "_") {
			continue
		}
		fields, ok := node.(map[string]any)
		if !ok {
			continue
		}
		contract := wiringEventContract{
			EventType:       eventType,
			Emitter:         fields["emitter"],
			Consumer:        fields["consumer"],
			Intercepted:     toBool(fields["intercepted"]),
			Passthrough:     fields["passthrough"],
			Routing:         strings.TrimSpace(asString(fields["routing"])),
			DeliveryChannel: normalizeDeliveryChannel(asString(fields["delivery_channel"])),
		}
		obj[eventType] = contract
	}
	return obj
}

func loadRuntimeManagedEvents(repoRoot string) map[string]struct{} {
	out := map[string]struct{}{}
	paths := []string{
		filepath.Join(repoRoot, "internal", "runtime", "pipeline", "scan_campaign_manager.go"),
		filepath.Join(repoRoot, "internal", "runtime", "pipeline", "scoring_node.go"),
	}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		for _, m := range eventTypeCallRe.FindAllStringSubmatch(string(raw), -1) {
			if len(m) < 2 {
				continue
			}
			evt := strings.TrimSpace(m[1])
			if evt == "" || strings.Contains(evt, "*") {
				continue
			}
			out[evt] = struct{}{}
		}
	}
	return out
}

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.TrimSpace(strings.ToLower(t))
		return s == "1" || s == "true" || s == "yes"
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

func loadWiringAgentsFromRoster(agentsDir string) ([]wiringAgent, error) {
	rosterPath := filepath.Join(agentsDir, "roster.yaml")
	raw, err := os.ReadFile(rosterPath)
	if err != nil {
		return nil, err
	}
	var roster rosterYAML
	if err := yaml.Unmarshal(raw, &roster); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(roster.Agents))
	for k := range roster.Agents {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]wiringAgent, 0, len(keys))
	for _, key := range keys {
		entry := roster.Agents[key]
		cfgPath := strings.TrimSpace(entry.ConfigPath)
		if cfgPath == "" {
			continue
		}
		full := filepath.Clean(filepath.Join(agentsDir, cfgPath))
		doc, err := os.ReadFile(full)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", full, err)
		}
		obj := map[string]any{}
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, fmt.Errorf("parse %s: %w", full, err)
		}
		id := strings.TrimSpace(asString(obj["id"]))
		if id == "" {
			id = strings.TrimSuffix(filepath.Base(full), filepath.Ext(full))
		}
		role := strings.TrimSpace(asString(obj["role"]))
		if role == "" {
			role = id
		}
		systemPrompt := strings.TrimSpace(asString(obj["system_prompt"]))
		if systemPrompt == "" {
			if contractPrompt, found, err := promptcontracts.Load(id, ""); err == nil && found {
				systemPrompt = strings.TrimSpace(contractPrompt)
			}
		}
		out = append(out, wiringAgent{
			ID:            id,
			Role:          role,
			Path:          full,
			Subscriptions: toStringSliceWiring(obj["subscriptions"]),
			SystemPrompt:  systemPrompt,
		})
	}
	return out, nil
}

func loadWiringSchemasFromRegistry() map[string]wiringSchema {
	registry := runtimecontracts.EventSchemaRegistry()
	out := make(map[string]wiringSchema, len(registry))
	for evt, schema := range registry {
		required := map[string]struct{}{}
		for _, r := range requiredList(schema.Schema["required"]) {
			required[r] = struct{}{}
		}
		props := map[string]struct{}{}
		for k := range schemaProperties(schema.Schema["properties"]) {
			props[k] = struct{}{}
		}
		for r := range required {
			props[r] = struct{}{}
		}
		out[evt] = wiringSchema{
			EventType: evt,
			Required:  required,
			Props:     props,
		}
	}
	return out
}

func buildToolToEventMap() map[string]string {
	registry := runtimecontracts.EventSchemaRegistry()
	out := map[string]string{}
	for evt := range registry {
		out[runtimetools.EmitToolName(evt)] = evt
	}
	return out
}

func collectRuntimeEmitSites(runtimeDir string) ([]wiringEmitSite, error) {
	files := make([]string, 0, 64)
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
	out := make([]wiringEmitSite, 0, 256)
	for _, path := range files {
		fileNode, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		for _, decl := range fileNode.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			funcName := fn.Name.Name
			varMap := collectFunctionMapVars(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if s, ok := extractPCPublishSite(call, path, funcName, fset, varMap); ok {
					out = append(out, s)
				}
				if s, ok := extractBusPublishSite(call, path, funcName, fset, varMap); ok {
					out = append(out, s)
				}
				return true
			})
		}
	}
	return out, nil
}

func collectFunctionMapVars(fn *ast.FuncDecl) map[string]wiringEmitSite {
	out := map[string]wiringEmitSite{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, lhs := range assign.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok || id == nil || strings.TrimSpace(id.Name) == "" || id.Name == "_" {
				continue
			}
			if i >= len(assign.Rhs) {
				continue
			}
			fields, dynamic, typed := resolveFieldSet(assign.Rhs[i], out)
			out[id.Name] = wiringEmitSite{Fields: fields, Dynamic: dynamic, TypedPayload: typed}
		}
		return true
	})
	return out
}

func extractPCPublishSite(call *ast.CallExpr, path, funcName string, fset *token.FileSet, varMap map[string]wiringEmitSite) (wiringEmitSite, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel == nil || sel.Sel == nil || sel.Sel.Name != "publish" {
		return wiringEmitSite{}, false
	}
	xid, ok := sel.X.(*ast.Ident)
	if !ok || xid == nil || xid.Name != "pc" {
		return wiringEmitSite{}, false
	}
	if len(call.Args) < 4 {
		return wiringEmitSite{}, false
	}
	eventType, ok := stringLiteral(call.Args[1])
	if !ok {
		return wiringEmitSite{}, false
	}
	fields, dynamic, typed := resolveFieldSet(call.Args[3], varMap)
	pos := fset.Position(call.Pos())
	return wiringEmitSite{
		EventType:    strings.TrimSpace(eventType),
		File:         path,
		Line:         pos.Line,
		FuncName:     funcName,
		Fields:       fields,
		Dynamic:      dynamic,
		TypedPayload: typed,
		Source:       "pipeline-coordinator",
	}, true
}

func extractBusPublishSite(call *ast.CallExpr, path, funcName string, fset *token.FileSet, varMap map[string]wiringEmitSite) (wiringEmitSite, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel == nil || sel.Sel == nil || sel.Sel.Name != "Publish" {
		return wiringEmitSite{}, false
	}
	if len(call.Args) < 2 {
		return wiringEmitSite{}, false
	}
	ev, ok := call.Args[1].(*ast.CompositeLit)
	if !ok {
		return wiringEmitSite{}, false
	}
	eventType := ""
	source := "runtime"
	fields := map[string]struct{}{}
	dynamic := true
	typed := false
	for _, elt := range ev.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		keyID, ok := kv.Key.(*ast.Ident)
		if !ok || keyID == nil {
			continue
		}
		switch keyID.Name {
		case "Type":
			eventType = parseEventTypeExpr(kv.Value)
		case "SourceAgent":
			if s, ok := stringLiteral(kv.Value); ok && strings.TrimSpace(s) != "" {
				source = s
			}
		case "Payload":
			fields, dynamic, typed = resolvePayloadFieldSet(kv.Value, varMap)
		}
	}
	if strings.TrimSpace(eventType) == "" {
		return wiringEmitSite{}, false
	}
	pos := fset.Position(call.Pos())
	return wiringEmitSite{
		EventType:    strings.TrimSpace(eventType),
		File:         path,
		Line:         pos.Line,
		FuncName:     funcName,
		Fields:       fields,
		Dynamic:      dynamic,
		TypedPayload: typed,
		Source:       source,
	}, true
}

func resolvePayloadFieldSet(expr ast.Expr, varMap map[string]wiringEmitSite) (map[string]struct{}, bool, bool) {
	if call, ok := expr.(*ast.CallExpr); ok && callName(call.Fun) == "mustJSON" && len(call.Args) > 0 {
		return resolveFieldSet(call.Args[0], varMap)
	}
	return resolveFieldSet(expr, varMap)
}

func resolveFieldSet(expr ast.Expr, varMap map[string]wiringEmitSite) (map[string]struct{}, bool, bool) {
	switch t := expr.(type) {
	case *ast.CompositeLit:
		if _, ok := t.Type.(*ast.MapType); ok {
			fields := map[string]struct{}{}
			for _, elt := range t.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				k, ok := stringLiteral(kv.Key)
				if !ok || strings.TrimSpace(k) == "" {
					continue
				}
				fields[strings.TrimSpace(k)] = struct{}{}
			}
			return fields, false, false
		}
		return map[string]struct{}{}, true, true
	case *ast.Ident:
		if known, ok := varMap[t.Name]; ok {
			return copySet(known.Fields), known.Dynamic, known.TypedPayload
		}
		return map[string]struct{}{}, true, false
	case *ast.CallExpr:
		name := callName(t.Fun)
		switch {
		case name == "payloadMap" && len(t.Args) > 0:
			fields, dynamic, _ := resolveFieldSet(t.Args[0], varMap)
			return fields, dynamic, true
		case name == "mustJSON" && len(t.Args) > 0:
			return resolveFieldSet(t.Args[0], varMap)
		case name == "json.Marshal" && len(t.Args) > 0:
			return resolveFieldSet(t.Args[0], varMap)
		case strings.HasPrefix(name, "pc.build"):
			return typedBuilderFields(name), false, true
		default:
			return map[string]struct{}{}, true, false
		}
	default:
		return map[string]struct{}{}, true, false
	}
}

func typedBuilderFields(name string) map[string]struct{} {
	switch {
	case strings.HasSuffix(name, "buildValidationStartedPayload"):
		return setOf("vertical_id", "vertical_name", "geography", "scoring_context")
	case strings.HasSuffix(name, "buildBrandRequestedPayload"):
		return setOf("vertical_id", "vertical_name", "geography", "business_brief")
	case strings.HasSuffix(name, "buildScanAssignedPayload"):
		return setOf("scan_id", "campaign_id", "mode", "geography", "geography_id", "taxonomy_categories", "priority", "campaign_context", "directive_id", "strategic_context", "requested_at", "planned_shards")
	case strings.HasSuffix(name, "buildSynthesisNeededPayload"):
		return setOf("conflicting_reports", "context")
	case strings.HasSuffix(name, "buildDedupAmbiguousPayload"):
		return setOf("dedup_id", "new_candidate", "existing_vertical", "similarity")
	case strings.HasSuffix(name, "buildVerticalDiscoveredPayload"):
		return setOf("vertical_id", "vertical_name", "name", "geography", "geographic_scope", "mode", "scan_id", "campaign_id", "signal_strength", "discovery_source", "raw_signals", "discovery_context")
	case strings.HasSuffix(name, "buildScanCompletedPayload"):
		return setOf("scan_id", "campaign_id", "mode", "geography", "reports_received", "agents_expected", "agents_complete", "verticals_discovered", "verticals_skipped", "pending_dedup", "timed_out", "shards_total", "shards_completed", "shards_failed")
	case strings.HasSuffix(name, "buildScoringRequestedPayload"):
		return setOf("vertical_id", "vertical_name", "geography", "mode", "rubric", "dimensions_requested", "discovery_context")
	case strings.HasSuffix(name, "buildScoringContestedPayload"):
		return setOf("vertical_id", "dimension", "scores", "evidence", "spread", "rubric", "mode")
	case strings.HasSuffix(name, "buildVerticalScoredPayload"):
		return setOf("vertical_id", "result", "reason", "composite_score", "viability_score", "market_score", "dimensions", "rubric", "partial", "mode", "vertical_name", "geography")
	case strings.HasSuffix(name, "buildVerticalShortlistedPayload"):
		return setOf("vertical_id", "composite_score", "viability_score", "scoring_payload")
	case strings.HasSuffix(name, "buildVerticalMarginalPayload"):
		return setOf("vertical_id", "composite_score", "viability_score", "dimensions", "promotion_eligible")
	case strings.HasSuffix(name, "buildVerticalRejectedPayload"):
		return setOf("vertical_id", "reason")
	case strings.HasSuffix(name, "buildValidationPackageReadyPayload"):
		return setOf("vertical_id", "research", "spec", "cto_notes", "brand")
	case strings.HasSuffix(name, "buildSpecValidationRequestedPayload"):
		return setOf("vertical_id", "spec_content", "spec_tier")
	case strings.HasSuffix(name, "buildCTOSpecReviewRequestedPayload"):
		return setOf("vertical_id", "mvp_spec", "business_brief", "vertical_context")
	case strings.HasSuffix(name, "buildSpecRevisionRequestedPayload"):
		return setOf("vertical_id", "cto_feedback")
	case strings.HasSuffix(name, "buildValidationMoreDataPayload"):
		return setOf("vertical_id", "questions")
	case strings.HasSuffix(name, "buildBrandRevisionNeededPayload"):
		return setOf("vertical_id", "vertical_name", "geography", "feedback", "brand")
	case strings.HasSuffix(name, "buildVerticalKilledPayload"):
		return setOf("vertical_id", "vertical_name", "geography", "source_event", "priority", "reason")
	default:
		return map[string]struct{}{}
	}
}

func parsePipelineInterceptorCoverage(pipelinePath string) (map[string]struct{}, map[string]struct{}, map[string]string, error) {
	fset := token.NewFileSet()
	fileNode, err := parser.ParseFile(fset, pipelinePath, nil, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	interceptEvents := map[string]struct{}{}
	handleEvents := map[string]struct{}{}
	handlerByEvent := map[string]string{}

	for _, decl := range fileNode.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		switch fn.Name.Name {
		case "interceptPolicy":
			collectSwitchCases(fn.Body, interceptEvents, nil)
		case "handleEvent":
			collectSwitchCases(fn.Body, handleEvents, handlerByEvent)
		}
	}

	workflowNodesPath := filepath.Join(filepath.Dir(pipelinePath), "workflow_nodes.go")
	workflowNodesFile, err := parser.ParseFile(fset, workflowNodesPath, nil, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, decl := range workflowNodesFile.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name == nil {
			continue
		}
		if fn.Name.Name != "workflowNodePolicyOverlay" && fn.Name.Name != "workflowNodeRuntimePolicyEvents" {
			continue
		}
		collectMapStringKeys(fn.Body, interceptEvents)
	}

	workflowRuntimePaths, err := filepath.Glob(filepath.Join(filepath.Dir(pipelinePath), "workflow_node*.go"))
	if err != nil {
		return nil, nil, nil, err
	}
	for _, runtimePath := range workflowRuntimePaths {
		workflowExecutorsFile, err := parser.ParseFile(fset, runtimePath, nil, 0)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, decl := range workflowExecutorsFile.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name == nil || fn.Name.Name != "Handle" {
				continue
			}
			collectSwitchCases(fn.Body, handleEvents, handlerByEvent)
		}
	}

	return interceptEvents, handleEvents, handlerByEvent, nil
}

func collectMapStringKeys(body *ast.BlockStmt, out map[string]struct{}) {
	ast.Inspect(body, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		if s, ok := stringLiteral(kv.Key); ok && strings.TrimSpace(s) != "" {
			out[strings.TrimSpace(s)] = struct{}{}
		}
		return true
	})
}

func collectSwitchCases(body *ast.BlockStmt, eventsOut map[string]struct{}, handlers map[string]string) {
	ast.Inspect(body, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok {
			return true
		}
		for _, c := range sw.Body.List {
			cc, ok := c.(*ast.CaseClause)
			if !ok {
				continue
			}
			if len(cc.List) == 0 {
				continue
			}
			handlerName := ""
			if handlers != nil {
				handlerName = firstHandlerCallName(cc.Body)
			}
			for _, expr := range cc.List {
				if s, ok := stringLiteral(expr); ok && strings.TrimSpace(s) != "" {
					eventsOut[strings.TrimSpace(s)] = struct{}{}
					if handlers != nil && strings.TrimSpace(handlerName) != "" {
						handlers[strings.TrimSpace(s)] = handlerName
					}
				}
			}
		}
		return false
	})
}

func firstHandlerCallName(stmts []ast.Stmt) string {
	for _, st := range stmts {
		exprStmt, ok := st.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := exprStmt.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel == nil || sel.Sel == nil {
			continue
		}
		if xid, ok := sel.X.(*ast.Ident); ok && xid != nil && xid.Name == "pc" {
			return sel.Sel.Name
		}
	}
	return ""
}

func emittedEventsByHandler(runtimeEmitted map[string][]wiringEmitSite, handler string) []string {
	set := map[string]struct{}{}
	for evt, sites := range runtimeEmitted {
		for _, s := range sites {
			if s.FuncName == handler {
				set[evt] = struct{}{}
			}
		}
	}
	return sortedKeys(set)
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
	if strings.HasPrefix(raw, "`") && strings.HasSuffix(raw, "`") {
		return strings.Trim(raw, "`"), true
	}
	if strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) {
		return strings.Trim(raw, `"`), true
	}
	return "", false
}

func extractEmitToolRefs(prompt string) []string {
	matches := emitRefRe.FindAllString(prompt, -1)
	if len(matches) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, m := range matches {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		set[m] = struct{}{}
	}
	return sortedKeys(set)
}

func matchingEvents(subscription string, allEvents []string) []string {
	out := make([]string, 0, 8)
	for _, evt := range allEvents {
		if matchesSubscription(subscription, evt) {
			out = append(out, evt)
		}
	}
	return out
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

func promptMentionsSubscription(prompt, sub string, matched []string) bool {
	low := strings.ToLower(prompt)
	if strings.Contains(low, strings.ToLower(strings.TrimSpace(sub))) {
		return true
	}
	if strings.Contains(sub, "*") {
		prefix := strings.TrimSuffix(strings.ToLower(sub), "*")
		prefix = strings.TrimSpace(prefix)
		if prefix != "" && strings.Contains(low, prefix) {
			return true
		}
		if strings.Contains(strings.ToLower(sub), ".*.scan_assigned") && strings.Contains(low, "{type}.scan_assigned") {
			return true
		}
	}
	for _, evt := range matched {
		if strings.Contains(low, strings.ToLower(evt)) {
			return true
		}
	}
	// Common non-literal phrasing in prompts.
	if strings.Contains(strings.ToLower(sub), "scan_assigned") && strings.Contains(low, "assignment") {
		return true
	}
	if strings.Contains(strings.ToLower(sub), "mailbox.") && strings.Contains(low, "mailbox") {
		return true
	}
	if strings.Contains(strings.ToLower(sub), "budget.") && strings.Contains(low, "budget") {
		return true
	}
	return false
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
		targets := []string{section}
		if len(inputScoped) > 0 {
			targets = inputScoped
		}
		for _, txt := range targets {
			for _, field := range candidates {
				if mentionsField(txt, field) {
					found[field] = struct{}{}
				}
			}
		}
	}
	return sortedKeys(found)
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
		var b strings.Builder
		b.WriteString(lines[i])
		b.WriteByte('\n')
		for j := i + 1; j < len(lines); j++ {
			cur := lines[j]
			trim := strings.TrimSpace(cur)
			if trim == "" {
				b.WriteByte('\n')
				continue
			}
			if isEventHeaderLine(trim) {
				break
			}
			b.WriteString(cur)
			b.WriteByte('\n')
		}
		sections = append(sections, b.String())
	}
	return sections
}

func isEventHeaderLine(line string) bool {
	line = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "-"), "*"))
	if !strings.Contains(line, ":") {
		return false
	}
	head := strings.TrimSpace(strings.SplitN(line, ":", 2)[0])
	if head == "" {
		return false
	}
	for _, r := range head {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' || r == '*' {
			continue
		}
		return false
	}
	return strings.Contains(head, ".")
}

func isInputHintLine(line string) bool {
	low := strings.ToLower(strings.TrimSpace(line))
	if low == "" {
		return false
	}
	if strings.Contains(low, "contains") || strings.Contains(low, "payload") || strings.Contains(low, "read ") || strings.Contains(low, "from runtime") {
		return true
	}
	if strings.Contains(low, "from payload") || strings.Contains(low, "input") {
		return true
	}
	return false
}

func mentionsField(text, field string) bool {
	text = strings.ToLower(text)
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "" {
		return false
	}
	if regexes, ok := fieldAliasRegexWiring[field]; ok {
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

func makeFieldCandidates(schema wiringSchema, runtimeSites []wiringEmitSite) []string {
	set := map[string]struct{}{}
	for p := range schema.Props {
		set[p] = struct{}{}
	}
	for _, site := range runtimeSites {
		for f := range site.Fields {
			set[f] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func missingFromSet(expected []string, actual map[string]struct{}) []string {
	missing := make([]string, 0, len(expected))
	for _, key := range expected {
		if _, ok := actual[key]; !ok {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	return missing
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func copySet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func setOf(values ...string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			out[v] = struct{}{}
		}
	}
	return out
}

func toStringSliceWiring(v any) []string {
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

func isWiringStrictMode() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("EMPIRE_WIRING_STRICT")))
	if raw == "" {
		return true
	}
	if raw == "0" || raw == "false" || raw == "no" || raw == "off" {
		return false
	}
	return raw == "1" || raw == "true" || raw == "yes" || raw == "on"
}

func isWiringPromptLintStrict() bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("EMPIRE_WIRING_PROMPT_LINT_STRICT")))
	if raw == "" {
		return false
	}
	if raw == "0" || raw == "false" || raw == "no" || raw == "off" {
		return false
	}
	return raw == "1" || raw == "true" || raw == "yes" || raw == "on"
}
