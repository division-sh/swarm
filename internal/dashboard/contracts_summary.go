package dashboard

import (
	"os"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"

	"gopkg.in/yaml.v3"
)

type verificationGatesDocument struct {
	SpecVersion string                    `yaml:"spec_version"`
	Gates       []verificationGateSummary `yaml:"gates"`
}

type verificationGateSummary struct {
	ID       string `yaml:"id"`
	Category string `yaml:"category"`
	Priority string `yaml:"priority"`
	Type     string `yaml:"type"`
}

func dashboardRepoRoot() string {
	candidates := make([]string, 0, 8)
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates,
			wd,
			filepath.Join(wd, ".."),
			filepath.Join(wd, "..", ".."),
		)
	}
	if _, file, _, ok := goruntime.Caller(0); ok {
		candidates = append(candidates,
			filepath.Join(filepath.Dir(file), "..", ".."),
		)
	}
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		if _, err := os.Stat(filepath.Join(abs, "contracts", "workflow-schema.yaml")); err == nil {
			return abs
		}
	}
	return ""
}

func dashboardContractBundle() (*runtimecontracts.WorkflowContractBundle, string, error) {
	repoRoot := dashboardRepoRoot()
	if repoRoot == "" {
		return nil, "", os.ErrNotExist
	}
	bundle, err := runtimecontracts.LoadEmpireWorkflowContractBundle(repoRoot)
	return bundle, repoRoot, err
}

func dashboardContractSummary() map[string]any {
	bundle, repoRoot, err := dashboardContractBundle()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}

	stages := make([]map[string]any, 0, len(bundle.Workflow.Workflow.Stages))
	stageIDs := make([]string, 0, len(bundle.Workflow.Workflow.Stages))
	stagePhaseMap := map[string]string{}
	phaseCounts := map[string]int{}
	validationStages := make([]string, 0, 8)
	for _, stage := range bundle.Workflow.Workflow.Stages {
		id := strings.TrimSpace(stage.ID)
		phase := strings.TrimSpace(stage.Phase)
		desc := strings.TrimSpace(stage.Description)
		if id == "" {
			continue
		}
		stageIDs = append(stageIDs, id)
		stagePhaseMap[id] = phase
		if phase != "" {
			phaseCounts[phase]++
		}
		if isValidationStageID(id) {
			validationStages = append(validationStages, id)
		}
		stages = append(stages, map[string]any{
			"id":          id,
			"phase":       phase,
			"description": desc,
		})
	}

	transitions, transitionsByTrigger := workflowTransitionSummaries(bundle)
	transitionOwnerCounts := map[string]int{}
	for _, transition := range transitions {
		owner := strings.TrimSpace(asString(transition["node"]))
		if owner != "" {
			transitionOwnerCounts[owner]++
		}
	}
	transitionTriggerCounts := map[string]int{}
	for trigger, entries := range transitionsByTrigger {
		transitionTriggerCounts[trigger] = len(entries)
	}

	timers := make([]map[string]any, 0, len(bundle.Workflow.Workflow.Timers))
	timerEvents := make([]string, 0, len(bundle.Workflow.Workflow.Timers))
	timerOwnerCounts := map[string]int{}
	for _, timer := range bundle.Workflow.Workflow.Timers {
		id := strings.TrimSpace(timer.ID)
		event := strings.TrimSpace(timer.Event)
		stage := strings.TrimSpace(timer.Stage)
		owner := strings.TrimSpace(timer.Owner)
		timerEvents = appendIfNonEmpty(timerEvents, event)
		if owner != "" {
			timerOwnerCounts[owner]++
		}
		timers = append(timers, map[string]any{
			"id":            id,
			"stage":         stage,
			"event":         event,
			"owner":         owner,
			"action":        strings.TrimSpace(timer.Action),
			"cancellation":  strings.TrimSpace(timer.Cancellation),
			"delay_seconds": timer.DelaySeconds,
			"delay_minutes": timer.DelayMinutes,
			"delay_hours":   timer.DelayHours,
			"delay_days":    timer.DelayDays,
			"recurring":     timer.Recurring,
		})
	}

	workflowStateFields := make([]string, 0, len(bundle.Platform.WorkflowState.Fields))
	for field := range bundle.Platform.WorkflowState.Fields {
		field = strings.TrimSpace(field)
		if field != "" {
			workflowStateFields = append(workflowStateFields, field)
		}
	}
	sort.Strings(workflowStateFields)

	builtinGuards := make([]string, 0, len(bundle.Platform.BuiltinHooks.Guards))
	for _, guard := range bundle.Platform.BuiltinHooks.Guards {
		builtinGuards = appendIfNonEmpty(builtinGuards, strings.TrimSpace(guard.ID))
	}
	sort.Strings(builtinGuards)

	builtinActions := make([]string, 0, len(bundle.Platform.BuiltinHooks.Actions))
	for _, action := range bundle.Platform.BuiltinHooks.Actions {
		builtinActions = appendIfNonEmpty(builtinActions, strings.TrimSpace(action.ID))
	}
	sort.Strings(builtinActions)

	complianceGroups := map[string]int{}
	complianceTotal := 0
	for group, rules := range bundle.Platform.ComplianceRules {
		name := strings.TrimSpace(group)
		if name == "" {
			continue
		}
		complianceGroups[name] = len(rules)
		complianceTotal += len(rules)
	}

	verificationSummary := map[string]any{
		"status": "unavailable",
	}
	if raw, err := os.ReadFile(bundle.Paths.VerificationGatesFile); err == nil {
		var doc verificationGatesDocument
		if yaml.Unmarshal(raw, &doc) == nil {
			priorityCounts := map[string]int{}
			categoryCounts := map[string]int{}
			preview := make([]map[string]any, 0, minInt(8, len(doc.Gates)))
			for _, gate := range doc.Gates {
				priority := strings.TrimSpace(gate.Priority)
				category := strings.TrimSpace(gate.Category)
				if priority != "" {
					priorityCounts[priority]++
				}
				if category != "" {
					categoryCounts[category]++
				}
				if len(preview) < 8 {
					preview = append(preview, map[string]any{
						"id":       strings.TrimSpace(gate.ID),
						"category": category,
						"priority": priority,
						"type":     strings.TrimSpace(gate.Type),
					})
				}
			}
			verificationSummary = map[string]any{
				"status":          "definitions_loaded",
				"spec_version":    strings.TrimSpace(doc.SpecVersion),
				"count":           len(doc.Gates),
				"priority_counts": priorityCounts,
				"category_counts": categoryCounts,
				"preview":         preview,
				"latest_results":  "not_persisted",
			}
		}
	}

	return map[string]any{
		"repo_root": repoRoot,
		"workflow": map[string]any{
			"name":                      strings.TrimSpace(bundle.Workflow.Workflow.Name),
			"version":                   strings.TrimSpace(bundle.Workflow.Workflow.Version),
			"entity":                    strings.TrimSpace(bundle.Workflow.Workflow.Entity),
			"entity_table":              strings.TrimSpace(bundle.Workflow.Workflow.EntityTable),
			"state_field":               strings.TrimSpace(bundle.Workflow.Workflow.StateField),
			"initial_stage":             strings.TrimSpace(bundle.Workflow.Workflow.InitialStage),
			"stages":                    stages,
			"stage_ids":                 stageIDs,
			"stage_phase_map":           stagePhaseMap,
			"phase_counts":              phaseCounts,
			"validation_stages":         validationStages,
			"terminal_stages":           bundle.Workflow.Workflow.TerminalStages,
			"transition_count":          len(bundle.Workflow.Workflow.Transitions),
			"transitions":               transitions,
			"transition_owner_counts":   transitionOwnerCounts,
			"transition_trigger_counts": transitionTriggerCounts,
			"timer_count":               len(bundle.Workflow.Workflow.Timers),
			"timer_events":              timerEvents,
			"timers":                    timers,
			"timer_owner_counts":        timerOwnerCounts,
		},
		"platform": map[string]any{
			"name":                  strings.TrimSpace(bundle.Platform.Platform.Name),
			"version":               strings.TrimSpace(bundle.Platform.Platform.Version),
			"workflow_state_fields": workflowStateFields,
			"builtin_hooks": map[string]any{
				"guards":  builtinGuards,
				"actions": builtinActions,
			},
			"compliance_rule_groups": complianceGroups,
			"compliance_rule_count":  complianceTotal,
		},
		"verification_gates": verificationSummary,
		"paths": map[string]any{
			"workflow_schema":    dashboardRelPath(repoRoot, bundle.Paths.WorkflowSchemaFile),
			"platform_spec":      dashboardRelPath(repoRoot, bundle.Paths.PlatformSpecFile),
			"verification_gates": dashboardRelPath(repoRoot, bundle.Paths.VerificationGatesFile),
			"event_catalog":      dashboardRelPath(repoRoot, bundle.Paths.EventCatalogFile),
			"agent_registry":     dashboardRelPath(repoRoot, bundle.Paths.AgentRegistryFile),
			"system_nodes":       dashboardRelPath(repoRoot, bundle.Paths.SystemNodesFile),
			"guard_registry":     dashboardRelPath(repoRoot, bundle.Paths.GuardRegistryFile),
			"tool_schemas":       dashboardRelPath(repoRoot, bundle.Paths.ToolSchemasFile),
			"policy_definition":  dashboardRelPath(repoRoot, bundle.Paths.PolicyFile),
			"tooling_lock":       dashboardRelPath(repoRoot, bundle.Paths.ToolingLockFile),
			"canonical_ddl":      dashboardRelPath(repoRoot, bundle.Paths.DDLFile),
			"agent_config_map":   dashboardRelPath(repoRoot, bundle.Paths.AgentConfigMapFile),
		},
	}
}

func dashboardRelPath(repoRoot, target string) string {
	repoRoot = strings.TrimSpace(repoRoot)
	target = strings.TrimSpace(target)
	if repoRoot == "" || target == "" {
		return target
	}
	rel, err := filepath.Rel(repoRoot, target)
	if err != nil {
		return target
	}
	return filepath.ToSlash(rel)
}

func workflowTransitionSummaries(bundle *runtimecontracts.WorkflowContractBundle) ([]map[string]any, map[string][]map[string]any) {
	out := make([]map[string]any, 0)
	byTrigger := map[string][]map[string]any{}
	if bundle == nil {
		return out, byTrigger
	}
	for _, transition := range bundle.Workflow.Workflow.Transitions {
		trigger := strings.TrimSpace(transition.Trigger)
		entry := map[string]any{
			"id":                  strings.TrimSpace(transition.ID),
			"from":                normalizeContractStageList(transition.From),
			"to":                  strings.TrimSpace(transition.To),
			"trigger":             trigger,
			"node":                strings.TrimSpace(transition.Node),
			"guards":              transition.Guards,
			"actions":             transition.Actions,
			"allow_terminal_exit": transition.AllowTerminalExit,
		}
		out = append(out, entry)
		if trigger != "" {
			byTrigger[trigger] = append(byTrigger[trigger], entry)
		}
	}
	return out, byTrigger
}

func normalizeContractStageList(raw any) []string {
	switch v := raw.(type) {
	case string:
		stage := strings.TrimSpace(v)
		if stage == "" {
			return nil
		}
		return []string{stage}
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			stage := strings.TrimSpace(asString(item))
			if stage != "" {
				out = append(out, stage)
			}
		}
		return out
	default:
		return nil
	}
}

func isValidationStageID(stage string) bool {
	switch strings.TrimSpace(stage) {
	case "researching", "mvp_speccing", "cto_spec_review", "branding":
		return true
	default:
		return false
	}
}

func appendIfNonEmpty(dst []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return dst
	}
	return append(dst, value)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
