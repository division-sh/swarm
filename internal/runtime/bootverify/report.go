package bootverify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Finding struct {
	CheckID     string
	Severity    string
	Message     string
	Location    string
	Remediation string
	Evidence    []string
}

const (
	SeverityHardInvalidity    = "hard_invalidity"
	SeveritySemanticDriftWarn = "semantic_drift_warning"
	SeverityAuditAnalysis     = "audit_analysis"
	SeverityLintEvidence      = "lint_evidence"
	legacySeverityError       = "error"
	legacySeverityWarning     = "warning"
)

const (
	FindingSurfaceLevelBlocker = "BLOCKER"
	FindingSurfaceLevelWarn    = "WARN"
	FindingSurfaceLevelInfo    = "INFO"
)

var stableHardInvalidityRemediation = map[string]string{
	"agent_permission_validation":             "Grant the required agent permission or remove the unauthorized tool/action from the contract.",
	"agent_prompt_lint_structural":            "Fix the agent prompt lint declaration so it uses supported structural fields, or remove the unsupported prompt lint entry.",
	"accumulator_entity_projection":           "Fix the accumulator projection so it writes declared entity fields through the supported accumulator projection shape.",
	"accumulator_input_producer_path":         "Add an accepted producer/source path for the accumulated input event or change the accumulator to consume an event that has one.",
	"accumulator_timeout_requires_timeout_ms": "Add a positive timeout_ms for timeout completion mode or change the accumulator completion mode.",
	"compute_module_value_rows":               "Fix the compute module value rows so each referenced module and value shape is declared consistently.",
	"condition_expression_validation":         "Fix the condition expression so it references declared fields and uses supported CEL syntax.",
	"condition_payload_alignment":             "Fix condition input references so they read declared payload fields for the triggering event.",
	"config_from_payload_alignment":           "Fix config_from_payload references so they read declared payload fields for the triggering event.",
	"contained_state_operation_compliance":    "Fix contained state operations so they target declared contained-state fields through supported syntax.",
	"credential_key_exists":                   "Configure the required credential or fix access to the credential store used by verifier credential checks.",
	"data_accumulation_expression_validation": "Fix the data_accumulation expression so it references declared fields and uses supported CEL syntax.",
	"dialect_compliance":                      "Replace the unsupported contract dialect form with the promoted platform-spec shape or remove it.",
	"emit_field_expression_validation":        "Fix the emit field expression so it references declared fields and uses supported CEL syntax.",
	"entity_write_target_compliance":          "Update the entity write target to a declared writable entity field or remove the invalid write.",
	"entity_writer_coverage":                  "Add a supported writer for each required entity field or remove/relax the field requirement in the contract.",
	"event_cycle_detection":                   "Break the event cycle by changing one handler emission, transition, or subscription so the static event graph is acyclic.",
	"event_runtime_wiring_validation":         "Add the missing runtime handler/owner for the event or remove the event transition that requires runtime handling.",
	"expression_field_reference_validation":   "Fix the expression field reference so it uses the supported namespace and declared field path.",
	"event_metadata_authority":                "Move platform-owned event metadata out of authored business payload declarations.",
	"fan_out_validation":                      "Fix fan_out so items_from names a declared collection, as names the item binding, identity uses that binding, and the emitted payload carries the identity.",
	"join_validation":                         "Use the canonical staged handler.join contract with typed membership, mandatory timeout, and supported entity/join outcomes.",
	"flow_boundary_create_entity_validation":  "Fix the cross-flow create_entity declaration so it preserves the receiving flow boundary.",
	"flow_data_access_validation":             "Use the supported flow data access form for the referenced field or remove the unsupported access.",
	"flow_package_dependency_binding":         "Declare the required imported package binding or remove the unresolved dependency reference.",
	"flow_package_import_completeness":        "Complete the imported package dependency binding or remove the incomplete import.",
	"flow_package_wildcard_observe_grant":     "Replace wildcard observe grants with explicit package dependency grants for each observed event.",
	"gate_schema_validation":                  "Fix the gate declaration so it uses supported gate fields and value types.",
	"generated_tool_schema_closure":           "Fix the generated tool schema inputs/outputs so they close over declared contract types.",
	"handler_field_compliance":                "Move the unsupported handler field to its promoted location or remove it.",
	"impl.platform_metadata_validation":       "Remove authored declarations that conflict with platform-owned metadata.",
	"invalid_field_detection":                 "Remove unsupported fields or replace them with the promoted contract field supported by the spec.",
	"managed_credential_state":                "Configure the required managed credential or remove the dependency that requires it.",
	"missing_external_select_entity":          "Add an explicit select_entity/select_or_create_entity for the external input or make the event topology in-flow.",
	"native_tools_valid":                      "Fix the native tool declaration so it references a supported runtime tool surface.",
	"node_state_schema_typed_counterpart":     "Add the typed node-state counterpart required by the schema or remove the unsupported jsonb-only state field.",
	"output_pin_key_carries_validation":       "Declare the output pin key/carries fields and ensure every authored emit site populates those payload fields.",
	"payload_field_coverage":                  "Populate every required emitted payload field or make the target event schema optional where appropriate.",
	"platform_tool_usage_hints":               "Fix platform tool usage hints so every referenced tool exists and every required usage hint is declared.",
	"platform_namespace_violation":            "Rename authored business fields away from platform-reserved namespace roots.",
	"platform_version_compatibility":          "Update the contract platform_version range or run with a compatible Swarm platform version.",
	"policy_conflict_detection":               "Resolve the conflicting policy declarations so only one authoritative value remains.",
	"policy_sheet_lookup_value_rows":          "Fix the policy sheet lookup value rows so each lookup key and target value is declared consistently.",
	"policy_sheet_validation_value_rows":      "Fix the policy sheet validation value rows so each row matches the declared policy sheet schema.",
	"primary_entity_validation":               "Declare a valid primary entity or remove the stateful surface that requires one.",
	"prompt_exists":                           "Add the referenced prompt file or update the agent prompt_ref.",
	"redundant_in_topology_select_entity":     "Remove redundant select_entity/select_or_create_entity from an in-topology handler.",
	"reply_lineage_missing":                   "Add the required reply lineage metadata or remove the reply-mode handler contract.",
	"required_agents_match":                   "Update required agent declarations so subscriptions and emitted events match the contract topology.",
	"required_mcp_tool_availability":          "Expose the required MCP tool from the configured server or update the agent tool requirement.",
	"select_entity_validation":                "Fix the select_entity/select_or_create_entity declaration so it uses supported source and target fields.",
	"semantic_drift_payload_completeness":     "Populate every required payload field for emitted events or update the event schema to match the emitted payload.",
	"single_node_per_event":                   "Ensure only one system node owns the exact event handler or split the event ownership.",
	"singleton_coordinator_validation":        "Fix the singleton coordinator declaration so it names supported contained state fields.",
	"state_machine_coherence":                 "Fix the state machine declarations so initial, terminal, and transition states are declared consistently.",
	"template_instance_validation":            "Fix the template instance declaration so instance keys and policies are valid.",
	"timer_validation":                        "Use only supported timer start_on/cancel_on forms and ensure referenced states/events are declared and reachable.",
	"transition_ownership_validation":         "Move the transition owner to the handler that owns the triggering event or change the transition.",
	"transition_reference_validation":         "Fix transition references so all events and states are declared and reachable.",
	"workflow_contract_validation":            "Fix the contract source/load error named by the message, then rerun `swarm verify`.",
	"workspace_class_exists":                  "Declare the referenced workspace class or update the agent workspace_class.",
	"write_pin_ownership_validation":          "Fix write pin ownership so the writer and target field share the supported owner boundary.",
}

var routingRemediationSplitCheckIDs = map[string]struct{}{
	"composition_connect_validation":         {},
	"connect_key_adapter_cardinality":        {},
	"connect_key_adapter_duplicate_source":   {},
	"connect_key_adapter_duplicate_target":   {},
	"connect_key_adapter_invalid":            {},
	"connect_key_adapter_missing_source":     {},
	"connect_key_adapter_missing_target":     {},
	"connect_key_adapter_partial":            {},
	"connect_key_adapter_source_missing":     {},
	"connect_key_adapter_target_missing":     {},
	"connect_key_adapter_unsupported":        {},
	"connect_map_unknown_address_key":        {},
	"cross_flow_pin_ambiguity_validation":    {},
	"flow_package_pin_bind_alias_validation": {},
	"input_pin_wiring":                       {},
	"instance_key_mismatch":                  {},
	"key_types_incompatible":                 {},
	"output_carries_address_key":             {},
	"output_carries_instance_key":            {},
	"output_key_missing":                     {},
	"pin_target_resolution":                  {},
	"producer_flow_missing":                  {},
	"producer_output_pin_missing":            {},
	"producer_reference_invalid":             {},
	"receiver_address_rule_invalid":          {},
	"receiver_address_rule_missing":          {},
	"receiver_flow_missing":                  {},
	"receiver_input_pin_missing":             {},
	"receiver_instance_key_invalid":          {},
	"receiver_instance_key_unavailable":      {},
	"receiver_reference_invalid":             {},
	"receiver_root_unsupported":              {},
	"receiver_route_key_missing":             {},
}

type FindingOption func(*Finding)

func WithRemediation(remediation string) FindingOption {
	return func(f *Finding) {
		f.Remediation = remediation
	}
}

func WithEvidence(evidence ...string) FindingOption {
	return func(f *Finding) {
		f.Evidence = append(f.Evidence, evidence...)
	}
}

func NewHardInvalidityFinding(checkID, location, message, remediation string, evidence ...string) Finding {
	return Finding{
		CheckID:     checkID,
		Severity:    SeverityHardInvalidity,
		Location:    location,
		Message:     message,
		Remediation: remediation,
		Evidence:    evidence,
	}
}

type Report struct {
	Findings []Finding
}

type Options struct {
	Credentials             runtimecredentials.Store
	ManagedCredentials      runtimemanagedcredentials.Store
	CheckMCPReachable       bool
	ValidateModelResolution bool
	LLMProfile              llmselection.Profile
	ModelAliases            llmselection.ModelAliases
	HarnessInjections       []runtimecontracts.FlowInputProducerInjection
}

func Run(ctx context.Context, source semanticview.Source, opts Options) Report {
	report := Report{}
	if source == nil {
		report.Add(Finding{
			CheckID:  "workflow_contract_validation",
			Severity: SeverityHardInvalidity,
			Message:  "semantic source is not configured",
			Location: "global",
		})
		return report
	}
	checkCtx := newCheckerContext(ctx, source, opts)
	for _, check := range bootCheckRegistry {
		for _, finding := range check.Run(checkCtx) {
			report.Add(finding)
		}
	}
	for _, check := range supplementalChecks {
		for _, finding := range check.Run(checkCtx) {
			report.Add(finding)
		}
	}

	report.Sort()
	return report
}

func (r *Report) Add(f Finding) {
	f.CheckID = strings.TrimSpace(f.CheckID)
	if f.CheckID == "" {
		f.CheckID = "workflow_contract_validation"
	}
	f.Severity = normalizeFindingSeverity(f.Severity)
	f.Message = strings.TrimSpace(f.Message)
	f.Location = strings.TrimSpace(f.Location)
	f.Remediation = strings.TrimSpace(f.Remediation)
	f.Evidence = normalizeFindingEvidence(f.Evidence)
	if findingRequiresRemediation(f) && f.Remediation == "" {
		f.Remediation = defaultRemediationForFinding(f)
	}
	r.Findings = append(r.Findings, f)
}

func (r Report) Errors() []Finding {
	return r.HardInvalidities()
}

func (r Report) HardInvalidities() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeverityHardInvalidity {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) Warnings() []Finding {
	return r.SemanticDriftWarnings()
}

func (r Report) SemanticDriftWarnings() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeveritySemanticDriftWarn {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) AuditAnalyses() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeverityAuditAnalysis {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) LintEvidence() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeverityLintEvidence {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) HasErrors() bool {
	return r.HasHardInvalidities()
}

func (r Report) HasHardInvalidities() bool {
	for _, finding := range r.Findings {
		if finding.Severity == SeverityHardInvalidity {
			return true
		}
	}
	return false
}

func (r *Report) Sort() {
	sort.Slice(r.Findings, func(i, j int) bool {
		leftSeverity := severityRank(r.Findings[i].Severity)
		rightSeverity := severityRank(r.Findings[j].Severity)
		if leftSeverity == rightSeverity {
			if r.Findings[i].CheckID == r.Findings[j].CheckID {
				if r.Findings[i].Location == r.Findings[j].Location {
					return r.Findings[i].Message < r.Findings[j].Message
				}
				return r.Findings[i].Location < r.Findings[j].Location
			}
			return r.Findings[i].CheckID < r.Findings[j].CheckID
		}
		return leftSeverity < rightSeverity
	})
}

func normalizeFindingSeverity(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", legacySeverityError, SeverityHardInvalidity:
		return SeverityHardInvalidity
	case legacySeverityWarning, SeveritySemanticDriftWarn:
		return SeveritySemanticDriftWarn
	case SeverityAuditAnalysis:
		return SeverityAuditAnalysis
	case SeverityLintEvidence:
		return SeverityLintEvidence
	default:
		return SeverityHardInvalidity
	}
}

func severityRank(severity string) int {
	switch normalizeFindingSeverity(severity) {
	case SeverityHardInvalidity:
		return 0
	case SeveritySemanticDriftWarn:
		return 1
	case SeverityAuditAnalysis:
		return 2
	case SeverityLintEvidence:
		return 3
	default:
		return 4
	}
}

func FindingSurfaceLevel(f Finding, blocking bool) string {
	return SurfaceLevelForSeverity(f.Severity, blocking)
}

func SurfaceLevelForSeverity(severity string, blocking bool) string {
	if blocking {
		return FindingSurfaceLevelBlocker
	}
	switch normalizeFindingSeverity(severity) {
	case SeverityHardInvalidity:
		return FindingSurfaceLevelBlocker
	case SeveritySemanticDriftWarn:
		return FindingSurfaceLevelWarn
	case SeverityAuditAnalysis, SeverityLintEvidence:
		return FindingSurfaceLevelInfo
	default:
		return FindingSurfaceLevelBlocker
	}
}

func FormatSurfaceFinding(f Finding, blocking bool) string {
	return FormatTypedDiagnosticFinding(TypedDiagnosticFinding{
		CheckID:     f.CheckID,
		Severity:    f.Severity,
		Location:    f.Location,
		Message:     f.Message,
		Remediation: f.Remediation,
		Evidence:    f.Evidence,
	}, blocking)
}

type TypedDiagnosticFinding struct {
	CheckID     string
	Severity    string
	Location    string
	Message     string
	Remediation string
	Evidence    []string
}

func FormatTypedDiagnosticFinding(f TypedDiagnosticFinding, blocking bool) string {
	checkID := strings.TrimSpace(f.CheckID)
	if checkID == "" {
		checkID = "workflow_contract_validation"
	}
	location := strings.TrimSpace(f.Location)
	if location == "" {
		location = "global"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] %s @ %s: %s",
		SurfaceLevelForSeverity(f.Severity, blocking),
		checkID,
		location,
		strings.TrimSpace(f.Message),
	)
	if remediation := strings.TrimSpace(f.Remediation); remediation != "" {
		fmt.Fprintf(&b, "\n  remediation: %s", remediation)
	}
	evidence := normalizeFindingEvidence(f.Evidence)
	if len(evidence) > 0 {
		b.WriteString("\n  evidence:")
		for _, item := range evidence {
			fmt.Fprintf(&b, "\n    - %s", item)
		}
	}
	return b.String()
}

func normalizeFindingEvidence(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func findingRequiresRemediation(f Finding) bool {
	return normalizeFindingSeverity(f.Severity) == SeverityHardInvalidity
}

func defaultRemediationForFinding(f Finding) string {
	checkID := strings.TrimSpace(f.CheckID)
	if _, split := routingRemediationSplitCheckIDs[checkID]; split {
		return "Fix or remove the invalid routing declaration identified by this finding, then rerun `swarm verify`."
	}
	if remediation := stableHardInvalidityRemediation[checkID]; remediation != "" {
		return remediation
	}
	return "Fix or remove the invalid contract declaration identified by this finding, then rerun `swarm verify`."
}

func locationFromMessage(message string) string {
	fields := strings.Fields(strings.TrimSpace(message))
	if len(fields) < 2 {
		return "global"
	}
	switch fields[0] {
	case "agent", "node", "flow", "event", "timer", "transition", "root", "write":
		return strings.TrimSpace(strings.Trim(fields[1], "\"'"))
	default:
		return "global"
	}
}

func workspaceClassFindings(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	classes, err := rootWorkspaceClasses(source)
	if err != nil {
		return []Finding{{
			CheckID:  "workspace_class_exists",
			Severity: SeverityHardInvalidity,
			Message:  err.Error(),
			Location: "global",
		}}
	}
	out := make([]Finding, 0)
	for agentID, entry := range source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		class := strings.TrimSpace(entry.WorkspaceClass)
		if class == "" {
			continue
		}
		scope, ok := classes[class]
		if !ok {
			out = append(out, Finding{
				CheckID:  "workspace_class_exists",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("agent %s references undefined workspace_class %q", agentID, class),
				Location: agentID,
			})
			continue
		}
		if scope != "per-agent" && scope != "per-flow-instance" {
			out = append(out, Finding{
				CheckID:  "workspace_class_exists",
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("workspace_class %q declares unsupported workspace_scope %q", class, scope),
				Location: class,
			})
		}
	}
	return out
}

func rootWorkspaceClasses(source semanticview.Source) (map[string]string, error) {
	value, ok := semanticview.PolicyValueForFlow(source, "", "workspace_classes")
	if !ok {
		return map[string]string{}, nil
	}
	root, ok := anyMap(value.Value)
	if !ok {
		return nil, fmt.Errorf("workspace_classes must be a mapping")
	}
	out := make(map[string]string, len(root))
	for className, raw := range root {
		entry, ok := anyMap(raw)
		if !ok {
			return nil, fmt.Errorf("workspace_classes.%s must be a mapping", strings.TrimSpace(className))
		}
		out[strings.TrimSpace(className)] = strings.TrimSpace(anyString(entry["workspace_scope"]))
	}
	return out, nil
}

func anyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	default:
		return nil, false
	}
}

func anyString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func fmtCredentialWarning(key string, requiredBy []string) string {
	key = strings.TrimSpace(key)
	if len(requiredBy) == 0 {
		return fmt.Sprintf("credential %s is missing", key)
	}
	sort.Strings(requiredBy)
	return fmt.Sprintf("credential %s is missing (required by %s)", key, strings.Join(requiredBy, ", "))
}
