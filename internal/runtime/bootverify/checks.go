package bootverify

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerequiredagents "github.com/division-sh/swarm/internal/runtime/requiredagents"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type Check struct {
	ID       string
	Severity string
	Run      func(*checkerContext) []Finding
}

type checkerContext struct {
	ctx    context.Context
	source semanticview.Source
	opts   Options

	mcpDiscoveryLoaded bool
	mcpDiscoveredTools map[string]runtimemcp.DiscoveredTool
	mcpDiscoveryErrors []error

	permissionLoaded   bool
	permissionFindings []Finding

	permissionWarningLoaded   bool
	permissionWarningFindings []Finding

	workspaceLoaded   bool
	workspaceFindings []Finding

	promptLoaded   bool
	promptFindings []Finding

	promptSchemaGuardLoaded   bool
	promptSchemaGuardFindings []Finding

	toolLoaded   bool
	toolFindings []Finding

	requiredMCPLoaded   bool
	requiredMCPFindings []Finding

	toolUsageLoaded   bool
	toolUsageFindings []Finding

	generatedToolSchemaClosureLoaded   bool
	generatedToolSchemaClosureFindings []Finding

	runtimeToolNamesLoaded bool
	runtimeToolNames       map[string]struct{}

	policyLoaded   bool
	policyFindings []Finding

	eventWarningLoaded   bool
	eventWarningFindings []Finding

	conditionPolicyLoaded   bool
	conditionPolicyFindings []Finding

	conditionPayloadLoaded   bool
	conditionPayloadFindings []Finding

	configPayloadLoaded   bool
	configPayloadFindings []Finding

	payloadCoverageLoaded   bool
	payloadCoverageFindings []Finding

	entityWriteTargetComplianceLoaded   bool
	entityWriteTargetComplianceFindings []Finding

	payloadCompletenessLoaded   bool
	payloadCompletenessFindings []Finding

	deadEventSchemaLoaded   bool
	deadEventSchemaFindings []Finding

	eventMetadataAuthorityLoaded   bool
	eventMetadataAuthorityFindings []Finding

	dialectLoaded   bool
	dialectFindings []Finding

	invalidLoaded   bool
	invalidFindings []Finding

	entityWriterCoverageLoaded   bool
	entityWriterCoverageFindings []Finding

	entityReaderCoverageLoaded   bool
	entityReaderCoverageFindings []Finding

	crossSurfaceNamedTypeUseLoaded   bool
	crossSurfaceNamedTypeUseFindings []Finding

	handlerLoaded   bool
	handlerFindings []Finding

	cycleLoaded   bool
	cycleFindings []Finding

	requiredLoaded   bool
	requiredFindings []Finding

	stateLoaded   bool
	stateFindings []Finding

	stateReachabilityLoaded   bool
	stateReachabilityFindings []Finding

	nodeStateSchemaLoaded   bool
	nodeStateSchemaFindings []Finding

	flowPackageImportLoaded   bool
	flowPackageImportFindings []Finding

	accumulatorProjectionLoaded   bool
	accumulatorProjectionFindings []Finding
	accumulatorSafetyLoaded       bool
	accumulatorSafetyFindings     []Finding

	producesDriftLoaded   bool
	producesDriftFindings []Finding

	nativeLoaded   bool
	nativeFindings []Finding

	namespaceLoaded   bool
	namespaceFindings []Finding

	credentialLoaded   bool
	credentialFindings []Finding

	mcpLoaded   bool
	mcpFindings []Finding

	phantomLoaded   bool
	phantomFindings []Finding

	singleNodeLoaded   bool
	singleNodeFindings []Finding

	platformMetaLoaded   bool
	platformMetaFindings []Finding

	transitionRefLoaded   bool
	transitionRefFindings []Finding

	conditionExprLoaded   bool
	conditionExprFindings []Finding

	dataAccumulationExprLoaded   bool
	dataAccumulationExprFindings []Finding

	emitFieldExprLoaded   bool
	emitFieldExprFindings []Finding

	fanOutLoaded   bool
	fanOutFindings []Finding

	entityRefLoaded   bool
	entityRefFindings []Finding

	transitionOwnerLoaded   bool
	transitionOwnerFindings []Finding

	eventRuntimeLoaded   bool
	eventRuntimeFindings []Finding

	timerLoaded   bool
	timerFindings []Finding

	writePinLoaded   bool
	writePinFindings []Finding

	gateSchemaLoaded   bool
	gateSchemaFindings []Finding

	inputPinLoaded   bool
	inputPinFindings []Finding

	crossFlowPinAmbiguityLoaded   bool
	crossFlowPinAmbiguityFindings []Finding

	flowBoundaryCreateEntityLoaded   bool
	flowBoundaryCreateEntityFindings []Finding

	deprecatedLoaded   bool
	deprecatedFindings []Finding
}

var bootCheckRegistry = []Check{
	{ID: "event_metadata_authority", Severity: SeverityHardInvalidity, Run: checkEventMetadataAuthority},
	{ID: "event_chain_integrity", Severity: "warning", Run: checkEventChainIntegrity},
	{ID: semanticview.TypedPubSubFailureAuthorizationAmbiguous, Severity: SeverityHardInvalidity, Run: checkTypedPubSubAuthorization},
	{ID: "event_consumer_exists", Severity: "warning", Run: checkEventConsumerExists},
	{ID: "event_producer_exists", Severity: "warning", Run: checkEventProducerExists},
	{ID: "legacy_qualified_subscription", Severity: "warning", Run: checkLegacyQualifiedSubscription},
	{ID: "semantic_drift_dead_event_schema", Severity: "warning", Run: checkSemanticDriftDeadEventSchema},
	{ID: "entity_writer_coverage", Severity: SeverityHardInvalidity, Run: checkEntityWriterCoverage},
	{ID: "payload_field_coverage", Severity: "error", Run: checkPayloadFieldCoverage},
	{ID: "entity_write_target_compliance", Severity: SeverityHardInvalidity, Run: checkEntityWriteTargetCompliance},
	{ID: "contained_state_operation_compliance", Severity: SeverityHardInvalidity, Run: checkContainedStateOperationCompliance},
	{ID: "semantic_drift_payload_completeness", Severity: "error", Run: checkSemanticDriftPayloadCompleteness},
	{ID: "condition_payload_alignment", Severity: "error", Run: checkConditionPayloadAlignment},
	{ID: "condition_policy_alignment", Severity: "warning", Run: checkConditionPolicyAlignment},
	{ID: "state_machine_coherence", Severity: "error", Run: checkStateMachineCoherence},
	{ID: "semantic_drift_unreachable_state", Severity: "warning", Run: checkSemanticDriftUnreachableState},
	{ID: "node_state_schema_typed_counterpart", Severity: SeverityHardInvalidity, Run: checkNodeStateSchemaTypedCounterpart},
	{ID: "accumulator_entity_projection", Severity: SeverityHardInvalidity, Run: checkAccumulatorEntityProjection},
	{ID: "accumulate_all_bounded_escape", Severity: "warning", Run: checkAccumulateAllBoundedEscape},
	{ID: "accumulator_timeout_requires_timeout_ms", Severity: SeverityHardInvalidity, Run: checkAccumulatorTimeoutRequiresTimeout},
	{ID: "accumulator_input_producer_path", Severity: SeverityHardInvalidity, Run: checkAccumulatorInputProducerPath},
	{ID: "required_agents_match", Severity: "error", Run: checkRequiredAgentsMatch},
	{ID: "handler_field_compliance", Severity: "error", Run: checkHandlerFieldCompliance},
	{ID: policySheetLookupCheckID, Severity: SeverityHardInvalidity, Run: checkPolicySheetLookupValueRows},
	{ID: policySheetValidationCheckID, Severity: SeverityHardInvalidity, Run: checkPolicySheetValidationValueRows},
	{ID: computeModuleCheckID, Severity: SeverityHardInvalidity, Run: checkComputeModuleValueRows},
	{ID: fanOutValidationCheckID, Severity: SeverityHardInvalidity, Run: checkFanOutValidation},
	{ID: joinValidationCheckID, Severity: SeverityHardInvalidity, Run: checkJoinValidation},
	{ID: loopValidationCheckID, Severity: SeverityHardInvalidity, Run: checkLoopValidation},
	{ID: stageGateValidationCheckID, Severity: SeverityHardInvalidity, Run: checkStageGateValidation},
	{ID: "tool_resolution", Severity: "warning", Run: checkToolResolution},
	{ID: "required_mcp_tool_availability", Severity: SeverityHardInvalidity, Run: checkRequiredMCPToolAvailability},
	{ID: "platform_tool_usage_hints", Severity: SeverityHardInvalidity, Run: checkPlatformToolUsageHints},
	{ID: "generated_tool_schema_closure", Severity: SeverityHardInvalidity, Run: checkGeneratedToolSchemaClosure},
	{ID: "prompt_exists", Severity: "warning", Run: checkPromptExists},
	{ID: "produces_drift", Severity: "warning", Run: checkProducesDrift},
	{ID: "invalid_field_detection", Severity: "error", Run: checkInvalidFieldDetection},
	{ID: "policy_conflict_detection", Severity: "warning", Run: checkPolicyConflictDetection},
	{ID: "event_cycle_detection", Severity: "error", Run: checkEventCycleDetection},
	{ID: "dialect_compliance", Severity: "error", Run: checkDialectCompliance},
	{ID: "single_node_per_event", Severity: "error", Run: checkSingleNodePerEvent},
	{ID: "config_from_payload_alignment", Severity: "error", Run: checkConfigFromPayloadAlignment},
	{ID: "phantom_produces", Severity: "warning", Run: checkPhantomProduces},
	{ID: "native_tools_valid", Severity: "error", Run: checkNativeToolsValid},
	{ID: "mcp_server_reachable", Severity: "warning", Run: checkMCPServerReachable},
	{ID: "platform_namespace_violation", Severity: "error", Run: checkPlatformNamespaceViolation},
	{ID: "workspace_class_exists", Severity: "error", Run: checkWorkspaceClassExists},
	{ID: "credential_key_exists", Severity: "warning", Run: checkCredentialKeyExists},
	{ID: "agent_permission_validation", Severity: "error", Run: checkAgentPermissionValidation},
	{ID: "platform_version_compatibility", Severity: SeverityHardInvalidity, Run: checkPlatformVersionCompatibility},
	{ID: "transition_reference_validation", Severity: "error", Run: checkTransitionReferenceValidation},
	{ID: "condition_expression_validation", Severity: "error", Run: checkConditionExpressionValidation},
	{ID: "data_accumulation_expression_validation", Severity: "error", Run: checkDataAccumulationExpressionValidation},
	{ID: "emit_field_expression_validation", Severity: "error", Run: checkEmitFieldExpressionValidation},
	{ID: "expression_field_reference_validation", Severity: "warning", Run: checkExpressionFieldReferenceValidation},
	{ID: "entity_reader_coverage", Severity: SeverityLintEvidence, Run: checkEntityReaderCoverage},
	{ID: "primary_entity_validation", Severity: "error", Run: checkPrimaryEntityValidation},
	{ID: "template_instance_validation", Severity: "error", Run: checkTemplateInstanceValidation},
	{ID: "singleton_coordinator_validation", Severity: "error", Run: checkSingletonCoordinatorValidation},
	{ID: "cross_surface_named_type_use", Severity: SeverityLintEvidence, Run: checkCrossSurfaceNamedTypeUse},
	{ID: "transition_ownership_validation", Severity: "error", Run: checkTransitionOwnershipValidation},
	{ID: "event_runtime_wiring_validation", Severity: "error", Run: checkEventRuntimeWiringValidation},
	{ID: "timer_validation", Severity: "error", Run: checkTimerValidation},
	{ID: "write_pin_ownership_validation", Severity: "error", Run: checkWritePinOwnershipValidation},
	{ID: "gate_schema_validation", Severity: "error", Run: checkGateSchemaValidation},
	{ID: "flow_package_import_completeness", Severity: SeverityHardInvalidity, Run: checkFlowPackageImportCompleteness},
	{ID: "flow_package_dependency_binding", Severity: SeverityHardInvalidity, Run: checkFlowPackageDependencyBinding},
	{ID: "flow_package_pin_bind_alias_validation", Severity: SeverityHardInvalidity, Run: checkFlowPackagePinBindAliasValidation},
	{ID: "flow_package_wildcard_observe_grant", Severity: SeverityHardInvalidity, Run: checkFlowPackageWildcardObserveGrant},
	{ID: "output_pin_key_carries_validation", Severity: "error", Run: checkOutputPinKeyCarriesValidation},
	{ID: "composition_connect_validation", Severity: "error", Run: checkCompositionConnectValidation},
	{ID: "input_pin_wiring", Severity: SeverityHardInvalidity, Run: checkInputPinWiring},
	{ID: "pin_target_resolution", Severity: "error", Run: checkPinTargetResolution},
	{ID: "redundant_in_topology_select_entity", Severity: SeverityHardInvalidity, Run: checkRedundantInTopologySelectEntity},
	{ID: "missing_external_select_entity", Severity: "error", Run: checkMissingExternalSelectEntity},
	{ID: "cross_flow_pin_ambiguity_validation", Severity: "error", Run: checkCrossFlowPinAmbiguityValidation},
	{ID: "select_entity_validation", Severity: "error", Run: checkSelectEntityValidation},
	{ID: "flow_boundary_create_entity_validation", Severity: "error", Run: checkFlowBoundaryCreateEntityValidation},
	{ID: "flow_data_access_validation", Severity: "error", Run: checkFlowDataAccessValidation},
}

var supplementalChecks = []Check{
	{ID: "impl.platform_metadata_validation", Severity: "error", Run: checkPlatformMetadataValidation},
	{ID: "impl.deprecated_contract_alias", Severity: "warning", Run: checkDeprecatedContractAlias},
	{ID: "agent_prompt_lint_structural", Severity: SeverityHardInvalidity, Run: checkPromptSchemaGuardStructural},
}

func newCheckerContext(ctx context.Context, source semanticview.Source, opts Options) *checkerContext {
	return &checkerContext{
		ctx:    ctx,
		source: source,
		opts:   opts,
	}
}

func (c *checkerContext) appendAgentModelAliasFindings(findings []Finding, agentLabel, model string) []Finding {
	if c != nil && c.opts.ValidateModelResolution {
		resolved, err := llmselection.ResolveModel(c.opts.LLMProfile, llmselection.ModelResolution{
			Model:  model,
			Models: c.opts.ModelAliases,
		})
		if err != nil {
			backend := strings.TrimSpace(c.opts.LLMProfile.ID)
			if backend == "" {
				backend = "selected backend"
			}
			return append(findings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s model alias resolution failed for %s: %v", agentLabel, backend, err),
				Location: agentLabel,
			})
		}
		if strings.TrimSpace(resolved.ConcreteModel) == "" {
			return append(findings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s model alias resolution produced an empty concrete model", agentLabel),
				Location: agentLabel,
			})
		}
		return findings
	}
	if _, err := llmselection.RequireModelAlias(model); err != nil {
		return append(findings, Finding{
			CheckID:  "invalid_field_detection",
			Severity: "error",
			Message:  fmt.Sprintf("agent %s invalid model alias: %v", agentLabel, err),
			Location: agentLabel,
		})
	}
	return findings
}

func checkPayloadFieldCoverage(c *checkerContext) []Finding  { return c.payloadFieldCoverage() }
func checkStateMachineCoherence(c *checkerContext) []Finding { return c.stateMachineCoherence() }
func checkNodeStateSchemaTypedCounterpart(c *checkerContext) []Finding {
	return c.nodeStateSchemaTypedCounterpart()
}
func checkRequiredAgentsMatch(c *checkerContext) []Finding        { return c.requiredAgentsMatch() }
func checkPromptExists(c *checkerContext) []Finding               { return c.promptExists() }
func checkInvalidFieldDetection(c *checkerContext) []Finding      { return c.invalidFieldDetection() }
func checkPolicyConflictDetection(c *checkerContext) []Finding    { return c.policyConflicts() }
func checkDialectCompliance(c *checkerContext) []Finding          { return c.dialectCompliance() }
func checkNativeToolsValid(c *checkerContext) []Finding           { return c.nativeTools() }
func checkFlowDataAccessValidation(c *checkerContext) []Finding   { return c.flowDataAccess() }
func checkPlatformNamespaceViolation(c *checkerContext) []Finding { return c.platformNamespace() }
func checkWorkspaceClassExists(c *checkerContext) []Finding       { return c.workspace() }
func checkCredentialKeyExists(c *checkerContext) []Finding        { return c.credentials() }
func checkMCPServerReachable(c *checkerContext) []Finding         { return c.mcp() }
func checkAgentPermissionValidation(c *checkerContext) []Finding {
	return uniqueFindings(append(c.permissions(), c.permissionWarnings()...))
}
func checkPlatformMetadataValidation(c *checkerContext) []Finding { return c.platformMetadata() }
func checkPlatformVersionCompatibility(c *checkerContext) []Finding {
	return c.platformVersionCompatibility()
}
func checkDeprecatedContractAlias(c *checkerContext) []Finding     { return c.deprecatedAliases() }
func checkPromptSchemaGuardStructural(c *checkerContext) []Finding { return c.promptSchemaGuard() }
func checkCrossSurfaceNamedTypeUse(c *checkerContext) []Finding {
	return c.crossSurfaceNamedTypeUse()
}

func (c *checkerContext) permissions() []Finding {
	if c.permissionLoaded {
		return c.permissionFindings
	}
	c.permissionLoaded = true
	_, permissionErrors := runtimetools.ValidateAgentPermissions(c.source)
	for _, permissionErr := range permissionErrors {
		msg := strings.TrimSpace(permissionErr.Error())
		c.permissionFindings = append(c.permissionFindings, Finding{
			CheckID:  "agent_permission_validation",
			Severity: "error",
			Message:  msg,
			Location: locationFromMessage(msg),
		})
	}
	return c.permissionFindings
}

func (c *checkerContext) promptSchemaGuard() []Finding {
	if c.promptSchemaGuardLoaded {
		return c.promptSchemaGuardFindings
	}
	c.promptSchemaGuardLoaded = true
	bundle, ok := semanticview.Bundle(c.source)
	if !ok || bundle == nil {
		return nil
	}
	findings, err := runtimecontracts.PromptSchemaGuardFindingsForBundle(bundle)
	if err != nil {
		c.promptSchemaGuardFindings = append(c.promptSchemaGuardFindings, Finding{
			CheckID:  "agent_prompt_lint_structural",
			Severity: SeverityHardInvalidity,
			Message:  err.Error(),
			Location: "global",
		})
		return c.promptSchemaGuardFindings
	}
	for _, finding := range findings {
		c.promptSchemaGuardFindings = append(c.promptSchemaGuardFindings, Finding{
			CheckID:  "agent_prompt_lint_structural",
			Severity: SeverityHardInvalidity,
			Message:  strings.TrimSpace(finding.Message),
			Location: strings.TrimSpace(finding.Location),
		})
	}
	return c.promptSchemaGuardFindings
}

func (c *checkerContext) permissionWarnings() []Finding {
	if c.permissionWarningLoaded {
		return c.permissionWarningFindings
	}
	c.permissionWarningLoaded = true
	for _, item := range mergedAgentPermissionWarnings(c.source) {
		c.permissionWarningFindings = append(c.permissionWarningFindings, Finding{
			CheckID:  "agent_permission_validation",
			Severity: "warning",
			Message:  strings.TrimSpace(item.Message),
			Location: locationFromMessage(item.Message),
		})
	}
	return c.permissionWarningFindings
}

func (c *checkerContext) platformMetadata() []Finding {
	if c.platformMetaLoaded {
		return c.platformMetaFindings
	}
	c.platformMetaLoaded = true
	if strings.TrimSpace(c.source.PlatformSpec().Platform.Name) == "" {
		c.platformMetaFindings = append(c.platformMetaFindings, Finding{
			CheckID:  "impl.platform_metadata_validation",
			Severity: "error",
			Message:  "platform.name missing",
			Location: "global",
		})
	}
	if strings.TrimSpace(c.source.PlatformSpec().Platform.Version) == "" {
		c.platformMetaFindings = append(c.platformMetaFindings, Finding{
			CheckID:  "impl.platform_metadata_validation",
			Severity: "error",
			Message:  "platform.version missing",
			Location: "global",
		})
	}
	return c.platformMetaFindings
}

func (c *checkerContext) deprecatedAliases() []Finding {
	if c.deprecatedLoaded {
		return c.deprecatedFindings
	}
	c.deprecatedLoaded = true
	return c.deprecatedFindings
}

func (c *checkerContext) workspace() []Finding {
	if c.workspaceLoaded {
		return c.workspaceFindings
	}
	c.workspaceLoaded = true
	c.workspaceFindings = workspaceClassFindings(c.source)
	return c.workspaceFindings
}

func (c *checkerContext) promptExists() []Finding {
	if c.promptLoaded {
		return c.promptFindings
	}
	c.promptLoaded = true
	bundle, hasBundle := semanticview.Bundle(c.source)
	for _, scope := range c.source.ProjectScopes() {
		scopeLabel := projectScopeLabel(scope.Key, scope.Manifest.Name)
		source := runtimecontracts.ContractItemSource{PackageKey: strings.TrimSpace(scope.Key), Layer: "project"}
		c.promptFindings = append(c.promptFindings, promptFindingsForScope(bundle, hasBundle, scope.PromptsDir, scopeLabel, scope.Agents, source, "")...)
	}
	for _, scope := range c.source.FlowScopes() {
		scopeLabel := flowScopeLabel(scope.ID, scope.Path)
		source := runtimecontracts.ContractItemSource{
			PackageKey: strings.TrimSpace(scope.PackageKey),
			FlowID:     strings.TrimSpace(scope.ID),
			Layer:      "flow",
		}
		c.promptFindings = append(c.promptFindings, promptFindingsForScope(bundle, hasBundle, scope.PromptsDir, scopeLabel, scope.Agents, source, scope.Mode)...)
	}
	return c.promptFindings
}

func (c *checkerContext) policyConflicts() []Finding {
	if c.policyLoaded {
		return c.policyFindings
	}
	c.policyLoaded = true
	projectScopes := c.source.ProjectScopes()
	if len(projectScopes) == 0 {
		return nil
	}
	root := rootPolicyScope(projectScopes)
	if len(root.Policy.Values) == 0 {
		return nil
	}
	for _, flow := range c.source.FlowScopes() {
		for key, value := range flow.Policy.Values {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			rootValue, ok := root.Policy.Values[key]
			if !ok {
				continue
			}
			if !reflect.DeepEqual(rootValue.Value, value.Value) {
				location := flowScopeLabel(flow.ID, flow.Path)
				c.policyFindings = append(c.policyFindings, Finding{
					CheckID:  "policy_conflict_detection",
					Severity: "warning",
					Message:  fmt.Sprintf("'%s': root=%v, %s=%v", key, rootValue.Value, location, value.Value),
					Location: location,
				})
			}
		}
	}
	return c.policyFindings
}

func sortedSetKeysLocal[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (c *checkerContext) dialectCompliance() []Finding {
	if c.dialectLoaded {
		return c.dialectFindings
	}
	c.dialectLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if handlerDeclaresConflictingCompletion(handler) {
				c.dialectFindings = append(c.dialectFindings, Finding{
					CheckID:  "dialect_compliance",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s declares both on_complete and rules", nodeID, eventType),
					Location: nodeID,
				})
			}
			if handlerDeclaresConflictingCreateEntityAccumulation(handler) {
				c.dialectFindings = append(c.dialectFindings, Finding{
					CheckID:  "dialect_compliance",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s declares both create_entity and accumulate", nodeID, eventType),
					Location: nodeID,
				})
			}
			if strings.TrimSpace(handler.Condition) != "" {
				c.dialectFindings = append(c.dialectFindings, Finding{
					CheckID:  "dialect_compliance",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s uses deprecated handler-level condition", nodeID, eventType),
					Location: nodeID,
				})
			}
			if strings.TrimSpace(handler.Logic) != "" {
				c.dialectFindings = append(c.dialectFindings, Finding{
					CheckID:  "dialect_compliance",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s uses deprecated logic field", nodeID, eventType),
					Location: nodeID,
				})
			}
		}
	}
	return c.dialectFindings
}

func (c *checkerContext) invalidFieldDetection() []Finding {
	if c.invalidLoaded {
		return c.invalidFindings
	}
	c.invalidLoaded = true
	for _, err := range runtimetools.ValidateExternalDispatchRateLimitDeclarations(c.source) {
		c.invalidFindings = append(c.invalidFindings, Finding{
			CheckID:  "invalid_field_detection",
			Severity: "error",
			Message:  err.Error(),
			Location: "rate_limit",
		})
	}
	for _, scope := range c.source.ProjectScopes() {
		scopeLabel := projectScopeLabel(scope.Key, scope.Manifest.Name)
		if strings.TrimSpace(scope.Manifest.Name) == "" {
			c.invalidFindings = append(c.invalidFindings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("project package %s missing required field name", scopeLabel),
				Location: scopeLabel,
			})
		}
		if strings.TrimSpace(scope.Manifest.Version) == "" {
			c.invalidFindings = append(c.invalidFindings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("project package %s missing required field version", scopeLabel),
				Location: scopeLabel,
			})
		}
		for nodeID, node := range scope.Nodes {
			nodeID = strings.TrimSpace(nodeID)
			nodeLabel := scopedObjectLabel(scopeLabel, nodeID)
			if nodeID == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node in scope %s missing required field id", scopeLabel),
					Location: scopeLabel,
				})
				continue
			}
			if authoredID := strings.TrimSpace(node.ID); !runtimecontracts.SystemNodeIDMatchesKey(nodeID, authoredID) {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s id %q must match map key", nodeLabel, authoredID),
					Location: nodeLabel,
				})
			}
			if strings.TrimSpace(node.ExecutionType) != "" {
				if err := runtimecontracts.ValidateSystemNodeExecutionType(node.ExecutionType); err != nil {
					c.invalidFindings = append(c.invalidFindings, Finding{
						CheckID:  "invalid_field_detection",
						Severity: "error",
						Message:  fmt.Sprintf("node %s execution_type must be %s", nodeLabel, runtimecontracts.SystemNodeExecutionType),
						Location: nodeLabel,
					})
				}
			}
			if len(c.source.NodeRuntimeSubscriptions(nodeID)) == 0 {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s missing declared subscription surface", nodeLabel),
					Location: nodeLabel,
				})
			}
			if len(node.EventHandlers) == 0 {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s missing required field event_handlers", nodeLabel),
					Location: nodeLabel,
				})
			}
		}
		for agentID, agent := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			agentLabel := scopedObjectLabel(scopeLabel, agentID)
			if agentID == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent in scope %s missing required field id", scopeLabel),
					Location: scopeLabel,
				})
				continue
			}
			c.invalidFindings = c.appendAgentModelAliasFindings(c.invalidFindings, agentLabel, agent.Model)
			if strings.TrimSpace(agent.ConversationMode) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field mode", agentLabel),
					Location: agentLabel,
				})
			} else if _, err := sessions.ParseAuthoredAgentMode(agent.ConversationMode); err != nil {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s has invalid mode: %v", agentLabel, err),
					Location: agentLabel,
				})
			}
			c.invalidFindings = appendAgentSessionScopeFindings(c.invalidFindings, c.source, scopeLabel, semanticview.ResolveAgentSessionScopeProof(c.source, semanticview.AgentSessionScopeLocator{
				AgentID:         agentID,
				ProjectScopeKey: scope.Key,
			}), agentID, agent)
			if len(agent.Subscriptions) == 0 {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field subscriptions", agentLabel),
					Location: agentLabel,
				})
			}
		}
	}
	for flowID, schema := range c.source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if strings.TrimSpace(schema.Name) == "" {
			c.invalidFindings = append(c.invalidFindings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("flow schema %s missing required field name", flowID),
				Location: flowID,
			})
		}
		if len(schema.States) == 0 && strings.TrimSpace(schema.InitialState) != "" {
			c.invalidFindings = append(c.invalidFindings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("flow schema %s missing required field states", flowID),
				Location: flowID,
			})
		}
	}
	for _, scope := range c.source.FlowScopes() {
		scopeLabel := flowScopeLabel(scope.ID, scope.Path)
		for nodeID, node := range scope.Nodes {
			nodeID = strings.TrimSpace(nodeID)
			nodeLabel := scopedObjectLabel(scopeLabel, nodeID)
			if nodeID == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node in scope %s missing required field id", scopeLabel),
					Location: scopeLabel,
				})
				continue
			}
			if authoredID := strings.TrimSpace(node.ID); !runtimecontracts.SystemNodeIDMatchesKey(nodeID, authoredID) {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s id %q must match map key", nodeLabel, authoredID),
					Location: nodeLabel,
				})
			}
			if strings.TrimSpace(node.ExecutionType) != "" {
				if err := runtimecontracts.ValidateSystemNodeExecutionType(node.ExecutionType); err != nil {
					c.invalidFindings = append(c.invalidFindings, Finding{
						CheckID:  "invalid_field_detection",
						Severity: "error",
						Message:  fmt.Sprintf("node %s execution_type must be %s", nodeLabel, runtimecontracts.SystemNodeExecutionType),
						Location: nodeLabel,
					})
				}
			}
			if len(c.source.NodeRuntimeSubscriptions(nodeID)) == 0 {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s missing declared subscription surface", nodeLabel),
					Location: nodeLabel,
				})
			}
			if len(node.EventHandlers) == 0 {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s missing required field event_handlers", nodeLabel),
					Location: nodeLabel,
				})
			}
		}
		for agentID, agent := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			agentLabel := scopedObjectLabel(scopeLabel, agentID)
			if agentID == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent in scope %s missing required field id", scopeLabel),
					Location: scopeLabel,
				})
				continue
			}
			c.invalidFindings = c.appendAgentModelAliasFindings(c.invalidFindings, agentLabel, agent.Model)
			if strings.TrimSpace(agent.ConversationMode) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field mode", agentLabel),
					Location: agentLabel,
				})
			} else if _, err := sessions.ParseAuthoredAgentMode(agent.ConversationMode); err != nil {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s has invalid mode: %v", agentLabel, err),
					Location: agentLabel,
				})
			}
			c.invalidFindings = appendAgentSessionScopeFindings(c.invalidFindings, c.source, scopeLabel, semanticview.ResolveAgentSessionScopeProof(c.source, semanticview.AgentSessionScopeLocator{
				AgentID: agentID,
				FlowID:  scope.ID,
			}), agentID, agent)
			if len(agent.Subscriptions) == 0 {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field subscriptions", agentLabel),
					Location: agentLabel,
				})
			}
		}
	}
	return c.invalidFindings
}

func appendAgentSessionScopeFindings(findings []Finding, source semanticview.Source, scopeLabel string, proof semanticview.AgentSessionScopeProof, agentID string, agent runtimecontracts.AgentRegistryEntry) []Finding {
	agentLabel := scopedObjectLabel(scopeLabel, agentID)
	mode, err := sessions.ParseAuthoredAgentMode(agent.ConversationMode)
	if err != nil {
		return findings
	}
	sessionScope, err := sessions.ValidateAuthoredSessionScopeIntent(mode, agent.SessionScope)
	if err != nil {
		return append(findings, Finding{
			CheckID:  "invalid_field_detection",
			Severity: "error",
			Message:  fmt.Sprintf("agent %s has invalid session_scope: %v", agentLabel, err),
			Location: agentLabel,
		})
	}
	switch sessionScope {
	case sessions.SessionScopeFlow:
		if strings.TrimSpace(proof.OwningFlowID) == "" {
			return append(findings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s session_scope flow requires flow-scoped declaration", agentLabel),
				Location: agentLabel,
			})
		}
	case sessions.SessionScopeEntity:
		if strings.TrimSpace(proof.OwningFlowID) == "" {
			return append(findings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s session_scope entity requires flow-scoped declaration", agentLabel),
				Location: agentLabel,
			})
		}
		if flowIsStateless(source, proof.OwningFlowID) {
			return append(findings, Finding{
				CheckID:  "invalid_field_detection",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s session_scope entity requires stateful flow %s", agentLabel, validationFlowLabel(proof.OwningFlowID)),
				Location: agentLabel,
			})
		}
	}
	return findings
}

func (c *checkerContext) requiredAgentsMatch() []Finding {
	if c.requiredLoaded {
		return c.requiredFindings
	}
	c.requiredLoaded = true
	for _, scope := range runtimerequiredagents.AllScopes(c.source) {
		for _, finding := range runtimerequiredagents.CheckScope(scope) {
			c.requiredFindings = append(c.requiredFindings, requiredAgentBootFinding(finding))
		}
	}
	return c.requiredFindings
}

func requiredAgentBootFinding(finding runtimerequiredagents.Finding) Finding {
	scopeID := strings.TrimSpace(finding.ScopeID)
	if scopeID == "" {
		scopeID = "root"
	}
	message := ""
	switch finding.Kind {
	case runtimerequiredagents.FindingMissingRole:
		if scopeID == "root" {
			message = "root schema required_agents entry missing role"
		} else {
			message = fmt.Sprintf("flow %s required_agents entry missing role", scopeID)
		}
	case runtimerequiredagents.FindingMissingAgent:
		if scopeID == "root" {
			message = fmt.Sprintf("root schema required agent role %s missing from agents.yaml", finding.Role)
		} else {
			message = fmt.Sprintf("flow %s required agent role %s missing from agents.yaml", scopeID, finding.Role)
		}
	case runtimerequiredagents.FindingMissingSubscriptions:
		if scopeID == "root" {
			message = fmt.Sprintf("root required agent %s subscriptions mismatch (%s)", finding.AgentID, runtimerequiredagents.MissingList(finding.Missing))
		} else {
			message = fmt.Sprintf("flow %s required agent %s subscriptions mismatch (%s)", scopeID, finding.AgentID, runtimerequiredagents.MissingList(finding.Missing))
		}
	case runtimerequiredagents.FindingMissingEmits:
		if scopeID == "root" {
			message = fmt.Sprintf("root required agent %s emits mismatch (%s)", finding.AgentID, runtimerequiredagents.MissingList(finding.Missing))
		} else {
			message = fmt.Sprintf("flow %s required agent %s emits mismatch (%s)", scopeID, finding.AgentID, runtimerequiredagents.MissingList(finding.Missing))
		}
	default:
		message = fmt.Sprintf("%s required_agents mismatch", scopeID)
	}
	return Finding{
		CheckID:  "required_agents_match",
		Severity: "error",
		Message:  message,
		Location: scopeID,
	}
}

func (c *checkerContext) stateMachineCoherence() []Finding {
	if c.stateLoaded {
		return c.stateFindings
	}
	c.stateLoaded = true
	for _, entry := range lifecycleFlowSchemas(c.source) {
		flowID := entry.flowID
		schema := entry.schema
		c.stateFindings = append(c.stateFindings, stageDeclarationCoherenceFindings(flowID, schema)...)
		if strings.TrimSpace(flowID) == "" && !schema.UsesAuthoredStages() {
			continue
		}
		states := stringSet(c.source.FlowStates(flowID))
		initial := strings.TrimSpace(c.source.FlowInitialStage(flowID))
		if initial != "" {
			if _, ok := states[initial]; !ok {
				c.stateFindings = append(c.stateFindings, Finding{
					CheckID:  "state_machine_coherence",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s initial_state %s missing from states", flowID, initial),
					Location: strings.TrimSpace(flowID),
				})
			}
		}
	}
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		flowID := nodeFlowID(c.source, nodeID)
		declaredStates := declaredStatesForFlow(c.source, flowID)
		if len(declaredStates) == 0 {
			continue
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, target := range handlerAdvanceTargets(handler) {
				target = strings.TrimSpace(target)
				if target == "" {
					continue
				}
				if flowIsStateless(c.source, flowID) {
					c.stateFindings = append(c.stateFindings, Finding{
						CheckID:  "state_machine_coherence",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s advances_to is invalid in stateless flow %s", nodeID, eventType, validationFlowLabel(flowID)),
						Location: nodeID,
					})
					continue
				}
				if _, ok := declaredStates[target]; ok {
					continue
				}
				c.stateFindings = append(c.stateFindings, Finding{
					CheckID:  "state_machine_coherence",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s advances_to %s outside flow %s states", nodeID, eventType, target, validationFlowLabel(flowID)),
					Location: nodeID,
				})
			}
		}
	}
	for _, transition := range c.source.DerivedHandlerTransitions() {
		target := strings.TrimSpace(transition.AdvancesTo)
		if target == "" {
			continue
		}
		if flowIsStateless(c.source, strings.TrimSpace(transition.FlowID)) {
			c.stateFindings = append(c.stateFindings, Finding{
				CheckID:  "state_machine_coherence",
				Severity: "error",
				Message:  fmt.Sprintf("handler transition %s advances_to is invalid in stateless flow %s", transition.ID, validationFlowLabel(strings.TrimSpace(transition.FlowID))),
				Location: strings.TrimSpace(transition.ID),
			})
			continue
		}
		validTargets := declaredStatesForFlow(c.source, strings.TrimSpace(transition.FlowID))
		if len(validTargets) == 0 {
			continue
		}
		if _, ok := validTargets[target]; ok {
			continue
		}
		c.stateFindings = append(c.stateFindings, Finding{
			CheckID:  "state_machine_coherence",
			Severity: "error",
			Message:  fmt.Sprintf("handler transition %s advances_to %s outside flow %s states", transition.ID, target, validationFlowLabel(strings.TrimSpace(transition.FlowID))),
			Location: strings.TrimSpace(transition.ID),
		})
	}
	return c.stateFindings
}

func (c *checkerContext) nativeTools() []Finding {
	if c.nativeLoaded {
		return c.nativeFindings
	}
	c.nativeLoaded = true
	addNativeFindings := func(agentLabel string, agent runtimecontracts.AgentRegistryEntry) {
		for key, value := range agent.NativeTools {
			key = strings.TrimSpace(key)
			switch key {
			case "bash", "web_search", "file_io":
			default:
				c.nativeFindings = append(c.nativeFindings, Finding{
					CheckID:  "native_tools_valid",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s native_tools.%s is not a recognized capability", agentLabel, key),
					Location: strings.TrimSpace(agentLabel),
				})
				continue
			}
			if _, ok := value.(bool); ok {
				continue
			}
			c.nativeFindings = append(c.nativeFindings, Finding{
				CheckID:  "native_tools_valid",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s native_tools.%s must be boolean", agentLabel, key),
				Location: strings.TrimSpace(agentLabel),
			})
		}
	}
	for _, scope := range c.source.ProjectScopes() {
		scopeLabel := projectScopeLabel(scope.Key, scope.Manifest.Name)
		for agentID, agent := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			if agentID == "" {
				continue
			}
			addNativeFindings(scopedObjectLabel(scopeLabel, agentID), agent)
		}
	}
	for _, scope := range c.source.FlowScopes() {
		scopeLabel := flowScopeLabel(scope.ID, scope.Path)
		for agentID, agent := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			if agentID == "" {
				continue
			}
			addNativeFindings(scopedObjectLabel(scopeLabel, agentID), agent)
		}
	}
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		addNativeFindings(agentID, agent)
	}
	return uniqueFindings(c.nativeFindings)
}

func (c *checkerContext) flowDataAccess() []Finding {
	var findings []Finding
	for _, item := range flowdata.ValidateSource(c.source) {
		location := strings.TrimSpace(item.AgentLabel)
		if location == "" {
			location = "flow_data_access"
		}
		filename := strings.TrimSpace(item.Filename)
		message := strings.TrimSpace(item.Message)
		if filename != "" {
			message = fmt.Sprintf("%s (%s)", message, filename)
		}
		findings = append(findings, Finding{
			CheckID:  "flow_data_access_validation",
			Severity: SeverityHardInvalidity,
			Message:  message,
			Location: location,
		})
	}
	return uniqueFindings(findings)
}

func (c *checkerContext) platformNamespace() []Finding {
	if c.namespaceLoaded {
		return c.namespaceFindings
	}
	c.namespaceLoaded = true

	addEventCatalogClaim := func(eventType, location string) {
		if finding, ok := platformProducerClaimFinding(
			c.source,
			eventType,
			location,
			func(eventType string) string {
				return fmt.Sprintf(runtimecontracts.PlatformEventRedeclarationMessage, eventType)
			},
			func(eventType string) string {
				return fmt.Sprintf("event %s uses reserved platform.* namespace", eventType)
			},
		); ok {
			c.namespaceFindings = append(c.namespaceFindings, finding)
		}
	}

	for eventType := range c.source.EventEntries() {
		eventType = strings.TrimSpace(eventType)
		addEventCatalogClaim(eventType, eventType)
	}
	for _, scope := range c.source.ProjectScopes() {
		scopeLabel := projectScopeLabel(scope.Key, scope.Manifest.Name)
		for eventType := range scope.Events {
			eventType = strings.TrimSpace(eventType)
			addEventCatalogClaim(eventType, scopedObjectLabel(scopeLabel, eventType))
		}
	}
	for _, scope := range c.source.FlowScopes() {
		scopeLabel := flowScopeLabel(scope.ID, scope.Path)
		for eventType := range scope.Events {
			eventType = strings.TrimSpace(eventType)
			addEventCatalogClaim(eventType, scopedObjectLabel(scopeLabel, eventType))
		}
	}

	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, assertion := range census.ProducerAssertions() {
		for _, eventType := range assertion.EventTypes {
			eventType = strings.TrimSpace(eventType)
			if finding, ok := platformProducerClaimFinding(
				c.source,
				eventType,
				assertion.NodeID,
				func(eventType string) string {
					return fmt.Sprintf("node %s produces references platform-emitted event %s; platform owns this event", strings.TrimSpace(assertion.NodeID), eventType)
				},
				func(eventType string) string {
					return fmt.Sprintf("node %s produces references reserved platform.* namespace event %s", strings.TrimSpace(assertion.NodeID), eventType)
				},
			); ok {
				c.namespaceFindings = append(c.namespaceFindings, finding)
			}
		}
	}
	for _, endpoint := range census.Producers() {
		c.namespaceFindings = append(c.namespaceFindings, platformProducerEndpointFindings(c.source, endpoint)...)
	}
	for _, endpoint := range census.OutputPins() {
		flowID := strings.TrimSpace(endpoint.FlowID)
		location := flowID
		if flowID == "" {
			location = "root schema"
		}
		if finding, ok := platformProducerClaimFinding(
			c.source,
			endpoint.Event.Authored,
			location,
			func(eventType string) string {
				if flowID == "" {
					return fmt.Sprintf("root schema pins.outputs.events references platform-emitted event %s; platform owns this event", eventType)
				}
				return fmt.Sprintf("flow %s pins.outputs.events references platform-emitted event %s; platform owns this event", flowID, eventType)
			},
			func(eventType string) string {
				if flowID == "" {
					return fmt.Sprintf("root schema pins.outputs.events references reserved platform.* namespace event %s", eventType)
				}
				return fmt.Sprintf("flow %s pins.outputs.events references reserved platform.* namespace event %s", flowID, eventType)
			},
		); ok {
			c.namespaceFindings = append(c.namespaceFindings, finding)
		}
	}
	c.namespaceFindings = uniqueFindings(c.namespaceFindings)
	sort.SliceStable(c.namespaceFindings, func(i, j int) bool {
		if c.namespaceFindings[i].Location == c.namespaceFindings[j].Location {
			return c.namespaceFindings[i].Message < c.namespaceFindings[j].Message
		}
		return c.namespaceFindings[i].Location < c.namespaceFindings[j].Location
	})
	return c.namespaceFindings
}

func platformProducerEndpointFindings(source semanticview.Source, endpoint semanticview.AuthoredEventEndpoint) []Finding {
	if endpoint.Kind == semanticview.EventEndpointExternal || endpoint.Kind == semanticview.EventEndpointPlatform {
		return nil
	}
	eventType := endpoint.Event.Authored
	location := strings.TrimSpace(endpoint.FlowID)
	if location == "" {
		location = "root"
	}
	var catalogMessage, reservedMessage func(string) string
	switch endpoint.Kind {
	case semanticview.EventEndpointAgent:
		label := strings.TrimSpace(endpoint.AgentID)
		location = label
		if endpoint.FlowID != "" {
			location = scopedObjectLabel(flowScopeLabel(endpoint.FlowID, endpoint.FlowPath), label)
		}
		catalogMessage = func(eventType string) string {
			return fmt.Sprintf("agent %s emit_events references platform-emitted event %s; platform owns this event", location, eventType)
		}
		reservedMessage = func(eventType string) string {
			return fmt.Sprintf("agent %s emit_events references reserved platform.* namespace event %s", location, eventType)
		}
	case semanticview.EventEndpointNodeHandler:
		location = strings.TrimSpace(endpoint.NodeID)
		catalogMessage = func(eventType string) string {
			return fmt.Sprintf("node %s handler %s emits platform-emitted event %s; platform owns this event", endpoint.NodeID, endpoint.HandlerEvent, eventType)
		}
		reservedMessage = func(eventType string) string {
			return fmt.Sprintf("node %s handler %s emits reserved platform.* namespace event %s", endpoint.NodeID, endpoint.HandlerEvent, eventType)
		}
	case semanticview.EventEndpointNodeGenerated:
		location = strings.TrimSpace(endpoint.NodeID)
		catalogMessage = func(eventType string) string {
			return fmt.Sprintf("node %s produces platform-emitted event %s; platform owns this event", endpoint.NodeID, eventType)
		}
		reservedMessage = func(eventType string) string {
			return fmt.Sprintf("node %s produces reserved platform.* namespace event %s", endpoint.NodeID, eventType)
		}
	case semanticview.EventEndpointRequiredAgentRole:
		location = strings.TrimSpace(endpoint.Role)
		catalogMessage = func(eventType string) string {
			return fmt.Sprintf("required agent role %s emits platform-emitted event %s; platform owns this event", endpoint.Role, eventType)
		}
		reservedMessage = func(eventType string) string {
			return fmt.Sprintf("required agent role %s emits reserved platform.* namespace event %s", endpoint.Role, eventType)
		}
	case semanticview.EventEndpointTimer:
		location = strings.TrimSpace(endpoint.TimerID)
		catalogMessage = func(eventType string) string {
			return fmt.Sprintf("timer %s fires platform-emitted event %s; platform owns this event", endpoint.TimerID, eventType)
		}
		reservedMessage = func(eventType string) string {
			return fmt.Sprintf("timer %s fires reserved platform.* namespace event %s", endpoint.TimerID, eventType)
		}
	case semanticview.EventEndpointAutoEmit:
		catalogMessage = func(eventType string) string {
			return fmt.Sprintf("flow %s auto_emit_on_create references platform-emitted event %s; platform owns this event", location, eventType)
		}
		reservedMessage = func(eventType string) string {
			return fmt.Sprintf("flow %s auto_emit_on_create references reserved platform.* namespace event %s", location, eventType)
		}
	default:
		return nil
	}
	finding, ok := platformProducerClaimFinding(source, eventType, location, catalogMessage, reservedMessage)
	if !ok {
		return nil
	}
	return []Finding{finding}
}

func platformProducerClaimFinding(source semanticview.Source, eventType, location string, catalogMessage, reservedMessage func(string) string) (Finding, bool) {
	eventType = strings.TrimSpace(eventType)
	if source == nil || eventType == "" {
		return Finding{}, false
	}
	message := ""
	switch {
	case runtimecontracts.PlatformEventCatalogContains(source.PlatformSpec(), eventType):
		message = catalogMessage(eventType)
	case strings.HasPrefix(eventType, "platform."):
		message = reservedMessage(eventType)
	default:
		return Finding{}, false
	}
	return Finding{
		CheckID:  "platform_namespace_violation",
		Severity: "error",
		Message:  strings.TrimSpace(message),
		Location: strings.TrimSpace(location),
	}, true
}

func promptFindingsForScope(bundle *runtimecontracts.WorkflowContractBundle, hasBundle bool, promptsDir, scopeLabel string, agents map[string]runtimecontracts.AgentRegistryEntry, source runtimecontracts.ContractItemSource, mode string) []Finding {
	out := make([]Finding, 0, len(agents))
	for agentID := range agents {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		location := scopedObjectLabel(scopeLabel, agentID)
		if hasBundle {
			resolution, ok, err := runtimecontracts.ResolvePromptFileForContractAgent(bundle, agentID, agents[agentID], source, mode)
			if err != nil || !ok {
				out = append(out, Finding{
					CheckID:  "prompt_exists",
					Severity: "warning",
					Message:  fmt.Sprintf("%s/%s: no prompt file", strings.TrimSpace(scopeLabel), agentID),
					Location: location,
				})
				continue
			}
			text, err := promptTextForResolvedFile(resolution.Path)
			if err != nil {
				continue
			}
			if promptHasOpenTODO(text) {
				out = append(out, Finding{
					CheckID:  "prompt_exists",
					Severity: "warning",
					Message:  fmt.Sprintf("%s/%s: prompt contains TODO", strings.TrimSpace(scopeLabel), agentID),
					Location: location,
				})
			}
			continue
		}

		for _, finding := range promptFindingsForLegacyDir(promptsDir, scopeLabel, map[string]runtimecontracts.AgentRegistryEntry{agentID: agents[agentID]}) {
			out = append(out, finding)
		}
	}
	return out
}

func promptFindingsForLegacyDir(promptsDir, scopeLabel string, agents map[string]runtimecontracts.AgentRegistryEntry) []Finding {
	out := make([]Finding, 0, len(agents))
	for agentID := range agents {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		location := scopedObjectLabel(scopeLabel, agentID)
		if strings.TrimSpace(promptsDir) == "" {
			out = append(out, Finding{
				CheckID:  "prompt_exists",
				Severity: "warning",
				Message:  fmt.Sprintf("%s/%s: no prompt file", strings.TrimSpace(scopeLabel), agentID),
				Location: location,
			})
			continue
		}
		resolution, ok, err := runtimecontracts.ResolvePromptFileForContractAgent(&runtimecontracts.WorkflowContractBundle{
			Paths: runtimecontracts.ContractPaths{ProjectPromptsDir: strings.TrimSpace(promptsDir)},
		}, agentID, agents[agentID], runtimecontracts.ContractItemSource{}, "")
		if err != nil || !ok {
			out = append(out, Finding{
				CheckID:  "prompt_exists",
				Severity: "warning",
				Message:  fmt.Sprintf("%s/%s: no prompt file", strings.TrimSpace(scopeLabel), agentID),
				Location: location,
			})
			continue
		}
		text, err := promptTextForResolvedFile(resolution.Path)
		if err != nil {
			continue
		}
		if promptHasOpenTODO(text) {
			out = append(out, Finding{
				CheckID:  "prompt_exists",
				Severity: "warning",
				Message:  fmt.Sprintf("%s/%s: prompt contains TODO", strings.TrimSpace(scopeLabel), agentID),
				Location: location,
			})
		}
	}
	return out
}

func promptTextForResolvedFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func promptHasOpenTODO(text string) bool {
	return strings.Contains(text, "<!-- TODO") && !strings.Contains(text, "<!-- DEFERRED")
}

func projectScopeLabel(key, manifestName string) string {
	if key = strings.TrimSpace(key); key != "" {
		return key
	}
	if manifestName = strings.TrimSpace(manifestName); manifestName != "" {
		return manifestName
	}
	return "root"
}

func flowScopeLabel(id, path string) string {
	if id = strings.TrimSpace(id); id != "" {
		return id
	}
	return strings.TrimSpace(path)
}

func scopedObjectLabel(scopeLabel, objectID string) string {
	scopeLabel = strings.TrimSpace(scopeLabel)
	objectID = strings.TrimSpace(objectID)
	if scopeLabel == "" {
		return objectID
	}
	if objectID == "" {
		return scopeLabel
	}
	return scopeLabel + "/" + objectID
}

func rootPolicyScope(scopes []semanticview.ProjectScope) semanticview.ProjectScope {
	if len(scopes) == 0 {
		return semanticview.ProjectScope{}
	}
	root := scopes[0]
	for _, scope := range scopes[1:] {
		if scope.Depth < root.Depth {
			root = scope
		}
	}
	return root
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortedSetKeys(items map[string]struct{}) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for key := range items {
		key = strings.TrimSpace(key)
		if key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func anyAgentNeedsNativeCapability(source semanticview.Source, capability string) bool {
	if source == nil {
		return false
	}
	capability = strings.TrimSpace(capability)
	if capability == "" {
		return false
	}
	for _, agent := range source.AgentEntries() {
		raw, ok := agent.NativeTools[capability]
		flag, isBool := raw.(bool)
		if ok && isBool && flag {
			return true
		}
	}
	return false
}

var bootverifyEntityReferencePattern = regexp.MustCompile(`entity\.([a-zA-Z_][a-zA-Z0-9_]*)`)

func entityReferences(expression string) []string {
	refs := runtimepipeline.WorkflowEntityReferences(expression)
	out := make([]string, 0, len(refs))
	seen := map[string]struct{}{}
	for _, ref := range refs {
		field := runtimepipeline.WorkflowEntityReferenceField(ref)
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

func entitySchemaFields(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	out := map[string]struct{}{}
	schema := source.WorkflowEntitySchema()
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			name := strings.TrimSpace(field.Name)
			if name != "" {
				out[name] = struct{}{}
			}
		}
	}
	return out
}

func flowSchemaIsTemplate(source semanticview.Source, flowID string) bool {
	if source == nil {
		return false
	}
	schema, ok := source.FlowSchemaByID(strings.TrimSpace(flowID))
	if !ok {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(schema.Mode), "template")
}

func flowIsStateless(source semanticview.Source, flowID string) bool {
	if source == nil {
		return false
	}
	return strings.TrimSpace(source.FlowInitialStage(strings.TrimSpace(flowID))) == "" && len(source.FlowStates(strings.TrimSpace(flowID))) == 0
}

func nodeFlowID(source semanticview.Source, nodeID string) string {
	if source == nil {
		return ""
	}
	if contractSource, ok := source.NodeContractSource(strings.TrimSpace(nodeID)); ok {
		return strings.TrimSpace(contractSource.FlowID)
	}
	return ""
}

func declaredStatesForFlow(source semanticview.Source, flowID string) map[string]struct{} {
	flowID = strings.TrimSpace(flowID)
	var states []string
	var terminals []string
	if flowID == "" {
		states = source.FlowStates("")
		terminals = source.FlowTerminalStages("")
	} else {
		states = source.FlowStates(flowID)
		terminals = source.FlowTerminalStages(flowID)
	}
	out := stringSet(states)
	for _, terminal := range terminals {
		if terminal = strings.TrimSpace(terminal); terminal != "" {
			out[terminal] = struct{}{}
		}
	}
	return out
}

type lifecycleFlowSchemaEntry struct {
	flowID string
	schema runtimecontracts.FlowSchemaDocument
}

type rootFlowSchemaProvider interface {
	RootFlowSchema() (runtimecontracts.FlowSchemaDocument, bool)
}

func lifecycleFlowSchemas(source semanticview.Source) []lifecycleFlowSchemaEntry {
	if source == nil {
		return nil
	}
	out := make([]lifecycleFlowSchemaEntry, 0, len(source.FlowSchemaEntries())+1)
	if provider, ok := source.(rootFlowSchemaProvider); ok {
		if schema, ok := provider.RootFlowSchema(); ok {
			out = append(out, lifecycleFlowSchemaEntry{flowID: "", schema: schema})
		}
	}
	for flowID, schema := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		out = append(out, lifecycleFlowSchemaEntry{flowID: flowID, schema: schema})
	}
	return out
}

func flowUsesAuthoredStages(source semanticview.Source, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	for _, entry := range lifecycleFlowSchemas(source) {
		if strings.TrimSpace(entry.flowID) == flowID {
			return entry.schema.UsesAuthoredStages()
		}
	}
	return false
}

func bundleUsesAuthoredStages(source semanticview.Source) bool {
	for _, entry := range lifecycleFlowSchemas(source) {
		if entry.schema.UsesAuthoredStages() {
			return true
		}
	}
	return false
}

func stageDeclarationCoherenceFindings(flowID string, schema runtimecontracts.FlowSchemaDocument) []Finding {
	if !schema.UsesAuthoredStages() {
		return nil
	}
	label := validationFlowLabel(flowID)
	location := strings.TrimSpace(flowID)
	if location == "" {
		location = "root"
	}
	findings := make([]Finding, 0, 3)
	if schema.HasLegacyLifecycleFields() {
		findings = append(findings, Finding{
			CheckID:  "state_machine_coherence",
			Severity: "error",
			Message:  fmt.Sprintf("flow %s declares stages and legacy lifecycle fields; stages is mutually exclusive with initial_state, states, and terminal_states", label),
			Location: location,
		})
	}
	if len(schema.StageDeclarations.Entries) == 0 {
		return findings
	}
	initialCount := schema.StageDeclarations.InitialCount()
	if initialCount != 1 {
		findings = append(findings, Finding{
			CheckID:  "state_machine_coherence",
			Severity: "error",
			Message:  fmt.Sprintf("flow %s stages must declare exactly one initial stage; got %d", label, initialCount),
			Location: location,
		})
	}
	terminalCount := schema.StageDeclarations.TerminalCount()
	if terminalCount == 0 {
		findings = append(findings, Finding{
			CheckID:  "state_machine_coherence",
			Severity: "error",
			Message:  fmt.Sprintf("flow %s stages must declare at least one terminal stage", label),
			Location: location,
		})
	}
	return findings
}

func validationFlowLabel(flowID string) string {
	if strings.TrimSpace(flowID) == "" {
		return "root"
	}
	return strings.TrimSpace(flowID)
}

func handlerAdvanceTargets(handler runtimecontracts.SystemNodeEventHandler) []string {
	return runtimecontracts.HandlerAdvanceTargets(handler)
}

func uniqueFindings(items []Finding) []Finding {
	if len(items) <= 1 {
		return items
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]Finding, 0, len(items))
	for _, item := range items {
		key := strings.Join([]string{
			strings.TrimSpace(item.CheckID),
			strings.TrimSpace(item.Severity),
			strings.TrimSpace(item.Location),
			strings.TrimSpace(item.Message),
		}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func handlerEmits(handler runtimecontracts.SystemNodeEventHandler) []string {
	return runtimecontracts.HandlerEmitEvents(handler)
}

func branchRuleEmits(rule runtimecontracts.HandlerRuleEntry) []string {
	return runtimecontracts.RuleEmitEvents(rule)
}

type permissionWarning struct {
	Message string
}

func mergedAgentPermissionWarnings(source semanticview.Source) []permissionWarning {
	if source == nil {
		return nil
	}
	agents := source.AgentEntries()
	ids := make([]string, 0, len(agents))
	for agentID := range agents {
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			ids = append(ids, agentID)
		}
	}
	sort.Strings(ids)
	out := make([]permissionWarning, 0, len(ids))
	for _, agentID := range ids {
		agent := agents[agentID]
		flowID := ""
		if sourceInfo, ok := source.AgentContractSource(agentID); ok {
			flowID = strings.TrimSpace(sourceInfo.FlowID)
		}
		scopeLabel := validationFlowLabel(flowID)
		policy := source.ResolvedPolicyForFlow(flowID)
		out = append(out, agentPermissionWarningsLocal(source, scopeLabel, agentID, agent, policy)...)
	}
	return out
}

func agentPermissionWarningsLocal(source semanticview.Source, scopeLabel, agentID string, agent runtimecontracts.AgentRegistryEntry, policy runtimecontracts.PolicyDocument) []permissionWarning {
	if source == nil {
		return nil
	}
	perms, err := resolvedAgentPermissionsLocal(agent, policy)
	if err != nil {
		return []permissionWarning{{Message: fmt.Sprintf("%s/%s permissions resolution failed: %v", strings.TrimSpace(scopeLabel), strings.TrimSpace(agentID), err)}}
	}
	permSet := stringSet(perms)
	tools := agent.ConfiguredTools()
	out := make([]permissionWarning, 0, len(tools))
	for _, toolID := range tools {
		toolID = strings.TrimSpace(toolID)
		if toolID == "" {
			continue
		}
		entry, _ := source.ToolEntryForAgent(agentID, toolID)
		required := toolRequiredPermissionLocal(toolID, entry)
		if required == "" {
			continue
		}
		if _, ok := permSet[required]; ok {
			continue
		}
		out = append(out, permissionWarning{Message: fmt.Sprintf("%s/%s: tool %q missing permission %q", strings.TrimSpace(scopeLabel), strings.TrimSpace(agentID), toolID, required)})
	}
	return out
}

func toolRequiredPermissionLocal(toolID string, entry runtimecontracts.ToolSchemaEntry) string {
	if perm := strings.TrimSpace(entry.Permission); perm != "" {
		return perm
	}
	if perm := strings.TrimSpace(entry.RequiredPermission); perm != "" {
		return perm
	}
	return ""
}

func resolvedAgentPermissionsLocal(agent runtimecontracts.AgentRegistryEntry, policy runtimecontracts.PolicyDocument) ([]string, error) {
	perms := make([]string, 0, len(agent.Permissions)+4)
	bundleName := strings.TrimSpace(agent.PermissionsBundle)
	if bundleName != "" {
		bundlePerms, ok, err := permissionBundlePermissionsLocal(policy, bundleName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("unknown permissions_bundle %q", bundleName)
		}
		perms = append(perms, bundlePerms...)
	}
	perms = append(perms, agent.Permissions...)
	return normalizeStringSliceLocal(perms), nil
}

func permissionBundlePermissionsLocal(policy runtimecontracts.PolicyDocument, bundle string) ([]string, bool, error) {
	root, ok := policy.Values["permission_bundles"]
	if !ok {
		return nil, false, nil
	}
	bundles, ok := normalizePolicyMapLocal(root.Value)
	if !ok {
		return nil, false, fmt.Errorf("permission_bundles must be a mapping")
	}
	rawBundle, ok := bundles[strings.TrimSpace(bundle)]
	if !ok {
		return nil, false, nil
	}
	bundleMap, ok := normalizePolicyMapLocal(rawBundle)
	if !ok {
		return nil, false, fmt.Errorf("permission_bundles.%s must be a mapping", bundle)
	}
	rawPerms, ok := bundleMap["permissions"]
	if !ok {
		return nil, false, fmt.Errorf("permission_bundles.%s.permissions is required", bundle)
	}
	perms, err := stringsFromPolicyValueLocal(rawPerms)
	if err != nil {
		return nil, false, fmt.Errorf("permission_bundles.%s.permissions: %w", bundle, err)
	}
	return perms, true, nil
}

func normalizePolicyMapLocal(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case runtimecontracts.PolicyValue:
		return normalizePolicyMapLocal(typed.Value)
	case map[string]any:
		return typed, true
	case map[string]runtimecontracts.PolicyValue:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item.Value
		}
		return out, true
	default:
		return nil, false
	}
}

func stringsFromPolicyValueLocal(value any) ([]string, error) {
	switch typed := value.(type) {
	case runtimecontracts.PolicyValue:
		return stringsFromPolicyValueLocal(typed.Value)
	case []string:
		return normalizeStringSliceLocal(typed), nil
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("expected string list")
			}
			out = append(out, text)
		}
		return normalizeStringSliceLocal(out), nil
	default:
		return nil, fmt.Errorf("expected string list")
	}
}

func normalizeStringSliceLocal(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func eventExists(source semanticview.Source, eventType string) bool {
	if source == nil {
		return false
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	if runtimecontracts.PlatformEventCatalogContains(source.PlatformSpec(), eventType) || strings.HasPrefix(eventType, "platform.") {
		return true
	}
	if _, ok := source.ResolvedEventCatalog()[eventType]; ok {
		return true
	}
	if _, ok := source.EventEntry(eventType); ok {
		return true
	}
	if !strings.Contains(eventType, "*") {
		return false
	}
	for _, candidate := range runtimecontracts.PlatformEventCatalogNames(source.PlatformSpec()) {
		if routeMatchesLocal(eventType, strings.TrimSpace(candidate)) {
			return true
		}
	}
	for candidate := range source.ResolvedEventCatalog() {
		if routeMatchesLocal(eventType, strings.TrimSpace(candidate)) {
			return true
		}
	}
	for candidate := range source.EventEntries() {
		if routeMatchesLocal(eventType, strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}

func handlerPatternMatchesLocal(pattern, eventType string) bool {
	pattern = strings.TrimSpace(pattern)
	eventType = strings.TrimSpace(eventType)
	if pattern == "" || eventType == "" {
		return false
	}
	if pattern == eventType {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	return routeMatchesLocal(pattern, eventType)
}

func routeMatchesLocal(pattern, eventType string) bool {
	return eventidentity.MatchPattern(pattern, eventType)
}

func stringValueLocal(v any) string {
	if typed, ok := v.(string); ok {
		return typed
	}
	return ""
}

func gateNameLocal(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case runtimecontracts.GateSpec:
		return strings.TrimSpace(typed.Name)
	case *runtimecontracts.GateSpec:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.Name)
	default:
		return strings.TrimSpace(stringValueLocal(v))
	}
}

func handlerDeclaresConflictingCompletion(handler runtimecontracts.SystemNodeEventHandler) bool {
	return len(handler.Rules) > 0 && handlerHasOnComplete(handler)
}

func handlerDeclaresConflictingCreateEntityAccumulation(handler runtimecontracts.SystemNodeEventHandler) bool {
	return handler.CreateEntity && handler.Accumulate != nil
}

func handlerHasOnComplete(handler runtimecontracts.SystemNodeEventHandler) bool {
	if len(handler.OnComplete) > 0 {
		return true
	}
	return handler.Accumulate != nil && len(handler.Accumulate.OnComplete) > 0
}

func workflowFindEventCyclesLocal(graph map[string]map[string]struct{}) [][]string {
	seen := map[string]struct{}{}
	cycles := make([][]string, 0)
	var walk func(start, current string, path []string)
	walk = func(start, current string, path []string) {
		for _, next := range sortedSetKeys(graph[current]) {
			if next == start && len(path) > 1 {
				cycle := append(append([]string{}, path...), next)
				key := strings.Join(cycle, "->")
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				cycles = append(cycles, cycle)
				continue
			}
			if _, ok := graph[next]; !ok || containsString(path, next) {
				continue
			}
			walk(start, next, append(path, next))
		}
	}
	for _, start := range sortedSetKeysFromGraphLocal(graph) {
		walk(start, start, []string{start})
	}
	return cycles
}

func sortedSetKeysFromGraphLocal(graph map[string]map[string]struct{}) []string {
	keys := make([]string, 0, len(graph))
	for key := range graph {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
