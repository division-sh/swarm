package bootverify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/eventidentity"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
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

	toolLoaded   bool
	toolFindings []Finding

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

	payloadCompletenessLoaded   bool
	payloadCompletenessFindings []Finding

	dialectLoaded   bool
	dialectFindings []Finding

	invalidLoaded   bool
	invalidFindings []Finding

	handlerLoaded   bool
	handlerFindings []Finding

	cycleLoaded   bool
	cycleFindings []Finding

	requiredLoaded   bool
	requiredFindings []Finding

	stateLoaded   bool
	stateFindings []Finding

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

	payloadTransformExprLoaded   bool
	payloadTransformExprFindings []Finding

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
	{ID: "event_chain_integrity", Severity: "warning", Run: checkEventChainIntegrity},
	{ID: "event_consumer_exists", Severity: "warning", Run: checkEventConsumerExists},
	{ID: "event_producer_exists", Severity: "warning", Run: checkEventProducerExists},
	{ID: "payload_field_coverage", Severity: "error", Run: checkPayloadFieldCoverage},
	{ID: "semantic_drift_payload_completeness", Severity: "warning", Run: checkSemanticDriftPayloadCompleteness},
	{ID: "condition_payload_alignment", Severity: "error", Run: checkConditionPayloadAlignment},
	{ID: "condition_policy_alignment", Severity: "warning", Run: checkConditionPolicyAlignment},
	{ID: "state_machine_coherence", Severity: "error", Run: checkStateMachineCoherence},
	{ID: "required_agents_match", Severity: "error", Run: checkRequiredAgentsMatch},
	{ID: "handler_field_compliance", Severity: "error", Run: checkHandlerFieldCompliance},
	{ID: "tool_resolution", Severity: "warning", Run: checkToolResolution},
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
	{ID: "transition_reference_validation", Severity: "error", Run: checkTransitionReferenceValidation},
	{ID: "condition_expression_validation", Severity: "error", Run: checkConditionExpressionValidation},
	{ID: "data_accumulation_expression_validation", Severity: "error", Run: checkDataAccumulationExpressionValidation},
	{ID: "payload_transform_expression_validation", Severity: "error", Run: checkPayloadTransformExpressionValidation},
	{ID: "expression_field_reference_validation", Severity: "warning", Run: checkExpressionFieldReferenceValidation},
	{ID: "transition_ownership_validation", Severity: "error", Run: checkTransitionOwnershipValidation},
	{ID: "event_runtime_wiring_validation", Severity: "error", Run: checkEventRuntimeWiringValidation},
	{ID: "timer_validation", Severity: "error", Run: checkTimerValidation},
	{ID: "write_pin_ownership_validation", Severity: "error", Run: checkWritePinOwnershipValidation},
	{ID: "gate_schema_validation", Severity: "error", Run: checkGateSchemaValidation},
	{ID: "input_pin_wiring", Severity: "warning", Run: checkInputPinWiring},
	{ID: "cross_flow_pin_ambiguity_validation", Severity: "error", Run: checkCrossFlowPinAmbiguityValidation},
	{ID: "flow_boundary_create_entity_validation", Severity: "error", Run: checkFlowBoundaryCreateEntityValidation},
}

var supplementalChecks = []Check{
	{ID: "impl.platform_metadata_validation", Severity: "error", Run: checkPlatformMetadataValidation},
	{ID: "impl.deprecated_contract_alias", Severity: "warning", Run: checkDeprecatedContractAlias},
}

func newCheckerContext(ctx context.Context, source semanticview.Source, opts Options) *checkerContext {
	return &checkerContext{
		ctx:    ctx,
		source: source,
		opts:   opts,
	}
}

func checkPayloadFieldCoverage(c *checkerContext) []Finding       { return c.payloadFieldCoverage() }
func checkStateMachineCoherence(c *checkerContext) []Finding      { return c.stateMachineCoherence() }
func checkRequiredAgentsMatch(c *checkerContext) []Finding        { return c.requiredAgentsMatch() }
func checkHandlerFieldCompliance(c *checkerContext) []Finding     { return c.handlerFieldCompliance() }
func checkToolResolution(c *checkerContext) []Finding             { return c.toolResolution() }
func checkPromptExists(c *checkerContext) []Finding               { return c.promptExists() }
func checkInvalidFieldDetection(c *checkerContext) []Finding      { return c.invalidFieldDetection() }
func checkPolicyConflictDetection(c *checkerContext) []Finding    { return c.policyConflicts() }
func checkDialectCompliance(c *checkerContext) []Finding          { return c.dialectCompliance() }
func checkNativeToolsValid(c *checkerContext) []Finding           { return c.nativeTools() }
func checkPlatformNamespaceViolation(c *checkerContext) []Finding { return c.platformNamespace() }
func checkWorkspaceClassExists(c *checkerContext) []Finding       { return c.workspace() }
func checkCredentialKeyExists(c *checkerContext) []Finding        { return c.credentials() }
func checkMCPServerReachable(c *checkerContext) []Finding         { return c.mcp() }
func checkAgentPermissionValidation(c *checkerContext) []Finding {
	return uniqueFindings(append(c.permissions(), c.permissionWarnings()...))
}
func checkPlatformMetadataValidation(c *checkerContext) []Finding { return c.platformMetadata() }
func checkGateSchemaValidation(c *checkerContext) []Finding       { return c.gateSchemaValidation() }
func checkDeprecatedContractAlias(c *checkerContext) []Finding    { return c.deprecatedAliases() }

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

func (c *checkerContext) gateSchemaValidation() []Finding {
	if c.gateSchemaLoaded {
		return c.gateSchemaFindings
	}
	c.gateSchemaLoaded = true
	nodes := c.source.NodeEntries()
	for _, transition := range c.source.DerivedHandlerTransitions() {
		if gate := gateNameLocal(transition.SetsGate); gate != "" {
			node, ok := nodes[strings.TrimSpace(transition.NodeID)]
			if !ok {
				continue
			}
			validGates := stateSchemaGateNamesLocal(node.GateState)
			if len(validGates) > 0 {
				if _, ok := validGates[gate]; !ok {
					c.gateSchemaFindings = append(c.gateSchemaFindings, Finding{
						CheckID:  "gate_schema_validation",
						Severity: "error",
						Message:  fmt.Sprintf("handler transition %s sets_gate %s not recognized in node %s gate_state schema", transition.ID, gate, transition.NodeID),
						Location: strings.TrimSpace(transition.ID),
					})
				}
			}
		}
	}
	return c.gateSchemaFindings
}

func isBackpropEvent(eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	return eventType != "" && strings.HasSuffix(eventType, "_backprop")
}

func platformVersionAtLeast(raw string, major, minor, patch int) bool {
	raw = strings.TrimSpace(raw)
	for _, prefix := range []string{">=", ">", "=", "~", "^"} {
		raw = strings.TrimPrefix(raw, prefix)
	}
	raw = strings.TrimSpace(strings.TrimPrefix(raw, "v"))
	if raw == "" {
		return false
	}
	parts := strings.Split(raw, ".")
	parse := func(index int) int {
		if index >= len(parts) {
			return 0
		}
		value, _ := strconv.Atoi(strings.TrimSpace(parts[index]))
		return value
	}
	gotMajor, gotMinor, gotPatch := parse(0), parse(1), parse(2)
	switch {
	case gotMajor != major:
		return gotMajor > major
	case gotMinor != minor:
		return gotMinor > minor
	default:
		return gotPatch >= patch
	}
}

func (c *checkerContext) deprecatedAliases() []Finding {
	if c.deprecatedLoaded {
		return c.deprecatedFindings
	}
	c.deprecatedLoaded = true
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		if len(agent.Tools) == 0 && len(agent.ToolsTier2) > 0 {
			c.deprecatedFindings = append(c.deprecatedFindings, Finding{
				CheckID:  "impl.deprecated_contract_alias",
				Severity: "warning",
				Message:  fmt.Sprintf("agent %s uses deprecated tools_tier2; rename to tools", agentID),
				Location: agentID,
			})
		}
	}
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
	for _, scope := range c.source.ProjectScopes() {
		scopeLabel := projectScopeLabel(scope.Key, scope.Manifest.Name)
		c.promptFindings = append(c.promptFindings, promptFindingsForDir(scope.PromptsDir, scopeLabel, scope.Agents)...)
	}
	for _, scope := range c.source.FlowScopes() {
		scopeLabel := flowScopeLabel(scope.ID, scope.Path)
		c.promptFindings = append(c.promptFindings, promptFindingsForDir(scope.PromptsDir, scopeLabel, scope.Agents)...)
	}
	return c.promptFindings
}

func (c *checkerContext) toolResolution() []Finding {
	if c.toolLoaded {
		return c.toolFindings
	}
	c.toolLoaded = true
	mcpPrefixes := declaredMCPPrefixes(c.source)
	discoveredTools := c.mcpDiscovered()
	// Boot tool warnings must consume the same runtime inventory truth that the
	// generic runtime ships, then layer MCP discovery on top of it.
	runtimeToolNames := c.runtimeAvailableToolNames()
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		for _, toolID := range agent.ConfiguredTools() {
			toolID = strings.TrimSpace(toolID)
			if toolID == "" {
				continue
			}
			if entry, ok := c.source.ToolEntryForAgent(agentID, toolID); ok {
				if mcpToolEntryRequiresDiscovery(entry) && !toolReferenceAllowedByMCPCatalog(toolID, discoveredTools, mcpPrefixes) {
					c.toolFindings = append(c.toolFindings, Finding{
						CheckID:  "tool_resolution",
						Severity: "warning",
						Message:  fmt.Sprintf("agent %s references missing tool %s", agentID, toolID),
						Location: agentID,
					})
				}
				continue
			}
			if toolReferenceAllowedByMCPCatalog(toolID, discoveredTools, mcpPrefixes) {
				continue
			}
			if _, ok := runtimeToolNames[toolID]; ok {
				continue
			}
			c.toolFindings = append(c.toolFindings, Finding{
				CheckID:  "tool_resolution",
				Severity: "warning",
				Message:  fmt.Sprintf("agent %s references missing tool %s", agentID, toolID),
				Location: agentID,
			})
		}
	}
	return c.toolFindings
}

func (c *checkerContext) runtimeAvailableToolNames() map[string]struct{} {
	if c.runtimeToolNamesLoaded {
		return c.runtimeToolNames
	}
	c.runtimeToolNamesLoaded = true
	c.runtimeToolNames = make(map[string]struct{})
	for _, name := range runtimetools.RuntimeAvailableToolNamesForSource(c.source) {
		c.runtimeToolNames[strings.TrimSpace(name)] = struct{}{}
	}
	return c.runtimeToolNames
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

func (c *checkerContext) payloadFieldCoverage() []Finding {
	if c.payloadCoverageLoaded {
		return c.payloadCoverageFindings
	}
	c.payloadCoverageLoaded = true
	entityFields := entitySchemaFields(c.source)
	for _, transition := range c.source.WorkflowTransitions() {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		for _, field := range transition.DataAccumulation.TargetFields() {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, ok := entityFields[field]; ok {
				continue
			}
			c.payloadCoverageFindings = append(c.payloadCoverageFindings, Finding{
				CheckID:  "payload_field_coverage",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s data_accumulation writes '%s' missing from workflow entity_schema", id, field),
				Location: id,
			})
		}
	}
	return c.payloadCoverageFindings
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
	for agentID, agent := range c.source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		if len(agent.SubscriptionsBootstrap) > 0 {
			c.dialectFindings = append(c.dialectFindings, Finding{
				CheckID:  "dialect_compliance",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s uses deprecated subscriptions_bootstrap", agentID),
				Location: agentID,
			})
		}
	}
	return c.dialectFindings
}

func (c *checkerContext) invalidFieldDetection() []Finding {
	if c.invalidLoaded {
		return c.invalidFindings
	}
	c.invalidLoaded = true
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
			if strings.TrimSpace(node.ExecutionType) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s missing required field execution_type", nodeLabel),
					Location: nodeLabel,
				})
			} else if err := runtimecontracts.ValidateSystemNodeExecutionType(node.ExecutionType); err != nil {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s execution_type must be %s", nodeLabel, runtimecontracts.SystemNodeExecutionType),
					Location: nodeLabel,
				})
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
			if strings.TrimSpace(agent.ModelTier) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field model_tier", agentLabel),
					Location: agentLabel,
				})
			}
			if strings.TrimSpace(agent.ConversationMode) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field conversation_mode", agentLabel),
					Location: agentLabel,
				})
			} else if _, err := sessions.ParseConversationRuntimeMode(agent.ConversationMode); err != nil {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s has invalid conversation_mode: %v", agentLabel, err),
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
			if strings.TrimSpace(node.ExecutionType) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s missing required field execution_type", nodeLabel),
					Location: nodeLabel,
				})
			} else if err := runtimecontracts.ValidateSystemNodeExecutionType(node.ExecutionType); err != nil {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("node %s execution_type must be %s", nodeLabel, runtimecontracts.SystemNodeExecutionType),
					Location: nodeLabel,
				})
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
			if strings.TrimSpace(agent.ModelTier) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field model_tier", agentLabel),
					Location: agentLabel,
				})
			}
			if strings.TrimSpace(agent.ConversationMode) == "" {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s missing required field conversation_mode", agentLabel),
					Location: agentLabel,
				})
			} else if _, err := sessions.ParseConversationRuntimeMode(agent.ConversationMode); err != nil {
				c.invalidFindings = append(c.invalidFindings, Finding{
					CheckID:  "invalid_field_detection",
					Severity: "error",
					Message:  fmt.Sprintf("agent %s has invalid conversation_mode: %v", agentLabel, err),
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
	mode, err := sessions.ParseConversationRuntimeMode(agent.ConversationMode)
	if err != nil {
		return findings
	}
	sessionScope, err := sessions.ValidateSessionScopeIntent(mode, agent.SessionScope)
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

func (c *checkerContext) handlerFieldCompliance() []Finding {
	if c.handlerLoaded {
		return c.handlerFindings
	}
	c.handlerLoaded = true
	nodes := c.source.NodeEntries()
	for _, transition := range c.source.WorkflowTransitions() {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		for _, actionID := range transition.Actions {
			actionID = strings.TrimSpace(actionID)
			if actionID == "" {
				continue
			}
			action, ok := c.source.ActionInstructionByID(actionID)
			if !ok {
				continue
			}
			if action.Executable() || isSupportedWorkflowHandlerActionID(firstNonEmptyString(action.Builtin, action.Key.String())) {
				continue
			}
			c.handlerFindings = append(c.handlerFindings, Finding{
				CheckID:  "handler_field_compliance",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s action %s has no executable runtime implementation", id, actionID),
				Location: id,
			})
		}
		for _, guardID := range transition.Guards {
			guardID = strings.TrimSpace(guardID)
			if guardID == "" {
				continue
			}
			guard, ok := c.source.GuardInstructionByID(guardID)
			if !ok || guard.Executable() {
				continue
			}
			c.handlerFindings = append(c.handlerFindings, Finding{
				CheckID:  "handler_field_compliance",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s guard %s has no executable runtime implementation", id, guardID),
				Location: id,
			})
		}
	}
	for nodeID, node := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if actionID := strings.TrimSpace(handler.Action.ID); actionID != "" {
				if normalizeWorkflowBuiltinActionID(actionID) == "create_flow_instance" {
					templateID := strings.TrimSpace(handler.Action.Template)
					if templateID == "" {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s create_flow_instance is missing template", nodeID, eventType),
							Location: nodeID,
						})
					} else if !flowSchemaIsTemplate(c.source, templateID) {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s create_flow_instance template %s is not mode: template", nodeID, eventType, templateID),
							Location: nodeID,
						})
					}
				}
				if normalizeWorkflowBuiltinActionID(actionID) == "record_evidence" && strings.TrimSpace(handler.EvidenceTarget) == "" {
					c.handlerFindings = append(c.handlerFindings, Finding{
						CheckID:  "handler_field_compliance",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s record_evidence is missing evidence_target", nodeID, eventType),
						Location: nodeID,
					})
				}
				if !handlerActionExecutable(c.source, actionID) {
					c.handlerFindings = append(c.handlerFindings, Finding{
						CheckID:  "handler_field_compliance",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s action %s is not executable", nodeID, eventType, actionID),
						Location: nodeID,
					})
				}
			}
		}
	}
	c.handlerFindings = append(c.handlerFindings, runtimeHandledEventsMissingExecutors(c.source)...)
	return c.handlerFindings
}

func (c *checkerContext) requiredAgentsMatch() []Finding {
	if c.requiredLoaded {
		return c.requiredFindings
	}
	c.requiredLoaded = true
	for flowID, requiredAgents := range requiredAgentsByFlow(c.source) {
		for _, required := range requiredAgents {
			role := strings.TrimSpace(required.Role)
			if role == "" {
				c.requiredFindings = append(c.requiredFindings, Finding{
					CheckID:  "required_agents_match",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s required_agents entry missing role", flowID),
					Location: strings.TrimSpace(flowID),
				})
				continue
			}
			agentID, agent, ok := requiredAgentProvider(c.source, role)
			if !ok {
				c.requiredFindings = append(c.requiredFindings, Finding{
					CheckID:  "required_agents_match",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s required agent role %s missing from merged agents", flowID, role),
					Location: strings.TrimSpace(flowID),
				})
				continue
			}
			if diff := missingStrings(required.SubscribesTo, agentSubscriptions(agent)); diff != "" {
				c.requiredFindings = append(c.requiredFindings, Finding{
					CheckID:  "required_agents_match",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s required agent %s subscriptions mismatch (%s)", flowID, agentID, diff),
					Location: strings.TrimSpace(flowID),
				})
			}
			if diff := missingStrings(required.Emits, agent.EmitEvents); diff != "" {
				c.requiredFindings = append(c.requiredFindings, Finding{
					CheckID:  "required_agents_match",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s required agent %s emits mismatch (%s)", flowID, agentID, diff),
					Location: strings.TrimSpace(flowID),
				})
			}
		}
	}
	for _, required := range c.source.RequiredAgents() {
		role := strings.TrimSpace(required.Role)
		if role == "" {
			c.requiredFindings = append(c.requiredFindings, Finding{
				CheckID:  "required_agents_match",
				Severity: "error",
				Message:  "root schema required_agents entry missing role",
				Location: "root",
			})
			continue
		}
		agentID, agent, ok := requiredAgentProvider(c.source, role)
		if !ok {
			c.requiredFindings = append(c.requiredFindings, Finding{
				CheckID:  "required_agents_match",
				Severity: "error",
				Message:  fmt.Sprintf("root schema required agent role %s missing from merged agents", role),
				Location: "root",
			})
			continue
		}
		if diff := missingStrings(required.SubscribesTo, agentSubscriptions(agent)); diff != "" {
			c.requiredFindings = append(c.requiredFindings, Finding{
				CheckID:  "required_agents_match",
				Severity: "error",
				Message:  fmt.Sprintf("root required agent %s subscriptions mismatch (%s)", agentID, diff),
				Location: "root",
			})
		}
		if diff := missingStrings(required.Emits, agent.EmitEvents); diff != "" {
			c.requiredFindings = append(c.requiredFindings, Finding{
				CheckID:  "required_agents_match",
				Severity: "error",
				Message:  fmt.Sprintf("root required agent %s emits mismatch (%s)", agentID, diff),
				Location: "root",
			})
		}
	}
	return c.requiredFindings
}

func (c *checkerContext) stateMachineCoherence() []Finding {
	if c.stateLoaded {
		return c.stateFindings
	}
	c.stateLoaded = true
	for flowID := range c.source.FlowSchemaEntries() {
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
	if !anyAgentNeedsNativeCapability(c.source, "web_search") {
		return uniqueFindings(c.nativeFindings)
	}
	if _, ok := semanticview.PolicyValueForFlow(c.source, "", "web_search_provider"); !ok {
		return uniqueFindings(c.nativeFindings)
	}
	policy := policyValueMap(c.source.ResolvedPolicyForFlow(""))
	root, ok := anyMap(policy["web_search_provider"])
	if !ok {
		c.nativeFindings = append(c.nativeFindings, Finding{
			CheckID:  "native_tools_valid",
			Severity: "error",
			Message:  "policy.web_search_provider must be a mapping",
			Location: "global",
		})
		return uniqueFindings(c.nativeFindings)
	}
	provider := strings.ToLower(strings.TrimSpace(anyString(root["provider"])))
	switch provider {
	case "brave", "serper", "tavily":
	case "custom":
		if _, ok := anyMap(root["http"]); !ok {
			c.nativeFindings = append(c.nativeFindings, Finding{
				CheckID:  "native_tools_valid",
				Severity: "error",
				Message:  "policy.web_search_provider.http is required for custom provider",
				Location: "global",
			})
		}
		if strings.TrimSpace(anyString(root["response_path"])) == "" {
			c.nativeFindings = append(c.nativeFindings, Finding{
				CheckID:  "native_tools_valid",
				Severity: "error",
				Message:  "policy.web_search_provider.response_path is required for custom provider",
				Location: "global",
			})
		}
		fieldMapping, ok := anyMap(root["field_mapping"])
		if !ok {
			c.nativeFindings = append(c.nativeFindings, Finding{
				CheckID:  "native_tools_valid",
				Severity: "error",
				Message:  "policy.web_search_provider.field_mapping is required for custom provider",
				Location: "global",
			})
		} else {
			for _, field := range []string{"title", "url", "snippet"} {
				if strings.TrimSpace(anyString(fieldMapping[field])) != "" {
					continue
				}
				c.nativeFindings = append(c.nativeFindings, Finding{
					CheckID:  "native_tools_valid",
					Severity: "error",
					Message:  fmt.Sprintf("policy.web_search_provider.field_mapping.%s is required for custom provider", field),
					Location: "global",
				})
			}
		}
	default:
		c.nativeFindings = append(c.nativeFindings, Finding{
			CheckID:  "native_tools_valid",
			Severity: "error",
			Message:  fmt.Sprintf("policy.web_search_provider.provider %q is not supported", provider),
			Location: "global",
		})
	}
	return uniqueFindings(c.nativeFindings)
}

func (c *checkerContext) platformNamespace() []Finding {
	if c.namespaceLoaded {
		return c.namespaceFindings
	}
	c.namespaceLoaded = true
	for eventType := range c.source.EventEntries() {
		eventType = strings.TrimSpace(eventType)
		if !strings.HasPrefix(eventType, "platform.") {
			continue
		}
		c.namespaceFindings = append(c.namespaceFindings, Finding{
			CheckID:  "platform_namespace_violation",
			Severity: "error",
			Message:  fmt.Sprintf("event %s uses reserved platform.* namespace", eventType),
			Location: eventType,
		})
	}
	for agentID, agent := range c.source.AgentEntries() {
		for _, eventType := range agent.EmitEvents {
			eventType = strings.TrimSpace(eventType)
			if !strings.HasPrefix(eventType, "platform.") {
				continue
			}
			c.namespaceFindings = append(c.namespaceFindings, Finding{
				CheckID:  "platform_namespace_violation",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s emit_events references reserved platform.* namespace event %s", strings.TrimSpace(agentID), eventType),
				Location: strings.TrimSpace(agentID),
			})
		}
	}
	for nodeID, node := range c.source.NodeEntries() {
		for _, eventType := range node.Produces {
			eventType = strings.TrimSpace(eventType)
			if !strings.HasPrefix(eventType, "platform.") {
				continue
			}
			c.namespaceFindings = append(c.namespaceFindings, Finding{
				CheckID:  "platform_namespace_violation",
				Severity: "error",
				Message:  fmt.Sprintf("node %s produces references reserved platform.* namespace event %s", strings.TrimSpace(nodeID), eventType),
				Location: strings.TrimSpace(nodeID),
			})
		}
		for eventType, handler := range c.source.NodeEventHandlers(nodeID) {
			for _, emitted := range handlerEmits(handler) {
				emitted = strings.TrimSpace(emitted)
				if !strings.HasPrefix(emitted, "platform.") {
					continue
				}
				c.namespaceFindings = append(c.namespaceFindings, Finding{
					CheckID:  "platform_namespace_violation",
					Severity: "error",
					Message:  fmt.Sprintf("node %s handler %s emits reserved platform.* namespace event %s", strings.TrimSpace(nodeID), strings.TrimSpace(eventType), emitted),
					Location: strings.TrimSpace(nodeID),
				})
			}
		}
	}
	sort.SliceStable(c.namespaceFindings, func(i, j int) bool {
		if c.namespaceFindings[i].Location == c.namespaceFindings[j].Location {
			return c.namespaceFindings[i].Message < c.namespaceFindings[j].Message
		}
		return c.namespaceFindings[i].Location < c.namespaceFindings[j].Location
	})
	return c.namespaceFindings
}

func (c *checkerContext) credentials() []Finding {
	if c.credentialLoaded {
		return c.credentialFindings
	}
	c.credentialLoaded = true
	if c.opts.Credentials == nil {
		return nil
	}
	missing, err := runtimecredentials.MissingRequired(c.ctx, c.opts.Credentials, c.source)
	if err != nil {
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:  "credential_key_exists",
			Severity: "error",
			Message:  strings.TrimSpace(err.Error()),
			Location: "global",
		})
		return c.credentialFindings
	}
	for _, item := range missing {
		requiredBy := make([]string, 0, len(item.RequiredBy))
		for _, ref := range item.RequiredBy {
			requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+" "+strings.TrimSpace(ref.Name))
		}
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:  "credential_key_exists",
			Severity: "warning",
			Message:  fmtCredentialWarning(item.Key, requiredBy),
			Location: item.Key,
		})
	}
	return c.credentialFindings
}

func (c *checkerContext) mcp() []Finding {
	if c.mcpLoaded {
		return c.mcpFindings
	}
	c.mcpLoaded = true
	for _, refreshErr := range c.mcpDiscoveryErrs() {
		msg := strings.TrimSpace(refreshErr.Error())
		c.mcpFindings = append(c.mcpFindings, Finding{
			CheckID:  "mcp_server_reachable",
			Severity: "warning",
			Message:  msg,
			Location: locationFromMessage(msg),
		})
	}
	return c.mcpFindings
}

func (c *checkerContext) mcpDiscovered() map[string]runtimemcp.DiscoveredTool {
	c.ensureMCPDiscovery()
	return c.mcpDiscoveredTools
}

func (c *checkerContext) mcpDiscoveryErrs() []error {
	c.ensureMCPDiscovery()
	return c.mcpDiscoveryErrors
}

func (c *checkerContext) ensureMCPDiscovery() {
	if c.mcpDiscoveryLoaded {
		return
	}
	c.mcpDiscoveryLoaded = true
	if !c.opts.CheckMCPReachable {
		return
	}
	client := runtimemcp.NewClient(c.opts.Credentials)
	c.mcpDiscoveryErrors = client.Refresh(c.ctx, c.source)
	c.mcpDiscoveredTools = client.DiscoveredTools()
}

func promptFindingsForDir(promptsDir, scopeLabel string, agents map[string]runtimecontracts.AgentRegistryEntry) []Finding {
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
		path := filepath.Join(strings.TrimSpace(promptsDir), agentID+".md")
		content, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				out = append(out, Finding{
					CheckID:  "prompt_exists",
					Severity: "warning",
					Message:  fmt.Sprintf("%s/%s: no prompt file", strings.TrimSpace(scopeLabel), agentID),
					Location: location,
				})
			}
			continue
		}
		text := string(content)
		if strings.Contains(text, "<!-- TODO") && !strings.Contains(text, "<!-- DEFERRED") {
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

func declaredMCPPrefixes(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "mcp_servers")
	if !ok {
		return nil
	}
	root, ok := anyMap(value.Value)
	if !ok || len(root) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(root))
	for _, raw := range root {
		server, ok := anyMap(raw)
		if !ok {
			continue
		}
		prefix := strings.TrimSpace(anyString(server["prefix"]))
		if prefix != "" {
			out[prefix] = struct{}{}
		}
	}
	return out
}

func toolReferenceAllowedByMCPPrefix(toolID string, prefixes map[string]struct{}) bool {
	if len(prefixes) == 0 {
		return false
	}
	prefix, _, ok := strings.Cut(strings.TrimSpace(toolID), ".")
	if !ok || strings.TrimSpace(prefix) == "" {
		return false
	}
	_, exists := prefixes[strings.TrimSpace(prefix)]
	return exists
}

func toolReferenceAllowedByMCPCatalog(toolID string, discovered map[string]runtimemcp.DiscoveredTool, prefixes map[string]struct{}) bool {
	toolID = strings.TrimSpace(toolID)
	if toolID == "" {
		return false
	}
	if len(discovered) > 0 {
		_, ok := discovered[toolID]
		return ok
	}
	return toolReferenceAllowedByMCPPrefix(toolID, prefixes)
}

func mcpToolEntryRequiresDiscovery(entry runtimecontracts.ToolSchemaEntry) bool {
	return strings.EqualFold(strings.TrimSpace(entry.HandlerType), "mcp")
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

func requiredAgentsByFlow(source semanticview.Source) map[string][]runtimecontracts.FlowRequiredAgent {
	out := map[string][]runtimecontracts.FlowRequiredAgent{}
	if source == nil {
		return out
	}
	for flowID := range source.FlowSchemaEntries() {
		out[strings.TrimSpace(flowID)] = source.FlowRequiredAgents(flowID)
	}
	return out
}

func requiredAgentProvider(source semanticview.Source, role string) (string, runtimecontracts.AgentRegistryEntry, bool) {
	if source == nil {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	role = strings.TrimSpace(role)
	if role == "" {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	requiredKey := normalizeRoleKey(role)
	agents := source.AgentEntries()
	for agentID, agent := range agents {
		if normalizeRoleKey(agentID) == requiredKey {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	for agentID, agent := range agents {
		if roleMatches(agentID, agent.Role, role) {
			return strings.TrimSpace(agentID), agent, true
		}
	}
	return "", runtimecontracts.AgentRegistryEntry{}, false
}

func roleMatches(agentID, agentRole, requiredRole string) bool {
	requiredKey := normalizeRoleKey(requiredRole)
	if requiredKey == "" {
		return false
	}
	return normalizeRoleKey(agentID) == requiredKey || normalizeRoleKey(agentRole) == requiredKey
}

func normalizeRoleKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "_", "-")
	raw = strings.Join(strings.Fields(raw), "-")
	return raw
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
	schema, ok := source.FlowSchemaByID(strings.TrimSpace(flowID))
	if !ok {
		return false
	}
	return strings.TrimSpace(schema.InitialState) == "" && len(schema.States) == 0
}

func agentSubscriptions(agent runtimecontracts.AgentRegistryEntry) []string {
	values := make([]string, 0, len(agent.SubscribesTo)+len(agent.Subscriptions)+len(agent.SubscriptionsBootstrap))
	values = append(values, agent.SubscribesTo...)
	values = append(values, agent.Subscriptions...)
	values = append(values, agent.SubscriptionsBootstrap...)
	return values
}

func missingStrings(expected, actual []string) string {
	actualSet := stringSet(actual)
	missing := make([]string, 0)
	for _, value := range expected {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := actualSet[value]; !ok {
			missing = append(missing, value)
		}
	}
	sort.Strings(missing)
	return strings.Join(missing, ", ")
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
		for _, stage := range source.WorkflowStages() {
			if id := strings.TrimSpace(stage.ID); id != "" {
				states = append(states, id)
			}
		}
		terminals = source.WorkflowTerminalStages()
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

func validationFlowLabel(flowID string) string {
	if strings.TrimSpace(flowID) == "" {
		return "root"
	}
	return strings.TrimSpace(flowID)
}

func handlerAdvanceTargets(handler runtimecontracts.SystemNodeEventHandler) []string {
	out := make([]string, 0, 8)
	if target := strings.TrimSpace(handler.AdvancesTo); target != "" {
		out = append(out, target)
	}
	for _, rule := range handler.Rules {
		if target := strings.TrimSpace(rule.AdvancesTo); target != "" {
			out = append(out, target)
		}
	}
	for _, rule := range handler.OnComplete {
		if target := strings.TrimSpace(rule.AdvancesTo); target != "" {
			out = append(out, target)
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if target := strings.TrimSpace(rule.AdvancesTo); target != "" {
				out = append(out, target)
			}
		}
	}
	return out
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
	out := make([]string, 0, len(handler.Rules)+4)
	for _, emitted := range handler.Emits.Values() {
		emitted = strings.TrimSpace(emitted)
		if emitted != "" {
			out = append(out, emitted)
		}
	}
	for _, rule := range handler.Rules {
		for _, emitted := range rule.Emits.Values() {
			emitted = strings.TrimSpace(emitted)
			if emitted != "" {
				out = append(out, emitted)
			}
		}
	}
	for _, branch := range handler.OnComplete {
		for _, emitted := range branchRuleEmits(branch) {
			out = append(out, emitted)
		}
	}
	if handler.FanOut != nil {
		if emitted := strings.TrimSpace(handler.FanOut.EmitPerItem); emitted != "" {
			out = append(out, emitted)
		}
	}
	return out
}

func branchRuleEmits(rule runtimecontracts.HandlerRuleEntry) []string {
	out := make([]string, 0, 4)
	for _, emitted := range rule.Emits.Values() {
		emitted = strings.TrimSpace(emitted)
		if emitted != "" {
			out = append(out, emitted)
		}
	}
	if rule.FanOut != nil {
		if emitted := strings.TrimSpace(rule.FanOut.EmitPerItem); emitted != "" {
			out = append(out, emitted)
		}
	}
	return out
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
	if strings.HasPrefix(eventType, "platform.") {
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

func stateSchemaGateNamesLocal(schema runtimecontracts.NodeGateStateSchema) map[string]struct{} {
	gates := map[string]struct{}{}
	for _, f := range schema.Gates {
		if strings.TrimSpace(f.Name) != "" {
			gates[strings.TrimSpace(f.Name)] = struct{}{}
		}
	}
	return gates
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

func handlerHasOnComplete(handler runtimecontracts.SystemNodeEventHandler) bool {
	if len(handler.OnComplete) > 0 {
		return true
	}
	return handler.Accumulate != nil && len(handler.Accumulate.OnComplete) > 0
}

func supportedWorkflowRuntimeExecutorIDs(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	for nodeID, entry := range source.NodeEntries() {
		if strings.TrimSpace(nodeID) == "" {
			continue
		}
		if len(source.NodeEventHandlers(nodeID)) > 0 || len(entry.EventHandlers) > 0 {
			out[nodeID] = struct{}{}
		}
	}
	return out
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeWorkflowBuiltinActionID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func isSupportedWorkflowActionBuiltin(id string) bool {
	return runtimecontracts.IsSupportedHandlerActionID(normalizeWorkflowBuiltinActionID(id))
}

func isSupportedWorkflowHandlerActionID(id string) bool {
	return runtimecontracts.IsSupportedHandlerActionID(normalizeWorkflowBuiltinActionID(id))
}

func handlerActionExecutable(source semanticview.Source, actionID string) bool {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return true
	}
	if isSupportedWorkflowHandlerActionID(actionID) {
		return true
	}
	entry, ok := source.ActionInstructionByID(actionID)
	return ok && entry.Executable()
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
