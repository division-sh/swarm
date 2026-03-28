package bootverify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
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

	workflowLoaded   bool
	workflowFindings []Finding

	permissionLoaded   bool
	permissionFindings []Finding

	workspaceLoaded   bool
	workspaceFindings []Finding

	credentialLoaded   bool
	credentialFindings []Finding

	mcpLoaded   bool
	mcpFindings []Finding

	phantomLoaded   bool
	phantomFindings []Finding

	singleNodeLoaded   bool
	singleNodeFindings []Finding
}

var bootCheckRegistry = []Check{
	{ID: "event_chain_integrity", Severity: "warning", Run: checkEventChainIntegrity},
	{ID: "event_consumer_exists", Severity: "warning", Run: checkEventConsumerExists},
	{ID: "event_producer_exists", Severity: "warning", Run: checkEventProducerExists},
	{ID: "payload_field_coverage", Severity: "error", Run: checkPayloadFieldCoverage},
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
}

var supplementalChecks = []Check{
	{ID: "agent_permission_validation", Severity: "error", Run: checkAgentPermissionValidation},
}

func newCheckerContext(ctx context.Context, source semanticview.Source, opts Options) *checkerContext {
	return &checkerContext{
		ctx:    ctx,
		source: source,
		opts:   opts,
	}
}

func checkEventChainIntegrity(c *checkerContext) []Finding       { return c.workflowByCheck("event_chain_integrity") }
func checkEventConsumerExists(c *checkerContext) []Finding       { return c.workflowByCheck("event_consumer_exists") }
func checkEventProducerExists(c *checkerContext) []Finding       { return c.workflowByCheck("event_producer_exists") }
func checkPayloadFieldCoverage(c *checkerContext) []Finding      { return c.workflowByCheck("payload_field_coverage") }
func checkConditionPayloadAlignment(c *checkerContext) []Finding { return c.workflowByCheck("condition_payload_alignment") }
func checkConditionPolicyAlignment(c *checkerContext) []Finding  { return c.workflowByCheck("condition_policy_alignment") }
func checkStateMachineCoherence(c *checkerContext) []Finding     { return c.workflowByCheck("state_machine_coherence") }
func checkRequiredAgentsMatch(c *checkerContext) []Finding       { return c.workflowByCheck("required_agents_match") }
func checkHandlerFieldCompliance(c *checkerContext) []Finding    { return c.workflowByCheck("handler_field_compliance") }
func checkToolResolution(c *checkerContext) []Finding            { return c.workflowByCheck("tool_resolution") }
func checkPromptExists(c *checkerContext) []Finding              { return c.workflowByCheck("prompt_exists") }
func checkProducesDrift(c *checkerContext) []Finding             { return c.workflowByCheck("produces_drift") }
func checkInvalidFieldDetection(c *checkerContext) []Finding     { return c.workflowByCheck("invalid_field_detection") }
func checkPolicyConflictDetection(c *checkerContext) []Finding   { return c.workflowByCheck("policy_conflict_detection") }
func checkEventCycleDetection(c *checkerContext) []Finding       { return c.workflowByCheck("event_cycle_detection") }
func checkDialectCompliance(c *checkerContext) []Finding         { return c.workflowByCheck("dialect_compliance") }
func checkConfigFromPayloadAlignment(c *checkerContext) []Finding {
	return c.workflowByCheck("config_from_payload_alignment")
}
func checkNativeToolsValid(c *checkerContext) []Finding         { return c.workflowByCheck("native_tools_valid") }
func checkPlatformNamespaceViolation(c *checkerContext) []Finding { return c.workflowByCheck("platform_namespace_violation") }
func checkWorkspaceClassExists(c *checkerContext) []Finding     { return c.workspace() }
func checkCredentialKeyExists(c *checkerContext) []Finding      { return c.credentials() }
func checkMCPServerReachable(c *checkerContext) []Finding       { return c.mcp() }
func checkAgentPermissionValidation(c *checkerContext) []Finding { return c.permissions() }
func checkPhantomProduces(c *checkerContext) []Finding          { return c.phantomProduces() }
func checkSingleNodePerEvent(c *checkerContext) []Finding       { return c.singleNodePerEvent() }

func (c *checkerContext) workflow() []Finding {
	if c.workflowLoaded {
		return c.workflowFindings
	}
	c.workflowLoaded = true
	warnings, err := runtimepipeline.ValidateWorkflowContractsDetailed(c.source)
	if err != nil {
		c.workflowFindings = append(c.workflowFindings, findingsFromValidationError(err)...)
	}
	for _, warning := range warnings {
		c.workflowFindings = append(c.workflowFindings, Finding{
			CheckID:  warningCheckID(warning.Category),
			Severity: "warning",
			Message:  strings.TrimSpace(warning.Message),
			Location: locationFromMessage(warning.Message),
		})
	}
	return c.workflowFindings
}

func (c *checkerContext) workflowByCheck(checkID string) []Finding {
	items := c.workflow()
	out := make([]Finding, 0)
	for _, finding := range items {
		if finding.CheckID == checkID {
			out = append(out, finding)
		}
	}
	return out
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

func (c *checkerContext) workspace() []Finding {
	if c.workspaceLoaded {
		return c.workspaceFindings
	}
	c.workspaceLoaded = true
	c.workspaceFindings = workspaceClassFindings(c.source)
	return c.workspaceFindings
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
	if !c.opts.CheckMCPReachable {
		return nil
	}
	client := runtimemcp.NewClient(c.opts.Credentials)
	for _, refreshErr := range client.Refresh(c.ctx, c.source) {
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

func (c *checkerContext) phantomProduces() []Finding {
	if c.phantomLoaded {
		return c.phantomFindings
	}
	c.phantomLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		emitted := map[string]struct{}{}
		for _, handler := range node.EventHandlers {
			for _, eventType := range handlerEmits(handler) {
				eventType = strings.TrimSpace(eventType)
				if eventType != "" {
					emitted[eventType] = struct{}{}
				}
			}
		}
		for _, eventType := range node.Produces {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := emitted[eventType]; ok {
				continue
			}
			c.phantomFindings = append(c.phantomFindings, Finding{
				CheckID:  "phantom_produces",
				Severity: "warning",
				Message:  fmt.Sprintf("node %s produces lists %s but no handler emits it", strings.TrimSpace(nodeID), eventType),
				Location: strings.TrimSpace(nodeID),
			})
		}
	}
	return c.phantomFindings
}

func (c *checkerContext) singleNodePerEvent() []Finding {
	if c.singleNodeLoaded {
		return c.singleNodeFindings
	}
	c.singleNodeLoaded = true
	owners := map[string][]string{}
	for nodeID, node := range c.source.NodeEntries() {
		for eventType := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" || strings.Contains(eventType, "*") {
				continue
			}
			owners[eventType] = append(owners[eventType], strings.TrimSpace(nodeID))
		}
	}
	eventNames := make([]string, 0, len(owners))
	for eventType := range owners {
		eventNames = append(eventNames, eventType)
	}
	sort.Strings(eventNames)
	for _, eventType := range eventNames {
		nodeIDs := owners[eventType]
		if len(nodeIDs) <= 1 {
			continue
		}
		sort.Strings(nodeIDs)
		c.singleNodeFindings = append(c.singleNodeFindings, Finding{
			CheckID:  "single_node_per_event",
			Severity: "error",
			Message:  fmt.Sprintf("event %s has multiple owning nodes: %s", eventType, strings.Join(nodeIDs, ", ")),
			Location: eventType,
		})
	}
	return c.singleNodeFindings
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
