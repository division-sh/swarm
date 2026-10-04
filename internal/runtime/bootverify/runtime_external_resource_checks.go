package bootverify

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (c *checkerContext) credentials() []Finding {
	if c.opts.Purpose == StructuralValidation {
		return nil
	}
	if c.credentialLoaded {
		return c.credentialFindings
	}
	c.credentialLoaded = true
	c.credentialObservation = &AdmissionObservation{Status: AdmissionPassed, StartedAt: time.Now().UTC()}
	defer func() { c.credentialObservation.FinishedAt = time.Now().UTC() }()
	missing, err := MissingStaticCredentialRequirements(c.ctx, c.source, c.opts)
	if err != nil {
		c.credentialObservation.Status, c.credentialObservation.Reason = AdmissionUnavailable, "required static credential inspection failed"
		c.credentialObservation.FailureClass = failures.ClassDependencyUnavailable
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:      "credential_key_exists",
			Severity:     "error",
			Message:      strings.TrimSpace(err.Error()),
			Location:     "global",
			FailureClass: failures.ClassDependencyUnavailable,
		})
		return c.credentialFindings
	}
	for _, item := range missing {
		requiredBy := make([]string, 0, len(item.RequiredBy))
		toolRequirements := []string{}
		for _, ref := range item.RequiredBy {
			requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+" "+strings.TrimSpace(ref.Name))
			if strings.TrimSpace(ref.Kind) == "tool" {
				toolRequirements = append(toolRequirements, strings.TrimSpace(ref.Name))
			}
		}
		message := fmtCredentialWarning(item.Key, requiredBy)
		if len(toolRequirements) > 0 {
			message = AppendLiveEffectReachability(message, c.opts.EffectReachability, toolRequirements)
		}
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:      "credential_key_exists",
			Severity:     "warning",
			Message:      message,
			Location:     item.Key,
			FailureClass: failures.ClassAuthenticationNeeded,
		})
	}
	managed, err := MissingManagedCredentialRequirements(c.ctx, c.source, c.opts)
	if err != nil {
		c.credentialObservation.Status, c.credentialObservation.Reason = AdmissionUnavailable, "required managed credential inspection failed"
		c.credentialObservation.FailureClass = failures.ClassDependencyUnavailable
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:      "managed_credential_state",
			Severity:     "error",
			Message:      strings.TrimSpace(err.Error()),
			Location:     "global",
			FailureClass: failures.ClassDependencyUnavailable,
		})
		return c.credentialFindings
	}
	for _, item := range managed {
		requiredBy := make([]string, 0, len(item.RequiredBy))
		toolRequirements := []string{}
		for _, ref := range item.RequiredBy {
			requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+" "+strings.TrimSpace(ref.Name))
			if strings.TrimSpace(ref.Kind) == "tool" {
				toolRequirements = append(toolRequirements, strings.TrimSpace(ref.Name))
			}
		}
		message := AppendLiveEffectReachability(fmtManagedCredentialWarning(item, requiredBy), c.opts.EffectReachability, toolRequirements)
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:      "managed_credential_state",
			Severity:     "warning",
			Message:      message,
			Location:     item.Key,
			FailureClass: failures.ClassAuthenticationNeeded,
		})
	}
	return c.credentialFindings
}

func appendLiveAgentReachability(message string, liveAgentIDs []string) string {
	if len(liveAgentIDs) == 0 {
		return message
	}
	sort.Strings(liveAgentIDs)
	return message + " (reachable from live agents " + strings.Join(liveAgentIDs, ", ") + ")"
}

func AppendLiveEffectReachability(message string, reachability SourceBootEffectReachability, toolIDs []string) string {
	message = appendLiveAgentReachability(message, reachability.LiveAgentIDs())
	sites := []string{}
	seen := map[string]struct{}{}
	for _, toolID := range toolIDs {
		for _, site := range reachability.LiveWorkflowActivitySites(toolID) {
			if _, duplicate := seen[site]; duplicate {
				continue
			}
			seen[site] = struct{}{}
			sites = append(sites, site)
		}
	}
	if len(sites) == 0 {
		return message
	}
	sort.Strings(sites)
	return message + " (reachable from live workflow activities " + strings.Join(sites, ", ") + ")"
}

func MissingStaticCredentialRequirements(ctx context.Context, source semanticview.Source, opts Options) ([]runtimecredentials.Descriptor, error) {
	index := runtimecredentials.BuildRequirementIndex(source)
	keys := sortedSetKeysLocal(index)
	out := make([]runtimecredentials.Descriptor, 0, len(keys))
	for _, key := range keys {
		requirements := liveStaticCredentialRequirements(source, opts, index[key])
		if len(requirements) == 0 {
			continue
		}
		if opts.Credentials == nil {
			return nil, staticCredentialInspectionError(key, fmt.Errorf("credential store is not configured"))
		}
		descriptor, err := runtimecredentials.Describe(ctx, opts.Credentials, source, key)
		if err != nil {
			return nil, staticCredentialInspectionError(key, err)
		}
		descriptor.RequiredBy = requirements
		if !descriptor.Present {
			out = append(out, descriptor)
		}
	}
	return out, nil
}

func staticCredentialInspectionError(key string, cause error) error {
	failure := failures.Wrap(failures.ClassDependencyUnavailable, "credential_inspection_unavailable", "bootverify", "static_credential_inspection", map[string]any{"credential_key": key}, cause)
	return fmt.Errorf("required static credential inspection is unavailable for %s: %w", key, failure)
}

func liveStaticCredentialRequirements(_ semanticview.Source, opts Options, requirements []runtimecredentials.Requirement) []runtimecredentials.Requirement {
	out := make([]runtimecredentials.Requirement, 0, len(requirements))
	for _, requirement := range requirements {
		if requiresLiveCredential(opts, requirement.Kind, requirement.Name) {
			out = append(out, requirement)
		}
	}
	return out
}

func liveManagedCredentialRequirements(_ semanticview.Source, opts Options, requirements []runtimemanagedcredentials.Requirement) []runtimemanagedcredentials.Requirement {
	out := make([]runtimemanagedcredentials.Requirement, 0, len(requirements))
	for _, requirement := range requirements {
		if requiresLiveCredential(opts, requirement.Kind, requirement.Name) {
			out = append(out, requirement)
		}
	}
	return out
}

func requiresLiveCredential(opts Options, kind, name string) bool {
	return strings.TrimSpace(kind) != "tool" || opts.EffectReachability.ToolCredentialRequired(name)
}

func MissingManagedCredentialRequirements(ctx context.Context, source semanticview.Source, opts Options) ([]runtimemanagedcredentials.RequirementDescriptor, error) {
	index := runtimemanagedcredentials.BuildRequirementIndex(source)
	applicable := false
	for _, key := range sortedSetKeysLocal(index) {
		if len(liveManagedCredentialRequirements(source, opts, index[key])) == 0 {
			continue
		}
		applicable = true
		if opts.ManagedCredentials == nil {
			failure := failures.New(failures.ClassDependencyUnavailable, "managed_credential_inspection_unavailable", "bootverify", "managed_credential_inspection", map[string]any{"credential_key": key})
			return nil, fmt.Errorf("required managed credential inspection is unavailable for %s: %w", key, failure)
		}
	}
	if !applicable {
		return nil, nil
	}
	descriptors, err := runtimemanagedcredentials.ListRequirementDescriptors(ctx, opts.ManagedCredentials, source)
	if err != nil {
		return nil, failures.Wrap(failures.ClassDependencyUnavailable, "managed_credential_inspection_unavailable", "bootverify", "managed_credential_inspection", map[string]any{"requirement_keys": sortedSetKeysLocal(index)}, err)
	}
	out := make([]runtimemanagedcredentials.RequirementDescriptor, 0)
	for _, descriptor := range descriptors {
		descriptor.RequiredBy = liveManagedCredentialRequirements(source, opts, descriptor.RequiredBy)
		for _, requirement := range descriptor.RequiredBy {
			evaluation := runtimemanagedcredentials.EvaluateRequirement(descriptor, requirement)
			if !evaluation.Satisfied {
				out = append(out, evaluation.Descriptor)
				break
			}
		}
	}
	return out, nil
}

func fmtManagedCredentialWarning(item runtimemanagedcredentials.RequirementDescriptor, requiredBy []string) string {
	key := strings.TrimSpace(item.Key)
	status := strings.TrimSpace(item.Status)
	if status == "" {
		status = runtimemanagedcredentials.StatusUnconnected
	}
	message := fmt.Sprintf("managed credential %s is %s", key, status)
	if !item.Present {
		message = fmt.Sprintf("managed credential %s is missing", key)
	}
	if failure := strings.TrimSpace(item.Failure); failure != "" {
		message += ": " + failure
	}
	if len(requiredBy) > 0 {
		sort.Strings(requiredBy)
		message += " (required by " + strings.Join(requiredBy, ", ") + ")"
	}
	return message
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
	c.mcpObservation = &AdmissionObservation{Status: AdmissionNotRun, Reason: "portable validation does not perform MCP discovery"}
	if c.opts.Purpose == StructuralValidation {
		return
	}
	configs, err := runtimemcp.ServerConfigs(c.source)
	if err != nil {
		c.mcpDiscoveryErrors = []error{err}
		c.mcpObservation.Status, c.mcpObservation.Reason = AdmissionFailed, "MCP configuration admission failed"
		c.mcpObservation.FailureClass = failures.ClassSchemaInvalid
		return
	}
	if len(configs) == 0 {
		c.mcpObservation.Status, c.mcpObservation.Reason = AdmissionNotApplicable, "the selected source declares no MCP server"
		return
	}
	if !c.opts.CheckMCPReachable {
		c.mcpObservation.Reason = "MCP discovery is disabled for the declared servers"
		c.mcpObservation.FailureClass = failures.ClassDependencyUnavailable
		return
	}
	c.mcpObservation.Status, c.mcpObservation.Reason = AdmissionPassed, ""
	c.mcpObservation.StartedAt = time.Now().UTC()
	client := runtimemcp.NewClient(c.opts.Credentials)
	c.mcpDiscoveryErrors = client.Refresh(c.ctx, c.source, runtimemcp.DiscoveryOptions{ServerTimeout: c.opts.MCPDiscoveryTimeout})
	c.mcpDiscoveredTools = client.DiscoveredTools()
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), 5*time.Second)
	cleanupErr := client.Close(cleanupCtx)
	cancel()
	if cleanupErr != nil {
		c.mcpDiscoveryErrors = append(c.mcpDiscoveryErrors, fmt.Errorf("MCP discovery cleanup failed: %w", cleanupErr))
	}
	c.mcpObservation.FinishedAt = time.Now().UTC()
	if len(c.mcpDiscoveryErrors) > 0 {
		c.mcpObservation.Status, c.mcpObservation.Reason = AdmissionUnavailable, "MCP discovery or owned resource cleanup failed"
		c.mcpObservation.FailureClass = failures.ClassDependencyUnavailable
	}
}
