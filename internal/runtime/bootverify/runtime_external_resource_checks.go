package bootverify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (c *checkerContext) credentials() []Finding {
	if c.credentialLoaded {
		return c.credentialFindings
	}
	c.credentialLoaded = true
	missing, err := MissingStaticCredentialRequirements(c.ctx, c.source, c.opts)
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
	managed, err := MissingManagedCredentialRequirements(c.ctx, c.source, c.opts)
	if err != nil {
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:  "managed_credential_state",
			Severity: "error",
			Message:  strings.TrimSpace(err.Error()),
			Location: "global",
		})
		return c.credentialFindings
	}
	for _, item := range managed {
		requiredBy := make([]string, 0, len(item.RequiredBy))
		for _, ref := range item.RequiredBy {
			requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+" "+strings.TrimSpace(ref.Name))
		}
		c.credentialFindings = append(c.credentialFindings, Finding{
			CheckID:  "managed_credential_state",
			Severity: "warning",
			Message:  fmtManagedCredentialWarning(item, requiredBy),
			Location: item.Key,
		})
	}
	return c.credentialFindings
}

func MissingStaticCredentialRequirements(ctx context.Context, source semanticview.Source, opts Options) ([]runtimecredentials.Descriptor, error) {
	if opts.Credentials == nil {
		return nil, nil
	}
	missing, err := runtimecredentials.MissingRequired(ctx, opts.Credentials, source)
	if err != nil {
		return nil, err
	}
	out := make([]runtimecredentials.Descriptor, 0, len(missing))
	for _, descriptor := range missing {
		descriptor.RequiredBy = liveStaticCredentialRequirements(source, opts, descriptor.RequiredBy)
		if len(descriptor.RequiredBy) > 0 {
			out = append(out, descriptor)
		}
	}
	return out, nil
}

func liveStaticCredentialRequirements(_ semanticview.Source, opts Options, requirements []runtimecredentials.Requirement) []runtimecredentials.Requirement {
	out := make([]runtimecredentials.Requirement, 0, len(requirements))
	for _, requirement := range requirements {
		if requiresLiveCredential(opts, requirement.Kind) {
			out = append(out, requirement)
		}
	}
	return out
}

func liveManagedCredentialRequirements(_ semanticview.Source, opts Options, requirements []runtimemanagedcredentials.Requirement) []runtimemanagedcredentials.Requirement {
	out := make([]runtimemanagedcredentials.Requirement, 0, len(requirements))
	for _, requirement := range requirements {
		if requiresLiveCredential(opts, requirement.Kind) {
			out = append(out, requirement)
		}
	}
	return out
}

func requiresLiveCredential(opts Options, kind string) bool {
	return opts.ExecutionMode != executionmode.Mock || strings.TrimSpace(kind) != "tool"
}

func MissingManagedCredentialRequirements(ctx context.Context, source semanticview.Source, opts Options) ([]runtimemanagedcredentials.RequirementDescriptor, error) {
	descriptors, err := runtimemanagedcredentials.ListRequirementDescriptors(ctx, opts.ManagedCredentials, source)
	if err != nil {
		return nil, err
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
	if !c.opts.CheckMCPReachable {
		return
	}
	client := runtimemcp.NewClient(c.opts.Credentials)
	c.mcpDiscoveryErrors = client.Refresh(c.ctx, c.source)
	c.mcpDiscoveredTools = client.DiscoveredTools()
}
