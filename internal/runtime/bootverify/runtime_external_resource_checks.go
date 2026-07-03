package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
)

func (c *checkerContext) credentials() []Finding {
	if c.credentialLoaded {
		return c.credentialFindings
	}
	c.credentialLoaded = true
	if c.opts.Credentials != nil {
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
	}
	managed, err := runtimemanagedcredentials.MissingOrUnusableRequired(c.ctx, c.opts.ManagedCredentials, c.source)
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
