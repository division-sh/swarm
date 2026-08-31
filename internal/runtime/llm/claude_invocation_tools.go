package llm

import (
	"context"
	"fmt"
	"slices"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type claudeInvocationToolProjection struct {
	BuiltinSelection        []string
	PermissionAdmission     []string
	ExpectedProviderBuiltin []string
}

func (p claudeInvocationToolProjection) builtinSelectionArg() string {
	return strings.Join(p.BuiltinSelection, ",")
}

func (p claudeInvocationToolProjection) permissionAdmissionArg() string {
	return strings.Join(p.PermissionAdmission, ",")
}

type conversationForkSandboxInvocationPolicy struct {
	runtimeToolNames []string
}

type conversationForkSandboxInvocationPolicyContextKey struct{}

// WithConversationForkSandboxInvocationPolicy records the exact tool set
// produced by a previously validated ConversationForkSandboxPolicy.
func WithConversationForkSandboxInvocationPolicy(ctx context.Context, runtimeToolNames []string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, conversationForkSandboxInvocationPolicyContextKey{}, conversationForkSandboxInvocationPolicy{
		runtimeToolNames: append([]string(nil), runtimeToolNames...),
	})
}

func ConversationForkSandboxInvocationPolicyFromContext(ctx context.Context) ([]string, bool) {
	if ctx == nil {
		return nil, false
	}
	policy, ok := ctx.Value(conversationForkSandboxInvocationPolicyContextKey{}).(conversationForkSandboxInvocationPolicy)
	if !ok {
		return nil, false
	}
	return append([]string(nil), policy.runtimeToolNames...), true
}

func projectClaudeInvocationTools(ctx context.Context, actor models.AgentConfig, tools []ToolDefinition) (claudeInvocationToolProjection, error) {
	surface, hasManagedSurface := managedcapabilities.FromContext(ctx)
	forkPolicyTools, hasForkPolicy := ConversationForkSandboxInvocationPolicyFromContext(ctx)
	if hasManagedSurface == hasForkPolicy {
		return claudeInvocationToolProjection{}, fmt.Errorf("Claude invocation requires exactly one capability authority")
	}

	if hasManagedSurface {
		if authority, ok := runtimeeffects.AuthorityFromContext(ctx); ok && authority.Kind == runtimeeffects.AuthorityConversationForkChat {
			return claudeInvocationToolProjection{}, fmt.Errorf("managed Claude capability surface conflicts with conversation fork authority")
		}
		if err := surface.Validate(); err != nil {
			return claudeInvocationToolProjection{}, fmt.Errorf("managed Claude capability surface is invalid: %w", err)
		}
		if !capabilitySurfaceMatchesActorConfig(surface, actor) {
			return claudeInvocationToolProjection{}, fmt.Errorf("managed Claude capability surface actor mismatch")
		}
		return newClaudeInvocationToolProjection(
			surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin),
			append(
				surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin),
				surface.PlannedBindingNames(managedcapabilities.BindingMCPTool)...,
			),
		), nil
	}

	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthorityConversationForkChat {
		return claudeInvocationToolProjection{}, fmt.Errorf("conversation fork Claude invocation requires exact execution authority")
	}
	policyTools, err := exactCanonicalToolNames(forkPolicyTools)
	if err != nil {
		return claudeInvocationToolProjection{}, fmt.Errorf("conversation fork sandbox policy tools are invalid: %w", err)
	}
	transport := buildConversationForkSandboxTransportSurface(tools)
	if !slices.Equal(policyTools, transport.RuntimeToolNames) {
		return claudeInvocationToolProjection{}, fmt.Errorf("conversation fork Claude tools do not match sandbox policy")
	}
	permissionAdmission := append([]string(nil), transport.ProviderMCPTools...)
	permissionAdmission = append(permissionAdmission, transport.LocalFallbackTools...)
	return newClaudeInvocationToolProjection(nil, permissionAdmission), nil
}

func newClaudeInvocationToolProjection(providerBuiltins, permissionAdmission []string) claudeInvocationToolProjection {
	expectedProviderBuiltins := append([]string(nil), providerBuiltins...)
	slices.Sort(expectedProviderBuiltins)
	expectedProviderBuiltins = slices.Compact(expectedProviderBuiltins)
	builtinSelection := append([]string(nil), expectedProviderBuiltins...)
	builtinSelection = append(builtinSelection, claudeControlToolNames()...)
	permissionAdmission = append([]string(nil), permissionAdmission...)
	permissionAdmission = append(permissionAdmission, claudeControlToolNames()...)
	slices.Sort(builtinSelection)
	slices.Sort(permissionAdmission)
	return claudeInvocationToolProjection{
		BuiltinSelection:        slices.Compact(builtinSelection),
		PermissionAdmission:     slices.Compact(permissionAdmission),
		ExpectedProviderBuiltin: expectedProviderBuiltins,
	}
}

func validateClaudeInvocationProviderBuiltins(projection claudeInvocationToolProjection, response *Response) error {
	actual := exactCLIProviderVisibleTools(response)
	if !slices.Equal(projection.ExpectedProviderBuiltin, actual) {
		return fmt.Errorf(
			"provider-visible capability mismatch: expected [%s], got [%s]",
			strings.Join(projection.ExpectedProviderBuiltin, ", "),
			strings.Join(actual, ", "),
		)
	}
	return nil
}

func exactCanonicalToolNames(names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		canonical := toolidentity.CanonicalName(name)
		if canonical == "" {
			return nil, fmt.Errorf("empty tool name")
		}
		if _, duplicate := seen[canonical]; duplicate {
			return nil, fmt.Errorf("duplicate tool name %q", canonical)
		}
		seen[canonical] = struct{}{}
		out = append(out, canonical)
	}
	slices.Sort(out)
	return out, nil
}
