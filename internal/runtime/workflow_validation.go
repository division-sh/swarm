package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type WorkflowContractValidationOptions struct {
	Purpose                        runtimebootverify.ValidationPurpose
	ExecutionPosture               executionposture.Posture
	Credentials                    runtimecredentials.Store
	ProviderCredentials            runtimecredentials.Store
	ManagedCredentials             runtimemanagedcredentials.Store
	CheckMCPReachable              bool
	MCPDiscoveryTimeout            time.Duration
	StrictEmitSchemas              bool
	FatalToolImplementationWarning bool
	FatalBootWarnings              bool
	ExcludedFatalBootWarningChecks []string
	ValidateLLMModelResolution     bool
	LLMProfile                     llmselection.Profile
	ModelAliases                   llmselection.ModelAliases
	ProviderTriggerCatalog         *providertriggers.CatalogSnapshot
	ChannelPlans                   []packs.SatisfactionPlan
	ChannelActivationPublication   channelonboarding.ChannelActivationPublication
}

type WorkflowContractValidationResult struct {
	BootReport                       runtimebootverify.Report
	ToolImplementationWarnings       []error
	MissingEmitSchemaEventTypes      []string
	GeneratedEmitSchemaErrors        []error
	GeneratedToolSchemaClosureErrors []error
	CapabilitySubjects               []packs.Subject
	mockConnectorResponses           *providerconnectors.MockResponsePlan
	bootEffectReachability           runtimebootverify.SourceBootEffectReachability
}

func DefaultWorkflowContractValidationOptions(credentials runtimecredentials.Store, posture executionposture.Posture) WorkflowContractValidationOptions {
	profile, err := llmselection.ResolveActiveBackend(llmselection.DefaultBackendID())
	if err != nil {
		panic(fmt.Sprintf("resolve built-in default LLM profile: %v", err))
	}
	opts := workflowContractValidationPolicyOptions()
	opts.ExecutionPosture = posture
	opts.Credentials = credentials
	opts.LLMProfile = profile
	opts.CheckMCPReachable = true
	return opts
}

func workflowContractValidationPolicyOptions() WorkflowContractValidationOptions {
	return WorkflowContractValidationOptions{
		StrictEmitSchemas:              runtimeEnvBool("SWARM_EMIT_SCHEMA_STRICT", true),
		FatalToolImplementationWarning: bootWarningsFatal(),
		FatalBootWarnings:              bootWarningsFatal(),
		ExcludedFatalBootWarningChecks: []string{"tool_resolution", "inbound_unsigned_webhook"},
	}
}

func StructuralWorkflowContractValidationOptions() WorkflowContractValidationOptions {
	opts := workflowContractValidationPolicyOptions()
	opts.Purpose = runtimebootverify.StructuralValidation
	return opts
}

// ValidateWorkflowContractSurface is the canonical verify/boot contract-validation entrypoint
// for prompt guards, bootverify errors, tool implementation validation, and explicit emit-schema coverage.
func ValidateWorkflowContractSurface(ctx context.Context, source semanticview.Source, opts WorkflowContractValidationOptions) (result WorkflowContractValidationResult, err error) {
	result = WorkflowContractValidationResult{
		BootReport: runtimebootverify.Report{Purpose: opts.Purpose, Observations: workflowAdmissionObservations(opts.Purpose)},
	}
	reportBlocks := false
	registryExecuted := false
	blockingPrerequisite := ""
	if source != nil {
		if bundle, ok := semanticview.Bundle(source); ok && bundle.SourceArtifact != nil {
			result.BootReport.SourceArtifactHash = bundle.SourceArtifact.BundleHash()
		}
	}
	phase := "source_validation"
	result.startAdmissionClause(phase)
	defer func() {
		err = result.settleWorkflowAdmission(ctx, err, phase, reportBlocks, registryExecuted, blockingPrerequisite)
	}()
	if source == nil {
		return result, fmt.Errorf("semantic source is required")
	}
	if !opts.Purpose.Valid() {
		return result, fmt.Errorf("invalid validation purpose")
	}
	if opts.Purpose != runtimebootverify.StructuralValidation && !opts.ExecutionPosture.Valid() {
		return result, fmt.Errorf("runtime execution posture is required")
	}
	if opts.Purpose == runtimebootverify.StructuralValidation {
		// Validate supplied source schemas without selecting actors or requiring
		// mock completeness. No deployment credential or activation is observed.
		if _, err := providerconnectors.CompileMockResponsePlan(source); err != nil {
			return result, err
		}
	} else {
		bootEffects, err := runtimebootverify.PrepareSourceBootEffectContext(source, opts.LLMProfile, opts.ExecutionPosture)
		if err != nil {
			return result, err
		}
		source = bootEffects.Source
		result.mockConnectorResponses = bootEffects.MockConnectorResponses
		result.bootEffectReachability = bootEffects.Reachability
	}

	result.finishAdmissionClause(phase)
	phase = ""
	registryExecuted = true
	registered := runtimebootverify.Run(ctx, source, runtimebootverify.Options{
		ExecutionPosture:        opts.ExecutionPosture,
		Purpose:                 opts.Purpose,
		Credentials:             opts.Credentials,
		ManagedCredentials:      opts.ManagedCredentials,
		EffectReachability:      result.bootEffectReachability,
		CheckMCPReachable:       opts.CheckMCPReachable,
		MCPDiscoveryTimeout:     opts.MCPDiscoveryTimeout,
		ValidateModelResolution: opts.ValidateLLMModelResolution,
		LLMProfile:              opts.LLMProfile,
		ModelAliases:            opts.ModelAliases,
	})
	registered.Observations = append(result.BootReport.Observations, registered.Observations...)
	result.BootReport = registered
	if result.BootReport.HasErrors() {
		reportBlocks = true
		blockingPrerequisite = result.BootReport.Errors()[0].CheckID
		return result, fmt.Errorf("boot verification failed:\n%s", formatWorkflowValidationFindings(result.BootReport.Errors(), true))
	}
	if opts.FatalBootWarnings {
		warnings := filterWorkflowValidationFindings(result.BootReport.Warnings(), opts.ExcludedFatalBootWarningChecks...)
		if len(warnings) > 0 {
			reportBlocks = true
			blockingPrerequisite = warnings[0].CheckID
			return result, fmt.Errorf("boot verification blocked by policy-escalated findings:\n%s", formatWorkflowValidationFindings(warnings, true))
		}
	}

	phase = "tool_implementation_validation"
	result.startAdmissionClause(phase)
	warnings, err := runtimetools.ValidateToolImplementations(source)
	result.ToolImplementationWarnings = warnings
	if err != nil {
		return result, fmt.Errorf("tool implementation validation failed: %w", err)
	}
	if opts.FatalToolImplementationWarning && len(warnings) > 0 {
		return result, fmt.Errorf("tool implementation warnings:\n%s", formatValidationErrors(warnings))
	}
	result.finishAdmissionClause(phase)

	phase = "emit_schema_coverage"
	result.startAdmissionClause(phase)
	emitRegistry := runtimetools.NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	result.MissingEmitSchemaEventTypes = emitRegistry.GeneratedEmitSchemasForAgentRoles()
	if opts.StrictEmitSchemas && len(result.MissingEmitSchemaEventTypes) > 0 {
		sample := result.MissingEmitSchemaEventTypes
		if len(sample) > 10 {
			sample = sample[:10]
		}
		return result, fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries (sample: %s)", len(result.MissingEmitSchemaEventTypes), strings.Join(sample, ", "))
	}
	result.finishAdmissionClause(phase)
	phase = "generated_emit_schema_validation"
	result.startAdmissionClause(phase)
	result.GeneratedEmitSchemaErrors = runtimetools.ValidateGeneratedEmitToolSchemasForSource(source)
	if len(result.GeneratedEmitSchemaErrors) > 0 {
		return result, fmt.Errorf("generated emit tool schema validation failed:\n%s", formatValidationErrors(result.GeneratedEmitSchemaErrors))
	}
	result.finishAdmissionClause(phase)
	phase = "durable_activity_surface"
	result.startAdmissionClause(phase)
	activityErrors := validateDurableActivitySurface(source)
	if len(activityErrors) > 0 {
		return result, fmt.Errorf("durable activity validation failed:\n%s", formatValidationErrors(activityErrors))
	}
	result.finishAdmissionClause(phase)
	phase = "provider_connector_surface"
	result.startAdmissionClause(phase)
	connectorErrors := providerconnectors.ValidateSource(source)
	if len(connectorErrors) > 0 {
		return result, fmt.Errorf("provider connector validation failed:\n%s", formatValidationErrors(connectorErrors))
	}
	result.finishAdmissionClause(phase)
	phase = "standing_ingress_declaration"
	result.startAdmissionClause(phase)
	declarations, err := ResolveStandingTargetDeclarations(source, opts.ProviderTriggerCatalog)
	if err != nil {
		return result, fmt.Errorf("standing ingress validation failed: %w", err)
	}
	for _, finding := range unsignedRawAdmissionFindings(declarations) {
		result.BootReport.Add(finding)
	}
	result.finishAdmissionClause(phase)
	phase = ""
	result.BootReport.Sort()
	if opts.Purpose == runtimebootverify.StructuralValidation {
		return result, nil
	}
	phase = "provider_trigger_capability_projection"
	result.startAdmissionClause(phase)
	var providerCredentialOwner *runtimecredentials.SnapshotOwner
	if opts.ProviderCredentials != nil {
		providerCredentialOwner, err = runtimecredentials.NewSnapshotOwner(opts.ProviderCredentials)
		if err != nil {
			return result, fmt.Errorf("provider credential projection failed: %w", err)
		}
	}
	result.CapabilitySubjects, err = ProviderTriggerCapabilitySubjects(ctx, source, opts.ProviderTriggerCatalog, providerCredentialOwner)
	if err != nil {
		return result, fmt.Errorf("provider trigger capability projection failed: %w", err)
	}
	result.finishAdmissionClause(phase)
	phase = "channel_capability_projection"
	result.startAdmissionClause(phase)
	for _, plan := range opts.ChannelPlans {
		subject, subjectErr := plan.CapabilitySubject()
		if subjectErr != nil {
			return result, fmt.Errorf("channel pack capability projection failed: %w", subjectErr)
		}
		result.CapabilitySubjects = append(result.CapabilitySubjects, subject)
	}
	result.finishAdmissionClause(phase)
	phase = "channel_activation_publication"
	result.startAdmissionClause(phase)
	activationPublication := opts.ChannelActivationPublication
	if !activationPublication.Generation().Valid() {
		activationPublication, err = channelonboarding.NewDeclaredOnlyChannelActivationPublication(nil)
		if err != nil {
			return result, fmt.Errorf("channel activation publication failed: %w", err)
		}
	}
	if err := activationPublication.Validate(); err != nil {
		return result, fmt.Errorf("channel activation publication failed: %w", err)
	}
	for _, binding := range activationPublication.Bindings() {
		subject, subjectErr := binding.CapabilitySubject()
		if subjectErr != nil {
			return result, fmt.Errorf("channel outbound capability projection failed: %w", subjectErr)
		}
		result.CapabilitySubjects = append(result.CapabilitySubjects, subject)
	}
	result.finishAdmissionClause(phase)
	phase = "capability_projection_normalization"
	result.startAdmissionClause(phase)
	result.CapabilitySubjects, err = packs.NormalizeSubjects(result.CapabilitySubjects)
	if err != nil {
		return result, fmt.Errorf("capability projection normalization failed: %w", err)
	}
	result.finishAdmissionClause(phase)
	phase = ""

	return result, nil
}

func unsignedRawAdmissionFindings(declarations []StandingTargetDeclaration) []runtimebootverify.Finding {
	var findings []runtimebootverify.Finding
	for _, declaration := range declarations {
		for _, binding := range declaration.Ingress {
			if binding.AdmissionPlan.PolicySource() != providertriggers.PolicySourceRawDeclaration ||
				binding.AdmissionPlan.RequestAuthentication() != providertriggers.RequestAuthenticationNone ||
				binding.AdmissionPlan.AcknowledgedUnsigned() {
				continue
			}
			findings = append(findings, runtimebootverify.Finding{
				CheckID: "inbound_unsigned_webhook", Severity: runtimebootverify.SeveritySemanticDriftWarn,
				Location:    declaration.SourcePath,
				Message:     fmt.Sprintf("ingress alias %q provider %q accepts unsigned webhooks; anyone who learns /webhooks/%s/%s can POST events into this flow", declaration.Alias, binding.Provider, declaration.Alias, binding.Provider),
				Remediation: "add admission.acknowledge: unsigned_webhook to confirm this intentional public endpoint",
			})
		}
	}
	return findings
}

func formatWorkflowValidationFindings(findings []runtimebootverify.Finding, blocking bool) string {
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, runtimebootverify.FormatSurfaceFinding(finding, blocking))
	}
	return strings.Join(lines, "\n")
}

func formatValidationErrors(errs []error) string {
	lines := make([]string, 0, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		lines = append(lines, strings.TrimSpace(err.Error()))
	}
	return strings.Join(lines, "\n")
}

func filterWorkflowValidationFindings(findings []runtimebootverify.Finding, excludedCheckIDs ...string) []runtimebootverify.Finding {
	if len(findings) == 0 {
		return nil
	}
	excluded := make(map[string]struct{}, len(excludedCheckIDs))
	for _, checkID := range excludedCheckIDs {
		checkID = strings.TrimSpace(checkID)
		if checkID != "" {
			excluded[checkID] = struct{}{}
		}
	}
	out := make([]runtimebootverify.Finding, 0, len(findings))
	for _, finding := range findings {
		if _, skip := excluded[strings.TrimSpace(finding.CheckID)]; skip {
			continue
		}
		out = append(out, finding)
	}
	return out
}
