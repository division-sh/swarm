package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type StandingIngressBinding struct {
	Provider      string
	SigningSecret string
	EventLiteral  string
	EventTemplate string
	AdmissionPlan providertriggers.InboundAdmissionPlan
}

type StandingTargetDeclaration struct {
	PackageKey string
	SourcePath string
	FlowID     string
	FlowPath   string
	Alias      string
	Ingress    []StandingIngressBinding
}

type StandingTarget struct {
	BundleHash    string
	PackageKey    string
	SourcePath    string
	FlowID        string
	FlowPath      string
	Alias         string
	Provider      string
	RunID         string
	FlowInstance  string
	EntityID      string
	SigningSecret string
	AdmissionPlan providertriggers.InboundAdmissionPlan
}

type StandingActivation struct {
	BundleHash   string
	PackageKey   string
	FlowID       string
	RunID        string
	FlowInstance string
	EntityID     string
	Created      bool
}

type standingTargetPlan struct {
	declaration StandingTargetDeclaration
	runID       string
	instance    runtimeflowidentity.Instance
	targets     []StandingTarget
}

func (t StandingTarget) normalized() StandingTarget {
	t.BundleHash = strings.TrimSpace(t.BundleHash)
	t.PackageKey = strings.TrimSpace(t.PackageKey)
	t.SourcePath = strings.TrimSpace(t.SourcePath)
	t.FlowID = strings.TrimSpace(t.FlowID)
	t.FlowPath = strings.Trim(strings.TrimSpace(t.FlowPath), "/")
	t.Alias = strings.Trim(strings.TrimSpace(t.Alias), "/")
	t.Provider = providertriggers.NormalizeProviderName(t.Provider)
	t.RunID = strings.TrimSpace(t.RunID)
	t.FlowInstance = strings.Trim(strings.TrimSpace(t.FlowInstance), "/")
	t.EntityID = strings.TrimSpace(t.EntityID)
	t.SigningSecret = strings.TrimSpace(t.SigningSecret)
	return t
}

func (t StandingTarget) CapabilitySubject() (packs.Subject, error) {
	t = t.normalized()
	return t.AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
		BundleHash: t.BundleHash, Alias: t.Alias, SigningSecret: t.SigningSecret, SourcePath: t.SourcePath,
	})
}

func NormalizeStandingIngressAlias(alias string) (string, error) {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return "", fmt.Errorf("ingress alias is required")
	}
	for i := 0; i < len(alias); i++ {
		c := alias[i]
		alphaNumeric := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if alphaNumeric || i > 0 && (c == '.' || c == '_' || c == '-') {
			continue
		}
		return "", fmt.Errorf("ingress alias %q must be one URL-safe path segment matching [A-Za-z0-9][A-Za-z0-9._-]*; remove slashes, whitespace, escapes, or reserved characters", alias)
	}
	return alias, nil
}

func ResolveStandingTargetDeclarations(source semanticview.Source, catalog *providertriggers.CatalogSnapshot) ([]StandingTargetDeclaration, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, fmt.Errorf("standing target declarations require a bundle-backed semantic source")
	}
	declarations := make([]StandingTargetDeclaration, 0)
	aliases := map[string]string{}
	for _, pkg := range bundle.PackageTree {
		for _, ref := range pkg.Manifest.Flows {
			activation := strings.ToLower(strings.TrimSpace(ref.Activation))
			flowID := strings.TrimSpace(ref.ID)
			location := standingDeclarationLocation(pkg, flowID)
			if activation != "" && activation != runtimecontracts.ProjectFlowActivationStanding {
				return nil, fmt.Errorf("%s activation %q is unsupported; supported value: standing", location, ref.Activation)
			}
			if ref.Ingress != nil && activation != runtimecontracts.ProjectFlowActivationStanding {
				return nil, fmt.Errorf("%s ingress requires activation: standing", location)
			}
			if !ref.HasStandingActivation() {
				continue
			}
			if flowID == "" {
				return nil, fmt.Errorf("%s standing activation requires non-empty flow id", location)
			}
			if strings.TrimSpace(ref.Mode) != runtimecontracts.FlowModeSingleton {
				return nil, fmt.Errorf("%s activation: standing requires mode: singleton", location)
			}
			if _, err := bundle.ResolveFlowSingletonCoordinator(flowID); err != nil {
				return nil, fmt.Errorf("%s standing singleton is invalid: %w", location, err)
			}
			decl := StandingTargetDeclaration{
				PackageKey: strings.TrimSpace(pkg.Key),
				SourcePath: strings.TrimSpace(pkg.Paths.PackageFile),
				FlowID:     flowID,
				FlowPath:   strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"),
				Alias:      flowID,
			}
			if decl.FlowPath == "" {
				decl.FlowPath = flowID
			}
			if ref.Ingress != nil {
				alias := strings.TrimSpace(ref.Ingress.Alias)
				if alias == "" {
					alias = flowID
				}
				alias, err := NormalizeStandingIngressAlias(alias)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", location, err)
				}
				decl.Alias = alias
				if len(ref.Ingress.Providers) == 0 {
					return nil, fmt.Errorf("%s ingress.providers must contain at least one provider binding", location)
				}
				seenProviders := map[string]struct{}{}
				for _, binding := range ref.Ingress.Providers {
					provider := providertriggers.NormalizeProviderName(binding.Provider)
					if provider == "" {
						return nil, fmt.Errorf("%s ingress provider is required", location)
					}
					if _, exists := seenProviders[provider]; exists {
						return nil, fmt.Errorf("%s declares duplicate ingress provider %q; remove one binding", location, provider)
					}
					seenProviders[provider] = struct{}{}
					secret := strings.TrimSpace(binding.SigningSecret)
					plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
						Alias: decl.Alias, Provider: provider, SigningSecret: secret,
						Declaration: providerAdmissionDeclaration(binding.Admission),
					})
					if err != nil {
						return nil, fmt.Errorf("%s: %w", location, err)
					}
					eventNames := plan.EventNames()
					literal := strings.TrimSpace(eventNames.Literal)
					template := strings.TrimSpace(eventNames.Template)
					if err := validateStandingIngressPins(source, flowID, provider, eventNames); err != nil {
						return nil, fmt.Errorf("%s: %w", location, err)
					}
					decl.Ingress = append(decl.Ingress, StandingIngressBinding{
						Provider: provider, SigningSecret: secret, EventLiteral: literal, EventTemplate: template, AdmissionPlan: plan,
					})
				}
			}
			if len(decl.Ingress) > 0 {
				if previous, exists := aliases[decl.Alias]; exists {
					return nil, fmt.Errorf("duplicate standing ingress alias %q from %s and %s; rename one ingress alias", decl.Alias, previous, location)
				}
				aliases[decl.Alias] = location
			}
			declarations = append(declarations, decl)
		}
	}
	sort.Slice(declarations, func(i, j int) bool {
		if declarations[i].PackageKey == declarations[j].PackageKey {
			return declarations[i].FlowID < declarations[j].FlowID
		}
		return declarations[i].PackageKey < declarations[j].PackageKey
	})
	return declarations, nil
}

func providerAdmissionDeclaration(admission runtimecontracts.ProjectFlowIngressAdmission) providertriggers.AdmissionDeclaration {
	declaration := providertriggers.AdmissionDeclaration{
		Kind: admission.Kind, Acknowledge: admission.Acknowledge, Event: admission.Event, Payload: admission.Payload,
	}
	if admission.Pack != nil {
		declaration.PackID = admission.Pack.ID
	}
	if admission.Authentication != nil {
		declaration.Authentication = providertriggers.RawAuthenticationDeclaration{
			Kind: admission.Authentication.Kind, Header: admission.Authentication.Header,
			Prefix: admission.Authentication.Prefix, Encoding: admission.Authentication.Encoding,
		}
	}
	if admission.DeliveryID != nil {
		declaration.DeliveryID = providertriggers.RawDeliveryIDDeclaration{
			Source: admission.DeliveryID.Source, Header: admission.DeliveryID.Header, JSONPath: admission.DeliveryID.JSONPath,
		}
	}
	return declaration
}

func RecompileStandingTargetAdmissions(source semanticview.Source, catalog *providertriggers.CatalogSnapshot, existing []StandingTarget) ([]StandingTarget, error) {
	declarations, err := ResolveStandingTargetDeclarations(source, catalog)
	if err != nil {
		return nil, err
	}
	bindings := map[string]StandingIngressBinding{}
	for _, declaration := range declarations {
		for _, binding := range declaration.Ingress {
			bindings[declaration.Alias+"\x00"+binding.Provider] = binding
		}
	}
	if len(bindings) != len(existing) {
		return nil, fmt.Errorf("candidate provider-trigger catalog recompile found %d declared standing ingress targets, but loaded context carries %d", len(bindings), len(existing))
	}
	out := make([]StandingTarget, 0, len(existing))
	for _, target := range existing {
		target = target.normalized()
		binding, ok := bindings[target.Alias+"\x00"+target.Provider]
		if !ok {
			return nil, fmt.Errorf("candidate provider-trigger catalog recompile cannot resolve loaded standing target %q/%q", target.Alias, target.Provider)
		}
		target.SigningSecret = binding.SigningSecret
		target.AdmissionPlan = binding.AdmissionPlan
		out = append(out, target)
	}
	return out, nil
}

func EffectiveStandingIngressCapabilitySubjects(source semanticview.Source, catalog *providertriggers.CatalogSnapshot) ([]packs.Subject, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, fmt.Errorf("effective standing ingress capability subjects require a bundle-backed semantic source")
	}
	declarations, err := ResolveStandingTargetDeclarations(source, catalog)
	if err != nil {
		return nil, err
	}
	ingressCount := 0
	for _, declaration := range declarations {
		ingressCount += len(declaration.Ingress)
	}
	if ingressCount == 0 {
		return nil, nil
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return nil, err
	}
	subjects := make([]packs.Subject, 0, ingressCount)
	for _, declaration := range declarations {
		for _, binding := range declaration.Ingress {
			subject, err := binding.AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
				BundleHash: bundleHash, Alias: declaration.Alias, SigningSecret: binding.SigningSecret, SourcePath: declaration.SourcePath,
			})
			if err != nil {
				return nil, err
			}
			subjects = append(subjects, subject)
		}
	}
	return packs.NormalizeSubjects(subjects)
}

func standingDeclarationLocation(pkg runtimecontracts.LoadedProjectPackage, flowID string) string {
	path := strings.TrimSpace(pkg.Paths.PackageFile)
	if path == "" {
		path = "package.yaml"
	}
	return fmt.Sprintf("%s flows[%s]", path, firstNonEmpty(flowID, "<missing>"))
}

func validateStandingIngressPins(source semanticview.Source, flowID, provider string, eventNames providertriggers.EventNameManifest) error {
	literal := strings.TrimSpace(eventNames.Literal)
	template := strings.TrimSpace(eventNames.Template)
	if literal != "" {
		if _, err := resolveStandingInputEndpoint(source, flowID, literal); err == nil {
			return nil
		}
		return fmt.Errorf("ingress provider %q emits %q; add an exact external input pin for %q to flow %s", provider, literal, literal, flowID)
	}
	if template == "" {
		return fmt.Errorf("ingress provider %q has no canonical event-name policy", provider)
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	for _, endpoint := range census.InputPins() {
		if strings.TrimSpace(endpoint.FlowID) != strings.TrimSpace(flowID) || !eventNames.Accepts(endpoint.Event.Authored) {
			continue
		}
		if _, err := resolveStandingInputEndpointWithCensus(source, census, flowID, endpoint.Event.Authored); err == nil {
			return nil
		}
	}
	return fmt.Errorf("ingress provider %q emits template %q; add at least one exact external input pin matching that template to flow %s", provider, template, flowID)
}

func standingInputPinAdmitted(source semanticview.Source, flowID, eventName string) bool {
	_, err := resolveStandingInputEndpoint(source, flowID, eventName)
	return err == nil
}

func resolveStandingInputEndpoint(source semanticview.Source, flowID, eventName string) (semanticview.AuthoredEventEndpoint, error) {
	return resolveStandingInputEndpointWithCensus(source, semanticview.BuildAuthoredEventEndpointCensus(source), flowID, eventName)
}

func resolveStandingInputEndpointWithCensus(source semanticview.Source, census semanticview.AuthoredEventEndpointCensus, flowID, eventName string) (semanticview.AuthoredEventEndpoint, error) {
	flowID = strings.TrimSpace(flowID)
	eventName = strings.TrimSpace(eventName)
	if source == nil || flowID == "" || eventName == "" {
		return semanticview.AuthoredEventEndpoint{}, fmt.Errorf("standing ingress requires semantic source, flow id, and event name")
	}
	association := census.ResolveDeclaredInputEndpoint(flowID, eventName)
	endpoint, ok := association.Endpoint()
	if !ok {
		return semanticview.AuthoredEventEndpoint{}, association.Err()
	}
	producer := semanticview.ResolveFlowInputProducer(source, flowID, eventName)
	if !producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) &&
		!producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		return semanticview.AuthoredEventEndpoint{}, fmt.Errorf("event endpoint %q in flow %s is not declared as external ingress", eventName, flowID)
	}
	return endpoint, nil
}

func (rt *Runtime) EnsureStandingTargets(ctx context.Context) ([]StandingTarget, []StandingActivation, error) {
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, nil, err
	}
	if len(plans) == 0 {
		return nil, nil, nil
	}
	if rt.Stores.PipelineStore == nil || rt.Manager == nil || rt.Pipeline == nil {
		return nil, nil, fmt.Errorf("standing activation requires pipeline store, pipeline, and agent manager")
	}
	fact := rt.Options.BundleSourceFact.Normalized()
	source := rt.Options.WorkflowModule.SemanticSource()
	targets := make([]StandingTarget, 0)
	activations := make([]StandingActivation, 0, len(plans))
	for _, plan := range plans {
		declaration := plan.declaration
		runID := plan.runID
		instance := plan.instance
		runCtx := runtimecorrelation.WithRunID(ctx, runID)
		runCtx = runtimecorrelation.WithBundleSourceFact(runCtx, fact)
		created := false
		err := rt.Stores.PipelineStore.RunPipelineMutation(runCtx, func(txctx context.Context) error {
			if err := rt.Stores.PipelineStore.EnsureStandingRun(txctx, runID, declaration.PackageKey, declaration.FlowID, fact); err != nil {
				return err
			}
			wasCreated, err := rt.Manager.EnsureFlowInstance(txctx, runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: source,
				Instance:       instance,
				InitialState:   source.FlowInitialStage(declaration.FlowID),
				Config:         map[string]any{},
				Metadata: map[string]any{
					"activation":  runtimecontracts.ProjectFlowActivationStanding,
					"bundle_hash": fact.BundleHash,
					"package_key": declaration.PackageKey,
				},
			})
			if err != nil {
				return err
			}
			created = wasCreated
			if created {
				if err := rt.Pipeline.ArmFlowInstanceInitialStageTimers(txctx, instance.EntityID); err != nil {
					return fmt.Errorf("arm initial timers: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("activate standing flow %s: %w", declaration.FlowID, err)
		}
		if err := ensureLifecycleWorkflowSchedules(runCtx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline); err != nil {
			return nil, nil, fmt.Errorf("rehydrate standing flow %s schedules: %w", declaration.FlowID, err)
		}
		activations = append(activations, StandingActivation{
			BundleHash: fact.BundleHash, PackageKey: declaration.PackageKey, FlowID: declaration.FlowID,
			RunID: runID, FlowInstance: instance.InstancePath, EntityID: instance.EntityID, Created: created,
		})
		targets = append(targets, plan.targets...)
	}
	return targets, activations, nil
}

// PlanStandingTargets resolves all process-visible identities without mutating
// runtime or durable state. Startup uses it to reject cross-context collisions
// before any runtime starts.
func (rt *Runtime) PlanStandingTargets() ([]StandingTarget, error) {
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, err
	}
	targets := make([]StandingTarget, 0)
	for _, plan := range plans {
		targets = append(targets, plan.targets...)
	}
	return targets, nil
}

func (rt *Runtime) standingTargetPlans() ([]standingTargetPlan, error) {
	if rt == nil || rt.Options.WorkflowModule == nil {
		return nil, fmt.Errorf("runtime workflow module is required")
	}
	source := rt.Options.WorkflowModule.SemanticSource()
	declarations, err := ResolveStandingTargetDeclarations(source, rt.Options.ProviderTriggerCatalog)
	if err != nil {
		return nil, err
	}
	if len(declarations) == 0 {
		return nil, nil
	}
	fact := rt.Options.BundleSourceFact.Normalized()
	if err := runtimecontracts.ValidateBundleHash(fact.BundleHash); err != nil {
		return nil, fmt.Errorf("standing target bundle_hash: %w", err)
	}
	plans := make([]standingTargetPlan, 0, len(declarations))
	for _, declaration := range declarations {
		runID := runtimeflowidentity.StandingRunID(fact.BundleHash, declaration.PackageKey, declaration.FlowID)
		instance := runtimeflowidentity.Standing(source, declaration.FlowID, fact.BundleHash)
		plan := standingTargetPlan{declaration: declaration, runID: runID, instance: instance}
		for _, binding := range declaration.Ingress {
			plan.targets = append(plan.targets, StandingTarget{
				BundleHash: fact.BundleHash, PackageKey: declaration.PackageKey, SourcePath: declaration.SourcePath,
				FlowID: declaration.FlowID, FlowPath: declaration.FlowPath, Alias: declaration.Alias,
				Provider: binding.Provider, RunID: runID, FlowInstance: instance.InstancePath,
				EntityID: instance.EntityID, SigningSecret: binding.SigningSecret, AdmissionPlan: binding.AdmissionPlan,
			}.normalized())
		}
		plans = append(plans, plan)
	}
	return plans, nil
}
