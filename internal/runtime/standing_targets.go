package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

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

func ResolveStandingTargetDeclarations(source semanticview.Source, providers *providertriggers.Registry) ([]StandingTargetDeclaration, error) {
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
					if secret == "" {
						return nil, fmt.Errorf("%s ingress provider %q requires signing_secret", location, provider)
					}
					if providers == nil {
						return nil, fmt.Errorf("%s ingress provider %q cannot be verified: provider trigger registry is required", location, provider)
					}
					manifest, exists := providers.Manifest(provider)
					if !exists {
						return nil, fmt.Errorf("%s ingress provider %q is not installed", location, provider)
					}
					literal := strings.TrimSpace(manifest.EventName.Literal)
					template := strings.TrimSpace(manifest.EventName.Template)
					if err := validateStandingIngressPins(source, flowID, provider, literal, template); err != nil {
						return nil, fmt.Errorf("%s: %w", location, err)
					}
					decl.Ingress = append(decl.Ingress, StandingIngressBinding{
						Provider: provider, SigningSecret: secret, EventLiteral: literal, EventTemplate: template,
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

func standingDeclarationLocation(pkg runtimecontracts.LoadedProjectPackage, flowID string) string {
	path := strings.TrimSpace(pkg.Paths.PackageFile)
	if path == "" {
		path = "package.yaml"
	}
	return fmt.Sprintf("%s flows[%s]", path, firstNonEmpty(flowID, "<missing>"))
}

func validateStandingIngressPins(source semanticview.Source, flowID, provider, literal, template string) error {
	pins := source.FlowInputEventPins(flowID)
	if literal != "" {
		for _, pin := range pins {
			if strings.TrimSpace(pin.Event) == literal && strings.TrimSpace(pin.Source) == "external" {
				return nil
			}
		}
		return fmt.Errorf("ingress provider %q emits %q; add an exact external input pin for %q to flow %s", provider, literal, literal, flowID)
	}
	if template == "" {
		return fmt.Errorf("ingress provider %q has no canonical event-name policy", provider)
	}
	for _, pin := range pins {
		if strings.TrimSpace(pin.Source) == "external" && standingEventTemplateMatches(template, strings.TrimSpace(pin.Event)) {
			return nil
		}
	}
	return fmt.Errorf("ingress provider %q emits template %q; add at least one exact external input pin matching that template to flow %s", provider, template, flowID)
}

func standingEventTemplateMatches(template, event string) bool {
	template = strings.TrimSpace(template)
	event = strings.TrimSpace(event)
	if event == "" || strings.Count(template, "{event_type}") != 1 {
		return false
	}
	prefix, suffix, _ := strings.Cut(template, "{event_type}")
	if !strings.HasPrefix(event, prefix) || !strings.HasSuffix(event, suffix) {
		return false
	}
	middle := strings.TrimSuffix(strings.TrimPrefix(event, prefix), suffix)
	return strings.TrimSpace(middle) != ""
}

func standingInputPinAdmitted(source semanticview.Source, flowID, eventName string) bool {
	if source == nil || strings.TrimSpace(flowID) == "" || strings.TrimSpace(eventName) == "" {
		return false
	}
	for _, pin := range source.FlowInputEventPins(flowID) {
		if strings.TrimSpace(pin.Event) == strings.TrimSpace(eventName) && strings.TrimSpace(pin.Source) == "external" {
			return true
		}
	}
	return false
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
	declarations, err := ResolveStandingTargetDeclarations(source, rt.Options.ProviderTriggerRegistry)
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
				EntityID: instance.EntityID, SigningSecret: binding.SigningSecret,
			}.normalized())
		}
		plans = append(plans, plan)
	}
	return plans, nil
}
