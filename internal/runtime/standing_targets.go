package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestanding "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type StandingIngressBinding struct {
	Provider         string
	SigningSecret    string
	RawEventLiteral  string
	RawEventTemplate string
	AdmissionPlan    providertriggers.InboundAdmissionPlan
}

type StandingTargetDeclaration struct {
	SourcePath string
	FlowPath   string
	Alias      string
	Ingress    []StandingIngressBinding
	Clocks     []semanticview.ClockSchedule
}

type StandingTarget struct {
	BundleHash    string
	SourcePath    string
	FlowPath      string
	Alias         string
	Provider      string
	RunID         string
	SigningSecret string
	AdmissionPlan providertriggers.InboundAdmissionPlan
}

type StandingActivation struct {
	BundleHash   string
	FlowPath     string
	RunID        string
	Construction runtimeflowidentity.Instance
	Created      bool
	RunState     runtimestanding.State
}

type standingTargetPlan struct {
	declaration StandingTargetDeclaration
	targets     []StandingTarget
}

func (t StandingTarget) normalized() StandingTarget {
	t.BundleHash = strings.TrimSpace(t.BundleHash)
	t.SourcePath = strings.TrimSpace(t.SourcePath)
	t.FlowPath = strings.Trim(strings.TrimSpace(t.FlowPath), "/")
	t.RunID = strings.TrimSpace(t.RunID)
	t.SigningSecret = strings.TrimSpace(t.SigningSecret)
	return t
}

func (t StandingTarget) CapabilitySubject() (packs.Subject, error) {
	t = t.normalized()
	return t.AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
		BundleHash: t.BundleHash, FlowPath: t.FlowPath, Alias: t.Alias, SigningSecret: t.SigningSecret, SourcePath: t.SourcePath,
	})
}

func (c BundleContext) StandingTargetDeclarations() ([]StandingTargetDeclaration, error) {
	return ResolveStandingTargetDeclarations(c.Source, c.ProviderTriggerCatalog)
}

func ResolveStandingTargetDeclarations(source semanticview.Source, _ *providertriggers.CatalogSnapshot) ([]StandingTargetDeclaration, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, fmt.Errorf("standing target declarations require a bundle-backed semantic source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return nil, err
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return nil, err
	}
	return resolveDirectoryStandingDeclarations(source, bundle, providers)
}

func resolveDirectoryStandingDeclarations(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle, providers packadmission.ProviderSource) ([]StandingTargetDeclaration, error) {
	declarations := make([]StandingTargetDeclaration, 0)
	clocks := make(map[string][]semanticview.ClockSchedule)
	for _, declaration := range semanticview.ClockSchedules(source) {
		clocks[declaration.FlowID] = append(clocks[declaration.FlowID], declaration)
	}
	for _, view := range bundle.FlowViews() {
		flowID := view.Paths.FlowPath
		location := strings.TrimSpace(view.Paths.SchemaFile)
		if location == "" {
			location = "schema.yaml"
		}
		scope, err := providers.Scope(flowID)
		if err != nil {
			return nil, err
		}
		decl := StandingTargetDeclaration{SourcePath: location, FlowPath: flowID, Clocks: clocks[flowID]}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			return nil, err
		}
		inputs := make([]string, 0)
		for _, pin := range source.FlowInputEventPins(flowID) {
			inputs = append(inputs, pin.EventType())
		}
		for _, definition := range scope.ConsumedTriggerProviders(inputs) {
			member := definition.Provider().Directory() + "/trigger.yaml"
			entry, found := catalog.EntryByProvider(definition.Provider().Key())
			if !found {
				return nil, fmt.Errorf("flow %q consumed provider %q is absent from its exact captured catalog", flowID, definition.Provider().Key())
			}
			alias, present := bundle.FlowIngressAlias(flowID)
			if !present {
				return nil, fmt.Errorf("%s has no compiled directory webhook alias", location)
			}
			decl.Alias = alias
			secret := ""
			if required := providertriggers.DerivedRequires(entry.Manifest).Secrets; len(required) > 0 {
				secret = required[0]
			}
			plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
				Alias: alias, Provider: definition.Provider().Key(), SigningSecret: secret,
			})
			if err != nil {
				return nil, fmt.Errorf("%s deployment triggers[%q]: %w", location, member, err)
			}
			raw, present := plan.RawOutput()
			if !present {
				return nil, fmt.Errorf("%s provider %q compiled no raw output", location, definition.Provider().Key())
			}
			decl.Ingress = append(decl.Ingress, StandingIngressBinding{
				Provider: definition.Provider().Key(), SigningSecret: secret, AdmissionPlan: plan,
				RawEventLiteral: raw.EventName.Literal, RawEventTemplate: raw.EventName.Template,
			})
		}
		if len(decl.Ingress) == 0 && len(decl.Clocks) == 0 {
			continue
		}
		keyless, err := runtimepipeline.StandingConstructionIsKeyless(source, flowID)
		if err != nil {
			return nil, err
		}
		if keyless || len(clocks[flowID]) > 0 {
			if err := runtimepipeline.RequireStandingConstructionPath(source, flowID); err != nil {
				return nil, fmt.Errorf("%s standing constructor is invalid: %w", location, err)
			}
		}
		declarations = append(declarations, decl)
	}
	sort.Slice(declarations, func(i, j int) bool {
		return declarations[i].FlowPath < declarations[j].FlowPath
	})
	return declarations, nil
}

func baseStandingIngressCapabilitySubjects(source semanticview.Source, catalog *providertriggers.CatalogSnapshot) ([]packs.Subject, error) {
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
				BundleHash: bundleHash, FlowPath: declaration.FlowPath, Alias: declaration.Alias, SigningSecret: binding.SigningSecret, SourcePath: declaration.SourcePath,
			})
			if err != nil {
				return nil, err
			}
			subjects = append(subjects, subject)
		}
	}
	return packs.NormalizeSubjects(subjects)
}

// EvaluateProviderTriggerCapabilitySubject resolves one immutable effective
// target subject against the deployment credential store. Installed inventory
// is intentionally outside this target-scoped projection.
func EvaluateProviderTriggerCapabilitySubject(ctx context.Context, subject packs.Subject, owner *runtimecredentials.SnapshotOwner) (packs.Subject, error) {
	projection := owner.BeginSecretBindingProjection()
	evaluated, err := evaluateProviderTriggerCapabilitySubject(ctx, subject, projection)
	if err != nil {
		return packs.Subject{}, err
	}
	if err := projection.ValidateCurrent(ctx); err != nil {
		return packs.Subject{}, err
	}
	return evaluated, nil
}

func evaluateProviderTriggerCapabilitySubject(ctx context.Context, subject packs.Subject, projection *runtimecredentials.SecretBindingProjection) (packs.Subject, error) {
	normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil {
		return packs.Subject{}, err
	}
	if len(normalized) != 1 || normalized[0].Kind != packs.SubjectProviderTrigger || normalized[0].Applicability != "effective" {
		return packs.Subject{}, fmt.Errorf("target credential evaluation requires one effective provider trigger subject")
	}
	base := normalized[0]
	if base.TriggerAdmission.Transport == packs.ChannelTransportSession ||
		base.TriggerAdmission.RequestAuthentication == string(providertriggers.RequestAuthenticationNone) {
		return base, nil
	}
	if len(subject.Requirements) != 1 || subject.Requirements[0].Satisfied != nil || strings.TrimSpace(subject.Requirements[0].Status) != "" || strings.TrimSpace(subject.Requirements[0].Remediation) != "" || strings.TrimSpace(subject.Requirements[0].Source) != "" {
		return packs.Subject{}, fmt.Errorf("effective provider trigger subject %q must enter credential evaluation with one unevaluated target requirement", base.ID)
	}
	binding, err := projection.ObserveSecretBinding(ctx, base.Requirements[0].Name)
	if err != nil {
		return packs.Subject{}, err
	}
	evaluated := packs.CloneSubjects([]packs.Subject{base})[0]
	evaluated.Status = ""
	evaluated.Requirements[0] = packs.RequirementWithStatus(
		packs.RequirementSecret,
		base.Requirements[0].Name,
		packs.RequirementScopeTarget,
		string(binding.Status()),
		"credential_store",
	)
	normalized, err = packs.NormalizeSubjects([]packs.Subject{evaluated})
	if err != nil {
		return packs.Subject{}, err
	}
	return normalized[0], nil
}

func EvaluateStandingIngressCapabilitySubject(ctx context.Context, target StandingTarget, subject packs.Subject, owner *runtimecredentials.SnapshotOwner) (packs.Subject, error) {
	projection := owner.BeginSecretBindingProjection()
	evaluated, err := evaluateStandingIngressCapabilitySubject(ctx, target, subject, projection)
	if err != nil {
		return packs.Subject{}, err
	}
	if err := projection.ValidateCurrent(ctx); err != nil {
		return packs.Subject{}, err
	}
	return evaluated, nil
}

func evaluateStandingIngressCapabilitySubject(ctx context.Context, target StandingTarget, subject packs.Subject, projection *runtimecredentials.SecretBindingProjection) (packs.Subject, error) {
	expected, err := target.CapabilitySubject()
	if err != nil {
		return packs.Subject{}, err
	}
	actual, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil {
		return packs.Subject{}, err
	}
	if len(actual) != 1 || !reflect.DeepEqual(actual[0], expected) {
		return packs.Subject{}, fmt.Errorf("standing target %q/%q capability subject does not match its compiled admission authority", target.Alias, target.Provider)
	}
	return evaluateProviderTriggerCapabilitySubject(ctx, expected, projection)
}

func EffectiveStandingIngressCapabilitySubjects(ctx context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot, owner *runtimecredentials.SnapshotOwner) ([]packs.Subject, error) {
	base, err := baseStandingIngressCapabilitySubjects(source, catalog)
	if err != nil {
		return nil, err
	}
	return evaluateProviderTriggerCapabilitySubjects(ctx, base, owner)
}

func evaluateProviderTriggerCapabilitySubjects(ctx context.Context, base []packs.Subject, owner *runtimecredentials.SnapshotOwner) ([]packs.Subject, error) {
	projection := owner.BeginSecretBindingProjection()
	evaluated := make([]packs.Subject, 0, len(base))
	for _, subject := range base {
		current, err := evaluateProviderTriggerCapabilitySubject(ctx, subject, projection)
		if err != nil {
			return nil, err
		}
		evaluated = append(evaluated, current)
	}
	normalized, err := packs.NormalizeSubjects(evaluated)
	if err != nil {
		return nil, err
	}
	if err := projection.ValidateCurrent(ctx); err != nil {
		return nil, err
	}
	return normalized, nil
}

// ProviderTriggerCapabilitySubjects is the canonical installed-plus-effective
// trigger projection for contract verification surfaces.
func ProviderTriggerCapabilitySubjects(ctx context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot, owner *runtimecredentials.SnapshotOwner) ([]packs.Subject, error) {
	var subjects []packs.Subject
	if catalog != nil {
		installed, err := catalog.InstalledCapabilitySubjects()
		if err != nil {
			return nil, err
		}
		subjects = append(subjects, installed...)
	}
	effective, err := EffectiveStandingIngressCapabilitySubjects(ctx, source, catalog, owner)
	if err != nil {
		return nil, err
	}
	subjects = append(subjects, effective...)
	return packs.NormalizeSubjects(subjects)
}

func (rt *Runtime) EnsureStandingTargets(ctx context.Context) ([]StandingTarget, []StandingActivation, error) {
	targets, activations, err := rt.ensureStandingTargets(ctx, "")
	if err == nil {
		err = rt.restoreAdoptedStandingWorkflowTimers(ctx, activations)
	}
	return targets, activations, err
}

func (rt *Runtime) EnsureFlowTargets(ctx context.Context, flowPath string) ([]StandingTarget, []StandingActivation, error) {
	flowPath = strings.TrimSpace(flowPath)
	if flowPath == "" {
		return nil, nil, fmt.Errorf("flow path is required")
	}
	targets, activations, err := rt.ensureStandingTargets(ctx, flowPath)
	if err == nil {
		err = rt.restoreAdoptedStandingWorkflowTimers(ctx, activations)
	}
	return targets, activations, err
}

func (rt *Runtime) ensureStandingTargets(ctx context.Context, flowPath string) ([]StandingTarget, []StandingActivation, error) {
	return rt.ensureStandingTargetsMutation(ctx, flowPath)
}

func (rt *Runtime) ensureStandingTargetsMutation(ctx context.Context, flowPath string) ([]StandingTarget, []StandingActivation, error) {
	targets, activations, _, err := rt.standingTargetsMutation(ctx, flowPath, false)
	return targets, activations, err
}

func (rt *Runtime) PrepareStandingTargets(ctx context.Context) ([]StandingTarget, []StandingActivation, func() error, error) {
	return rt.standingTargetsMutation(ctx, "", true)
}

func (rt *Runtime) standingTargetsMutation(ctx context.Context, flowPath string, prepare bool) ([]StandingTarget, []StandingActivation, func() error, error) {
	if rt == nil {
		return nil, nil, nil, fmt.Errorf("standing activation requires a runtime")
	}
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, nil, nil, err
	}
	if len(plans) == 0 {
		return nil, nil, nil, nil
	}
	if err := rt.ValidateStandingIngressCredentials(ctx); err != nil {
		return nil, nil, nil, err
	}
	if rt.workOccurrence == nil {
		return nil, nil, nil, fmt.Errorf("standing activation requires a runtime work occurrence")
	}
	ctx, err = rt.compiledSourceContext(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx = rt.authorActivityContext(ctx)
	if _, hasPreparedOwner := worklifetime.OccurrenceFromContext(ctx); !hasPreparedOwner {
		ctx = worklifetime.WithRuntimeOccurrence(ctx, rt.workOccurrence)
	}
	if rt.Pipeline == nil || rt.Manager == nil {
		return nil, nil, nil, fmt.Errorf("standing activation requires pipeline store, pipeline, and agent manager")
	}
	fact := rt.Options.SourceArtifactFact
	source := rt.Options.WorkflowModule.SemanticSource()
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	selectedPlans := make([]standingTargetPlan, 0, len(plans))
	mutations := make([]runtimepipeline.LiveTargetMutation, 0, len(plans))
	observedAt := time.Now().UTC()
	for _, plan := range plans {
		if flowPath != "" && plan.declaration.FlowPath != flowPath {
			continue
		}
		declaration := plan.declaration
		graph, found := semanticview.WorkflowStageTopology(source, declaration.FlowPath)
		if !found || graph.FlowID != declaration.FlowPath || !graph.ValidStageCatalog() {
			return nil, nil, nil, fmt.Errorf("flow %q has no selected compiled stage topology", declaration.FlowPath)
		}
		initial, err := graph.InitialStoredStage()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("flow %q initial stage: %w", declaration.FlowPath, err)
		}
		initialState := ""
		if !initial.IsStatelessPosture() {
			initialState = initial.ID()
		}
		selectedPlans = append(selectedPlans, plan)
		mutations = append(mutations, runtimepipeline.LiveTargetMutation{
			FlowPath: declaration.FlowPath,
			Activation: runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: source,
				InitialState:   initialState,
				Bookkeeping: map[string]any{
					"bundle_hash": fact.BundleHash(),
					"flow_path":   declaration.FlowPath,
				},
				OccurredAt: observedAt,
			},
		})
	}
	request := runtimepipeline.LiveTargetMutationRequest{Targets: mutations, ObservedAt: observedAt}
	var results []runtimepipeline.LiveTargetMutationResult
	var complete func() error
	if prepare {
		results, complete, err = rt.Pipeline.PrepareLiveTargets(ctx, request, rt.Manager)
	} else {
		results, err = rt.Pipeline.CommitLiveTargets(ctx, request, rt.Manager)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("activate standing targets: %w", err)
	}
	if len(results) != len(selectedPlans) {
		return nil, nil, nil, fmt.Errorf("standing target mutation returned %d results for %d plans", len(results), len(selectedPlans))
	}
	targets := make([]StandingTarget, 0)
	activations := make([]StandingActivation, 0, len(selectedPlans))
	for i, plan := range selectedPlans {
		declaration := plan.declaration
		result := results[i]
		instance := result.Instance
		reconciliation := result.Reconciliation
		if !reconciliation.Source.Matches(fact) || (reconciliation.State != runtimestanding.StateLive && reconciliation.State != runtimestanding.StatePaused) {
			continue
		}
		activations = append(activations, StandingActivation{
			BundleHash: fact.BundleHash(), FlowPath: declaration.FlowPath,
			RunID: reconciliation.RunID, RunState: reconciliation.State, Construction: instance, Created: result.Created,
		})
		for _, target := range plan.targets {
			target.BundleHash = fact.BundleHash()
			target.RunID = reconciliation.RunID
			targets = append(targets, target.normalized())
		}
	}
	if prepare {
		finish := complete
		complete = func() error {
			if err := finish(); err != nil {
				return err
			}
			return rt.restoreAdoptedStandingWorkflowTimers(ctx, activations)
		}
	}
	return targets, activations, complete, nil
}

func (rt *Runtime) restoreAdoptedStandingWorkflowTimers(ctx context.Context, activations []StandingActivation) error {
	if rt == nil || rt.Pipeline == nil {
		return nil
	}
	fact := rt.Options.SourceArtifactFact
	restored := make(map[string]struct{}, len(activations))
	for _, activation := range activations {
		runID := strings.TrimSpace(activation.RunID)
		if activation.Created || (activation.RunState != runtimestanding.StateLive && activation.RunState != runtimestanding.StatePaused) || runID == "" {
			continue
		}
		if _, ok := restored[runID]; ok {
			continue
		}
		restored[runID] = struct{}{}
		runCtx := runtimecorrelation.WithRunID(ctx, runID)
		runCtx = runtimecorrelation.WithSourceArtifactFact(runCtx, fact)
		if err := rt.Pipeline.RestoreWorkflowTimers(runCtx); err != nil {
			return fmt.Errorf("restore adopted standing run %s workflow timers: %w", runID, err)
		}
	}
	return nil
}

// PlanStandingTargets describes declaration coordinates for collision preflight.
// Only selected-store reconciliation can supply a concrete run.
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

func (rt *Runtime) PlanLiveFlowScopes() ([]runtimestanding.LiveRunScope, error) {
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, err
	}
	out := make([]runtimestanding.LiveRunScope, 0, len(plans))
	for _, plan := range plans {
		out = append(out, runtimestanding.LiveRunScope{FlowPath: plan.declaration.FlowPath})
	}
	return out, nil
}

func (rt *Runtime) standingTargetPlans() ([]standingTargetPlan, error) {
	if rt == nil || rt.Options.WorkflowModule == nil {
		return nil, fmt.Errorf("runtime workflow module is required")
	}
	// Held-layer carriage exception: https://github.com/division-sh/swarm/pull/2794#issuecomment-6418146948.
	admission, err := rt.standingCredentials(rt.authorActivityContext(context.Background()))
	if err != nil {
		return nil, err
	}
	if err := validateEnabledStandingAliases(admission); err != nil {
		return nil, err
	}
	declarations := admission.declarations
	if len(declarations) == 0 {
		return nil, nil
	}
	fact := rt.Options.SourceArtifactFact
	if err := fact.Validate(); err != nil {
		return nil, fmt.Errorf("standing target bundle source fact: %w", err)
	}
	plans := make([]standingTargetPlan, 0, len(declarations))
	for _, declaration := range declarations {
		plan := standingTargetPlan{declaration: declaration}
		for _, binding := range declaration.Ingress {
			credentials := admission.bindings[standingIngressSelector(declaration.FlowPath, binding.Provider)]
			if !credentials.enabled {
				continue
			}
			plan.targets = append(plan.targets, StandingTarget{
				BundleHash: fact.BundleHash(), SourcePath: declaration.SourcePath,
				FlowPath: declaration.FlowPath, Alias: declaration.Alias,
				Provider: binding.Provider, SigningSecret: credentials.signingKey, AdmissionPlan: binding.AdmissionPlan,
			}.normalized())
		}
		plans = append(plans, plan)
	}
	return plans, nil
}
