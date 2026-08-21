package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
	PackageKey string
	SourcePath string
	FlowID     string
	FlowPath   string
	Alias      string
	Ingress    []StandingIngressBinding
}

type StandingTarget struct {
	BundleHash          string
	ServiceID           string
	PackageKey          string
	SourcePath          string
	FlowID              string
	FlowPath            string
	Alias               string
	Provider            string
	RunID               string
	Generation          int64
	PublicationSequence int64
	InstanceID          string
	FlowInstance        string
	EntityID            string
	SigningSecret       string
	AdmissionPlan       providertriggers.InboundAdmissionPlan
}

type StandingActivation struct {
	BundleHash          string
	ServiceID           string
	PackageKey          string
	FlowID              string
	RunID               string
	Generation          int64
	PublicationSequence int64
	InstanceID          string
	FlowInstance        string
	EntityID            string
	EffectiveState      string
	Created             bool
}

type standingTargetPlan struct {
	declaration StandingTargetDeclaration
	serviceID   string
	generation  int64
	runID       string
	instance    runtimeflowidentity.Instance
	targets     []StandingTarget
}

func (t StandingTarget) normalized() StandingTarget {
	t.BundleHash = strings.TrimSpace(t.BundleHash)
	t.ServiceID = strings.TrimSpace(t.ServiceID)
	t.PackageKey = strings.TrimSpace(t.PackageKey)
	t.SourcePath = strings.TrimSpace(t.SourcePath)
	t.FlowID = strings.TrimSpace(t.FlowID)
	t.FlowPath = strings.Trim(strings.TrimSpace(t.FlowPath), "/")
	t.Alias = strings.Trim(strings.TrimSpace(t.Alias), "/")
	t.Provider = providertriggers.NormalizeProviderName(t.Provider)
	t.RunID = strings.TrimSpace(t.RunID)
	t.InstanceID = strings.TrimSpace(t.InstanceID)
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
			if _, err := bundle.ResolveFlowSingleton(flowID); err != nil {
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
					rawOutput, ok := plan.RawOutput()
					if !ok {
						return nil, fmt.Errorf("%s: ingress provider %q compiled no raw output", location, provider)
					}
					literal := strings.TrimSpace(rawOutput.EventName.Literal)
					template := strings.TrimSpace(rawOutput.EventName.Template)
					if err := validateStandingIngressRawPin(source, flowID, provider, rawOutput.EventName); err != nil {
						return nil, fmt.Errorf("%s: %w", location, err)
					}
					decl.Ingress = append(decl.Ingress, StandingIngressBinding{
						Provider: provider, SigningSecret: secret, RawEventLiteral: literal, RawEventTemplate: template, AdmissionPlan: plan,
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
	if base.TriggerAdmission.RequestAuthentication == string(providertriggers.RequestAuthenticationNone) {
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

func standingDeclarationLocation(pkg runtimecontracts.LoadedProjectPackage, flowID string) string {
	path := strings.TrimSpace(pkg.Paths.PackageFile)
	if path == "" {
		path = "package.yaml"
	}
	return fmt.Sprintf("%s flows[%s]", path, firstNonEmpty(flowID, "<missing>"))
}

func validateStandingIngressRawPin(source semanticview.Source, flowID, provider string, eventNames providertriggers.EventNameManifest) error {
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
	producer := runtimepinrouting.ResolveFlowInputProducer(source, flowID, eventName)
	if !producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) &&
		!producer.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		return semanticview.AuthoredEventEndpoint{}, fmt.Errorf("event endpoint %q in flow %s is not declared as external ingress", eventName, flowID)
	}
	return endpoint, nil
}

func (rt *Runtime) EnsureStandingTargets(ctx context.Context) ([]StandingTarget, []StandingActivation, error) {
	targets, activations, err := rt.ensureStandingTargets(ctx, "")
	if err == nil {
		err = rt.restoreAdoptedStandingWorkflowTimers(ctx, activations)
	}
	return targets, activations, err
}

func (rt *Runtime) EnsureStandingServiceTargets(ctx context.Context, serviceID string) ([]StandingTarget, []StandingActivation, error) {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return nil, nil, fmt.Errorf("standing service_id is required")
	}
	targets, activations, err := rt.ensureStandingTargets(ctx, serviceID)
	if err == nil {
		err = rt.restoreAdoptedStandingWorkflowTimers(ctx, activations)
	}
	return targets, activations, err
}

// EnsureStandingReplacementTargets atomically reconciles a hot replacement's
// declaration set before publishing its process-local standing targets.
func (rt *Runtime) EnsureStandingReplacementTargets(ctx context.Context, predecessor *Runtime) ([]StandingTarget, []StandingActivation, error) {
	if rt == nil {
		return nil, nil, fmt.Errorf("replacement runtime is required")
	}
	candidates, err := rt.PlanStandingServiceCandidates()
	if err != nil {
		return nil, nil, err
	}
	var previous []runtimepipeline.StandingServiceCandidate
	if predecessor != nil {
		previous, err = predecessor.PlanStandingServiceCandidates()
		if err != nil {
			return nil, nil, err
		}
	}
	if len(previous) == 0 && len(candidates) == 0 {
		return nil, nil, nil
	}
	if rt.workOccurrence == nil {
		return nil, nil, fmt.Errorf("replacement runtime work occurrence is required")
	}
	ctx = worklifetime.WithRuntimeOccurrence(ctx, rt.workOccurrence)
	owner := rt.Pipeline
	if owner == nil {
		return nil, nil, fmt.Errorf("standing replacement requires pipeline store")
	}
	if len(previous) > 0 && predecessor.Pipeline == nil {
		return nil, nil, fmt.Errorf("standing predecessor requires pipeline store")
	}
	targets, activations, err := rt.ensureStandingTargetsMutation(ctx, "", previous, true)
	if err == nil {
		err = rt.restoreAdoptedStandingWorkflowTimers(ctx, activations)
	}
	return targets, activations, err
}

func (rt *Runtime) ensureStandingTargets(ctx context.Context, serviceID string) ([]StandingTarget, []StandingActivation, error) {
	return rt.ensureStandingTargetsMutation(ctx, serviceID, nil, false)
}

func (rt *Runtime) ensureStandingTargetsMutation(ctx context.Context, serviceID string, previous []runtimepipeline.StandingServiceCandidate, replace bool) ([]StandingTarget, []StandingActivation, error) {
	if rt == nil {
		return nil, nil, fmt.Errorf("standing activation requires a runtime")
	}
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, nil, err
	}
	if len(plans) == 0 {
		return nil, nil, nil
	}
	if rt.workOccurrence == nil {
		return nil, nil, fmt.Errorf("standing activation requires a runtime work occurrence")
	}
	ctx = rt.authorActivityContext(ctx)
	if _, hasPreparedOwner := worklifetime.OccurrenceFromContext(ctx); !hasPreparedOwner {
		ctx = worklifetime.WithRuntimeOccurrence(ctx, rt.workOccurrence)
	}
	if rt.Pipeline == nil || rt.Manager == nil {
		return nil, nil, fmt.Errorf("standing activation requires pipeline store, pipeline, and agent manager")
	}
	fact := rt.Options.BundleSourceFact
	source := rt.Options.WorkflowModule.SemanticSource()
	selectedPlans := make([]standingTargetPlan, 0, len(plans))
	mutations := make([]runtimepipeline.StandingTargetMutation, 0, len(plans))
	observedAt := time.Now().UTC()
	for _, plan := range plans {
		if serviceID != "" && plan.serviceID != serviceID {
			continue
		}
		declaration := plan.declaration
		instance := plan.instance
		selectedPlans = append(selectedPlans, plan)
		mutations = append(mutations, runtimepipeline.StandingTargetMutation{
			Candidate: runtimepipeline.StandingServiceCandidate{
				ServiceID: plan.serviceID, PackageKey: declaration.PackageKey, FlowID: declaration.FlowID,
				InstanceID: instance.InstanceID, EntityID: instance.EntityID, Source: fact,
			},
			Activation: runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: source,
				Instance:       instance,
				InitialState:   source.FlowInitialStage(declaration.FlowID),
				Config:         map[string]any{},
				Bookkeeping: map[string]any{
					"activation":  runtimecontracts.ProjectFlowActivationStanding,
					"bundle_hash": fact.BundleHash(),
					"package_key": declaration.PackageKey,
				},
				OccurredAt: observedAt,
			},
		})
	}
	results, err := rt.Pipeline.CommitStandingTargets(ctx, runtimepipeline.StandingTargetMutationRequest{
		Previous: previous, Targets: mutations, Replace: replace, ObservedAt: observedAt,
	}, rt.Manager)
	if err != nil {
		return nil, nil, fmt.Errorf("activate standing targets: %w", err)
	}
	if len(results) != len(selectedPlans) {
		return nil, nil, fmt.Errorf("standing target mutation returned %d results for %d plans", len(results), len(selectedPlans))
	}
	targets := make([]StandingTarget, 0)
	activations := make([]StandingActivation, 0, len(selectedPlans))
	for i, plan := range selectedPlans {
		declaration := plan.declaration
		instance := plan.instance
		result := results[i]
		reconciliation := result.Reconciliation
		if reconciliation.EffectiveState != "active" {
			activations = append(activations, StandingActivation{
				BundleHash: fact.BundleHash(), ServiceID: reconciliation.ServiceID, PackageKey: declaration.PackageKey,
				FlowID: declaration.FlowID, RunID: reconciliation.RunID, Generation: reconciliation.Generation,
				PublicationSequence: reconciliation.PublicationSequence, InstanceID: instance.InstanceID,
				FlowInstance: instance.InstancePath, EntityID: instance.EntityID,
				EffectiveState: reconciliation.EffectiveState, Created: false,
			})
			for _, target := range plan.targets {
				target.BundleHash = fact.BundleHash()
				target.RunID = reconciliation.RunID
				target.Generation = reconciliation.Generation
				target.PublicationSequence = reconciliation.PublicationSequence
				targets = append(targets, target.normalized())
			}
			continue
		}
		activations = append(activations, StandingActivation{
			BundleHash: fact.BundleHash(), ServiceID: plan.serviceID, PackageKey: declaration.PackageKey, FlowID: declaration.FlowID,
			RunID: reconciliation.RunID, Generation: reconciliation.Generation, PublicationSequence: result.PublicationSequence, InstanceID: instance.InstanceID,
			FlowInstance: instance.InstancePath, EntityID: instance.EntityID,
			EffectiveState: reconciliation.EffectiveState, Created: result.Created,
		})
		for _, target := range plan.targets {
			target.BundleHash = fact.BundleHash()
			target.RunID = reconciliation.RunID
			target.Generation = reconciliation.Generation
			target.PublicationSequence = result.PublicationSequence
			targets = append(targets, target.normalized())
		}
	}
	return targets, activations, nil
}

func (rt *Runtime) restoreAdoptedStandingWorkflowTimers(ctx context.Context, activations []StandingActivation) error {
	if rt == nil || rt.Pipeline == nil {
		return nil
	}
	fact := rt.Options.BundleSourceFact
	restored := make(map[string]struct{}, len(activations))
	for _, activation := range activations {
		runID := strings.TrimSpace(activation.RunID)
		if activation.Created || activation.EffectiveState != "active" || runID == "" {
			continue
		}
		if _, ok := restored[runID]; ok {
			continue
		}
		restored[runID] = struct{}{}
		runCtx := runtimecorrelation.WithRunID(ctx, runID)
		runCtx = runtimecorrelation.WithBundleSourceFact(runCtx, fact)
		if err := rt.Pipeline.RestoreWorkflowTimers(runCtx); err != nil {
			return fmt.Errorf("restore adopted standing run %s workflow timers: %w", runID, err)
		}
	}
	return nil
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

func (rt *Runtime) PlanStandingServiceCandidates() ([]runtimepipeline.StandingServiceCandidate, error) {
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, err
	}
	fact := rt.Options.BundleSourceFact
	out := make([]runtimepipeline.StandingServiceCandidate, 0, len(plans))
	for _, plan := range plans {
		out = append(out, runtimepipeline.StandingServiceCandidate{
			ServiceID: plan.serviceID, PackageKey: plan.declaration.PackageKey, FlowID: plan.declaration.FlowID,
			InstanceID: plan.instance.InstanceID, EntityID: plan.instance.EntityID, Source: fact,
		})
	}
	return out, nil
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
	fact := rt.Options.BundleSourceFact
	if err := fact.Validate(); err != nil {
		return nil, fmt.Errorf("standing target bundle source fact: %w", err)
	}
	plans := make([]standingTargetPlan, 0, len(declarations))
	for _, declaration := range declarations {
		serviceID := runtimeflowidentity.StandingServiceID(declaration.PackageKey, declaration.FlowID)
		generation := int64(1)
		runID := runtimeflowidentity.StandingGenerationRunID(serviceID, generation)
		instance := runtimeflowidentity.StandingForService(source, declaration.FlowID, serviceID)
		plan := standingTargetPlan{declaration: declaration, serviceID: serviceID, generation: generation, runID: runID, instance: instance}
		for _, binding := range declaration.Ingress {
			plan.targets = append(plan.targets, StandingTarget{
				BundleHash: fact.BundleHash(), ServiceID: serviceID, PackageKey: declaration.PackageKey, SourcePath: declaration.SourcePath,
				FlowID: declaration.FlowID, FlowPath: declaration.FlowPath, Alias: declaration.Alias,
				Provider: binding.Provider, RunID: runID, Generation: generation, PublicationSequence: 1,
				InstanceID: instance.InstanceID, FlowInstance: instance.InstancePath,
				EntityID: instance.EntityID, SigningSecret: binding.SigningSecret, AdmissionPlan: binding.AdmissionPlan,
			}.normalized())
		}
		plans = append(plans, plan)
	}
	return plans, nil
}
