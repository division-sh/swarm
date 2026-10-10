package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	sessionexecution "github.com/division-sh/swarm/internal/sessionprovider/execution"
)

type servePrebindingActivation struct {
	Operation channelonboarding.Operation
	Candidate channelonboarding.Candidate
}

type serveChannelActivationSnapshot struct {
	Activations []channelonboarding.CompiledActivation
	Prebinding  []servePrebindingActivation
}

func serveChannelOnboardingCatalog(manager *runtime.RuntimeContextManager) (*channelonboarding.CandidateCatalog, error) {
	if manager == nil {
		return nil, fmt.Errorf("runtime context manager is required for channel onboarding")
	}
	candidates := []channelonboarding.Candidate{}
	for _, contextDef := range manager.LoadedContexts() {
		declarations, err := runtime.ResolveStandingTargetDeclarations(contextDef.Source, contextDef.ProviderTriggerCatalog)
		if err != nil {
			return nil, err
		}
		planned, err := serveChannelContextCandidates(contextDef, declarations)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, planned...)
	}
	return channelonboarding.NewCandidateCatalog(candidates)
}

func serveChannelContextCandidates(contextDef runtime.BundleContext, declarations []runtime.StandingTargetDeclaration) ([]channelonboarding.Candidate, error) {
	bundleIdentity := fmt.Sprintf("%s@%s#%s", strings.TrimSpace(contextDef.BundleIdentity.WorkflowName), strings.TrimSpace(contextDef.BundleIdentity.WorkflowVersion), strings.TrimSpace(contextDef.BundleIdentity.BundleHash))
	if strings.Trim(bundleIdentity, "@#") == "" {
		return nil, fmt.Errorf("runtime context %s has no exact bundle identity", contextDef.BundleHash())
	}
	var candidates []channelonboarding.Candidate
	for _, plan := range contextDef.ChannelPlans {
		planned, err := serveChannelCandidatesForPlan(contextDef, bundleIdentity, declarations, plan)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, planned...)
	}
	return candidates, nil
}

func serveChannelCandidatesForPlan(contextDef runtime.BundleContext, bundleIdentity string, declarations []runtime.StandingTargetDeclaration, plan packs.SatisfactionPlan) ([]channelonboarding.Candidate, error) {
	profile, ok := plan.OnboardingProfile()
	if !ok {
		return nil, nil
	}
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		return nil, err
	}
	generation, err := plan.Generation()
	if err != nil {
		return nil, err
	}
	posture := channelonboarding.ActivationPosture(profile.ActivationPosture())
	ceremony := channelonboarding.IdentityCeremony(profile.IdentityCeremony())
	var candidates []channelonboarding.Candidate
	for _, declaration := range declarations {
		for _, binding := range declaration.Ingress {
			if binding.Provider != profile.Provider() {
				continue
			}
			target := serveChannelDeclarationTarget(contextDef, declaration, binding)
			coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
				BundleHash: contextDef.BundleHash(), BundleIdentity: bundleIdentity,
				PackInventoryGeneration: contextDef.PackInventoryDigest, RuntimeInstanceID: contextDef.RuntimeInstanceID,
				ContextPublicationGeneration: contextDef.PublicationGeneration,
				PlanGeneration:               generation, TargetGeneration: uint64(target.Generation),
			}
			candidates = append(candidates, channelonboarding.Candidate{
				SourceLabel: contextDef.BundleIdentity.SourceLabel,
				Provider:    profile.Provider(), Interface: identity, Coordinate: coordinate,
				Target: channelonboarding.CandidateTarget{
					Selector: fmt.Sprintf("ingress:%s:%s", target.FlowPath, target.Provider), ServiceID: target.ServiceID, FlowPath: target.FlowPath,
					Alias: target.Alias, Provider: target.Provider, Generation: uint64(target.Generation), PublicationSequence: target.PublicationSequence,
					AdmissionGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: target.SigningSecret,
				},
				Posture: posture, Ceremony: ceremony,
				ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential(),
				ConfirmationOperation: profile.ConfirmationOperation(), ConnectionHealth: profile.ConnectionHealth(), Plan: plan,
			})
		}
	}
	return candidates, nil
}

func serveChannelDeclarationTarget(contextDef runtime.BundleContext, declaration runtime.StandingTargetDeclaration, binding runtime.StandingIngressBinding) runtime.StandingTarget {
	for _, executable := range contextDef.StandingTargets {
		if executable.FlowPath == declaration.FlowPath && executable.Provider == binding.Provider {
			return executable
		}
	}
	return runtime.StandingTarget{
		ServiceID: runtimeflowidentity.StandingServiceID(declaration.FlowPath), FlowPath: declaration.FlowPath,
		Alias: declaration.Alias, Provider: binding.Provider, SigningSecret: binding.SigningSecret, AdmissionPlan: binding.AdmissionPlan,
	}
}

func rejectWebhookPrebindingWithoutPublicIngress(snapshot serveChannelActivationSnapshot) error {
	for _, prebinding := range snapshot.Prebinding {
		if prebinding.Candidate.Posture == channelonboarding.ActivationWebhookRegistration {
			return channelonboarding.NewTerminalActivationError(
				"public_ingress_unavailable",
				fmt.Errorf("webhook channel onboarding requires --dev --expose or an explicit public webhook origin and listener"),
			)
		}
	}
	return nil
}

func channelOnboardingDeclaredPlans(contextDef runtime.BundleContext) []packs.OutboundBindingPlan {
	return contextDef.DeclaredChannelPublication.Bindings()
}

func compileServeChannelActivationSnapshot(ctx context.Context, manager *runtime.RuntimeContextManager, store channelonboarding.Store, identities *operatorchannel.Service, credentials *runtimecredentials.SnapshotOwner) (serveChannelActivationSnapshot, error) {
	if manager == nil {
		return serveChannelActivationSnapshot{}, fmt.Errorf("runtime context manager is required for channel activations")
	}
	declared := []channelonboarding.CompiledActivation{}
	projection := credentials.BeginSecretBindingProjection()
	for _, contextDef := range manager.LoadedContexts() {
		compiled, err := compileServeDeclaredChannelActivations(ctx, manager, contextDef, credentials, projection)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		declared = append(declared, compiled...)
	}
	learned := []channelonboarding.CompiledActivation{}
	prebinding := []servePrebindingActivation{}
	if store != nil || identities != nil {
		if store == nil || identities == nil {
			return serveChannelActivationSnapshot{}, fmt.Errorf("learned channel activations require both selected store and identity owner")
		}
		catalog, err := serveChannelOnboardingCatalog(manager)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		operations, err := store.ListChannelOnboardingOperations(ctx)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		activations, err := store.ListCurrentConnectedChannelActivations(ctx)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		learned, err = compileServeLearnedChannelActivations(ctx, store, catalog, activations, projection)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		prebinding, err = compileServePrebindingActivations(ctx, catalog, operations, projection)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
	}
	merged, err := channelonboarding.MergeCompiledActivations(declared, learned)
	if err != nil {
		return serveChannelActivationSnapshot{}, err
	}
	sort.Slice(prebinding, func(i, j int) bool { return prebinding[i].Operation.SlotKey < prebinding[j].Operation.SlotKey })
	if err := projection.ValidateCurrent(ctx); err != nil {
		return serveChannelActivationSnapshot{}, err
	}
	return serveChannelActivationSnapshot{Activations: merged, Prebinding: prebinding}, nil
}

func compileServeLearnedChannelActivations(ctx context.Context, store channelonboarding.Store, catalog *channelonboarding.CandidateCatalog, activations []channelonboarding.ConnectedChannelActivation, projection *runtimecredentials.SecretBindingProjection) ([]channelonboarding.CompiledActivation, error) {
	learned := []channelonboarding.CompiledActivation{}
	for _, activation := range activations {
		candidate, current := catalog.FindExact(activation.Provider, activation.Interface, activation.Coordinate, activation.TargetSelector)
		if !current {
			// Historical selected-store rows remain readable but cannot grant
			// execution or registration authority to a successor publication.
			continue
		}
		compiled, err := channelonboarding.CompileLearnedActivation(candidate, activation)
		if err != nil {
			return nil, err
		}
		current, err = channelonboarding.AdmissionResponsibilityCurrent(ctx, store, channelonboarding.AdmissionResponsibility{
			OperationID: compiled.OnboardingOperationID, OperationRevision: compiled.OnboardingRevision,
			ActivationRevision: compiled.ActivationRevision, Coordinate: compiled.Coordinate,
			TargetSelector: candidate.Target.Selector, Provider: candidate.Provider, Credentials: compiled.CredentialAdmissions,
		}, true)
		if err != nil {
			return nil, err
		}
		if !current {
			continue
		}
		executable, err := observeChannelActivationAdmissions(ctx, projection, compiled.CredentialAdmissions)
		if err != nil {
			return nil, err
		}
		if executable {
			learned = append(learned, compiled)
		}
	}
	return learned, nil
}

func compileServePrebindingActivations(ctx context.Context, catalog *channelonboarding.CandidateCatalog, operations []channelonboarding.Operation, projection *runtimecredentials.SecretBindingProjection) ([]servePrebindingActivation, error) {
	prebinding := []servePrebindingActivation{}
	for _, op := range operations {
		if !op.Phase.SupportsPrebindingRegistration() {
			continue
		}
		candidate, current := catalog.FindExact(op.Provider, op.Interface, op.Coordinate, op.TargetSelector)
		if !current {
			continue
		}
		executable, err := observeChannelActivationAdmissions(ctx, projection, op.CredentialAdmissions)
		if err != nil {
			return nil, err
		}
		if !executable {
			continue
		}
		prebinding = append(prebinding, servePrebindingActivation{Operation: op, Candidate: candidate})
	}
	return prebinding, nil
}

func compileServeDeclaredChannelActivations(ctx context.Context, manager *runtime.RuntimeContextManager, contextDef runtime.BundleContext, credentials *runtimecredentials.SnapshotOwner, projection *runtimecredentials.SecretBindingProjection) ([]channelonboarding.CompiledActivation, error) {
	prior, err := priorServeDeclaredActivations(manager, contextDef)
	if err != nil {
		return nil, err
	}
	declared := []channelonboarding.CompiledActivation{}
	for _, binding := range channelOnboardingDeclaredPlans(contextDef) {
		enabled, err := serveDeclaredRegistrationEnabled(ctx, manager, contextDef, binding)
		if err != nil {
			return nil, err
		}
		if !enabled {
			continue
		}
		coordinate, err := declaredActivationCoordinate(contextDef, binding)
		if err != nil {
			return nil, err
		}
		var admissions []channelonboarding.CredentialAdmission
		if previous, admitted := prior[binding.BindingID()]; admitted {
			if !previous.Coordinate.Matches(coordinate) {
				return nil, fmt.Errorf("%w: declared activation coordinate changed without replacement", channelonboarding.ErrRevisionConflict)
			}
			admissions = previous.CredentialAdmissions
		} else {
			admissions, err = declaredActivationCredentialAdmissions(ctx, credentials, coordinate, binding)
		}
		if err != nil {
			return nil, err
		}
		current, err := observeChannelActivationAdmissions(ctx, projection, admissions)
		if err != nil {
			return nil, err
		}
		if !current {
			continue
		}
		declared = append(declared, channelonboarding.CompiledActivation{
			Source: channelonboarding.ActivationSourceDeclared, Coordinate: coordinate,
			Plan: binding, CredentialAdmissions: admissions,
		})
	}
	return declared, nil
}

func priorServeDeclaredActivations(manager *runtime.RuntimeContextManager, contextDef runtime.BundleContext) (map[string]channelonboarding.CompiledActivation, error) {
	prior := map[string]channelonboarding.CompiledActivation{}
	if !contextDef.ChannelActivationGeneration.Valid() {
		return prior, nil
	}
	lease, available, err := manager.AcquireChannelActivationPublication(contextDef.BundleHash(), contextDef.PublicationGeneration)
	if err != nil {
		return nil, err
	}
	if available {
		for _, activation := range lease.Activations() {
			if activation.Source == channelonboarding.ActivationSourceDeclared {
				prior[activation.Plan.BindingID()] = activation
			}
		}
		lease.Release()
	}
	return prior, nil
}

func serveDeclaredRegistrationEnabled(ctx context.Context, manager *runtime.RuntimeContextManager, contextDef runtime.BundleContext, binding packs.OutboundBindingPlan) (bool, error) {
	selector := binding.RegistrationTarget()
	if selector == "" {
		return true, nil
	}
	_, enabled, err := findContextTarget(contextDef, selector)
	if err != nil {
		return false, err
	}
	if !enabled {
		declarations, err := runtime.ResolveStandingTargetDeclarations(contextDef.Source, contextDef.ProviderTriggerCatalog)
		if err != nil {
			return false, err
		}
		declared := false
		for _, declaration := range declarations {
			for _, ingress := range declaration.Ingress {
				if selector == fmt.Sprintf("ingress:%s:%s", declaration.FlowPath, ingress.Provider) {
					declared = true
				}
			}
		}
		if !declared {
			return false, fmt.Errorf("channels.bindings.%s.register has no admitted declaration %q", binding.BindingID(), selector)
		}
		// The runtime's admitted target set owns enabledness. Readback and
		// renewal cannot promote a still-declared dormant registration.
		return false, nil
	}
	use, lookup, err := manager.AcquireBundleHash(ctx, contextDef.BundleHash())
	if err != nil {
		return false, err
	}
	if !lookup.Loaded() || use == nil {
		return false, fmt.Errorf("registration source context is no longer loaded")
	}
	err = use.Runtime().ValidateStandingIngressCredentials(use.WorkContext())
	err = errors.Join(err, use.Done())
	return err == nil, err
}

func reconcileServeProviderRegistrations(ctx context.Context, generation runtimepublicingress.Generation, manager *runtime.RuntimeContextManager, store channelonboarding.Store, identities *operatorchannel.Service, credentials *runtimecredentials.SnapshotOwner, controller *runtimepublicingress.ProviderRegistrationController) error {
	snapshot, err := compileServeChannelActivationSnapshot(ctx, manager, store, identities, credentials)
	if err != nil {
		return err
	}
	if err := publishServeChannelActivationSnapshot(ctx, manager, snapshot); err != nil {
		return err
	}
	selection, err := resolveServeRegistrationPairs(snapshot, manager)
	if err != nil {
		return err
	}
	defer selection.Release()
	return controller.Reconcile(ctx, generation, selection.Pairs)
}

func observeChannelActivationAdmissions(ctx context.Context, projection *runtimecredentials.SecretBindingProjection, admissions []channelonboarding.CredentialAdmission) (bool, error) {
	executable := true
	for _, admission := range admissions {
		if err := admission.Validate(); err != nil {
			return false, err
		}
		_, current, err := projection.ObserveAdmittedActivationCredential(ctx,
			runtimecredentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}, admission.Receipt)
		if err != nil {
			return false, err
		}
		executable = executable && current
	}
	return executable, nil
}

func declaredActivationCredentialAdmissions(ctx context.Context, owner *runtimecredentials.SnapshotOwner, _ channelonboarding.ChannelRuntimeContextCoordinate, binding packs.OutboundBindingPlan) ([]channelonboarding.CredentialAdmission, error) {
	keys := binding.CredentialStoreKeys()
	if len(keys) == 0 {
		return nil, nil
	}
	if owner == nil {
		return nil, fmt.Errorf("declared channel activation %q requires credential snapshot ownership", binding.BindingID())
	}
	roles := make([]string, 0, len(keys))
	for role := range keys {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	admissions := make([]channelonboarding.CredentialAdmission, 0, len(roles))
	for _, role := range roles {
		key := strings.TrimSpace(keys[role])
		evidence, err := owner.SealCurrentValue(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("declared channel activation %q credential %q: %w", binding.BindingID(), role, err)
		}
		admissions = append(admissions, channelonboarding.CredentialAdmission{
			Role: role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: evidence.Seal,
		})
	}
	return admissions, nil
}

func declaredActivationCoordinate(contextDef runtime.BundleContext, binding packs.OutboundBindingPlan) (channelonboarding.ChannelRuntimeContextCoordinate, error) {
	bundleHash := contextDef.SourceArtifactFact.BundleHash()
	bundleIdentity := fmt.Sprintf("%s@%s#%s", strings.TrimSpace(contextDef.BundleIdentity.WorkflowName), strings.TrimSpace(contextDef.BundleIdentity.WorkflowVersion), strings.TrimSpace(contextDef.BundleIdentity.BundleHash))
	generation, err := binding.PlanGeneration()
	if err != nil {
		return channelonboarding.ChannelRuntimeContextCoordinate{}, err
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: bundleHash, BundleIdentity: bundleIdentity,
		PackInventoryGeneration: contextDef.PackInventoryDigest, RuntimeInstanceID: contextDef.RuntimeInstanceID,
		ContextPublicationGeneration: contextDef.PublicationGeneration,
		PlanGeneration:               generation,
	}
	if selector := binding.RegistrationTarget(); selector != "" {
		target, err := exactContextTarget(contextDef, selector)
		if err != nil {
			return channelonboarding.ChannelRuntimeContextCoordinate{}, fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err)
		}
		coordinate.TargetGeneration = uint64(target.Generation)
	}
	if err := coordinate.ValidateContext(); err != nil {
		return channelonboarding.ChannelRuntimeContextCoordinate{}, err
	}
	return coordinate, nil
}

type serveChannelActivationRefresher struct {
	manager     *runtime.RuntimeContextManager
	store       channelonboarding.Store
	identities  *operatorchannel.Service
	credentials *runtimecredentials.SnapshotOwner
	ingress     *runtimepublicingress.ReadinessOwner
	preflight   func(context.Context, servePrebindingActivation) error
	reconcile   func(context.Context) error
	testBarrier channelonboarding.TestLifecycleBarrier
}

type serveConnectedChannelRecovery interface {
	Recover(context.Context) error
}

type serveConnectedChannelLocalReconciler interface {
	ReconcileLocal(context.Context) error
}

type serveConnectedChannelContextRetirer interface {
	RetireContext(context.Context, string, uint64, string, string, string) (channelonboarding.TeardownOperation, error)
}

func reconcileRetiredConnectedChannelContexts(ctx context.Context, manager *runtime.RuntimeContextManager, store channelonboarding.Store, retire serveConnectedChannelContextRetirer) error {
	if manager == nil || store == nil || retire == nil {
		return fmt.Errorf("connected channel context retirement reconciliation requires manager, store, and retirement owner")
	}
	catalog, err := serveChannelOnboardingCatalog(manager)
	if err != nil {
		return err
	}
	inventory, err := channelonboarding.InspectRetainedActivations(ctx, store)
	if err != nil {
		return err
	}
	operationByID := make(map[string]channelonboarding.Operation, len(inventory.Operations))
	for _, operation := range inventory.Operations {
		operationByID[operation.OperationID] = operation
	}
	retired := map[string]struct{}{}
	for _, activation := range inventory.Activations {
		operation := operationByID[activation.OperationID]
		if _, current := catalog.FindDurableSuccessor(
			operation.Provider, operation.Interface, operation.Coordinate, operation.TargetSelector, operation.Posture, operation.Ceremony,
		); current {
			continue
		}
		coordinate := activation.Coordinate
		key := coordinate.BundleHash + "\x00" + fmt.Sprint(coordinate.ContextPublicationGeneration)
		if _, alreadyRetired := retired[key]; alreadyRetired {
			continue
		}
		requestKey := operatorchannel.Hash(
			"channel-context-retirement-key-v1", coordinate.BundleHash, fmt.Sprint(coordinate.ContextPublicationGeneration),
		)
		requestHash := operatorchannel.Hash(
			"channel-context-retirement-request-v1", coordinate.BundleHash, fmt.Sprint(coordinate.ContextPublicationGeneration),
		)
		if _, err := retire.RetireContext(
			context.WithoutCancel(ctx), coordinate.BundleHash, coordinate.ContextPublicationGeneration,
			requestKey, requestHash, "runtime_context_retired",
		); err != nil {
			return fmt.Errorf("retire unavailable connected channel runtime context %s: %w", coordinate.BundleHash, err)
		}
		retired[key] = struct{}{}
	}
	return nil
}

func (r *serveChannelActivationRefresher) AdmitChannelTarget(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) (channelonboarding.Candidate, error) {
	if r == nil || r.manager == nil {
		return channelonboarding.Candidate{}, fmt.Errorf("channel target admission requires runtime context ownership")
	}
	if err := r.manager.AdmitChannelStandingTarget(ctx, op, candidate, r.testBarrier); err != nil {
		return channelonboarding.Candidate{}, err
	}
	catalog, err := serveChannelOnboardingCatalog(r.manager)
	if err != nil {
		return channelonboarding.Candidate{}, err
	}
	promoted, found := catalog.FindDurableSuccessor(candidate.Provider, candidate.Interface, candidate.Coordinate, candidate.Target.Selector, candidate.Posture, candidate.Ceremony)
	if !found {
		return channelonboarding.Candidate{}, fmt.Errorf("admitted channel target has no exact declaration owner")
	}
	return promoted, promoted.Validate()
}

func (r *serveChannelActivationRefresher) PreflightChannelActivation(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	if r == nil {
		return fmt.Errorf("serve channel activation refresher is required")
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if op.Provider != candidate.Provider || op.Interface.Normalized() != candidate.Interface.Normalized() ||
		!op.Coordinate.Matches(candidate.Coordinate) || op.TargetSelector != candidate.Target.Selector || op.Posture != candidate.Posture {
		return fmt.Errorf("channel activation preflight candidate contradicts its exact onboarding operation")
	}
	if r.preflight == nil {
		return nil
	}
	err := r.preflight(ctx, servePrebindingActivation{Operation: op, Candidate: candidate})
	var collision *runtimepublicingress.SlotCollisionError
	if errors.As(err, &collision) {
		return channelonboarding.NewTerminalActivationError("provider_slot_collision", err)
	}
	var rejected *runtimeregistration.ProviderCredentialRejectedError
	if !errors.As(err, &rejected) {
		return err
	}
	storeKey := ""
	for _, admission := range op.CredentialAdmissions {
		if admission.Role == candidate.ProviderCredentialRole {
			storeKey = admission.StoreKey
			break
		}
	}
	if storeKey == "" {
		return fmt.Errorf("provider rejected a channel credential without an exact admitted occurrence: %w", err)
	}
	return errors.Join(&channelonboarding.CredentialRequiredError{
		OperationID: op.OperationID, Role: candidate.ProviderCredentialRole, StoreKey: storeKey,
	}, err)
}

func activateServeAfterConnectedChannelTeardownRecovery(ctx context.Context, teardown serveConnectedChannelRecovery, activate func() error, onboarding serveConnectedChannelLocalReconciler) error {
	if teardown == nil || onboarding == nil || activate == nil {
		return fmt.Errorf("connected channel recovery requires teardown, onboarding, and activation owners")
	}
	if err := teardown.Recover(ctx); err != nil {
		return fmt.Errorf("recover connected channel teardown: %w", err)
	}
	if err := activate(); err != nil {
		return fmt.Errorf("activate serve runtime after channel teardown recovery: %w", err)
	}
	if err := onboarding.ReconcileLocal(ctx); err != nil {
		return fmt.Errorf("reconcile local connected channel onboarding against loaded runtime contexts: %w", err)
	}
	return nil
}

type serveConnectedChannelReadiness struct {
	manager     *runtime.RuntimeContextManager
	store       channelonboarding.Store
	native      channelnative.Store
	identities  *operatorchannel.Service
	credentials *runtimecredentials.SnapshotOwner
	effects     runtimeeffects.OutcomeStore
	ingress     *runtimepublicingress.ReadinessOwner
	now         func() time.Time
}

func (o *serveConnectedChannelReadiness) ProjectConnectedChannelReadiness(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) (channelonboarding.ConnectedChannelReadiness, bool, error) {
	if o == nil || o.manager == nil || o.store == nil || o.identities == nil || o.credentials == nil || o.effects == nil || o.ingress == nil {
		return channelonboarding.ConnectedChannelReadiness{}, false, fmt.Errorf("connected channel readiness dependencies are incomplete")
	}
	activation, err := o.store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		if errors.Is(err, channelonboarding.ErrNotFound) {
			return channelonboarding.ProjectReadiness(channelonboarding.ReadinessFacts{
				Coordinate: op.Coordinate, Interface: op.Interface, PlanGeneration: op.Coordinate.PlanGeneration,
				Posture: op.Posture, ObservedAt: o.observedAt(),
			}), true, nil
		}
		return channelonboarding.ConnectedChannelReadiness{}, false, err
	}
	now := o.observedAt()
	facts := channelonboarding.ReadinessFacts{
		Coordinate: activation.Coordinate, Interface: activation.Interface, ActivationRevision: activation.Revision,
		PlanGeneration: activation.Coordinate.PlanGeneration, ActivationCurrent: activation.Status == channelonboarding.ActivationCurrent && activation.OperationID == op.OperationID && activation.Revision == op.ActivationRevision,
		BindingRevision: activation.BindingRevision, ExpectedBindingRevision: activation.BindingRevision,
		ProofID: activation.ProofID, ProofRevision: activation.ProofRevision, ExpectedProofRevision: activation.ProofRevision,
		Posture: activation.Posture, TargetGeneration: activation.Coordinate.TargetGeneration,
		ExpectedTargetGeneration: candidate.Target.Generation, ObservedAt: now,
	}

	planID := channelonboarding.LearnedBindingID(activation.SlotKey)
	publicationLease, available, publicationErr := o.manager.AcquireChannelActivationPublication(activation.Coordinate.BundleHash, activation.Coordinate.ContextPublicationGeneration)
	if publicationErr != nil {
		return channelonboarding.ConnectedChannelReadiness{}, false, publicationErr
	}
	if available {
		defer publicationLease.Release()
		facts.ActivationGeneration = publicationLease.Generation()
	}
	planCurrent := false
	nativeRequired := false
	if available {
		for _, compiled := range publicationLease.Activations() {
			if compiled.Plan.BindingID() == planID && compiled.Coordinate.Matches(activation.Coordinate) && compiled.ActivationRevision == activation.Revision {
				planCurrent = true
				nativeRequired = compiled.Plan.HasNativeInbox()
				break
			}
		}
	}
	if !planCurrent {
		facts.PlanGeneration = plangeneration.Generation{}
	}

	binding, proofCurrent, bindingErr := o.identities.CurrentBindingReadiness(ctx, activation.Interface)
	if bindingErr != nil {
		if !errors.Is(bindingErr, operatorchannel.ErrNotFound) && !errors.Is(bindingErr, operatorchannel.ErrCredentialStale) {
			return channelonboarding.ConnectedChannelReadiness{}, false, bindingErr
		}
		facts.ExpectedBindingRevision = 0
	} else {
		facts.ExpectedBindingRevision = binding.Revision
		facts.ProofCurrent = proofCurrent
		facts.ExpectedProofRevision = binding.ProofRevision
	}

	credentialProjection := o.credentials.BeginSecretBindingProjection()
	facts.CredentialsCurrent, err = observeChannelActivationAdmissions(ctx, credentialProjection, activation.CredentialAdmissions)
	if err != nil {
		return channelonboarding.ConnectedChannelReadiness{}, false, err
	}
	if err := credentialProjection.ValidateCurrent(ctx); err != nil {
		return channelonboarding.ConnectedChannelReadiness{}, false, err
	}

	confirmationID := strings.TrimSpace(op.ConfirmationOperationID)
	if confirmationID != "" {
		outcome, found, outcomeErr := o.effects.GetExternalEffectOutcome(ctx, confirmationID)
		if outcomeErr != nil {
			return channelonboarding.ConnectedChannelReadiness{}, false, outcomeErr
		}
		facts.ConfirmationTerminalSuccess = found && outcome.Kind == runtimeeffects.KindChannelConfirmation && outcome.AuthorityKind == runtimeeffects.AuthorityChannelConfirmation && outcome.AuthorityID == confirmationID && outcome.TerminalSuccess()
	}
	facts.ConfirmationActivationRevision = op.ActivationRevision
	facts.ConfirmationBindingRevision = op.BindingRevision

	switch activation.Posture {
	case channelonboarding.ActivationWebhookRegistration:
		registration, found := o.ingress.ChannelRegistrationCurrent(ctx, now, planID, activation.TargetSelector, activation.Provider)
		if found && registration.Exposure != nil {
			facts.ExposureGeneration = registration.Exposure.GenerationID
			facts.ExpectedExposureGeneration = registration.Exposure.GenerationID
			facts.RegistrationActivationGeneration = registration.ActivationGeneration
			facts.RegistrationCurrent = registration.Current && registration.ActivationGeneration.Equal(facts.ActivationGeneration)
		}
	case channelonboarding.ActivationSessionConnection:
		return channelonboarding.ConnectedChannelReadiness{Reason: channelonboarding.ReadinessSessionUnavailable, Coordinate: activation.Coordinate, ObservedAt: now}, true, nil
	}
	projection := channelonboarding.ProjectReadiness(facts)
	projection.NativeInboxRequired = nativeRequired
	if !nativeRequired {
		return projection, true, nil
	}
	if o.native == nil {
		return projection, false, fmt.Errorf("native inbox qualification readback owner is unavailable")
	}
	qualification, err := o.native.ReadNativeInboxQualification(ctx, activation.ActivationID)
	if err != nil {
		return projection, false, err
	}
	projection.NativeInbox = &qualification
	return projection, true, nil
}

func (o *serveConnectedChannelReadiness) observedAt() time.Time {
	if o != nil && o.now != nil {
		return o.now().UTC()
	}
	return time.Now().UTC()
}

type channelConfirmationEffectStore interface {
	runtimeeffects.Store
	runtimeeffects.OutcomeStore
	runtimeeffects.ChannelOnboardingOutcomeStore
}

type serveChannelConfirmationDispatcher struct {
	effects              channelConfirmationEffectStore
	credentials          *runtimecredentials.SnapshotOwner
	posture              executionposture.Posture
	runtimeInstanceID    string
	httpClient           *http.Client
	sessionChannelWrites map[string]sessionexecution.Channel
	now                  func() time.Time
}

func newServeChannelConfirmationDispatcher(
	effects channelConfirmationEffectStore,
	credentials *runtimecredentials.SnapshotOwner,
	posture executionposture.Posture,
	runtimeInstanceID string,
	httpClient *http.Client,
) (*serveChannelConfirmationDispatcher, error) {
	if effects == nil || credentials == nil || !posture.Valid() || strings.TrimSpace(runtimeInstanceID) == "" {
		return nil, fmt.Errorf("channel confirmation requires effect, credential, posture, and runtime owners")
	}
	return &serveChannelConfirmationDispatcher{
		effects: effects, credentials: credentials, posture: posture,
		runtimeInstanceID: strings.TrimSpace(runtimeInstanceID), httpClient: httpClient,
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (d *serveChannelConfirmationDispatcher) ReconcileChannelEffectsBeforeRebind(ctx context.Context, op channelonboarding.Operation) (channelonboarding.EffectRebindDisposition, error) {
	disposition := channelonboarding.EffectRebindDisposition{RetryAllowed: true}
	if d == nil || d.effects == nil {
		return disposition, fmt.Errorf("channel effect reconciliation owner is unavailable")
	}
	outcomes, err := d.effects.ReconcileChannelOnboardingEffectOutcomes(ctx, op.OperationID, d.now().UTC())
	if err != nil {
		return disposition, err
	}
	return classifyChannelEffectsBeforeRebind(op, outcomes)
}

func classifyChannelEffectsBeforeRebind(op channelonboarding.Operation, outcomes []runtimeeffects.ChannelOnboardingEffectOutcome) (channelonboarding.EffectRebindDisposition, error) {
	disposition := channelonboarding.EffectRebindDisposition{RetryAllowed: true}
	coordinate := op.Coordinate.Normalized()
	confirmationID := strings.TrimSpace(op.ConfirmationOperationID)
	confirmationFound := false
	for _, outcome := range outcomes {
		if err := validateChannelEffectProvenance(op, outcome); err != nil {
			return disposition, err
		}
		currentOccurrence := outcome.BundleHash == coordinate.BundleHash &&
			outcome.BundleIdentity == coordinate.BundleIdentity &&
			outcome.PackInventoryGeneration == coordinate.PackInventoryGeneration &&
			outcome.RuntimeInstanceID == coordinate.RuntimeInstanceID &&
			outcome.ContextPublicationGeneration == coordinate.ContextPublicationGeneration &&
			outcome.PlanGeneration.Equal(coordinate.PlanGeneration) &&
			outcome.TargetGeneration == coordinate.TargetGeneration
		if !currentOccurrence && !channelEffectTerminal(outcome) {
			return disposition, fmt.Errorf("channel effect %s retains nonterminal authority for a historical runtime occurrence", outcome.OperationID)
		}
		if outcome.AuthorityKind == runtimeeffects.AuthorityChannelConfirmation && outcome.OperationID == confirmationID {
			if confirmationFound {
				return disposition, fmt.Errorf("channel confirmation %s has duplicate durable outcomes", confirmationID)
			}
			confirmationFound = true
		}
		if outcome.TerminalSuccess() {
			continue
		}
		if outcome.State == runtimeeffects.StateTerminalFailure && outcome.AttemptState == runtimeeffects.StateTerminalFailure && !outcome.Launched {
			if outcome.AuthorityKind == runtimeeffects.AuthorityChannelConfirmation && outcome.OperationID == confirmationID {
				disposition.RemintConfirmationOperation = true
			}
			continue
		}
		disposition.RetryAllowed = false
		disposition.BlockingEffectOperationID = outcome.OperationID
		disposition.BlockingEffectState = string(outcome.AttemptState)
		if outcome.State != outcome.AttemptState {
			disposition.BlockingEffectState = string(outcome.State) + "/" + string(outcome.AttemptState)
		}
		return disposition, nil
	}
	if confirmationID != "" && !confirmationFound {
		disposition.RemintConfirmationOperation = true
	}
	return disposition, nil
}

func validateChannelEffectProvenance(op channelonboarding.Operation, outcome runtimeeffects.ChannelOnboardingEffectOutcome) error {
	if outcome.OperationID == "" || outcome.AuthorityID == "" ||
		outcome.OnboardingOperationID != op.OperationID || outcome.OnboardingRevision < 1 || outcome.OnboardingRevision > op.Revision {
		return fmt.Errorf("channel effect journal identity contradicts onboarding operation %s", op.OperationID)
	}
	if outcome.BundleHash == "" || outcome.BundleIdentity == "" ||
		outcome.PackInventoryGeneration == "" || outcome.RuntimeInstanceID == "" ||
		outcome.ContextPublicationGeneration == 0 || !outcome.PlanGeneration.Valid() || outcome.TargetGeneration == 0 {
		return fmt.Errorf("channel effect %s lacks exact runtime occurrence provenance", outcome.OperationID)
	}
	switch outcome.AuthorityKind {
	case runtimeeffects.AuthorityServeRegistration:
		if outcome.Kind != runtimeeffects.KindServeRegistration {
			return fmt.Errorf("channel registration %s has contradictory effect kind %s", outcome.OperationID, outcome.Kind)
		}
	case runtimeeffects.AuthorityChannelConfirmation:
		if outcome.Kind != runtimeeffects.KindChannelConfirmation || outcome.OperationID != outcome.AuthorityID {
			return fmt.Errorf("channel confirmation %s has contradictory effect kind %s", outcome.OperationID, outcome.Kind)
		}
	default:
		return fmt.Errorf("channel effect %s has unsupported authority %s", outcome.OperationID, outcome.AuthorityKind)
	}
	return nil
}

func channelEffectTerminal(outcome runtimeeffects.ChannelOnboardingEffectOutcome) bool {
	return outcome.TerminalSuccess() ||
		(outcome.State == runtimeeffects.StateTerminalFailure && outcome.AttemptState == runtimeeffects.StateTerminalFailure) ||
		(outcome.State == runtimeeffects.StateOutcomeUncertain && outcome.AttemptState == runtimeeffects.StateOutcomeUncertain)
}

func (d *serveChannelConfirmationDispatcher) DispatchChannelConfirmation(ctx context.Context, request channelonboarding.ConfirmationRequest) (channelonboarding.ConfirmationResult, error) {
	if err := request.Candidate.Plan.RequireExecutableProvider(); err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}
	if d == nil || d.effects == nil || d.credentials == nil {
		return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation dispatcher is unavailable")
	}
	op, activation, binding := request.Operation, request.Activation, request.Binding
	operationID := strings.TrimSpace(op.ConfirmationOperationID)
	if operationID == "" || op.Phase != channelonboarding.PhaseDeliveringConfirmation ||
		activation.Status != channelonboarding.ActivationCurrent || activation.OperationID != op.OperationID ||
		activation.Revision != op.ActivationRevision || activation.BindingRevision != op.BindingRevision ||
		binding.Status != operatorchannel.BindingCurrent || binding.Revision != op.BindingRevision {
		return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation request is not exact-current")
	}
	compiled, err := channelonboarding.CompileLearnedActivation(request.Candidate, activation)
	if err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}
	if compiled.ActivationRevision != activation.Revision || !compiled.Coordinate.Matches(op.Coordinate) {
		return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation compiled activation contradicts onboarding responsibility")
	}
	if outcome, found, err := d.effects.GetExternalEffectOutcome(ctx, operationID); err != nil {
		return channelonboarding.ConfirmationResult{}, err
	} else if found {
		if outcome.OperationID != operationID || outcome.Kind != runtimeeffects.KindChannelConfirmation ||
			outcome.AuthorityKind != runtimeeffects.AuthorityChannelConfirmation || outcome.AuthorityID != operationID {
			return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation journal identity is contradictory")
		}
		if outcome.TerminalSuccess() {
			return channelonboarding.ConfirmationResult{OperationID: operationID, TerminalSuccess: true}, nil
		}
		switch outcome.AttemptState {
		case runtimeeffects.StateAuthorized, runtimeeffects.StateTerminalFailure:
			// Authorized work has not launched; terminal prelaunch failure may
			// admit a new ordinal under the journal's exact retry rules.
		default:
			return channelonboarding.ConfirmationResult{OperationID: operationID}, fmt.Errorf("channel confirmation %s is %s; refusing blind resend", operationID, outcome.AttemptState)
		}
	}

	confirmationText := "Swarm channel connected."
	if binding.ConversationScope == operatorchannel.ConversationScopeShared {
		confirmationText += " Future notices, decision cards, and updates sent here will be visible to this group."
	}
	publicInput := map[string]any{
		"presentation": map[string]any{"text": confirmationText},
	}
	if compiled.Plan.Capabilities().Vector().ActionsAsButtons {
		publicInput["actions"] = []any{}
	}
	_, providerInput, err := compiled.Plan.PrepareOperation(request.Candidate.ConfirmationOperation, publicInput)
	if err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}
	toolID, tool, err := request.Candidate.Plan.ConnectorOperation(request.Candidate.ConfirmationOperation)
	if err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}
	credentials, err := resolveChannelDeliveryCredentials(ctx, d.credentials, compiled.Plan, activation.CredentialAdmissions, tool)
	if err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}

	now := d.now().UTC()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityChannelConfirmation, ID: operationID,
		ExecutionOwner: "channel-onboarding:" + op.OperationID, LeaseExpiresAt: now.Add(5 * time.Minute),
		FenceGeneration: op.Coordinate.ContextPublicationGeneration,
		ExecutionMode:   runtimeeffects.ExecutionMode(d.posture.RootMode()),
		ChannelConfirmation: runtimeeffects.ChannelConfirmationAuthority{
			EffectOperationID: operationID, OnboardingOperationID: op.OperationID, OnboardingRevision: op.Revision,
			ActivationID: activation.ActivationID, ActivationRevision: activation.Revision, BindingRevision: binding.Revision,
			PrincipalID: binding.PrincipalID, BundleHash: op.Coordinate.BundleHash,
			BundleIdentity: op.Coordinate.BundleIdentity, PackInventoryGeneration: op.Coordinate.PackInventoryGeneration,
			RuntimeInstanceID: op.Coordinate.RuntimeInstanceID, ContextPublicationGeneration: op.Coordinate.ContextPublicationGeneration,
			PlanGeneration: op.Coordinate.PlanGeneration, TargetGeneration: op.Coordinate.TargetGeneration,
		},
	}
	if !authority.Valid() {
		return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation authority is invalid")
	}
	effectCtx := runtimeeffects.WithExecutionMode(ctx, authority.ExecutionMode)
	effectCtx = runtimeeffects.WithController(effectCtx, runtimeeffects.NewController(d.effects).WithExecutionPosture(d.posture))
	effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(d.runtimeInstanceID, op.Coordinate.BundleHash))
	delivery, err := executeChannelWrite(
		effectCtx, activation.OperationID, request.Candidate.ConfirmationOperation, toolID, tool, providerInput, credentials,
		map[string]string{
			"onboarding_operation_id": op.OperationID,
			"activation_id":           activation.ActivationID,
			"binding_revision":        fmt.Sprint(binding.Revision),
			"bundle_hash":             op.Coordinate.BundleHash,
			"plan_generation":         op.Coordinate.PlanGeneration.Diagnostic(),
		},
		d.sessionChannelWrites[activation.OperationID],
		channelCredentialHTTPExecutor(d.httpClient, d.credentials, compiled.Plan, activation.CredentialAdmissions, tool),
	)
	if err != nil {
		return channelonboarding.ConfirmationResult{OperationID: operationID}, err
	}
	if delivery.OperationID != operationID {
		return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation journal returned contradictory operation %q", delivery.OperationID)
	}
	return channelonboarding.ConfirmationResult{OperationID: operationID, TerminalSuccess: true}, nil
}

func (r *serveChannelActivationRefresher) RefreshChannelActivations(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("serve channel activation refresher is required")
	}
	if err := r.publishChannelActivations(ctx); err != nil {
		return err
	}
	return r.reconcileChannelRegistrations(ctx)
}

func (r *serveChannelActivationRefresher) RefreshChannelActivationCandidates(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("serve channel activation refresher is required")
	}
	return r.RefreshChannelActivations(ctx)
}

func (r *serveChannelActivationRefresher) PublishChannelActivation(ctx context.Context, op channelonboarding.Operation, activation channelonboarding.ConnectedChannelActivation) error {
	if r == nil {
		return fmt.Errorf("serve channel activation refresher is required")
	}
	if op.Phase != channelonboarding.PhasePublishingProcessActivation || !exactChannelActivationHandoff(op, activation) {
		return fmt.Errorf("successor channel activation process publication request is contradictory")
	}
	if err := r.publishChannelActivations(ctx); err != nil {
		return err
	}
	lease, current, err := r.manager.AcquireChannelActivationPublication(activation.Coordinate.BundleHash, activation.Coordinate.ContextPublicationGeneration)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("successor channel activation process publication is unavailable")
	}
	defer lease.Release()
	wantBinding := channelonboarding.LearnedBindingID(activation.SlotKey)
	for _, compiled := range lease.Activations() {
		if compiled.Source == channelonboarding.ActivationSourceLearned && compiled.Plan.BindingID() == wantBinding &&
			compiled.ActivationRevision == activation.Revision && compiled.Coordinate.Matches(activation.Coordinate) {
			return nil
		}
	}
	return fmt.Errorf("successor channel activation %s is absent from exact process publication %s", op.OperationID, lease.Generation().Diagnostic())
}

func (r *serveChannelActivationRefresher) PromoteChannelRegistration(ctx context.Context, op channelonboarding.Operation, activation channelonboarding.ConnectedChannelActivation) error {
	if r == nil {
		return fmt.Errorf("serve channel activation refresher is required")
	}
	if op.Phase != channelonboarding.PhasePromotingRegistration || !exactChannelActivationHandoff(op, activation) {
		return fmt.Errorf("successor channel registration promotion request is contradictory")
	}
	if err := r.reconcileChannelRegistrations(ctx); err != nil {
		return err
	}
	if activation.Posture != channelonboarding.ActivationWebhookRegistration {
		return nil
	}
	if r.ingress == nil {
		return fmt.Errorf("webhook channel activation registration owner is unavailable")
	}
	lease, current, err := r.manager.AcquireChannelActivationPublication(activation.Coordinate.BundleHash, activation.Coordinate.ContextPublicationGeneration)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("successor channel activation process publication is unavailable during registration promotion")
	}
	defer lease.Release()
	registration, found := r.ingress.ChannelRegistrationCurrent(ctx, time.Now().UTC(), channelonboarding.LearnedBindingID(activation.SlotKey), activation.TargetSelector, activation.Provider)
	if !found || !registration.Current || !registration.ActivationGeneration.Equal(lease.Generation()) {
		return fmt.Errorf("successor channel registration is not current for exact process publication")
	}
	return nil
}

func exactChannelActivationHandoff(op channelonboarding.Operation, activation channelonboarding.ConnectedChannelActivation) bool {
	return activation.Status == channelonboarding.ActivationCurrent &&
		activation.OperationID == op.OperationID && activation.SlotKey == op.SlotKey &&
		activation.Revision == op.ActivationRevision && activation.BindingRevision == op.BindingRevision &&
		activation.Coordinate.Matches(op.Coordinate)
}

func (r *serveChannelActivationRefresher) reconcileChannelRegistrations(ctx context.Context) error {
	if r.reconcile != nil {
		err := r.reconcile(ctx)
		var collision *runtimepublicingress.SlotCollisionError
		if errors.As(err, &collision) {
			return channelonboarding.NewTerminalActivationError("provider_slot_collision", err)
		}
		var failure *runtimefailures.Error
		if errors.As(err, &failure) && failure.Failure.Detail.Code == "mock_external_effect_forbidden" {
			return channelonboarding.NewTerminalActivationError(failure.Failure.Detail.Code, err)
		}
		return err
	}
	return nil
}

func (r *serveChannelActivationRefresher) publishChannelActivations(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("serve channel activation refresher is required")
	}
	snapshot, err := compileServeChannelActivationSnapshot(ctx, r.manager, r.store, r.identities, r.credentials)
	if err != nil {
		return err
	}
	return publishServeChannelActivationSnapshot(ctx, r.manager, snapshot)
}

func publishServeChannelActivationSnapshot(ctx context.Context, manager *runtime.RuntimeContextManager, snapshot serveChannelActivationSnapshot) error {
	byContext := map[string][]channelonboarding.CompiledActivation{}
	coordinates := map[string]channelonboarding.ChannelRuntimeContextCoordinate{}
	for _, activation := range snapshot.Activations {
		key := activation.Coordinate.BundleHash + "\x00" + activation.Coordinate.RuntimeInstanceID + "\x00" + fmt.Sprint(activation.Coordinate.ContextPublicationGeneration)
		byContext[key] = append(byContext[key], activation)
		coordinates[key] = activation.Coordinate
	}
	for _, contextDef := range manager.LoadedContexts() {
		bundleHash := contextDef.SourceArtifactFact.BundleHash()
		key := bundleHash + "\x00" + contextDef.RuntimeInstanceID + "\x00" + fmt.Sprint(contextDef.PublicationGeneration)
		if coordinate, found := coordinates[key]; found && !coordinate.MatchesContextOccurrence(contextDef.RuntimeInstanceID, contextDef.PublicationGeneration) {
			return fmt.Errorf("channel activation runtime publication changed during refresh")
		}
		publication, err := channelonboarding.NewChannelActivationPublication(byContext[key])
		if err != nil {
			return err
		}
		if err := manager.ReplaceChannelActivationsContext(ctx, bundleHash, contextDef.PublicationGeneration, publication); err != nil {
			return err
		}
	}
	return nil
}
