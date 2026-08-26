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
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
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
		bundleHash, bundleSource := contextDef.BundleSourceFact.StorageValues()
		bundleIdentity := fmt.Sprintf("%s@%s#%s", strings.TrimSpace(contextDef.BundleIdentity.WorkflowName), strings.TrimSpace(contextDef.BundleIdentity.WorkflowVersion), strings.TrimSpace(contextDef.BundleIdentity.BundleHash))
		if strings.Trim(bundleIdentity, "@#") == "" {
			return nil, fmt.Errorf("runtime context %s has no exact bundle identity", bundleHash)
		}
		for _, plan := range contextDef.ChannelPlans {
			profile, ok := plan.OnboardingProfile()
			if !ok {
				continue
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
			if posture == channelonboarding.ActivationSessionConnection {
				// Session candidates become live only when #2341 projects an exact
				// service-fulfillment target into the runtime context.
				continue
			}
			for _, target := range contextDef.StandingTargets {
				if strings.TrimSpace(target.Provider) != profile.Provider() || target.Generation <= 0 {
					continue
				}
				selector := fmt.Sprintf("ingress:%s:%s:%s", target.PackageKey, target.FlowID, target.Provider)
				coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
					BundleHash: bundleHash, BundleSource: bundleSource, BundleIdentity: bundleIdentity,
					PackInventoryGeneration:      contextDef.PackInventoryDigest,
					ContextPublicationGeneration: contextDef.PublicationGeneration,
					PlanGeneration:               generation, TargetGeneration: uint64(target.Generation),
				}
				candidates = append(candidates, channelonboarding.Candidate{
					Provider: profile.Provider(), Interface: identity, Coordinate: coordinate,
					Target: channelonboarding.CandidateTarget{
						Selector: selector, ServiceID: target.ServiceID, PackageKey: target.PackageKey, FlowID: target.FlowID,
						Alias: target.Alias, Provider: target.Provider, Generation: uint64(target.Generation), PublicationSequence: target.PublicationSequence,
						AdmissionGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: target.SigningSecret,
					},
					Posture: posture, Ceremony: ceremony,
					ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential(),
					ConfirmationOperation: profile.ConfirmationOperation(), ConnectionHealth: profile.ConnectionHealth(), Plan: plan,
				})
			}
		}
	}
	return channelonboarding.NewCandidateCatalog(candidates)
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
	for _, contextDef := range manager.LoadedContexts() {
		for _, binding := range channelOnboardingDeclaredPlans(contextDef) {
			coordinate, err := declaredActivationCoordinate(contextDef, binding)
			if err != nil {
				return serveChannelActivationSnapshot{}, err
			}
			admissions, err := declaredActivationCredentialAdmissions(ctx, credentials, coordinate, binding)
			if err != nil {
				return serveChannelActivationSnapshot{}, err
			}
			declared = append(declared, channelonboarding.CompiledActivation{
				Source: channelonboarding.ActivationSourceDeclared, Coordinate: coordinate,
				Plan: binding, CredentialAdmissions: admissions,
			})
		}
	}
	learned := []channelonboarding.CompiledActivation{}
	prebinding := []servePrebindingActivation{}
	currentLearnedSlots := map[string]struct{}{}
	if store != nil || identities != nil {
		if store == nil || identities == nil {
			return serveChannelActivationSnapshot{}, fmt.Errorf("learned channel activations require both selected store and identity owner")
		}
		catalog, err := serveChannelOnboardingCatalog(manager)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		activations, err := store.ListCurrentConnectedChannelActivations(ctx)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		for _, activation := range activations {
			candidate, current := catalog.FindExact(activation.Provider, activation.Interface, activation.Coordinate, activation.TargetSelector)
			if !current {
				// Historical selected-store rows remain readable but cannot grant
				// execution or registration authority to a successor publication.
				continue
			}
			binding, err := identities.CurrentBinding(ctx, activation.Interface)
			if err != nil {
				return serveChannelActivationSnapshot{}, err
			}
			compiled, err := channelonboarding.CompileLearnedActivation(candidate, activation, binding)
			if err != nil {
				return serveChannelActivationSnapshot{}, err
			}
			learned = append(learned, compiled)
			currentLearnedSlots[activation.SlotKey] = struct{}{}
		}
		operations, err := store.ListChannelOnboardingOperations(ctx)
		if err != nil {
			return serveChannelActivationSnapshot{}, err
		}
		for _, op := range operations {
			if op.Phase != channelonboarding.PhaseActivatingProvider && op.Phase != channelonboarding.PhaseAwaitingExternalIdentity && op.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation && op.Phase != channelonboarding.PhasePublishingActivation {
				continue
			}
			candidate, current := catalog.FindExact(op.Provider, op.Interface, op.Coordinate, op.TargetSelector)
			if !current {
				continue
			}
			if _, activated := currentLearnedSlots[op.SlotKey]; activated {
				continue
			}
			prebinding = append(prebinding, servePrebindingActivation{Operation: op, Candidate: candidate})
		}
	}
	merged, err := channelonboarding.MergeCompiledActivations(declared, learned)
	if err != nil {
		return serveChannelActivationSnapshot{}, err
	}
	sort.Slice(prebinding, func(i, j int) bool { return prebinding[i].Operation.SlotKey < prebinding[j].Operation.SlotKey })
	return serveChannelActivationSnapshot{Activations: merged, Prebinding: prebinding}, nil
}

func declaredActivationCredentialAdmissions(ctx context.Context, owner *runtimecredentials.SnapshotOwner, coordinate channelonboarding.ChannelRuntimeContextCoordinate, binding packs.OutboundBindingPlan) ([]channelonboarding.CredentialAdmission, error) {
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
		bindingSnapshot, err := owner.ObserveSecretBinding(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("declared channel activation %q credential %q: %w", binding.BindingID(), role, err)
		}
		if !bindingSnapshot.Bound() {
			return nil, fmt.Errorf("declared channel activation %q credential %q is not usable", binding.BindingID(), role)
		}
		epoch := bindingSnapshot.Epoch()
		admissions = append(admissions, channelonboarding.CredentialAdmission{
			Role: role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionObserved,
			Receipt: operatorchannel.Hash(
				"declared-channel-credential-observation-v1", coordinate.BundleHash,
				fmt.Sprint(coordinate.ContextPublicationGeneration), binding.BindingID(), role, epoch,
			),
			Epoch: epoch,
		})
	}
	return admissions, nil
}

func declaredActivationCoordinate(contextDef runtime.BundleContext, binding packs.OutboundBindingPlan) (channelonboarding.ChannelRuntimeContextCoordinate, error) {
	bundleHash, bundleSource := contextDef.BundleSourceFact.StorageValues()
	bundleIdentity := fmt.Sprintf("%s@%s#%s", strings.TrimSpace(contextDef.BundleIdentity.WorkflowName), strings.TrimSpace(contextDef.BundleIdentity.WorkflowVersion), strings.TrimSpace(contextDef.BundleIdentity.BundleHash))
	generation, err := binding.PlanGeneration()
	if err != nil {
		return channelonboarding.ChannelRuntimeContextCoordinate{}, err
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: bundleHash, BundleSource: bundleSource, BundleIdentity: bundleIdentity,
		PackInventoryGeneration: contextDef.PackInventoryDigest, ContextPublicationGeneration: contextDef.PublicationGeneration,
		PlanGeneration: generation,
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
	reconcile   func(context.Context) error
}

type serveConnectedChannelRecovery interface {
	Recover(context.Context) error
}

func recoverServeConnectedChannelLifecycle(ctx context.Context, teardown, onboarding serveConnectedChannelRecovery) error {
	if teardown == nil || onboarding == nil {
		return fmt.Errorf("connected channel recovery requires teardown and onboarding owners")
	}
	if err := teardown.Recover(ctx); err != nil {
		return fmt.Errorf("recover connected channel teardown: %w", err)
	}
	if err := onboarding.Recover(ctx); err != nil {
		return fmt.Errorf("recover connected channel onboarding: %w", err)
	}
	return nil
}

type serveConnectedChannelReadiness struct {
	manager     *runtime.RuntimeContextManager
	store       channelonboarding.Store
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
	if available {
		for _, compiled := range publicationLease.Activations() {
			if compiled.Plan.BindingID() == planID && compiled.Coordinate.Matches(activation.Coordinate) && compiled.ActivationRevision == activation.Revision {
				planCurrent = true
				break
			}
		}
	}
	if !planCurrent {
		facts.PlanGeneration = plangeneration.Generation{}
	}

	binding, proofCurrent, bindingErr := o.identities.CurrentBindingReadiness(ctx, activation.Interface)
	if bindingErr != nil {
		if !errors.Is(bindingErr, operatorchannel.ErrNotFound) {
			return channelonboarding.ConnectedChannelReadiness{}, false, bindingErr
		}
		facts.ExpectedBindingRevision = 0
	} else {
		facts.ExpectedBindingRevision = binding.Revision
		facts.ProofCurrent = proofCurrent
		facts.ExpectedProofRevision = binding.ProofRevision
	}

	facts.CredentialsCurrent = true
	for _, admission := range activation.CredentialAdmissions {
		observed, observeErr := o.credentials.Observe(ctx, admission.StoreKey)
		if observeErr != nil || !observed.Present || observed.Epoch() != admission.Epoch {
			facts.CredentialsCurrent = false
			break
		}
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
		facts.ServiceFulfillmentGeneration = candidate.ConnectionHealth
		facts.ExpectedServiceGeneration = candidate.ConnectionHealth
		facts.SessionCurrent = false
	}
	return channelonboarding.ProjectReadiness(facts), true, nil
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
}

type serveChannelConfirmationDispatcher struct {
	effects           channelConfirmationEffectStore
	credentials       *runtimecredentials.SnapshotOwner
	posture           executionposture.Posture
	runtimeInstanceID string
	httpClient        *http.Client
	now               func() time.Time
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

func (d *serveChannelConfirmationDispatcher) DispatchChannelConfirmation(ctx context.Context, request channelonboarding.ConfirmationRequest) (channelonboarding.ConfirmationResult, error) {
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
	compiled, err := channelonboarding.CompileLearnedActivation(request.Candidate, activation, binding)
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

	publicInput := map[string]any{
		"presentation": map[string]any{"text": "Swarm channel connected."},
		"actions":      []any{},
	}
	_, providerInput, err := compiled.Plan.PrepareOperation(request.Candidate.ConfirmationOperation, publicInput)
	if err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}
	toolID, tool, err := request.Candidate.Plan.ConnectorOperation(request.Candidate.ConfirmationOperation)
	if err != nil {
		return channelonboarding.ConfirmationResult{}, err
	}
	credentialKeys := compiled.Plan.CredentialStoreKeys()
	admissions := make(map[string]channelonboarding.CredentialAdmission, len(activation.CredentialAdmissions))
	for _, admission := range activation.CredentialAdmissions {
		if err := admission.Validate(); err != nil {
			return channelonboarding.ConfirmationResult{}, err
		}
		admissions[admission.Role] = admission
	}
	credentials := make(map[string]any, len(tool.Credentials()))
	for _, logical := range tool.Credentials() {
		storeKey := strings.TrimSpace(credentialKeys[logical])
		admission, ok := admissions[logical]
		if !ok || storeKey == "" || admission.StoreKey != storeKey {
			return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation credential role %q is not admitted by the current activation", logical)
		}
		observed, err := d.credentials.ObserveSecretBinding(ctx, storeKey)
		if err != nil {
			return channelonboarding.ConfirmationResult{}, err
		}
		if !observed.Bound() || observed.Epoch() != admission.Epoch {
			return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation credential role %q is no longer current", logical)
		}
		credentials[logical] = observed.CredentialValue()
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
			ContextPublicationGeneration: op.Coordinate.ContextPublicationGeneration, PlanGeneration: op.Coordinate.PlanGeneration,
		},
	}
	if !authority.Valid() {
		return channelonboarding.ConfirmationResult{}, fmt.Errorf("channel confirmation authority is invalid")
	}
	effectCtx := runtimeeffects.WithExecutionMode(ctx, authority.ExecutionMode)
	effectCtx = runtimeeffects.WithController(effectCtx, runtimeeffects.NewController(d.effects).WithExecutionPosture(d.posture))
	effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(d.runtimeInstanceID, op.Coordinate.BundleHash))
	delivery, err := (runtimeregistration.HTTPExecutor{Client: d.httpClient}).DeliverChannelConfirmation(
		effectCtx, toolID, tool, providerInput, credentials,
		map[string]string{
			"onboarding_operation_id": op.OperationID,
			"activation_id":           activation.ActivationID,
			"binding_revision":        fmt.Sprint(binding.Revision),
			"bundle_hash":             op.Coordinate.BundleHash,
			"plan_generation":         op.Coordinate.PlanGeneration.Diagnostic(),
		},
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
	if r.reconcile != nil {
		err := r.reconcile(ctx)
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
	byContext := map[string][]channelonboarding.CompiledActivation{}
	coordinates := map[string]channelonboarding.ChannelRuntimeContextCoordinate{}
	for _, activation := range snapshot.Activations {
		key := activation.Coordinate.BundleHash + "\x00" + fmt.Sprint(activation.Coordinate.ContextPublicationGeneration)
		byContext[key] = append(byContext[key], activation)
		coordinates[key] = activation.Coordinate
	}
	for _, contextDef := range r.manager.LoadedContexts() {
		bundleHash := contextDef.BundleSourceFact.BundleHash()
		key := bundleHash + "\x00" + fmt.Sprint(contextDef.PublicationGeneration)
		if coordinate, found := coordinates[key]; found && coordinate.ContextPublicationGeneration != contextDef.PublicationGeneration {
			return fmt.Errorf("channel activation runtime publication changed during refresh")
		}
		publication, err := channelonboarding.NewChannelActivationPublication(byContext[key])
		if err != nil {
			return err
		}
		if err := r.manager.ReplaceChannelActivationsContext(ctx, bundleHash, contextDef.PublicationGeneration, publication); err != nil {
			return err
		}
	}
	return nil
}
