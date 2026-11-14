package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Boot observes admitted responsibility; it neither enrolls a holder nor changes
// source reachability. Retaining these closures fences the later runtime check.
func bootAdmittedCredentials(source semanticview.Source, store channelonboarding.Store, values credentials.Store, plans []packs.SatisfactionPlan) runtimebootverify.AdmittedCredentialInspector {
	frozen := map[string]runtimebootverify.AdmittedCredentialInspector{}
	return func(ctx context.Context, use credentials.RequirementUse) (credentials.AdmittedSnapshot, runtimebootverify.CredentialAdmissionExtent, error) {
		if use.Kind != "tool" || store == nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, nil
		}
		selection, err := selectBootCredentialUse(source, use)
		if err != nil || selection == nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		inspect, present := frozen[selection.flow]
		if !present {
			inspect, err = freezeBootCredentialObservation(ctx, source, store, values, plans, *selection)
			if err != nil || inspect == nil {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
			}
			frozen[selection.flow] = inspect
		}
		observed, admitted, err := inspect(ctx, use)
		if selection.independent {
			admitted = runtimebootverify.PartiallyAdmittedCredential
		}
		return observed, admitted, err
	}
}

type bootCredentialSelection struct {
	bundle       *contracts.WorkflowContractBundle
	flow         string
	plan         packs.SatisfactionPlan
	account      contracts.FlowOperatorAccount
	accountErr   error
	registration packs.CompiledChannelRegistration
	independent  bool
}

func selectBootCredentialUse(source semanticview.Source, use credentials.RequirementUse) (*bootCredentialSelection, error) {
	bundle, present := semanticview.Bundle(source)
	if !present || bundle == nil || bundle.SourceArtifact == nil {
		return nil, nil
	}
	providers, err := packadmission.ProviderSourceForBundle(bundle)
	if err != nil {
		return nil, err
	}
	flow := use.FlowID
	if flow == "" {
		flow = semanticview.RootExecutionFlowID(source)
	}
	scope, err := providers.Scope(flow)
	if err != nil {
		return nil, err
	}
	definition, account, accountErr := scope.ConnectionAccount(bundle)
	if !definition.Valid() {
		return nil, accountErr
	}
	plan, _ := definition.ChannelPlan()
	connector, _ := definition.ConnectorDescriptor()
	declared, owned := connector.Tools[use.Name]
	if !owned {
		return nil, nil
	}
	want, err := declared.CanonicalHash()
	got, actualErr := use.Entry.CanonicalHash()
	bound, independent := bootCredentialUseBound(source, bundle, flow, use.Name, account.Policy, got, plan)
	if err != nil || actualErr != nil || want != got || !bound {
		return nil, nil
	}
	registration, registered := plan.Registration()
	if !registered || !slices.Contains(append(registration.ProviderCredentials(), registration.SigningCredential()), use.Role) {
		return nil, nil
	}
	return &bootCredentialSelection{bundle: bundle, flow: flow, plan: plan, account: account, accountErr: accountErr,
		registration: registration, independent: independent}, nil
}

func freezeBootCredentialObservation(ctx context.Context, source semanticview.Source, store channelonboarding.Store, values credentials.Store, plans []packs.SatisfactionPlan, selection bootCredentialSelection) (runtimebootverify.AdmittedCredentialInspector, error) {
	ref := channelonboarding.AccountReference{Provider: selection.plan.Provider(), Account: selection.account.Policy}
	var op channelonboarding.Operation
	var operations channelonboarding.OperationStore
	var err error
	if selection.accountErr == nil && selection.account.FromPolicy {
		selected, ok := store.(interface {
			DeploymentChannelOperations() channelonboarding.OperationStore
		})
		if !ok {
			return nil, channelonboarding.ErrInvalidRequest
		}
		operations = selected.DeploymentChannelOperations()
		op, err = channelonboarding.RegisteredAccount(ctx, operations, ref, selection.plan.ProviderFacts())
		if errors.Is(err, channelonboarding.ErrNotFound) {
			return nil, nil
		}
	} else {
		learned, readErr := currentStandingCredentialAdmissions(ctx, store, source, plans, selection.bundle.SourceArtifact.BundleHash())
		if readErr != nil {
			return nil, readErr
		}
		admission, present := learned[standingIngressSelector(selection.flow, selection.plan.Provider())]
		if !present {
			return nil, selection.accountErr
		}
		op, err = store.GetChannelOnboarding(ctx, admission.operationID)
	}
	if err != nil {
		return nil, err
	}
	required := append(selection.registration.ProviderCredentials(), selection.registration.SigningCredential())
	if _, err := (standingCredentialPlanScope{required: required}).admit(op.CredentialAdmissions); err != nil {
		return nil, err
	}
	owner, err := credentials.NewSnapshotOwner(values)
	if err != nil {
		return nil, err
	}
	projection := owner.BeginSecretBindingProjection()
	expected := channelonboarding.AdmissionResponsibility{DeploymentRegistration: op.DeploymentRegistration, OperationID: op.OperationID,
		OperationRevision: op.Revision, ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
		Provider: op.Provider, Credentials: op.CredentialAdmissions, SessionAccount: op.SessionAccount}
	if !op.DeploymentRegistration && op.ActivationRevision > 0 {
		activation, err := store.GetConnectedChannelActivation(ctx, op.SlotKey)
		if err != nil {
			return nil, err
		}
		expected.OperationRevision, expected.ActivationRevision = activation.OperationRevision, activation.Revision
	}
	observation := frozenBootCredentialObservation{op: op, operations: operations, ref: ref, plan: selection.plan,
		expected: expected, projection: projection, store: store}
	return observation.inspect, nil
}

type frozenBootCredentialObservation struct {
	op         channelonboarding.Operation
	operations channelonboarding.OperationStore
	ref        channelonboarding.AccountReference
	plan       packs.SatisfactionPlan
	expected   channelonboarding.AdmissionResponsibility
	projection *credentials.SecretBindingProjection
	store      channelonboarding.Store
}

func (f frozenBootCredentialObservation) inspect(ctx context.Context, use credentials.RequirementUse) (credentials.AdmittedSnapshot, runtimebootverify.CredentialAdmissionExtent, error) {
	var observed credentials.AdmittedSnapshot
	for _, admission := range f.op.CredentialAdmissions {
		snapshot, current, err := observeStandingCredentialRole(ctx, f.projection, admission.StoreKey, admission, true)
		if err != nil || !current {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, fmt.Errorf("admitted boot credential is not current: %w", errors.Join(operatorchannel.ErrCredentialStale, err))
		}
		if admission.Role == use.Role {
			observed = snapshot
		}
	}
	if f.op.DeploymentRegistration {
		current, err := channelonboarding.RegisteredAccount(ctx, f.operations, f.ref, f.plan.ProviderFacts())
		if err != nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		if !f.expected.MatchesOperation(current, f.plan.ProviderFacts()) || current.PrincipalID != f.op.PrincipalID || current.Interface.Normalized() != f.op.Interface.Normalized() {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, channelonboarding.ErrRevisionConflict
		}
	} else if current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, f.store, f.expected, true); err != nil || !current {
		return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	if err := f.projection.ValidateCurrent(ctx); err != nil {
		return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
	}
	return observed, runtimebootverify.FullyAdmittedCredential, nil
}

func bootCredentialUseBound(source semanticview.Source, bundle *contracts.WorkflowContractBundle, flow, tool, account, hash string, plan packs.SatisfactionPlan) (bool, bool) {
	independent := false
	for _, agent := range semanticview.AgentDeclarations(source) {
		entry, found := bundle.ToolEntryForFlow(agent.OwnerFlowID, tool)
		got, _ := entry.CanonicalHash()
		if found && got == hash && (slices.Contains(agent.Entry.Tools, tool) || slices.Contains(agent.Entry.Tools, "*")) {
			independent = true // An unconstrained agent call is not an exact account use.
		}
	}
	bound := false
	for _, name := range plan.OperationNames() {
		id, _, err := plan.ConnectorDeclaration(name)
		bound = bound || err == nil && id == tool
	}
	if registration, present := plan.Registration(); present {
		for _, name := range []string{packs.RegistrationOperationIdentify, packs.RegistrationOperationApply, packs.RegistrationOperationReadback} {
			operation, err := registration.Operation(name)
			bound = bound || err == nil && operation.ToolID() == tool
		}
	}
	for _, site := range bundle.ActivitySites() {
		if site.Node.FlowPath() != flow || site.Spec.Tool != tool {
			continue
		}
		expression := site.Spec.Input["account"]
		if !(expression.HasLiteralValue() && expression.Literal == account || expression.HasRefValue() && expression.Ref == "policy.operator_account" || expression.HasCELValue() && expression.CEL == "policy.operator_account") {
			independent = true
			continue
		}
		bound = true
	}
	return bound, independent
}
