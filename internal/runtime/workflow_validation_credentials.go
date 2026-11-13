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
		bundle, present := semanticview.Bundle(source)
		if !present || bundle == nil || bundle.SourceArtifact == nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, nil
		}
		providers, err := packadmission.ProviderSourceForBundle(bundle)
		if err != nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		flow := use.FlowID
		if flow == "" {
			flow = semanticview.RootExecutionFlowID(source)
		}
		scope, err := providers.Scope(flow)
		if err != nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		definition, account, accountErr := scope.ConnectionAccount(bundle)
		if !definition.Valid() {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, accountErr
		}
		plan, _ := definition.ChannelPlan()
		connector, _ := definition.ConnectorDescriptor()
		declared, owned := connector.Tools[use.Name]
		actual, found := bundle.ToolEntryForFlow(flow, use.Name)
		if !owned || !found {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, nil
		}
		want, err := declared.CanonicalHash()
		got, actualErr := actual.CanonicalHash()
		bound, independent := bootCredentialUseBound(source, bundle, flow, use.Name, account.Policy, plan)
		if err != nil || actualErr != nil || want != got || !bound {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, nil
		}
		registration, registered := plan.Registration()
		if !registered || !slices.Contains(append(registration.ProviderCredentials(), registration.SigningCredential()), use.Role) {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, nil
		}
		if inspect, present := frozen[flow]; present {
			observed, admitted, err := inspect(ctx, use)
			if independent {
				admitted = runtimebootverify.PartiallyAdmittedCredential
			}
			return observed, admitted, err
		}
		ref := channelonboarding.AccountReference{Provider: plan.Provider(), Account: account.Policy}
		var op channelonboarding.Operation
		var operations channelonboarding.OperationStore
		if accountErr == nil && account.FromPolicy {
			selected, ok := store.(interface {
				DeploymentChannelOperations() channelonboarding.OperationStore
			})
			if !ok {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, channelonboarding.ErrInvalidRequest
			}
			operations = selected.DeploymentChannelOperations()
			op, err = channelonboarding.RegisteredAccount(ctx, operations, ref, plan.ProviderFacts())
			if errors.Is(err, channelonboarding.ErrNotFound) {
				rows, readErr := operations.ListChannelOnboardingOperations(ctx)
				if readErr != nil {
					return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, readErr
				}
				for _, previous := range rows {
					if previous.Provider == ref.Provider && previous.AccountReference == ref.Account && len(previous.CredentialAdmissions) != 0 {
						return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
					}
				}
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, nil
			}
		} else {
			learned, readErr := currentStandingCredentialAdmissions(ctx, store, source, plans, bundle.SourceArtifact.BundleHash())
			if readErr != nil {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, readErr
			}
			admission, present := learned[standingIngressSelector(flow, plan.Provider())]
			if !present {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, accountErr
			}
			op, err = store.GetChannelOnboarding(ctx, admission.operationID)
		}
		if err != nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		required := append(registration.ProviderCredentials(), registration.SigningCredential())
		if _, err := (standingCredentialPlanScope{required: required}).admit(op.CredentialAdmissions); err != nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		owner, err := credentials.NewSnapshotOwner(values)
		if err != nil {
			return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
		}
		projection := owner.BeginSecretBindingProjection()
		expected := channelonboarding.AdmissionResponsibility{DeploymentRegistration: op.DeploymentRegistration, OperationID: op.OperationID,
			OperationRevision: op.Revision, ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
			Provider: op.Provider, Credentials: op.CredentialAdmissions, SessionAccount: op.SessionAccount}
		if !op.DeploymentRegistration && op.ActivationRevision > 0 {
			activation, err := store.GetConnectedChannelActivation(ctx, op.SlotKey)
			if err != nil {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
			}
			expected.OperationRevision, expected.ActivationRevision = activation.OperationRevision, activation.Revision
		}
		inspect := func(ctx context.Context, use credentials.RequirementUse) (credentials.AdmittedSnapshot, runtimebootverify.CredentialAdmissionExtent, error) {
			var observed credentials.AdmittedSnapshot
			for _, admission := range op.CredentialAdmissions {
				snapshot, current, err := observeStandingCredentialRole(ctx, projection, admission.StoreKey, admission, true)
				if err != nil || !current {
					return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, fmt.Errorf("admitted boot credential is not current: %w", errors.Join(operatorchannel.ErrCredentialStale, err))
				}
				if admission.Role == use.Role {
					observed = snapshot
				}
			}
			if op.DeploymentRegistration {
				current, err := channelonboarding.RegisteredAccount(ctx, operations, ref, plan.ProviderFacts())
				if err != nil {
					return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
				}
				if !expected.MatchesOperation(current, plan.ProviderFacts()) || current.PrincipalID != op.PrincipalID || current.Interface.Normalized() != op.Interface.Normalized() {
					return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, channelonboarding.ErrRevisionConflict
				}
			} else if current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, store, expected, true); err != nil || !current {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, errors.Join(channelonboarding.ErrRevisionConflict, err)
			}
			if err := projection.ValidateCurrent(ctx); err != nil {
				return credentials.AdmittedSnapshot{}, runtimebootverify.NoAdmittedCredential, err
			}
			return observed, runtimebootverify.FullyAdmittedCredential, nil
		}
		frozen[flow] = inspect
		observed, admitted, err := inspect(ctx, use)
		if independent {
			admitted = runtimebootverify.PartiallyAdmittedCredential
		}
		return observed, admitted, err
	}
}

func bootCredentialUseBound(source semanticview.Source, bundle *contracts.WorkflowContractBundle, flow, tool, account string, plan packs.SatisfactionPlan) (bool, bool) {
	independent := false
	for _, agent := range semanticview.AgentDeclarationsForOwner(source, flow) {
		if slices.Contains(agent.Entry.Tools, tool) || slices.Contains(agent.Entry.Tools, "*") {
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
