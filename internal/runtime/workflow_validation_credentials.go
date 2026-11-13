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
	return func(ctx context.Context, use credentials.RequirementUse) (credentials.AdmittedSnapshot, bool, error) {
		if use.Kind != "tool" || store == nil {
			return credentials.AdmittedSnapshot{}, false, nil
		}
		bundle, present := semanticview.Bundle(source)
		if !present || bundle == nil || bundle.SourceArtifact == nil {
			return credentials.AdmittedSnapshot{}, false, nil
		}
		providers, err := packadmission.ProviderSourceForBundle(bundle)
		if err != nil {
			return credentials.AdmittedSnapshot{}, false, err
		}
		flow := use.FlowID
		if flow == "" {
			flow = semanticview.RootExecutionFlowID(source)
		}
		scope, err := providers.Scope(flow)
		if err != nil {
			return credentials.AdmittedSnapshot{}, false, err
		}
		definition, account, accountErr := scope.ConnectionAccount(bundle)
		if !definition.Valid() {
			return credentials.AdmittedSnapshot{}, false, accountErr
		}
		plan, _ := definition.ChannelPlan()
		connector, _ := definition.ConnectorDescriptor()
		declared, owned := connector.Tools[use.Name]
		actual, found := bundle.ToolEntryForFlow(flow, use.Name)
		if !owned || !found {
			return credentials.AdmittedSnapshot{}, false, nil
		}
		want, err := declared.CanonicalHash()
		got, actualErr := actual.CanonicalHash()
		if err != nil || actualErr != nil || want != got || !bootCredentialUseBound(source, bundle, flow, use.Name, account.Policy, plan) {
			return credentials.AdmittedSnapshot{}, false, nil
		}
		registration, registered := plan.Registration()
		if !registered || !slices.Contains(append(registration.ProviderCredentials(), registration.SigningCredential()), use.Role) {
			return credentials.AdmittedSnapshot{}, false, nil
		}
		if inspect, present := frozen[flow]; present {
			return inspect(ctx, use)
		}
		ref := channelonboarding.AccountReference{Provider: plan.Provider(), Account: account.Policy}
		var op channelonboarding.Operation
		var operations channelonboarding.OperationStore
		if accountErr == nil && account.FromPolicy {
			selected, ok := store.(interface {
				DeploymentChannelOperations() channelonboarding.OperationStore
			})
			if !ok {
				return credentials.AdmittedSnapshot{}, false, channelonboarding.ErrInvalidRequest
			}
			operations = selected.DeploymentChannelOperations()
			op, err = channelonboarding.RegisteredAccount(ctx, operations, ref, plan.ProviderFacts())
			if errors.Is(err, channelonboarding.ErrNotFound) {
				rows, readErr := operations.ListChannelOnboardingOperations(ctx)
				if readErr != nil {
					return credentials.AdmittedSnapshot{}, false, readErr
				}
				for _, previous := range rows {
					if previous.Provider == ref.Provider && previous.AccountReference == ref.Account && len(previous.CredentialAdmissions) != 0 {
						return credentials.AdmittedSnapshot{}, false, err
					}
				}
				return credentials.AdmittedSnapshot{}, false, nil
			}
		} else {
			learned, readErr := currentStandingCredentialAdmissions(ctx, store, source, plans, bundle.SourceArtifact.BundleHash())
			if readErr != nil {
				return credentials.AdmittedSnapshot{}, false, readErr
			}
			admission, present := learned[standingIngressSelector(flow, plan.Provider())]
			if !present {
				return credentials.AdmittedSnapshot{}, false, accountErr
			}
			op, err = store.GetChannelOnboarding(ctx, admission.operationID)
		}
		if err != nil {
			return credentials.AdmittedSnapshot{}, false, err
		}
		required := append(registration.ProviderCredentials(), registration.SigningCredential())
		if _, err := (standingCredentialPlanScope{required: required}).admit(op.CredentialAdmissions); err != nil {
			return credentials.AdmittedSnapshot{}, false, err
		}
		owner, err := credentials.NewSnapshotOwner(values)
		if err != nil {
			return credentials.AdmittedSnapshot{}, false, err
		}
		projection := owner.BeginSecretBindingProjection()
		expected := channelonboarding.AdmissionResponsibility{DeploymentRegistration: op.DeploymentRegistration, OperationID: op.OperationID,
			OperationRevision: op.Revision, ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
			Provider: op.Provider, Credentials: op.CredentialAdmissions, SessionAccount: op.SessionAccount}
		if !op.DeploymentRegistration && op.ActivationRevision > 0 {
			activation, err := store.GetConnectedChannelActivation(ctx, op.SlotKey)
			if err != nil {
				return credentials.AdmittedSnapshot{}, false, err
			}
			expected.OperationRevision, expected.ActivationRevision = activation.OperationRevision, activation.Revision
		}
		inspect := func(ctx context.Context, use credentials.RequirementUse) (credentials.AdmittedSnapshot, bool, error) {
			var observed credentials.AdmittedSnapshot
			for _, admission := range op.CredentialAdmissions {
				snapshot, current, err := observeStandingCredentialRole(ctx, projection, admission.StoreKey, admission, true)
				if err != nil || !current {
					return credentials.AdmittedSnapshot{}, false, fmt.Errorf("admitted boot credential is not current: %w", errors.Join(operatorchannel.ErrCredentialStale, err))
				}
				if admission.Role == use.Role {
					observed = snapshot
				}
			}
			if op.DeploymentRegistration {
				current, err := channelonboarding.RegisteredAccount(ctx, operations, ref, plan.ProviderFacts())
				if err != nil {
					return credentials.AdmittedSnapshot{}, false, err
				}
				if !expected.MatchesOperation(current, plan.ProviderFacts()) || current.PrincipalID != op.PrincipalID || current.Interface.Normalized() != op.Interface.Normalized() {
					return credentials.AdmittedSnapshot{}, false, channelonboarding.ErrRevisionConflict
				}
			} else if current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, store, expected, true); err != nil || !current {
				return credentials.AdmittedSnapshot{}, false, errors.Join(channelonboarding.ErrRevisionConflict, err)
			}
			if err := projection.ValidateCurrent(ctx); err != nil {
				return credentials.AdmittedSnapshot{}, false, err
			}
			return observed, true, nil
		}
		frozen[flow] = inspect
		return inspect(ctx, use)
	}
}

func bootCredentialUseBound(source semanticview.Source, bundle *contracts.WorkflowContractBundle, flow, tool, account string, plan packs.SatisfactionPlan) bool {
	for _, agent := range semanticview.AgentDeclarationsForOwner(source, flow) {
		if slices.Contains(agent.Entry.Tools, tool) || slices.Contains(agent.Entry.Tools, "*") {
			return false // An unconstrained agent call is not an exact account use.
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
			return false
		}
		bound = true
	}
	return bound
}
