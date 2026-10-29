package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Authored connection-provider sends select one registered account, never a
// source-wide token. The existing snapshot owner still owns credential reads.
func (pc *PipelineCoordinator) accountActivityCredentials(ctx context.Context, intent engine.ActivityIntent, tool contracts.ToolSchemaEntry) (map[string]any, []string, func(context.Context) error, bool, error) {
	if tool.Category() != contracts.ToolCategoryProviderConnector || pc == nil || pc.SemanticSource() == nil {
		return nil, nil, nil, false, nil
	}
	bundle, found := semanticview.Bundle(pc.SemanticSource())
	if !found || bundle == nil {
		return nil, nil, nil, false, fmt.Errorf("provider activity requires its admitted source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return nil, nil, nil, false, err
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return nil, nil, nil, false, err
	}
	scope, err := providers.Scope(intent.ExecutionFlowID.String())
	if err != nil {
		return nil, nil, nil, false, err
	}
	plan, declared, matched, err := exactAccountActivityConnector(scope, intent.Tool)
	if err != nil {
		return nil, nil, nil, matched, err
	}
	if !matched {
		return nil, nil, nil, false, nil
	}
	want, err := declared.CanonicalHash()
	actual, actualErr := tool.CanonicalHash()
	if err != nil || actualErr != nil || want != actual {
		return nil, nil, nil, true, fmt.Errorf("account activity differs from its exact admitted connector")
	}
	profile, admitted := plan.OnboardingProfile()
	input, object := intent.Input.ObjectMap()
	account, supplied := input["account"].String()
	if !admitted || !object || !supplied || pc.registeredAccounts == nil || pc.providerCredentials == nil {
		return nil, nil, nil, true, fmt.Errorf("connection-provider activity requires an explicit account and its deployment registration owners")
	}
	ref, err := channelonboarding.CanonicalAccount(profile.Provider(), account)
	if err != nil {
		return nil, nil, nil, true, fmt.Errorf("activity account: %w", err)
	}
	op, err := pc.activityAccountRegistration(ctx, ref)
	if err != nil {
		return nil, nil, nil, true, err
	}
	owner, err := credentials.NewSnapshotOwner(pc.providerCredentials)
	if err != nil {
		return nil, nil, nil, true, err
	}
	projection := owner.BeginSecretBindingProjection()
	values, secrets, err := admittedActivityCredentialValues(ctx, projection, tool.Credentials(), profile.ProviderCredential(), op.CredentialAdmissions)
	if err != nil {
		return nil, nil, nil, true, err
	}
	current := func(ctx context.Context) error {
		selected, err := pc.activityAccountRegistration(ctx, ref)
		if err != nil {
			return err
		}
		if selected.OperationID != op.OperationID || selected.PrincipalID != op.PrincipalID || selected.Revision < op.Revision ||
			selected.SessionAccount != op.SessionAccount || !slices.Equal(selected.CredentialAdmissions, op.CredentialAdmissions) {
			return channelonboarding.ErrRevisionConflict
		}
		return projection.ValidateCurrent(ctx)
	}
	return values, secrets, current, true, current(ctx)
}

func (pc *PipelineCoordinator) activityAccountRegistration(ctx context.Context, ref channelonboarding.AccountReference) (channelonboarding.Operation, error) {
	op, err := channelonboarding.RegisteredAccount(ctx, pc.registeredAccounts, ref)
	if err == channelonboarding.ErrNotFound {
		err = runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "channel_account_not_linked", "activity-runtime", "select_account", map[string]any{
			"auth_kind": "channel_account", "provider": ref.Provider, "account": ref.Account,
		})
	}
	return op, err
}

func exactAccountActivityConnector(scope packadmission.ProviderScope, toolID string) (packs.SatisfactionPlan, contracts.ToolSchemaEntry, bool, error) {
	var plan packs.SatisfactionPlan
	var declared contracts.ToolSchemaEntry
	matched := false
	for _, definition := range scope.Providers() {
		candidate, connected := definition.ChannelPlan()
		connector, present := definition.ConnectorDescriptor()
		if !connected || !present {
			continue
		}
		tool, owns := connector.Tools[toolID]
		if !owns {
			continue
		}
		if matched {
			return plan, declared, true, fmt.Errorf("account activity has competing connector owners")
		}
		plan, declared, matched = candidate, tool, true
	}
	return plan, declared, matched, nil
}

// Resolve only the selected account's admitted roles through the existing
// snapshot projection; this helper never selects an account or opens a store.
func admittedActivityCredentialValues(ctx context.Context, projection *credentials.SecretBindingProjection, roles []string, providerRole string, admissions []channelonboarding.CredentialAdmission) (map[string]any, []string, error) {
	values, secrets := map[string]any{}, []string{}
	for _, role := range roles {
		if role != providerRole {
			return nil, nil, fmt.Errorf("account activity requests an unowned credential role")
		}
		for _, admission := range admissions {
			if admission.Role != role {
				continue
			}
			if _, duplicate := values[role]; duplicate || admission.Validate() != nil {
				return nil, nil, channelonboarding.ErrConflict
			}
			value, current, err := projection.ObserveAdmittedActivationCredential(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}, admission.Receipt)
			if err != nil || !current {
				return nil, nil, errors.Join(operatorchannel.ErrCredentialStale, err)
			}
			values[role] = value.CredentialValue()
			secrets = append(secrets, value.CredentialValue())
		}
		if _, admitted := values[role]; !admitted {
			return nil, nil, fmt.Errorf("account activity credential is not admitted")
		}
	}
	return values, secrets, nil
}
