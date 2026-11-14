package cliapp

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/registration"
)

// Identity checks use compiled read slots and existing sealed admissions only.
// Pairing checkpoints, login and current native operation admission stay in boot.
func inspectServeChannelIdentities(ctx context.Context, inventory channelonboarding.RetainedActivationInventory, plans []packs.SatisfactionPlan, store credentials.Store) ([]credentials.ValueEvidence, error) {
	var evidence []credentials.ValueEvidence
	for _, operation := range inventory.Operations {
		if !operation.DeploymentRegistration || operation.Provider != "telegram" || operation.LogoutCompleted || len(operation.CredentialAdmissions) == 0 {
			continue
		}
		var profile packs.CompiledChannelRegistration
		found := false
		for _, plan := range plans {
			identity, err := plan.InterfaceIdentity()
			if err != nil {
				return nil, err
			}
			candidate, ok := plan.Registration()
			if ok && candidate.Provider() == operation.Provider && identity == operation.Interface {
				if found {
					return nil, channelonboarding.ErrConflict
				}
				profile, found = candidate, true
			}
		}
		if !found {
			return nil, channelonboarding.ErrNotFound
		}
		read, err := profile.Operation(packs.RegistrationOperationIdentify)
		if err != nil {
			return nil, err
		}
		owner, err := credentials.NewSnapshotOwner(store)
		if err != nil {
			return nil, err
		}
		values := map[string]any{}
		for _, admission := range operation.CredentialAdmissions {
			binding := credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}
			observed, current, err := owner.ObserveValueMatchingSeal(ctx, binding)
			if err != nil || !current {
				return nil, errors.Join(operatorchannel.ErrCredentialStale, err)
			}
			values[admission.Role] = observed.CredentialValue()
			evidence = append(evidence, binding)
		}
		input, err := read.Prepare(map[string]any{})
		if err != nil {
			return nil, err
		}
		output, err := (registration.HTTPExecutor{}).Read(ctx, read, input, values)
		if err != nil {
			return nil, err
		}
		projected, err := read.Project(output)
		if err != nil {
			return nil, err
		}
		if _, ok := output.(map[string]any); !ok || operation.AccountReference != "" && fmt.Sprint(projected["resource_id"]) != operation.AccountReference {
			return nil, channelonboarding.ErrConflict
		}
	}
	return evidence, nil
}
