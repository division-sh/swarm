package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func serveReadyDeploymentAccounts(ctx context.Context, operations []channelonboarding.Operation, read apiv1.MethodHandler, holder any) ([]channelonboarding.AccountStatus, error) {
	var rows []channelonboarding.AccountStatus
	for _, op := range operations {
		if !op.DeploymentRegistration || !op.AccountConfirmed || op.Phase == channelonboarding.PhaseRetired || op.Phase == channelonboarding.PhaseFailed {
			continue
		}
		if err := (channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference}).Validate(); err != nil {
			return nil, err
		}
		value, err := read(ctx, apiv1.Request{Method: "channel.list", Params: map[string]any{
			"provider": op.Provider, "account": op.AccountReference, "holder": holder,
		}})
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(value)
		var status channelonboarding.AccountStatus
		if err != nil || json.Unmarshal(raw, &status) != nil || status.Provider != op.Provider || status.Account != op.AccountReference {
			return nil, fmt.Errorf("deployment account status does not match its selected registration")
		}
		rows = append(rows, status)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Provider != rows[j].Provider {
			return rows[i].Provider < rows[j].Provider
		}
		return rows[i].Account < rows[j].Account
	})
	return rows, nil
}

func observeDeploymentAccountStatus(ctx context.Context, ref channelonboarding.AccountReference, runtimeOptions deploymentRuntimeOptions, operations channelonboarding.OperationStore, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex) (json.RawMessage, error) {
	ref, err := channelonboarding.CanonicalAccount(ref.Provider, ref.Account)
	if err != nil {
		return nil, err
	}
	status, op, err := channelonboarding.RegisteredAccountStatus(ctx, operations, ref)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	owner := clients[ref.Provider+":"+op.OperationID]
	mu.Unlock()
	if owner != nil {
		status, err = owner.AccountStatus(ctx, ref)
		if err != nil {
			return nil, err
		}
	}
	if err := observeDeploymentAccountHolder(ctx, runtimeOptions.manager, &status); err != nil {
		return nil, err
	}
	if err := observeDeploymentWebhookAccount(ctx, runtimeOptions, op, &status); err != nil {
		return nil, err
	}
	return json.Marshal(status)
}

func observeDeploymentAccountList(ctx context.Context, raw json.RawMessage, runtimeOptions deploymentRuntimeOptions, operations channelonboarding.OperationStore, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex) (json.RawMessage, error) {
	var list struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	for index := range list.Accounts {
		row := &list.Accounts[index]
		if row.Status == nil {
			continue // A pending pairing may not have an approved account yet.
		}
		ref := channelonboarding.AccountReference{Provider: row.Status.Provider, Account: row.Status.Account}
		operation := row.Operation
		if row.Status.Linked {
			_, current, err := channelonboarding.RegisteredAccountStatus(ctx, operations, ref)
			if err != nil {
				return nil, err
			}
			if current.OperationID != operation.OperationID || current.Revision != operation.Revision ||
				current.Provider != operation.Provider || current.AccountReference != operation.AccountReference {
				return nil, channelonboarding.ErrRevisionConflict
			}
			operation = current
		}
		mu.Lock()
		owner := clients[ref.Provider+":"+row.Operation.OperationID]
		mu.Unlock()
		if owner != nil {
			status, err := owner.AccountStatus(ctx, ref)
			if err != nil {
				return nil, err
			}
			row.Status = &status
		}
		if err := observeDeploymentAccountHolder(ctx, runtimeOptions.manager, row.Status); err != nil {
			return nil, err
		}
		if err := observeDeploymentWebhookAccount(ctx, runtimeOptions, operation, row.Status); err != nil {
			return nil, err
		}
	}
	return json.Marshal(list)
}

func observeDeploymentWebhookAccount(ctx context.Context, opts deploymentRuntimeOptions, op channelonboarding.Operation, status *channelonboarding.AccountStatus) (resultErr error) {
	if op.Posture != channelonboarding.ActivationWebhookRegistration || !status.Linked || len(status.Holders) != 1 {
		return nil
	}
	holder := status.Holders[0]
	use, _, availability, err := opts.manager.AcquireRun(ctx, holder.RunID)
	if err != nil {
		return err
	}
	if use == nil || !availability.Available() {
		status.State, status.Reason = "stopped", "holder_unavailable"
		if use != nil {
			return use.Done()
		}
		return nil
	}
	defer func() { resultErr = errors.Join(resultErr, use.Done()) }()
	if holder.BundleHash != use.Context.BundleHash() || holder.RunID == "" ||
		status.Provider != op.Provider || status.Account != op.AccountReference || !op.AccountConfirmed {
		return channelonboarding.ErrRevisionConflict
	}
	candidate, _, err := exactRegisteredBusinessCandidate(use.Context, op.Provider, holder.Flow)
	if err != nil {
		return err
	}
	activation, err := channelonboarding.CompileRegisteredActivation(*candidate, op)
	if err != nil {
		return err
	}
	current, found := opts.ingress.ChannelRegistrationCurrent(use.WorkContext(), time.Now(), activation.Plan.BindingID(), candidate.Target.Selector, op.Provider)
	status.State, status.Reason = "refused", "registration_not_current"
	if found && current.Current {
		status.State, status.Reason, status.Health = "live", "", "connected"
	}
	return nil
}

func observeDeploymentAccountHolder(ctx context.Context, manager *runtime.RuntimeContextManager, status *channelonboarding.AccountStatus) error {
	ref := channelonboarding.AccountReference{Provider: status.Provider, Account: status.Account}
	holders, err := manager.AccountHolders(ctx, ref)
	if err != nil && !errors.Is(err, channelonboarding.ErrConflict) {
		return err
	}
	status.Holders = holders
	if err != nil {
		status.State, status.Reason = "refused", "competing_holders"
	}
	return nil
}

// The source/run use is acquired for this delivery, not retained as pairing
// authority. A registration can survive source replacement without granting the
// replacement source permission to consume an old captured delivery.
func serveRegisteredBusinessSelector(manager *runtime.RuntimeContextManager) sessionprovider.RegisteredBusinessSelector {
	return func(ctx context.Context, op channelonboarding.Operation) (sessionprovider.RegisteredBusinessBinding, error) {
		var absent sessionprovider.RegisteredBusinessBinding
		ref := channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference}
		if !op.DeploymentRegistration || !op.AccountConfirmed || ref.Validate() != nil {
			return absent, channelonboarding.ErrInvalidRequest
		}
		holders, err := manager.AccountHolders(ctx, ref)
		if err != nil {
			return absent, err
		}
		if len(holders) == 0 {
			return absent, sessionprovider.ErrAccountUnclaimed
		}
		if len(holders) != 1 {
			return absent, channelonboarding.ErrConflict
		}
		holder := holders[0]
		use, _, available, err := manager.AcquireRun(ctx, holder.RunID)
		if err != nil || use == nil {
			return absent, errors.Join(channelonboarding.ErrRevisionConflict, err)
		}
		fail := func(err error) (sessionprovider.RegisteredBusinessBinding, error) {
			return absent, errors.Join(err, use.Done())
		}
		if !available.Available() || available.Status != string(runlifecycle.StateLive) || use.Context.BundleHash() != holder.BundleHash {
			return fail(channelonboarding.ErrRevisionConflict)
		}
		selected, declarations, err := exactRegisteredBusinessCandidate(use.Context, op.Provider, holder.Flow)
		if err != nil {
			return fail(err)
		}
		incoming, err := serveSessionIncomingSelection(use.Context, use.Runtime(), declarations, *selected)
		if err != nil {
			return fail(err)
		}
		owned, err := incoming.Bus.AdmitSourceArtifactFact(use.WorkContext())
		if err != nil {
			return fail(err)
		}
		current := func(ctx context.Context) error {
			rows, err := manager.AccountHolders(ctx, ref)
			if err != nil {
				return err
			}
			if len(rows) != 1 || rows[0] != holder || owned.Err() != nil {
				return channelonboarding.ErrRevisionConflict
			}
			return nil
		}
		return sessionprovider.RegisteredBusinessBinding{Context: owned, Incoming: *incoming, Coordinate: selected.Coordinate,
			TargetSelector: selected.Target.Selector, Current: current, Release: use.Done,
			Publication: inbound.BindingGeneration{RunID: holder.RunID}}, nil
	}
}

func exactRegisteredBusinessCandidate(contextDef runtime.BundleContext, provider, flow string) (*channelonboarding.Candidate, []runtime.StandingTargetDeclaration, error) {
	declarations, err := contextDef.StandingTargetDeclarations()
	if err != nil {
		return nil, nil, err
	}
	candidates, err := serveChannelContextCandidates(contextDef, declarations)
	if err != nil {
		return nil, nil, err
	}
	var selected *channelonboarding.Candidate
	for index := range candidates {
		candidate := &candidates[index]
		if candidate.Provider != provider || candidate.Target.FlowPath != flow {
			continue
		}
		if selected != nil {
			return nil, nil, channelonboarding.ErrConflict
		}
		selected = candidate
	}
	if selected == nil || selected.Coordinate.Validate() != nil {
		return nil, nil, channelonboarding.ErrRevisionConflict
	}
	return selected, declarations, nil
}
