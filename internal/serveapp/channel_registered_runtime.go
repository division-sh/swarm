package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func observeDeploymentAccountList(ctx context.Context, raw json.RawMessage, manager *runtime.RuntimeContextManager, clients map[string]*cliapp.DeploymentChannelControl, mu *sync.Mutex) (json.RawMessage, error) {
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
		if err := observeDeploymentAccountHolder(ctx, manager, row.Status); err != nil {
			return nil, err
		}
	}
	return json.Marshal(list)
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
		declarations, err := use.Context.StandingTargetDeclarations()
		if err != nil {
			return fail(err)
		}
		candidates, err := serveChannelContextCandidates(use.Context, declarations)
		if err != nil {
			return fail(err)
		}
		selected, err := exactRegisteredBusinessCandidate(candidates, op.Provider, holder.Flow)
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

func exactRegisteredBusinessCandidate(candidates []channelonboarding.Candidate, provider, flow string) (*channelonboarding.Candidate, error) {
	var selected *channelonboarding.Candidate
	for index := range candidates {
		candidate := &candidates[index]
		if candidate.Provider != provider || candidate.Target.FlowPath != flow {
			continue
		}
		if selected != nil {
			return nil, channelonboarding.ErrConflict
		}
		selected = candidate
	}
	if selected == nil || selected.Coordinate.Validate() != nil {
		return nil, channelonboarding.ErrRevisionConflict
	}
	return selected, nil
}
