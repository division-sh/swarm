package generationauthority

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type externalEffectResultKey struct{}

type externalEffectResult struct {
	request     effects.Settlement
	authority   []byte
	original    runtimeownership.MutationAuthority
	originalErr error
	accepted    bool
	attempt     atomic.Pointer[transaction]
}

// PrepareExternalEffectResult carries only the exact result request into native
// admission. Its persisted launch evidence is still checked under the fence.
func PrepareExternalEffectResult(ctx context.Context, result effects.Settlement, original runtimeownership.MutationAuthority, originalErr error) (context.Context, error) {
	if ctx == nil || result.OperationID == "" || result.AttemptID == "" || !result.Authority.Valid() {
		return nil, runtimeownership.ErrMutationAuthorityMissing
	}
	authority, err := canonicaljson.Bytes(result.Authority.Evidence())
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, externalEffectResultKey{}, &externalEffectResult{request: result, authority: authority, original: original, originalErr: originalErr}), nil
}

// ExternalEffectResultRetryContext is called only after the native retry owner
// has proved rollback and joined cleanup. Acceptance never survives that attempt.
func ExternalEffectResultRetryContext(parent, failed context.Context, tx *sql.Tx) (context.Context, error) {
	result, present := parent.Value(externalEffectResultKey{}).(*externalEffectResult)
	if !present || result.attempt.Load() == nil {
		return parent, nil
	}
	scope, _ := failed.Value(transactionKey{}).(*transaction)
	if scope == nil || result.attempt.Load() != scope {
		return nil, errors.New("external effect retry differs from its rolled-back native transaction")
	}
	scope.Lock()
	defer scope.Unlock()
	if !scope.active || scope.tx != tx {
		return nil, errors.New("external effect retry requires its exact native transaction")
	}
	return context.WithValue(parent, externalEffectResultKey{}, &externalEffectResult{
		request: result.request, authority: result.authority,
		original: result.original, originalErr: result.originalErr,
	}), nil
}

func admitExternalEffectResultTx(ctx context.Context, tx *sql.Tx, sqlite bool, result *externalEffectResult) error {
	scope, _ := ctx.Value(transactionKey{}).(*transaction)
	if scope == nil || (!result.attempt.CompareAndSwap(nil, scope) && result.attempt.Load() != scope) {
		return errors.New("external effect result cannot enter another native transaction")
	}
	if err := FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
	accepted, err := readExternalEffectResultTx(ctx, tx, result.request, result.authority, sqlite)
	if err != nil {
		return err
	}
	if !accepted {
		if result.originalErr != nil {
			return result.originalErr
		}
		if err := RequireCurrentRuntimeMutationTx(ctx, tx, result.original, sqlite); err != nil {
			return err
		}
	}
	scope.Lock()
	defer scope.Unlock()
	result.accepted = accepted
	scope.result = result
	return nil
}

// ExternalEffectResultAccepted is confined to the admitted native transaction;
// it is neither launch authority nor ordinary process mutation authority.
func ExternalEffectResultAccepted(ctx context.Context, tx *sql.Tx, result effects.Settlement) bool {
	if ctx == nil || tx == nil {
		return false
	}
	scope, _ := ctx.Value(transactionKey{}).(*transaction)
	if scope == nil {
		return false
	}
	scope.Lock()
	defer scope.Unlock()
	observed := scope.result
	if !scope.active || scope.tx != tx || observed == nil || !observed.accepted || observed.request.OperationID != result.OperationID || observed.request.AttemptID != result.AttemptID {
		return false
	}
	want, err := canonicaljson.Bytes(result.Authority.Evidence())
	return err == nil && string(observed.authority) == string(want)
}

func readExternalEffectResultTx(ctx context.Context, tx *sql.Tx, result effects.Settlement, authority []byte, sqlite bool) (bool, error) {
	query := `SELECT a.authority_evidence, o.request_fingerprint, a.launched_at IS NOT NULL, a.state,
		(o.authority_kind=$3 AND o.authority_id=$4 AND o.execution_mode=$5
		AND a.execution_mode=$5 AND a.execution_owner=$6 AND a.fence_generation=$7)
		FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.operation_id=$1::uuid AND a.attempt_id=$2::uuid`
	args := []any{result.OperationID, result.AttemptID, string(result.Authority.Kind), result.Authority.ID,
		string(result.Authority.ExecutionMode), result.Authority.ExecutionOwner, result.Authority.FenceGeneration}
	if sqlite {
		query = `SELECT a.authority_evidence, o.request_fingerprint, a.launched_at IS NOT NULL, a.state,
			(o.authority_kind=?3 AND o.authority_id=?4 AND o.execution_mode=?5
			AND a.execution_mode=?5 AND a.execution_owner=?6 AND a.fence_generation=?7)
			FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			WHERE a.operation_id=?1 AND a.attempt_id=?2`
	}
	var raw []byte
	var fingerprint string
	var accepted bool
	var matches bool
	var state effects.State
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw, &fingerprint, &accepted, &state, &matches); errors.Is(err, sql.ErrNoRows) {
		// Missing journal proof retains the original-custody fallback.
		return false, nil
	} else if err != nil {
		return false, err
	}
	stored, err := canonicaljson.Decode(raw)
	if err != nil {
		return false, err
	}
	journal, journalPresent := stored.Lookup("provider_call_journal")
	if journalPresent || strings.HasPrefix(fingerprint, effects.ProviderCallJournalVersion+":") {
		if !journalPresent {
			return false, errors.New("versioned provider result is missing its request journal")
		}
		rawJournal, err := canonicaljson.Encode(journal)
		if err != nil {
			return false, err
		}
		var frozen effects.ProviderCallJournal
		if err := json.Unmarshal(rawJournal, &frozen); err != nil {
			return false, err
		}
		if err := frozen.ValidateRequestBinding(result.OperationID, result.AttemptID, fingerprint); err != nil {
			return false, err
		}
		comparison, _ := stored.ObjectMap()
		delete(comparison, "provider_call_journal")
		stored, err = semanticvalue.ObjectFromMap(comparison)
		if err != nil {
			return false, err
		}
	}
	want, err := canonicaljson.Decode(authority)
	if err != nil {
		return false, err
	}
	if !matches || !stored.Equal(want) {
		return false, errors.New("external effect result differs from its original attempt authority")
	}
	if accepted && state != effects.StateLaunched && state != effects.StateResponseObserved && state != effects.StateSettled && state != effects.StateTerminalFailure && state != effects.StateOutcomeUncertain {
		return false, errors.New("external effect result has contradictory launch evidence")
	}
	if !accepted && (state == effects.StateLaunched || state == effects.StateResponseObserved) {
		return false, errors.New("external effect result is missing its launch evidence")
	}
	return accepted, nil
}
