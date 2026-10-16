package activityjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

// This is one captured installed role, never a lookup of the runtime's mutable
// current role. The immutable reference itself supplies no permission.
type activityExecutionGrant struct {
	evidence  runtimeownership.GrantEvidence
	reference processbinding.ActivityExecutionBinding
}

var _ runtimeownership.ActivityJournalGrantBinder = (*ActivityPostgresOwner)(nil)
var _ runtimeownership.ActivityJournalGrantBinder = (*ActivitySQLiteOwner)(nil)

func (s *ActivityPostgresOwner) BindActivityGrant(evidence runtimeownership.GrantEvidence) (runtimepipeline.ActivityAttemptJournal, error) {
	if s == nil || s.backend == nil || s.schemaGuard == nil || s.executionGrant != nil {
		return nil, errors.New("activity journal requires its unbound native postgres owner")
	}
	grant, err := captureActivityExecutionGrant(evidence)
	if err != nil {
		return nil, err
	}
	return &ActivityPostgresOwner{
		backend: s.backend, schemaGuard: s.schemaGuard, catalog: s.authorActivityCatalog(),
		channelChanges: s.channelChanges, executionGrant: grant,
	}, nil
}

func (s *ActivitySQLiteOwner) BindActivityGrant(evidence runtimeownership.GrantEvidence) (runtimepipeline.ActivityAttemptJournal, error) {
	if s == nil || s.backend == nil || s.schemaGuard == nil || s.executionGrant != nil {
		return nil, errors.New("activity journal requires its unbound native sqlite owner")
	}
	grant, err := captureActivityExecutionGrant(evidence)
	if err != nil {
		return nil, err
	}
	return &ActivitySQLiteOwner{
		backend: s.backend, schemaGuard: s.schemaGuard, catalog: s.authorActivityCatalog(),
		channelChanges: s.channelChanges, executionGrant: grant,
	}, nil
}

func (g *activityExecutionGrant) prepareRecord(ctx context.Context, record runtimepipeline.ActivityAttemptRecord, starting bool) (runtimepipeline.ActivityAttemptRecord, error) {
	if ctx == nil || g == nil {
		return runtimepipeline.ActivityAttemptRecord{}, errors.New("activity mutation requires its original installed execution role")
	}
	if err := ctx.Err(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if starting && record.ExecutionBinding.IsZero() {
		record.ExecutionBinding = g.reference
	}
	if record.ExecutionBinding != g.reference {
		return runtimepipeline.ActivityAttemptRecord{}, errors.New("activity mutation differs from its original installed execution role")
	}
	return record, nil
}

func captureActivityExecutionGrant(evidence runtimeownership.GrantEvidence) (*activityExecutionGrant, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	if evidence.State != runtimeownership.GrantAdmitted {
		return nil, errors.New("activity execution requires its installed admitted generation grant")
	}
	reference := processbinding.ActivityExecutionBinding{
		Version: processbinding.ActivityExecutionBindingVersion, GrantID: evidence.GrantID, StateVersion: evidence.StateVersion,
	}
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	// Clone the canonical DTO so later mutation of caller-owned slices/pointers
	// cannot change the original role used by an in-flight callback.
	raw, err := canonicaljson.Bytes(evidence)
	if err != nil {
		return nil, err
	}
	var captured runtimeownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &captured); err != nil {
		return nil, err
	}
	return &activityExecutionGrant{evidence: captured, reference: reference}, nil
}

func (g *activityExecutionGrant) proveOriginalTx(ctx context.Context, tx *sql.Tx, record runtimepipeline.ActivityAttemptRecord, sqlite bool) error {
	if ctx == nil || tx == nil || g == nil {
		return errors.New("activity mutation requires its original transaction and installed execution role")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !record.ExecutionMode.Valid() || record.ExecutionBinding != g.reference {
		return errors.New("activity mutation differs from its original installed execution role")
	}
	// This fence precedes every process/source/run/selected/activity lock and is
	// retained by the enclosing native attempt through commit.
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return err
	}
	historical, err := storestartup.ReadActivityExecutionGrantTx(ctx, tx, g.reference.GrantID, g.reference.StateVersion)
	if err != nil {
		return err
	}
	actual, err := canonicaljson.Bytes(historical)
	if err != nil {
		return err
	}
	expected, err := canonicaljson.Bytes(g.evidence)
	if err != nil {
		return err
	}
	if string(actual) != string(expected) {
		return errors.New("activity historical snapshot differs from its original installed role")
	}
	binding := processbinding.Binding{
		ProcessAuthorityID: historical.ProcessAuthorityID, ProcessOwnerID: historical.ProcessOwnerID, ProcessBootID: historical.ProcessBootID,
		GenerationGrantID: historical.GrantID, BundleHash: historical.BundleHash,
		RuntimeInstanceID: historical.RuntimeInstanceID, RuntimeGeneration: historical.RuntimeGeneration,
	}
	if err := agentpersistence.AuthorizeActivityExecutionTx(ctx, tx, binding, record.RunID, sqlite); err != nil {
		return err
	}
	// Currentness is deliberately separate from the exact historical lookup.
	// Compare the full captured snapshot, including state_version, against the
	// canonical current head while holding the same native fences.
	ownership, err := agentpersistence.InspectRunExecutionOwnershipTx(ctx, tx, historical, record.RunID, sqlite)
	if err != nil {
		return err
	}
	if ownership != manager.RunExecutionOwned {
		return fmt.Errorf("%w: activity run %s", manager.ErrRunExecutionNotOwned, record.RunID)
	}
	return ctx.Err()
}
