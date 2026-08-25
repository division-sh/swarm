// Package runforkrevisionfixture exposes the private revision adapter only to
// tests that need to construct exact historical fork snapshots.
package runforkrevisionfixture

import (
	"context"
	"database/sql"

	private "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type Family = private.Family

const (
	FamilyEvents                  = private.FamilyEvents
	FamilyEntityMutations         = private.FamilyEntityMutations
	FamilyEntityMetadata          = private.FamilyEntityMetadata
	FamilyEventDeliveries         = private.FamilyEventDeliveries
	FamilyCommittedReplayScopes   = private.FamilyCommittedReplayScopes
	FamilyEventReceipts           = private.FamilyEventReceipts
	FamilyDeadLetters             = private.FamilyDeadLetters
	FamilyTimers                  = private.FamilyTimers
	FamilyAgentSessions           = private.FamilyAgentSessions
	FamilyAgentTurns              = private.FamilyAgentTurns
	FamilyAgentConversationAudits = private.FamilyAgentConversationAudits
	FamilyReplyContexts           = private.FamilyReplyContexts
)

func AllFamilies() []Family { return private.AllFamilies() }

func Capture(ctx context.Context, tx *sql.Tx, runID string, families ...Family) (int64, error) {
	effects := private.NewEffects()
	if err := effects.Add(runID, families...); err != nil {
		return 0, err
	}
	results, err := private.FinalizePostgres(ctx, tx, effects)
	if err != nil {
		return 0, err
	}
	return results[runID].Revision, nil
}
