// Package runforkrevisionfixture exposes the private revision adapter only to
// tests that need to construct exact historical fork snapshots.
package runforkrevisionfixture

import private "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"

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
