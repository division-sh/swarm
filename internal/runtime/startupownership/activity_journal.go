package startupownership

import "github.com/division-sh/swarm/internal/runtime/pipeline"

// ActivityJournalGrantBinder is supplied by the retained native session.
// Binding captures attribution; every writer still proves current permission
// inside its original native transaction.
type ActivityJournalGrantBinder interface {
	BindActivityGrant(GrantEvidence) (pipeline.ActivityAttemptJournal, error)
}
