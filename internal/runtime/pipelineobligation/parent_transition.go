package pipelineobligation

import "context"

// ParentTransition excludes recovery scans until the parent transaction has
// settled. It is not authority to revoke a foreground processing claim.
type ParentTransition interface{ Done() }

// ParentTransitionOwner drains only recovery-scan claims for the selected run,
// across every bus using this store. Publication claims retain their own fence.
type ParentTransitionOwner interface {
	BeginParentTransition(context.Context, string) (ParentTransition, error)
}
