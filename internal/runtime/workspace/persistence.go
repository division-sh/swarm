package workspace

import (
	"context"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
)

// Lookup is the exact selected-store read owner used by workspace runtime
// construction. Database handles and backend query mechanics stay private to
// its implementation.
type Lookup interface {
	LookupWorkspaceEntity(context.Context, runtimecurrentstate.Identity) (WorkspaceEntityLookup, error)
	ListRuntimeWorkspaceContainers(context.Context, string) (RuntimeWorkspaceContainerSet, error)
}

type WorkspaceEntityLookup struct {
	Slug string
}

type RuntimeWorkspaceContainerSet struct {
	EntitySlugs []string
}
