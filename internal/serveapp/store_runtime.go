package serveapp

import (
	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// serveRuntimePersistence is the immutable dependency projection consumed by
// runtime construction and replacement. It deliberately excludes selected
// ownership, backend identity, optional products, and close authority.
type serveRuntimePersistence struct {
	deps         runtime.RuntimeDeps
	schema       store.SchemaBootstrapper
	workspace    workspace.Lookup
	bundleWriter bundlecatalog.ServeIngestWriter
	data         durabledata.ResourceAccessStore
}

func projectServeRuntimePersistence(owner *storeselected.Owner) serveRuntimePersistence {
	return serveRuntimePersistence{
		deps: owner.RuntimeDeps(), schema: owner.Schema(), workspace: owner.WorkspaceLookup(), bundleWriter: owner.ServeBundleIngestWriter(), data: owner.DataAccess(),
	}
}

func (s serveRuntimePersistence) runtimeDeps() runtime.RuntimeDeps {
	deps := s.deps
	deps.AuthorActivityRegistrars = append([]runtime.AuthorActivityCatalogRegistrar(nil), deps.AuthorActivityRegistrars...)
	return deps
}
