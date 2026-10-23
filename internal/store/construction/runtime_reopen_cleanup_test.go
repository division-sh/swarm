package construction_test

import (
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type reopenCleanupStore interface {
	storetest.RunFixtureStore
	runtimebus.RunLifecycleReadPersistence
	sourceartifactfixture.Writer
}
