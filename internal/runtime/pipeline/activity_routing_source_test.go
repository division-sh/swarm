package pipeline

import runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"

// Tests that exercise activity payload and lineage in isolation deliberately
// construct a non-connect source. Runtime call sites always supply the loaded
// semantic source through activityRequestEmitIntentForSource.
func activityRequestEmitIntent(intent runtimeengine.ActivityIntent) (runtimeengine.EmitIntent, error) {
	return activityRequestEmitIntentFromAdmittedSource(intent)
}
