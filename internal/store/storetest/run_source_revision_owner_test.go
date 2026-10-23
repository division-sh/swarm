package storetest

import (
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type sourceRevisionProofStore interface {
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
	runtimebus.RunLifecycleReadPersistence
	sourceartifactfixture.Writer
}
