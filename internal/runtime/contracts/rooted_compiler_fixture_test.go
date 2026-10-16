package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

// Compiler unit fixtures retain their genuine filesystem admission in memory.
// Decoded copies prove byte parity, not restored database or execution authority.
func capturedCompilerSource(t testing.TB, captured *sourceartifact.AdmittedSourceArtifact, copies ...*sourceartifact.AdmittedSourceArtifact) sourceartifact.RootedSource {
	t.Helper()
	return sourceartifactfixture.CapturedRoot(t, captured, copies...)
}
