package runfork

import (
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// SelectedResetPreparation is transient declaration/cut evidence. Cleanup must
// recapture its membership under the reset fence; it grants no execution rights.
type SelectedResetPreparation struct {
	Binding  RunForkSelectedContractBinding
	Source   correlation.SourceArtifactFact
	Plan     RunForkPlan
	Original semanticview.OriginalLoopCarriage
}
