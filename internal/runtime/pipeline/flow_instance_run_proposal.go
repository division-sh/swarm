package pipeline

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// This is construction context, not a native observation or commit permission.
// Publication supplies it only after an isolated native missing-run preflight.
type FlowInstanceRunProposal struct {
	admission events.AdmittedEvent
	source    correlation.SourceArtifactFact
}

func NewFlowInstanceRunProposal(source correlation.SourceArtifactFact, admission events.AdmittedEvent) (FlowInstanceRunProposal, error) {
	if source.Validate() != nil || admission.RunDisposition() != events.AdmittedRunCreateAuthorized || admission.ID() == "" || admission.Event().RunID() == "" {
		return FlowInstanceRunProposal{}, fmt.Errorf("run construction proposal requires its exact source and create-authorized event")
	}
	return FlowInstanceRunProposal{admission: admission, source: source}, nil
}

func (p FlowInstanceRunProposal) Present() bool { return p.admission.ID() != "" }

func (p FlowInstanceRunProposal) Validate(runID string, source correlation.SourceArtifactFact) error {
	if !p.Present() || p.admission.RunDisposition() != events.AdmittedRunCreateAuthorized ||
		p.admission.Event().RunID() != runID || source.Validate() != nil || !source.Matches(p.source) {
		return fmt.Errorf("run construction proposal crosses its admitted source or run")
	}
	return nil
}
