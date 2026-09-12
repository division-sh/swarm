package runfork

import "fmt"

// SelectedControl names controls that would require deferred selected execution.
// Stop has a separate terminal-disposition operation and is not in this set.
type SelectedControl string

const (
	ControlRunPause           SelectedControl = "run.pause"
	ControlRunContinue        SelectedControl = "run.continue"
	ControlAgentRestart       SelectedControl = "agent.restart"
	ControlAgentDirective     SelectedControl = "agent.send_directive"
	ControlAgentReplay        SelectedControl = "agent.replay"
	ControlEventReplay        SelectedControl = "event.replay"
	ControlEventPublish       SelectedControl = "event.publish"
	ControlMailboxDecide      SelectedControl = "mailbox.decide"
	ControlMailboxDefer       SelectedControl = "mailbox.defer"
	ControlMailboxBeginInput  SelectedControl = "mailbox.begin_input"
	ControlMailboxCancelInput SelectedControl = "mailbox.cancel_input"
)

func (c SelectedControl) Validate() error {
	switch c {
	case ControlRunPause, ControlRunContinue, ControlAgentRestart, ControlAgentDirective,
		ControlAgentReplay, ControlEventReplay, ControlEventPublish, ControlMailboxDecide,
		ControlMailboxDefer, ControlMailboxBeginInput, ControlMailboxCancelInput:
		return nil
	default:
		return fmt.Errorf("unknown selected-fork execution control %q", c)
	}
}

type SelectedForkControlUnsupported struct {
	Operation SelectedControl
	RunID     string
	BindingID string
}

func (e *SelectedForkControlUnsupported) Error() string {
	return fmt.Sprintf("%s is unsupported for selected fork %s (binding %s); selected deferred execution is not admitted", e.Operation, e.RunID, e.BindingID)
}
