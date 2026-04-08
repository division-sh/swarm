package pipeline

import "errors"

type ScheduleTerminalError struct {
	Stage             string
	TransitionApplied bool
	Err               error
}

func (e *ScheduleTerminalError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *ScheduleTerminalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func TerminalTransitionApplied(err error) bool {
	var terminalErr *ScheduleTerminalError
	return errors.As(err, &terminalErr) && terminalErr.TransitionApplied
}
