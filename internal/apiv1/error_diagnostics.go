package apiv1

import (
	"context"
	"runtime"
)

const diagnosticNodeLimit = 256
const diagnosticFrameLimit = 128

type internalFailureDiagnostics struct {
	Chain           []diagnosticErrorNode `json:"chain"`
	ChainIncomplete bool                  `json:"chain_incomplete,omitempty"`
	Stack           []diagnosticFrame     `json:"reporting_stack"`
	StackIncomplete bool                  `json:"stack_incomplete,omitempty"`
}

type diagnosticErrorNode struct {
	Parent             int    `json:"parent"`
	Branch             int    `json:"branch"`
	Kind               string `json:"kind"`
	Message            string `json:"message"`
	MessageUnavailable bool   `json:"message_unavailable,omitempty"`
}

type diagnosticFrame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

func renderInternalFailureDiagnostics(err error) internalFailureDiagnostics {
	var detail internalFailureDiagnostics
	work := 0
	// This bounds the representation, not bytes or time inside error methods.
	detail.walk(err, -1, 0, &work)
	var pcs [diagnosticFrameLimit + 1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n > diagnosticFrameLimit {
		detail.StackIncomplete = true
		n = diagnosticFrameLimit
	}
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		detail.Stack = append(detail.Stack, diagnosticFrame{frame.Function, frame.File, frame.Line})
		if !more {
			break
		}
	}
	return detail
}

func (detail *internalFailureDiagnostics) walk(err error, parent, branch int, work *int) {
	if *work == diagnosticNodeLimit {
		detail.ChainIncomplete = true
		return
	}
	*work++
	if err == nil {
		return
	}
	message, unavailable := diagnosticErrorMessage(err)
	index := len(detail.Chain)
	kind := "opaque"
	switch err {
	case context.Canceled:
		kind = "context.Canceled"
	case context.DeadlineExceeded:
		kind = "context.DeadlineExceeded"
	}
	detail.Chain = append(detail.Chain, diagnosticErrorNode{parent, branch, kind, message, unavailable})
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		detail.Chain[index].Kind = "joined"
		for child, cause := range wrapped.Unwrap() {
			detail.walk(cause, index, child, work)
			if detail.ChainIncomplete {
				return
			}
		}
	case interface{ Unwrap() error }:
		detail.Chain[index].Kind = "wrapped"
		detail.walk(wrapped.Unwrap(), index, 0, work)
	}
}

func diagnosticErrorMessage(err error) (message string, unavailable bool) {
	unavailable = true
	defer func() { _ = recover() }()
	message = err.Error()
	unavailable = false
	return
}
