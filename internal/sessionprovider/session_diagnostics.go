package sessionprovider

import (
	"context"
	"errors"
	"io"
	"syscall"

	"github.com/coder/websocket"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// SessionDiagnostic is observation only: no raw SDK values or execution rights.
type SessionDiagnostic struct {
	ConnectionID, OccurrenceID, Socket, Lifecycle string
	Errno, CloseStatus                            int
	Down                                          bool
	KeepAliveTimeout, KeepAliveRestored           bool
}

func (d SessionDiagnostic) Cause() string {
	if cause := map[string]string{
		"logged out": "logged out", "account temporarily banned": "account temporarily banned",
		"session replaced by another connection": "connection replaced",
		"connection rejected by WhatsApp":        "connection rejected",
		"WhatsApp authentication refresh failed": "authentication refresh failed",
		"WhatsApp client needs an update":        "client needs an update",
	}[d.Lifecycle]; cause != "" {
		return cause
	}
	if cause := map[string]string{"peer_close": "closed by the other side", "reset": "connection reset", "unreachable": "network unreachable", "timeout": "connection timed out"}[d.Socket]; cause != "" {
		return cause
	}
	return "cause unknown"
}

type sdkDiagnosticLog struct {
	waLog.Logger
	owner  *clientOccurrence
	socket bool
}

func (l sdkDiagnosticLog) Sub(module string) waLog.Logger {
	return sdkDiagnosticLog{Logger: l.Logger.Sub(module), owner: l.owner, socket: module == "Socket"}
}

func (l sdkDiagnosticLog) Errorf(format string, args ...any) {
	if !l.socket || format != "Error reading from websocket: %v" || len(args) != 1 {
		return
	}
	err, ok := args[0].(error)
	if !ok {
		return
	}
	cause, status, number := "unknown", websocket.CloseStatus(err), 0
	var errno syscall.Errno
	switch {
	case status == websocket.StatusNoStatusRcvd || status >= 1000 && status <= 1014 && status != 1004 && status != 1006 || status >= 3000 && status <= 4999:
		cause = "peer_close"
	case errors.Is(err, context.DeadlineExceeded):
		cause = "timeout"
	case errors.As(err, &errno):
		if kind := map[syscall.Errno]string{syscall.ECONNRESET: "reset", syscall.ENETUNREACH: "unreachable", syscall.EHOSTUNREACH: "unreachable", syscall.ETIMEDOUT: "timeout"}[errno]; kind != "" {
			cause, number = kind, int(errno)
		}
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		cause = "eof"
	}
	if status < 1000 || status > 4999 {
		status = -1
	}
	l.owner.mu.Lock()
	if l.owner.diagnostic.Socket == "" {
		l.owner.diagnostic.Socket, l.owner.diagnostic.Errno, l.owner.diagnostic.CloseStatus = cause, number, int(status)
	}
	l.owner.mu.Unlock()
}

func (o *clientOccurrence) observeKeepAlive(timeout bool) {
	o.mu.Lock()
	o.diagnostic.KeepAliveTimeout = o.diagnostic.KeepAliveTimeout || timeout
	o.diagnostic.KeepAliveRestored = o.diagnostic.KeepAliveRestored || !timeout
	o.mu.Unlock()
}

func (o *clientOccurrence) diagnosticSnapshot() SessionDiagnostic {
	o.mu.Lock()
	defer o.mu.Unlock()
	d := o.diagnostic
	d.ConnectionID, d.OccurrenceID, d.Down = o.connectionID, o.occurrenceID, o.fenced
	return d
}
