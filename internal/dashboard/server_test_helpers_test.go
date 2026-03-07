package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"empireai/internal/events"
)

type chatStubAgent struct{ id string }

func (a *chatStubAgent) ID() string { return a.id }
func (a *chatStubAgent) Type() string { return "stub" }
func (a *chatStubAgent) Subscriptions() []events.EventType {
	return []events.EventType{"board.chat", "board.directive"}
}
func (a *chatStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) { return nil, nil }
func (a *chatStubAgent) BoardStep(context.Context, string) (string, error) { return "OK", nil }

type controlStubAgent struct{ id string }

func (a *controlStubAgent) ID() string { return a.id }
func (a *controlStubAgent) Type() string { return "stub" }
func (a *controlStubAgent) Subscriptions() []events.EventType {
	return []events.EventType{"system.directive"}
}
func (a *controlStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) { return nil, nil }
func (a *controlStubAgent) BoardStep(context.Context, string) (string, error) { return "ACK", nil }

func authed(t interface{ Helper() }, method, path string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func authedReqAny(t interface{ Helper() }, method, path string, body any) *http.Request {
	t.Helper()
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}
