package cliapp

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

func cliAPIReadWebSocketJSON(conn *websocket.Conn, method, surface, endpoint, operation string, out any) error {
	_, reader, err := conn.NextReader()
	if err != nil {
		return &cliAPITransportError{surface: surface, endpoint: endpoint, operation: operation, err: err}
	}
	raw, err := io.ReadAll(io.LimitReader(reader, cliAPIResponseBudget+1))
	if err != nil {
		return &cliAPITransportError{surface: surface, endpoint: endpoint, operation: operation, err: err}
	}
	if len(raw) > cliAPIResponseBudget {
		return &cliAPIResponseBudgetError{
			Method: method, Surface: surface, Transport: "ws",
			Budget: cliAPIResponseBudget, Overrun: len(raw),
		}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &cliAPIProtocolError{surface: surface, endpoint: endpoint, operation: operation, err: err}
	}
	return nil
}

func cliAPIWebSocketHTTPError(surface, endpoint string, resp *http.Response) error {
	if resp == nil {
		return nil
	}
	message := http.StatusText(resp.StatusCode)
	if resp.Body != nil {
		defer resp.Body.Close()
		// The upgrade-failure body is bounded best-effort diagnostic TEXT;
		// truncation here is harmless and the truncated text remains useful.
		// This is NOT an RPC payload and deliberately carries no budget guard.
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err == nil && strings.TrimSpace(string(raw)) != "" {
			message = strings.TrimSpace(string(raw))
		}
	}
	return &cliAPIHTTPError{surface: surface, endpoint: endpoint, statusCode: resp.StatusCode, message: message}
}

func cliAPIIsNormalWebSocketClose(err error) bool {
	if err == nil {
		return false
	}
	var transportErr *cliAPITransportError
	if errors.As(err, &transportErr) {
		err = transportErr.err
	}
	if err == nil {
		return false
	}
	return websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
		strings.Contains(err.Error(), "use of closed network connection")
}
