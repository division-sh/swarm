package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

func cliAPIReadWebSocketJSON(conn *websocket.Conn, surface, endpoint, operation string, out any) error {
	_, raw, err := conn.ReadMessage()
	if err != nil {
		return &cliAPITransportError{surface: surface, endpoint: endpoint, operation: operation, err: err}
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
