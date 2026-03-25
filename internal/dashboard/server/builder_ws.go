package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var builderHealthHeartbeatInterval = 5 * time.Second

var builderWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (h *Handler) handleBuilderWS(w http.ResponseWriter, r *http.Request) {
	conn, err := builderWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &builderWSClient{
		handler:    h,
		conn:       conn,
		subscribed: map[string]context.CancelFunc{},
	}
	client.run(r.Context())
}

type builderWSClient struct {
	handler    *Handler
	conn       *websocket.Conn
	mu         sync.Mutex
	subscribed map[string]context.CancelFunc
}

func (c *builderWSClient) run(ctx context.Context) {
	defer c.close()
	for {
		var frame map[string]any
		if err := c.conn.ReadJSON(&frame); err != nil {
			return
		}
		switch strings.TrimSpace(asString(frame["type"])) {
		case "rpc":
			c.handleRPC(ctx, frame)
		case "subscribe":
			c.handleSubscribe(ctx, strings.TrimSpace(asString(frame["channel"])))
		case "unsubscribe":
			c.handleUnsubscribe(strings.TrimSpace(asString(frame["channel"])))
		}
	}
}

func (c *builderWSClient) handleRPC(ctx context.Context, frame map[string]any) {
	method := strings.TrimSpace(asString(frame["method"]))
	params, _ := frame["params"].(map[string]any)
	result, rpcErr := c.handler.dispatchBuilderRPC(ctx, method, params)
	response := builderRPCResponse{
		JSONRPC: "2.0",
		ID:      frame["id"],
		Result:  result,
		Error:   rpcErr,
	}
	_ = c.writeJSON(response)
}

func (c *builderWSClient) handleSubscribe(ctx context.Context, channel string) {
	if channel == "" {
		return
	}
	c.mu.Lock()
	if _, exists := c.subscribed[channel]; exists {
		c.mu.Unlock()
		return
	}
	subCtx, cancel := context.WithCancel(ctx)
	c.subscribed[channel] = cancel
	c.mu.Unlock()

	switch channel {
	case "engine:health":
		go c.runEngineHealth(subCtx, channel)
	default:
		if strings.HasPrefix(channel, "run:events:") && c.handler.runHub != nil {
			runID := strings.TrimSpace(strings.TrimPrefix(channel, "run:events:"))
			cancel = c.handler.runHub.subscribe(runID, func(data RunEventEnvelope) {
				_ = c.writeEvent(channel, data)
			})
			c.mu.Lock()
			c.subscribed[channel] = cancel
			c.mu.Unlock()
			return
		}
		c.handleUnsubscribe(channel)
	}
}

func (c *builderWSClient) handleUnsubscribe(channel string) {
	if channel == "" {
		return
	}
	c.mu.Lock()
	cancel := c.subscribed[channel]
	delete(c.subscribed, channel)
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *builderWSClient) runEngineHealth(ctx context.Context, channel string) {
	defer c.handleUnsubscribe(channel)
	ticker := time.NewTicker(builderHealthHeartbeatInterval)
	defer ticker.Stop()

	_ = c.writeEvent(channel, c.handler.builderHealthSnapshot(ctx))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.writeEvent(channel, c.handler.builderHealthSnapshot(ctx)); err != nil {
				return
			}
		}
	}
}

func (c *builderWSClient) writeEvent(channel string, data any) error {
	return c.writeJSON(builderWSEventFrame{
		Type:    "event",
		Channel: channel,
		Data:    data,
	})
}

func (c *builderWSClient) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *builderWSClient) close() {
	c.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(c.subscribed))
	for _, cancel := range c.subscribed {
		cancels = append(cancels, cancel)
	}
	c.subscribed = map[string]context.CancelFunc{}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	_ = c.conn.Close()
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func asStringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		if stringsSlice, ok := v.([]string); ok {
			out := make([]string, 0, len(stringsSlice))
			for _, item := range stringsSlice {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				out = append(out, item)
			}
			return out
		}
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, item := range raw {
		value := strings.TrimSpace(asString(item))
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}
