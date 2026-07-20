package builder

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/gorilla/websocket"
)

type wsClient struct {
	handler    *handler
	conn       *websocket.Conn
	subscribed map[string]context.CancelFunc
	mu         sync.Mutex
}

func (h *handler) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{
		handler:    h,
		conn:       conn,
		subscribed: map[string]context.CancelFunc{},
	}
	client.run(worklifetime.WithProcess(r.Context(), h.processWorkOwner))
}

func (c *wsClient) run(ctx context.Context) {
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

func (c *wsClient) handleRPC(ctx context.Context, frame map[string]any) {
	params, _ := frame["params"].(map[string]any)
	result, rpcErr := c.handler.dispatchRPC(ctx, strings.TrimSpace(asString(frame["method"])), params)
	_ = c.writeJSON(RPCResponse{JSONRPC: "2.0", ID: frame["id"], Result: result, Error: rpcErr})
}

func (c *wsClient) handleSubscribe(ctx context.Context, channel string) {
	if channel == "" {
		return
	}
	switch channel {
	case "engine:health":
		owner, ok := worklifetime.ProcessFromContext(ctx)
		if !ok {
			return
		}
		lease, err := owner.Begin(ctx)
		if err != nil {
			return
		}
		parent := lease.Context()
		var use RuntimeUse
		if c.handler.runtimeAcquirer != nil {
			use, err = c.handler.runtimeAcquirer.AcquireCurrentRuntime(parent)
			if err != nil {
				_ = lease.Done()
				return
			}
			parent = use.WorkContext()
		}
		subCtx, cancel := context.WithCancel(parent)
		c.mu.Lock()
		if _, exists := c.subscribed[channel]; exists {
			c.mu.Unlock()
			cancel()
			if use != nil {
				_ = use.Done()
			}
			_ = lease.Done()
			return
		}
		c.subscribed[channel] = cancel
		c.mu.Unlock()
		go func() {
			defer func() {
				if use != nil {
					_ = use.Done()
				}
				_ = lease.Done()
			}()
			c.runEngineHealth(subCtx, channel)
		}()
	default:
		if strings.HasPrefix(channel, "run:events:") && c.handler.runHub != nil {
			runID := strings.TrimSpace(strings.TrimPrefix(channel, "run:events:"))
			cancel := c.handler.runHub.subscribe(runID, func(data RunEventEnvelope) {
				_ = c.writeEvent(channel, data)
			})
			c.mu.Lock()
			if _, exists := c.subscribed[channel]; exists {
				c.mu.Unlock()
				cancel()
				return
			}
			c.subscribed[channel] = cancel
			c.mu.Unlock()
			return
		}
	}
}

func (c *wsClient) handleUnsubscribe(channel string) {
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

func (c *wsClient) runEngineHealth(ctx context.Context, channel string) {
	defer c.handleUnsubscribe(channel)
	ticker := time.NewTicker(healthHeartbeatInterval)
	defer ticker.Stop()
	_ = c.writeEvent(channel, c.handler.healthSnapshot(ctx))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.writeEvent(channel, c.handler.healthSnapshot(ctx)); err != nil {
				return
			}
		}
	}
}

func (c *wsClient) writeEvent(channel string, data any) error {
	return c.writeJSON(WSEventFrame{Type: "event", Channel: channel, Data: data})
}

func (c *wsClient) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *wsClient) close() {
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
