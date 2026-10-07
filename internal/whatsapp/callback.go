package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
)

type callbackFailure struct {
	ConnectionID string
	OccurrenceID string
	Reason       string
}

// callbackGuard catches inside the SDK's success-status handler. The SDK's
// outer panic recovery does not change its successful return value.
type callbackGuard struct {
	mu           sync.Mutex
	connectionID string
	occurrenceID string
	ctx          context.Context
	handle       func(context.Context, any) error
	record       func(context.Context, callbackFailure) error
	fenced       bool
	failure      error
}

func newCallbackGuard(ctx context.Context, connectionID, occurrenceID string,
	handle func(context.Context, any) error, record func(context.Context, callbackFailure) error,
) (*callbackGuard, error) {
	if ctx == nil || uuid.Validate(connectionID) != nil || uuid.Validate(occurrenceID) != nil || handle == nil || record == nil {
		return nil, fmt.Errorf("WhatsApp callback requires exact connection/occurrence and handler/failure owners")
	}
	return &callbackGuard{ctx: ctx, connectionID: connectionID, occurrenceID: occurrenceID, handle: handle, record: record}, nil
}

func (g *callbackGuard) install(client *whatsmeow.Client) (uint32, error) {
	if client == nil {
		return 0, fmt.Errorf("WhatsApp callback requires its owned SDK client")
	}
	return client.AddEventHandlerWithSuccessStatus(g.receive), nil
}

func (g *callbackGuard) receive(event any) (success bool) {
	g.mu.Lock()
	fenced := g.fenced
	g.mu.Unlock()
	if fenced || g.ctx.Err() != nil {
		return false
	}
	defer func() {
		if recover() != nil {
			// Panic payloads can contain private messages or provider state.
			g.fail("crash", errors.New("WhatsApp callback panicked"))
			success = false
		}
	}()
	if err := g.handle(g.ctx, event); err != nil {
		g.fail("capture_failed", err)
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.fenced
}

func (g *callbackGuard) fail(reason string, cause error) {
	g.mu.Lock()
	g.fenced = true
	g.failure = cause
	g.mu.Unlock()
	if err := g.recordFailure(callbackFailure{
		ConnectionID: g.connectionID, OccurrenceID: g.occurrenceID, Reason: reason,
	}); err != nil {
		g.mu.Lock()
		g.failure = errors.Join(cause, err)
		g.mu.Unlock()
	}
}

func (g *callbackGuard) recordFailure(failure callbackFailure) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("WhatsApp callback failure recorder panicked")
		}
	}()
	return g.record(context.WithoutCancel(g.ctx), failure)
}

func (g *callbackGuard) currentFailure() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failure
}
