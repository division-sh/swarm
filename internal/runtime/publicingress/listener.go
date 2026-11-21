package publicingress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

func (c *Controller) serveListener(listener net.Listener, cancelStartup context.CancelFunc) {
	defer close(c.serverDone)
	var err error
	if c.opts.Mode == ModeNativeTLS {
		err = c.server.ServeTLS(listener, "", "")
	} else {
		err = c.server.Serve(listener)
	}
	c.mu.Lock()
	if c.cutoff && errors.Is(err, http.ErrServerClosed) {
		c.mu.Unlock()
		return
	}
	c.serveErr = err
	if c.opts.Readiness != nil {
		c.opts.Readiness.RevokeExposure(err.Error())
	}
	started := c.supervising
	c.mu.Unlock()
	if !started {
		cancelStartup()
	} else if c.opts.OnFatal != nil {
		c.opts.OnFatal(err)
	}
}

// ListenerFailure is the latched Serve result, not the handler join result.
func (c *Controller) ListenerFailure() error {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.serveErr
}

func (c *Controller) publishListenerExposure(generation Generation, at time.Time, install bool) error {
	var authority string
	if c.opts.Readiness != nil {
		authority = c.startupAuthority()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.serveErr != nil {
		return c.serveErr
	}
	if install {
		c.generation = generation
	}
	if c.opts.Readiness != nil {
		c.opts.Readiness.SetExposure(ExposureEvidence{
			GenerationID: generation.ID, Mode: generation.Mode, PublicOrigin: generation.PublicOrigin,
			ListenAddress: generation.ListenAddress, StartupAuthorityID: authority, ObservedAt: at, ExpiresAt: at.Add(EvidenceTTL),
		})
	}
	return nil
}

func (c *Controller) admitDelegate(parent context.Context) (context.Context, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cutoff {
		return nil, nil
	}
	if len(c.delegates) == 0 {
		c.delegates = make(map[uint64]context.CancelFunc)
		c.delegateDone = make(chan struct{})
	}
	ctx, cancel := context.WithCancel(parent)
	c.delegateID++
	id := c.delegateID
	c.delegates[id] = cancel
	return ctx, func() {
		cancel()
		c.mu.Lock()
		delete(c.delegates, id)
		if len(c.delegates) == 0 {
			close(c.delegateDone)
		}
		c.mu.Unlock()
	}
}

func (c *Controller) CloseListener(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() { c.closeErr = c.closeListener(ctx) })
	return c.closeErr
}

func (c *Controller) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.stopOnce.Do(func() {
		c.SealUpdates(ctx)
		shutdownErr := c.CloseListener(ctx)
		if c.stopTransport != nil {
			c.stopTransport()
		}
		c.mu.RLock()
		process := c.process
		c.mu.RUnlock()
		if process != nil {
			_ = process.Stop()
			<-process.Done()
		}
		if c.opts.Readiness != nil {
			c.opts.Readiness.RevokeExposure("serve shutdown")
		}
		c.stopErr = errors.Join(c.sealErr, c.genErr, c.updateErr, shutdownErr)
	})
	return c.stopErr
}

func (c *Controller) closeListener(ctx context.Context) error {
	c.mu.Lock()
	c.cutoff = true
	done := c.delegateDone
	c.mu.Unlock()
	if c.server == nil {
		if c.listener != nil {
			_ = c.listener.Close()
		}
		return nil
	}

	result := c.server.Shutdown(ctx)
	if result != nil {
		result = errors.Join(result, c.server.Close())
	}
	if done != nil {
		result = c.joinDelegates(ctx, done, result)
	}
	// The Serve handoff is separate from real handler completion.
	<-c.serverDone
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.supervising && c.opts.OnFatal == nil {
		// Otherwise Start or OnFatal owns reporting the exact serving cause.
		result = errors.Join(result, c.serveErr)
	}
	return result
}

func (c *Controller) joinDelegates(ctx context.Context, done <-chan struct{}, result error) error {
	select {
	case <-done:
		return result
	default:
	}
	select {
	case <-done:
		return result
	case <-ctx.Done():
		if result == nil {
			result = c.server.Close()
		}
		if !errors.Is(result, ctx.Err()) {
			result = errors.Join(result, ctx.Err())
		}
	}
	// Closing sockets does not cancel a handler stuck above an unread body.
	c.mu.RLock()
	cancels := make([]context.CancelFunc, 0, len(c.delegates))
	for _, cancel := range c.delegates {
		cancels = append(cancels, cancel)
	}
	c.mu.RUnlock()
	for _, cancel := range cancels {
		cancel()
	}
	<-done
	return result
}
