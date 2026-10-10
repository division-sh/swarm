package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mau.fi/whatsmeow/store"
)

var errSDKStoreFenced = errors.New("WhatsApp provider state occurrence is fenced")
var errExplicitLogoutRequired = errors.New("WhatsApp provider state deletion requires explicit current logout")
var errSDKReplayStoreDisabled = errors.New("WhatsApp SDK outgoing retry store is disabled")
var errSDKDecryptedBufferDisabled = errors.New("WhatsApp SDK decrypted-event buffer is disabled")

type sdkStoreLeaseKey struct{}
type explicitLogoutKey struct{}

type sdkStoreLease struct {
	owner  *sdkStoreFence
	active bool
}

// sdkStoreFence covers the pinned SDK's state methods, including transactions
// which database/sql.Close and the SDK callback queue do not join themselves.
type sdkStoreFence struct {
	mu       sync.Mutex
	fenced   bool
	inFlight int
	drained  chan struct{}
}

func newSDKStoreFence() *sdkStoreFence {
	return &sdkStoreFence{drained: make(chan struct{})}
}

func (f *sdkStoreFence) acquire(ctx context.Context) (context.Context, func(), error) {
	return f.acquireScope(ctx, true)
}

func (f *sdkStoreFence) acquireCurrent(ctx context.Context) (context.Context, func(), error) {
	return f.acquireScope(ctx, false)
}

func (f *sdkStoreFence) acquireScope(ctx context.Context, continueTransaction bool) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, errSDKStoreFenced
	}
	if ctx.Err() != nil {
		return nil, nil, errors.Join(errSDKStoreFenced, context.Cause(ctx))
	}
	f.mu.Lock()
	parent, _ := ctx.Value(sdkStoreLeaseKey{}).(*sdkStoreLease)
	if f.fenced && (!continueTransaction || parent == nil || parent.owner != f || !parent.active) {
		f.mu.Unlock()
		return nil, nil, errSDKStoreFenced
	}
	lease := &sdkStoreLease{owner: f, active: true}
	f.inFlight++
	f.mu.Unlock()
	return context.WithValue(ctx, sdkStoreLeaseKey{}, lease), func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if !lease.active {
			return
		}
		lease.active = false
		f.inFlight--
		if f.fenced && f.inFlight == 0 {
			close(f.drained)
		}
	}, nil
}

func (f *sdkStoreFence) fence() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fenced {
		return
	}
	f.fenced = true
	if f.inFlight == 0 {
		close(f.drained)
	}
}

func (f *sdkStoreFence) join(ctx context.Context) error {
	f.fence()
	select {
	case <-f.drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("join WhatsApp provider state: %w", context.Cause(ctx))
	}
}

type sdkStores struct {
	mu        sync.Mutex
	fence     *sdkStoreFence
	session   store.AllSessionSpecificStores
	lids      store.LIDStore
	container store.DeviceContainer
	device    *store.Device
}

var _ store.AllStores = (*sdkStores)(nil)
var _ store.DeviceContainer = (*sdkStores)(nil)

func guardSDKStores(device *store.Device, lids store.LIDStore) (*sdkStores, error) {
	if device == nil || device.Container == nil || lids == nil {
		return nil, fmt.Errorf("WhatsApp private state requires device, container and alias owners")
	}
	session := store.AllSessionSpecificStores(&store.NoopStore{Error: errSDKStoreFenced})
	if device.Initialized {
		var ok bool
		session, ok = device.Identities.(store.AllSessionSpecificStores)
		if !ok {
			return nil, fmt.Errorf("WhatsApp device does not carry the complete pinned state owner")
		}
	}
	g := &sdkStores{fence: newSDKStoreFence(), session: session, lids: lids, container: device.Container, device: device}
	device.SetAllStores(g)
	device.LIDs = g
	device.Container = g
	return g, nil
}

func (g *sdkStores) sessionOwner() store.AllSessionSpecificStores {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.session
}

func (g *sdkStores) PutDevice(ctx context.Context, device *store.Device) error {
	if device != g.device {
		return fmt.Errorf("WhatsApp state cannot save a different device occurrence")
	}
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	err = g.container.PutDevice(ctx, device)
	if device.Identities != g && device.Initialized {
		session, ok := device.Identities.(store.AllSessionSpecificStores)
		if !ok {
			return errors.Join(err, fmt.Errorf("WhatsApp pairing did not initialize the complete pinned state owner"))
		}
		g.mu.Lock()
		g.session = session
		g.mu.Unlock()
		device.SetAllStores(g)
		device.LIDs = g
		device.Container = g
	}
	return err
}

func (g *sdkStores) DeleteDevice(ctx context.Context, device *store.Device) error {
	if ctx == nil {
		return errExplicitLogoutRequired
	}
	if ctx.Value(explicitLogoutKey{}) != g.fence || device != g.device {
		return errExplicitLogoutRequired
	}
	ctx, release, err := g.fence.acquireCurrent(ctx)
	if err != nil {
		return errors.Join(errExplicitLogoutRequired, err)
	}
	defer release()
	return g.container.DeleteDevice(ctx, device)
}

func (g *sdkStores) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DoDecryptionTxn(ctx, fn)
}
