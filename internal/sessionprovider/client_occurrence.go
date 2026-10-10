package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var errClientOccurrenceFenced = errors.New("WhatsApp client occurrence is fenced")
var errClientOccurrenceUsed = errors.New("WhatsApp client occurrence cannot reconnect")

// Each occurrence owns one SDK client and one connect attempt. A new connection
// requires a new owner after complete retirement; the SDK pointer is never an
// independently reconnectable transport. Selected-store admission and effects
// remain the callers' responsibility, not authority inferred by this lifetime.
type clientOccurrence struct {
	mu           sync.Mutex
	connectionID string
	occurrenceID string
	ctx          context.Context
	cancel       context.CancelFunc
	client       *whatsmeow.Client
	stores       *sdkStores
	callbacks    *callbackGuard
	pairing      *pairingQR
	started      bool
	fenced       bool
	inFlight     int
	drained      chan struct{}
	stopDone     chan struct{}
	stopOnce     sync.Once
}

func newClientOccurrence(ctx context.Context, connectionID, occurrenceID string,
	device *store.Device, lids store.LIDStore, log waLog.Logger,
) (*clientOccurrence, error) {
	if ctx == nil || ctx.Err() != nil || uuid.Validate(connectionID) != nil || uuid.Validate(occurrenceID) != nil {
		return nil, fmt.Errorf("WhatsApp client requires a current exact connection occurrence")
	}
	stores, err := guardSDKStores(device, lids)
	if err != nil {
		return nil, err
	}
	ownedCtx, cancel := context.WithCancel(ctx)
	o := &clientOccurrence{connectionID: connectionID, occurrenceID: occurrenceID,
		ctx: ownedCtx, cancel: cancel, stores: stores,
		drained: make(chan struct{}), stopDone: make(chan struct{})}
	client := whatsmeow.NewClient(device, log)
	o.client = client
	client.BackgroundEventCtx = ownedCtx
	client.EnableAutoReconnect = false
	client.InitialAutoReconnect = false
	client.DisableLoginAutoReconnect = true
	client.SynchronousAck = true
	client.EnableDecryptedEventBuffer = false
	client.UseRetryMessageStore = false
	client.AutoTrustIdentity = false
	client.AutomaticMessageRerequestFromPhone = false
	client.ManualHistorySyncDownload = true
	client.DisableManualHistorySyncReceipt = true
	client.PreRetryCallback = func(*events.Receipt, types.MessageID, int, *waE2E.Message) bool { return false }
	client.AddEventHandlerWithSuccessStatus(func(event any) bool {
		switch event.(type) {
		case *events.Disconnected, *events.ManualLoginReconnect, *events.LoggedOut,
			*events.StreamReplaced, *events.StreamError, *events.ConnectFailure,
			*events.CATRefreshError, *events.ClientOutdated, *events.TemporaryBan:
			o.fence()
		}
		// This handler owns lifetime events only, not message capture success.
		return true
	})
	return o, nil
}

func (o *clientOccurrence) bindCallbacks(handle func(context.Context, any) error,
	record func(context.Context, callbackFailure) error,
) (*callbackGuard, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fenced || o.ctx.Err() != nil {
		return nil, errClientOccurrenceFenced
	}
	if o.started || o.callbacks != nil {
		return nil, fmt.Errorf("WhatsApp callbacks must bind exactly once before connect")
	}
	if handle == nil || record == nil {
		return nil, fmt.Errorf("WhatsApp callbacks require capture and failure owners")
	}
	pairing := o.pairing
	guard, err := newCallbackGuard(o.ctx, o.connectionID, o.occurrenceID,
		func(ctx context.Context, event any) error {
			if pairing != nil {
				if err := pairing.handle(event); err != nil {
					return err
				}
			}
			return handle(ctx, event)
		},
		func(ctx context.Context, failure callbackFailure) error {
			o.fence()
			return record(ctx, failure)
		})
	if err != nil {
		return nil, err
	}
	if _, err := guard.install(o.client); err != nil {
		return nil, err
	}
	o.callbacks = guard
	return guard, nil
}

func (o *clientOccurrence) bindPairing(scope pairingQRScope) (*pairingQR, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fenced || o.ctx.Err() != nil {
		return nil, errClientOccurrenceFenced
	}
	if o.started || o.callbacks != nil || o.pairing != nil || o.client.Store.ID != nil ||
		scope.ConnectionID != o.connectionID || scope.OccurrenceID != o.occurrenceID {
		return nil, errPairingScope
	}
	pairing, err := newPairingQR(o.ctx, scope)
	if err != nil {
		return nil, err
	}
	o.pairing = pairing
	return pairing, nil
}

func (o *clientOccurrence) connect(ctx context.Context) (err error) {
	if ctx == nil {
		return errClientOccurrenceFenced
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	o.mu.Lock()
	if o.fenced || o.ctx.Err() != nil {
		o.mu.Unlock()
		return errClientOccurrenceFenced
	}
	if o.started {
		o.mu.Unlock()
		return errClientOccurrenceUsed
	}
	if o.pairing != nil && o.callbacks == nil {
		o.mu.Unlock()
		return fmt.Errorf("WhatsApp pairing requires its guarded public event consumer before connect")
	}
	o.started = true
	o.inFlight++
	o.mu.Unlock()
	defer func() {
		o.release()
		if err != nil {
			err = errors.Join(err, o.join(context.WithoutCancel(o.ctx)))
		}
	}()
	// The SDK retains its connect context for the socket's lifetime. Cancel
	// that owned occurrence only while this caller's attempt is pending.
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		o.fence()
		close(canceled)
	})
	err = o.client.ConnectContext(o.ctx)
	if !stop() {
		<-canceled
	}
	err = errors.Join(err, context.Cause(ctx))
	if err == nil && o.ctx.Err() != nil {
		err = errClientOccurrenceFenced
	}
	if err != nil {
		o.fence()
	}
	return err
}

func (o *clientOccurrence) acquire(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, nil, errClientOccurrenceFenced
	}
	o.mu.Lock()
	if o.fenced || !o.started || o.ctx.Err() != nil || !o.client.IsConnected() || !o.client.IsLoggedIn() {
		o.mu.Unlock()
		return nil, nil, errClientOccurrenceFenced
	}
	o.inFlight++
	o.mu.Unlock()
	workCtx, cancel := context.WithCancel(ctx)
	stopCancellation := context.AfterFunc(o.ctx, cancel)
	return workCtx, func() {
		stopCancellation()
		cancel()
		o.release()
	}, nil
}

func (o *clientOccurrence) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inFlight--
	if o.fenced && o.inFlight == 0 {
		close(o.drained)
	}
}

func (o *clientOccurrence) send(ctx context.Context, to types.JID, message *waE2E.Message, id types.MessageID) (whatsmeow.SendResponse, error) {
	workCtx, release, err := o.acquire(ctx)
	if err != nil {
		return whatsmeow.SendResponse{}, err
	}
	defer release()
	response, err := o.client.SendMessage(workCtx, to, message, whatsmeow.SendRequestExtra{ID: id})
	return response, err
}

func (o *clientOccurrence) logout(ctx context.Context) error {
	workCtx, release, err := o.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	workCtx = context.WithValue(workCtx, explicitLogoutKey{}, o.stores.fence)
	if err := o.client.Logout(workCtx); err != nil {
		return err
	}
	o.fence()
	return nil
}

func (o *clientOccurrence) fence() {
	o.mu.Lock()
	if !o.fenced {
		o.fenced = true
		if o.inFlight == 0 {
			close(o.drained)
		}
	}
	callbacks := o.callbacks
	pairing := o.pairing
	o.mu.Unlock()
	if pairing != nil {
		pairing.stop()
	}
	if callbacks != nil {
		callbacks.fence()
	}
	o.stores.fence.fence()
	o.cancel()
	o.stopOnce.Do(func() {
		go func() {
			o.client.Disconnect()
			close(o.stopDone)
		}()
	})
}

// A successful join does not close the provider database or release possession.
// The containing connection must do both and retain evidence if either fails.
func (o *clientOccurrence) join(ctx context.Context) error {
	o.fence()
	o.mu.Lock()
	callbacks := o.callbacks
	pairing := o.pairing
	o.mu.Unlock()
	if pairing != nil {
		if err := pairing.join(ctx); err != nil {
			return err
		}
	}
	if callbacks != nil {
		if err := callbacks.join(ctx); err != nil {
			return err
		}
	}
	for _, done := range []<-chan struct{}{o.drained, o.stopDone} {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("join WhatsApp client occurrence: %w", context.Cause(ctx))
		}
	}
	return o.stores.fence.join(ctx)
}
