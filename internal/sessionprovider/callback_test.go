package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
)

func TestWhatsAppCallbackPanicsReturnFailureAndFenceOccurrence(t *testing.T) {
	for _, failRecord := range []bool{false, true} {
		t.Run(map[bool]string{false: "recorded", true: "record_write_failed"}[failRecord], func(t *testing.T) {
			connectionID, occurrenceID := uuid.NewString(), uuid.NewString()
			var captured, recorded int
			guard, err := newCallbackGuard(context.Background(), connectionID, occurrenceID,
				func(context.Context, any) error {
					captured++
					panic("private message must never appear in failure evidence")
				}, func(_ context.Context, failure callbackFailure) error {
					recorded++
					if failure != (callbackFailure{ConnectionID: connectionID, OccurrenceID: occurrenceID, Reason: "crash"}) {
						t.Fatalf("wrong failure authority: %+v", failure)
					}
					if failRecord {
						return errors.New("failure evidence write failed")
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if guard.receive(&events.Message{}) || guard.receive(&events.Message{}) {
				t.Fatal("panic or fenced occurrence reported successful capture")
			}
			if captured != 1 || recorded != 1 || guard.currentFailure() == nil {
				t.Fatalf("capture=%d record=%d failure=%v", captured, recorded, guard.currentFailure())
			}
			if strings.Contains(guard.currentFailure().Error(), "private message") {
				t.Fatal("private panic payload escaped")
			}
		})
	}
}

func TestWhatsAppCallbackFailureAndSuccessStatus(t *testing.T) {
	for _, failure := range []error{nil, errors.New("capture commit rejected"), fmt.Errorf("%w: %w", errSenderPhoneRead, errClientOccurrenceFenced)} {
		guard, err := newCallbackGuard(context.Background(), uuid.NewString(), uuid.NewString(),
			func(context.Context, any) error { return failure },
			func(context.Context, callbackFailure) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if got := guard.receive(&events.Message{}); got != (failure == nil) {
			t.Fatalf("success=%t failure=%v", got, failure)
		}
		if !errors.Is(guard.currentFailure(), failure) {
			t.Fatalf("failure evidence lost: %v", guard.currentFailure())
		}
	}
}

func TestWhatsAppUnsupportedContentDispositionKeepsHealthyCallback(t *testing.T) {
	for _, row := range []struct {
		name string
		err  error
	}{
		{"single", errIncomingUnsupported},
		{"wrapped", fmt.Errorf("content: %w", errIncomingUnsupported)},
		{"joined_content_only", errors.Join(errIncomingUnsupported, errIncomingUnsupported)},
	} {
		t.Run(row.name, func(t *testing.T) {
			recorded, handled := 0, 0
			guard, err := newCallbackGuard(context.Background(), uuid.NewString(), uuid.NewString(),
				func(context.Context, any) error {
					handled++
					if handled == 1 {
						return row.err
					}
					return nil
				}, func(context.Context, callbackFailure) error { recorded++; return nil })
			if err != nil {
				t.Fatal(err)
			}
			if !guard.receive(&events.Message{}) || !guard.receive(&events.Message{}) ||
				guard.currentFailure() != nil || guard.fenced || recorded != 0 || handled != 2 {
				t.Fatal("content refusal lost the healthy callback", handled, recorded, guard.currentFailure())
			}
		})
	}
}

func TestWhatsAppUnsupportedContentCannotHideCallbackFailure(t *testing.T) {
	for _, failure := range []error{errIncomingMalformed, errCaptureScopeChanged, errSessionAccount,
		errClientOccurrenceFenced, errRuntimeConnection, context.Canceled, context.DeadlineExceeded, errors.New("capture write failed")} {
		t.Run(failure.Error(), func(t *testing.T) {
			recorded := 0
			guard, err := newCallbackGuard(context.Background(), uuid.NewString(), uuid.NewString(),
				func(context.Context, any) error { return errors.Join(errIncomingUnsupported, failure) },
				func(_ context.Context, row callbackFailure) error {
					if row.Reason != "capture_failed" {
						t.Error("real failure changed its reason", row.Reason)
					}
					recorded++
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if guard.receive(&events.Message{}) || !guard.fenced || recorded != 1 || !errors.Is(guard.currentFailure(), failure) {
				t.Fatal("unsupported branch swallowed a real failure", recorded, guard.currentFailure())
			}
		})
	}
}

func TestWhatsAppUnsupportedDispositionDoesNotOverrideCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	guard, err := newCallbackGuard(ctx, uuid.NewString(), uuid.NewString(),
		func(context.Context, any) error { cancel(); return errIncomingUnsupported },
		func(context.Context, callbackFailure) error {
			t.Error("content refusal recorded a fatal failure")
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if guard.receive(&events.Message{}) {
		t.Fatal("canceled callback granted the SDK an acknowledgment")
	}
}

func TestWhatsAppFailureRecorderPanicCannotEscapeToSDK(t *testing.T) {
	guard, err := newCallbackGuard(context.Background(), uuid.NewString(), uuid.NewString(),
		func(context.Context, any) error { return errors.New("capture failed") },
		func(context.Context, callbackFailure) error { panic("private failure-record contents") })
	if err != nil {
		t.Fatal(err)
	}
	if guard.receive(&events.Message{}) || guard.currentFailure() == nil {
		t.Fatal("failure recorder panic became success")
	}
	if strings.Contains(guard.currentFailure().Error(), "private failure-record") {
		t.Fatal("failure recorder panic leaked its payload")
	}
}

type refusedWhatsAppTransport struct{}

func (refusedWhatsAppTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("account-free refused dial")}
}

func TestWhatsAppPinnedSDKDispatchUsesFailureReturningCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := whatsmeow.NewClient(&store.Device{}, nil)
	client.InitialAutoReconnect = true
	client.SetPreLoginHTTPClient(&http.Client{Transport: refusedWhatsAppTransport{}})
	recorded := make(chan callbackFailure, 1)
	var handled atomic.Int32
	connectionID, occurrenceID := uuid.NewString(), uuid.NewString()
	guard, err := newCallbackGuard(ctx, connectionID, occurrenceID,
		func(_ context.Context, event any) error {
			if _, ok := event.(*events.Disconnected); !ok {
				return nil
			}
			handled.Add(1)
			panic("before owned event capture")
		}, func(_ context.Context, failure callbackFailure) error {
			recorded <- failure
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	handlerID, err := guard.install(client)
	if err != nil || handlerID == 0 {
		t.Fatalf("register SDK callback: %d %v", handlerID, err)
	}
	defer client.RemoveEventHandler(handlerID)
	defer client.Disconnect()
	if err := client.ConnectContext(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case failure := <-recorded:
		if failure != (callbackFailure{ConnectionID: connectionID, OccurrenceID: occurrenceID, Reason: "crash"}) {
			t.Fatalf("wrong callback failure: %+v", failure)
		}
	case <-ctx.Done():
		t.Fatal("pinned SDK did not dispatch its real connection-failure event")
	}
	if handled.Load() != 1 || guard.currentFailure() == nil || guard.receive(&events.Message{}) {
		t.Fatal("SDK callback panic did not fence the occurrence")
	}
}

func TestWhatsAppCallbackRejectsIncompleteOwners(t *testing.T) {
	handle := func(context.Context, any) error { return nil }
	record := func(context.Context, callbackFailure) error { return nil }
	for _, args := range []struct {
		ctx        context.Context
		connection string
		occurrence string
		handle     func(context.Context, any) error
		record     func(context.Context, callbackFailure) error
	}{
		{nil, uuid.NewString(), uuid.NewString(), handle, record},
		{context.Background(), "", uuid.NewString(), handle, record},
		{context.Background(), uuid.NewString(), "", handle, record},
		{context.Background(), uuid.NewString(), uuid.NewString(), nil, record},
		{context.Background(), uuid.NewString(), uuid.NewString(), handle, nil},
	} {
		if _, err := newCallbackGuard(args.ctx, args.connection, args.occurrence, args.handle, args.record); err == nil {
			t.Fatal("incomplete callback owner accepted")
		}
	}
}
