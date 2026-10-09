package sessionprovider

import (
	"context"
	"errors"
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
					if failure != (callbackFailure{connectionID, occurrenceID, "crash"}) {
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
	for _, failure := range []error{nil, errors.New("capture commit rejected")} {
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
		if failure != (callbackFailure{connectionID, occurrenceID, "crash"}) {
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
