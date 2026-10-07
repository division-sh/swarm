package whatsapp

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestWhatsAppCaptureAdmittedCallbackRetainsCommitAcrossFenceAndReopen(t *testing.T) {
	event := captureFixture(t)
	path := filepath.Join(t.TempDir(), "incoming.db")
	db, capture := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	entered, release := make(chan struct{}), make(chan struct{})
	guard, err := newCallbackGuard(context.Background(), event.Scope.Session.ConnectionID, event.OccurrenceID,
		func(ctx context.Context, _ any) error {
			close(entered)
			<-release
			return capture.capture(ctx, event)
		}, capture.recordFailure)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan bool, 1)
	go func() { result <- guard.receive(event) }()
	<-entered
	guard.fence()
	select {
	case <-guard.drained:
		t.Fatal("callback join released private state before admitted capture completed")
	default:
	}
	close(release)
	if <-result {
		t.Fatal("retired callback reported an acknowledgment despite its later commit")
	}
	if err := guard.join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, reopened := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	pending, err := reopened.pending(context.Background())
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], event) {
		t.Fatalf("admitted capture disappeared or adopted new authority: %+v, %v", pending, err)
	}
	if guard.receive(event) {
		t.Fatal("old callback reopened after provider database reopen")
	}
	redelivery := event
	redelivery.OccurrenceID = uuid.NewString()
	if err := reopened.capture(context.Background(), redelivery); err != nil {
		t.Fatal(err)
	}
	pending, err = reopened.pending(context.Background())
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], event) {
		t.Fatalf("redelivery rewrote original captured occurrence: %+v, %v", pending, err)
	}
}
