package channelonboarding

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestOnboardingQueuedResumeHonorsCallerCancellation(t *testing.T) {
	id := uuid.NewString()
	s := &Service{store: &cancellationTestStore{op: Operation{OperationID: id, Phase: PhasePreparing}}}
	unlock, err := s.lockDrive(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			unlock()
		}
	}()
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.Retry(caller, RetryInput{OperationID: id, ExpectedLocaleRevision: 1})
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		s.driveMu.Lock()
		queued := s.driveLocks[id].refs == 2
		s.driveMu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resume did not reach the existing operation serializer")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Error("queued resume lost cancellation", err)
		}
	case <-time.After(time.Second):
		t.Error("queued resume waited for another operation driver's completion")
		unlock()
		released = true
		<-done
	}
	if !released {
		unlock()
		released = true
	}
	s.driveMu.Lock()
	defer s.driveMu.Unlock()
	if len(s.driveLocks) != 0 {
		t.Fatal("queued cancellation stranded drive ownership")
	}
}
