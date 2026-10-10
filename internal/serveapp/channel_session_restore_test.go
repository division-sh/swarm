//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/internal/testutil/whatsappfixture"
	"github.com/google/uuid"
)

func seedServePairedDevice(t *testing.T, f *serveBootstrapTestFixture) string {
	t.Helper()
	if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err != nil {
		t.Fatal(err)
	}
	if err := f.adapter.connection(f.op.OperationID).Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	fixture, owner, err := sessionstate.OpenSDKFixture(f.ctx, filepath.Join(f.adapter.directory, f.op.SessionConnectionID, "provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	device := whatsappfixture.PairedDevice(t, owner)
	account := device.ID.ToNonAD().String()
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	return account
}

func TestServeSessionExplicitResumeRetainsPairedIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			serveBootstrapWireFixture(t, "paired", f.owner)
			accountRef := seedServePairedDevice(t, f)
			if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err == nil {
				t.Fatal("ordinary cache reuse silently reconnected a closed owner")
			}
			if err := f.adapter.ResumeSession(f.ctx, f.op, f.candidate); err != nil {
				t.Fatal("explicit original-reservation retry failed", err)
			}
			paired, ok, err := f.adapter.CheckpointSessionPairing(f.ctx, f.op)
			if err != nil || !ok || paired.SessionAccount.AccountRef != accountRef {
				t.Fatal("original private SDK account did not checkpoint", paired, ok, err)
			}
			headerPath := filepath.Join(f.adapter.directory, f.op.SessionConnectionID, "session.json")
			header, err := os.ReadFile(headerPath)
			if err != nil {
				t.Fatal(err)
			}
			original := f.adapter.connection(f.op.OperationID)
			if err := f.adapter.ResumeSession(f.ctx, paired, f.candidate); err != nil || f.adapter.connection(f.op.OperationID) != original {
				t.Fatal("healthy paired resume replaced its connection", err)
			}
			if err := original.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.adapter.ResumeSession(f.ctx, paired, f.candidate); err != nil {
				t.Fatal("explicit paired resume failed", err)
			}
			current := f.adapter.connection(f.op.OperationID)
			if current == original || current.ConnectionID() != paired.SessionConnectionID {
				t.Fatal("resume lost original connection reservation")
			}
			admission, err := f.adapter.AdmitSessionAccount(f.ctx, paired.SessionAccount)
			if err != nil {
				t.Fatal("restored SDK did not verify its original exact admission", err)
			}
			admission.Close()
			retained, err := f.adapter.store.GetChannelOnboarding(f.ctx, paired.OperationID)
			retainedHeader, headerErr := os.ReadFile(headerPath)
			if err != nil || headerErr != nil || retained.Revision != paired.Revision || retained.SessionAccount != paired.SessionAccount ||
				retained.ActivationRevision != 0 || retained.IdentityOperationID != "" || retained.Coordinate.TargetGeneration != 0 || !bytes.Equal(header, retainedHeader) {
				t.Fatal("restoration adopted identity or manufactured executable admission", retained, err, headerErr)
			}
			for index := 0; index < 2; index++ {
				if err := f.adapter.ResumeSession(f.ctx, paired, f.candidate); err != nil || f.adapter.connection(f.op.OperationID) != current {
					t.Fatal("repeated resume created another socket", err)
				}
			}
			if f.selections.Load() != 3 {
				t.Fatal("resume selected an unexpected number of owners", f.selections.Load())
			}
			if err := current.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServeSessionResumeRejectsContradictoryResponsibilityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			for _, mutate := range []func(*channelonboarding.Operation){
				func(op *channelonboarding.Operation) { op.Revision++ },
				func(op *channelonboarding.Operation) { op.PrincipalID = uuid.NewString() },
				func(op *channelonboarding.Operation) { op.SessionConnectionID = uuid.NewString() },
				func(op *channelonboarding.Operation) { op.Phase = channelonboarding.PhaseRetired },
				func(op *channelonboarding.Operation) { op.Phase = channelonboarding.PhasePreparing },
				func(op *channelonboarding.Operation) { op.TargetSelector = "ingress:foreign:whatsapp" },
				func(op *channelonboarding.Operation) { op.Posture = channelonboarding.ActivationWebhookRegistration },
				func(op *channelonboarding.Operation) { op.Coordinate.RuntimeInstanceID = uuid.NewString() },
			} {
				op := f.op
				mutate(&op)
				if err := f.adapter.ResumeSession(f.ctx, op, f.candidate); err == nil {
					t.Fatal("contradictory responsibility was restored", op)
				}
			}
			caller, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := f.adapter.ResumeSession(caller, f.op, f.candidate); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled resume acquired a connection", err)
			}
			if f.selections.Load() != 0 || f.adapter.connection(f.op.OperationID) != nil {
				t.Fatal("refused resume selected a runtime")
			}
			if entries, err := os.ReadDir(f.adapter.directory); err != nil || len(entries) != 0 {
				t.Fatal("refused resume opened state", entries, err)
			}
		})
	}
}

type serveSessionReadFaultStore struct {
	channelonboarding.Store
	reads  atomic.Int64
	failAt atomic.Int64
	cause  error
}

func (s *serveSessionReadFaultStore) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	if n := s.reads.Add(1); s.failAt.Load() != 0 && n == s.failAt.Load() {
		return channelonboarding.Operation{}, s.cause
	}
	return s.Store.GetChannelOnboarding(ctx, id)
}

func TestServeSessionResumePreservesObservationErrorWithoutReconnectBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			fault := &serveSessionReadFaultStore{Store: f.adapter.store, cause: errors.New("selected resume observation failed")}
			f.adapter.store = fault
			serveBootstrapWireFixture(t, "paired", f.owner)
			seedServePairedDevice(t, f)
			if err := f.adapter.ResumeSession(f.ctx, f.op, f.candidate); err != nil {
				t.Fatal(err)
			}
			paired, ok, err := f.adapter.CheckpointSessionPairing(f.ctx, f.op)
			if err != nil || !ok {
				t.Fatal(paired, ok, err)
			}
			original, selections := f.adapter.connection(f.op.OperationID), f.selections.Load()
			fault.reads.Store(0)
			fault.failAt.Store(2) // request validation passes; genuine connected reuse then observes the failure.
			if err := f.adapter.ResumeSession(f.ctx, paired, f.candidate); !errors.Is(err, fault.cause) {
				t.Fatal("selected I/O failure became a reconnect instruction", err)
			}
			fault.failAt.Store(0)
			if f.adapter.connection(f.op.OperationID) != original || f.selections.Load() != selections || original.CheckBootstrap(f.ctx) != nil {
				t.Fatal("observation error retired or replaced the healthy owner")
			}
			if err := original.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServeSessionResumeCancellationRetainsJoinAndPossessionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			entered := serveBootstrapWireFixture(t, "blocked_handshake", f.owner)
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- f.adapter.ResumeSession(caller, f.op, f.candidate) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				cancel()
				<-done
				t.Fatal("resume did not enter the actual SDK handshake")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("initial resume lost caller cancellation", err)
				}
			case <-time.After(time.Second):
				<-done
				t.Fatal("initial resume ignored caller cancellation")
			}
			old := f.adapter.connection(f.op.OperationID)
			if old == nil {
				t.Fatal("canceled resume lost its partial cleanup owner")
			}
			second, stop := context.WithTimeout(f.ctx, 100*time.Millisecond)
			err := f.adapter.ResumeSession(second, f.op, f.candidate)
			stop()
			if !errors.Is(err, context.DeadlineExceeded) || f.selections.Load() != 1 || f.adapter.connection(f.op.OperationID) != old {
				t.Fatal("overlapping resume replaced unresolved predecessor", err, f.selections.Load())
			}
			other, err := sessionprovider.OpenRuntimeBootstrap(f.owned, sessionprovider.RuntimeConnectionOptions{
				Directory: f.adapter.directory, OperationID: f.op.OperationID, Store: f.adapter.store, Plan: f.candidate.Plan})
			if other != nil {
				if err := other.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err == nil {
				t.Fatal("successor possessed state before original SDK join")
			}
			join, finish := context.WithTimeout(f.ctx, 30*time.Second)
			defer finish()
			if err := old.Close(join); err != nil {
				t.Fatal("original counted cleanup did not complete", err)
			}
			if err := f.owner.WaitForQuiescence(join); err != nil {
				t.Fatal("completed join left finite runtime work", err)
			}
			current, err := sessionprovider.OpenRuntimeBootstrap(f.owned, sessionprovider.RuntimeConnectionOptions{
				Directory: f.adapter.directory, OperationID: f.op.OperationID, Store: f.adapter.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal("completed join did not release original possession", err)
			}
			if err := current.Close(join); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServeSessionConcurrentResumeConstructsOneSuccessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			serveBootstrapWireFixture(t, "paired", f.owner)
			seedServePairedDevice(t, f)
			if err := f.adapter.ResumeSession(f.ctx, f.op, f.candidate); err != nil {
				t.Fatal(err)
			}
			paired, ok, err := f.adapter.CheckpointSessionPairing(f.ctx, f.op)
			if err != nil || !ok {
				t.Fatal(paired, ok, err)
			}
			if err := f.adapter.connection(f.op.OperationID).Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			start, results := make(chan struct{}), make(chan error, 6)
			var workers sync.WaitGroup
			for i := 0; i < 6; i++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					results <- f.adapter.ResumeSession(f.ctx, paired, f.candidate)
				}()
			}
			close(start)
			workers.Wait()
			close(results)
			for err := range results {
				if err != nil {
					t.Error("concurrent explicit resume failed", err)
				}
			}
			if f.selections.Load() != 3 {
				t.Fatal("concurrent resume installed duplicate SDK owners", f.selections.Load())
			}
			if err := f.adapter.connection(f.op.OperationID).Close(f.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
