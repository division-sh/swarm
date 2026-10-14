//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

type sessionConstructionCutKey struct{}

type sessionConstructionStore interface {
	channelonboarding.Store
	channelonboarding.TeardownStore
	inboundpublication.Runner
	pipeline.StandingServicePersistence
	ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error)
	LoadOperatorChannelClaimReceipt(context.Context, string) (operatorchannel.ClaimReceipt, bool, error)
	SettleSessionChannelClaim(context.Context, authority.Claim) (operatorchannel.ClaimSettlement, error)
	HasCurrentChannelInputDraft(context.Context, operatorchannel.InboundText, time.Time) (bool, error)
}

// Only the selected construction context reaches this cut. Onboarding reads
// use the real selected store without blocking behind the private open owner.
type sessionConstructionCut struct {
	sessionConstructionStore
	entered, release chan struct{}
	armed            atomic.Bool
}

func installSessionConstructionCut(t *testing.T, adapter *serveSessionBootstrap) (*sessionConstructionCut, *atomic.Int64, func()) {
	t.Helper()
	cut := &sessionConstructionCut{sessionConstructionStore: adapter.store.(sessionConstructionStore), entered: make(chan struct{}), release: make(chan struct{})}
	cut.armed.Store(true)
	adapter.store = cut
	selectRuntime := adapter.selectRuntime
	selections := &atomic.Int64{}
	adapter.selectRuntime = func(ctx context.Context, candidate channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, processbinding.Binding, func() error, error) {
		owned, incoming, binding, release, err := selectRuntime(ctx, candidate)
		if err != nil {
			return owned, incoming, binding, release, err
		}
		selections.Add(1)
		return context.WithValue(owned, sessionConstructionCutKey{}, true), incoming, binding, release, nil
	}
	var once sync.Once
	release := func() { once.Do(func() { close(cut.release) }) }
	t.Cleanup(release)
	return cut, selections, release
}

func requireSessionPendingResult(t *testing.T, result channelonboarding.Result, retained channelonboarding.Operation, err error) {
	t.Helper()
	pending, pendingErr := result.SessionPreparationPending()
	if err != nil || pendingErr != nil || !pending || !reflect.DeepEqual(result.Operation, retained) || result.IdentityOperation != nil ||
		result.Binding != nil || result.Readiness != nil && result.Readiness.Ready {
		t.Fatal("pending response changed responsibility or gained executable/pairing authority", result, err)
	}
}

func TestServeSessionPendingConstructionFailureRemainsARefusalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			requireSessionPrivateOpenFailure(t, backend)
		})
	}
}

func TestServeSessionPendingRequestProgressAndExplicitContinuationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, retirement := range []bool{false, true} {
			name := "continue"
			if retirement {
				name = "retire"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				requireSessionPendingRequestProgress(t, backend, retirement)
			})
		}
	}
}

func (s *sessionConstructionCut) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	if owned, _ := ctx.Value(sessionConstructionCutKey{}).(bool); owned && s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return s.sessionConstructionStore.GetChannelOnboarding(ctx, id)
}

func TestServeSessionRetryProjectsPendingOwnedConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			requireSessionRetryPendingConstruction(t, backend)
		})
	}
}

func requirePendingConcurrentSessionRequests(t *testing.T, service *channelonboarding.Service, ctx context.Context, op channelonboarding.Operation) {
	t.Helper()
	request, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	readback, err := service.Get(request, op.OperationID)
	requireSessionPendingResult(t, readback, op, err)
	type retryResult struct {
		result channelonboarding.Result
		err    error
	}
	results := make(chan retryResult, 2)
	for range 2 {
		go func() {
			result, err := service.Retry(request, channelonboarding.RetryInput{OperationID: op.OperationID})
			results <- retryResult{result, err}
		}()
	}
	for range 2 {
		result := <-results
		requireSessionPendingResult(t, result.result, op, result.err)
	}
}

func requireExplicitSessionContinuation(t *testing.T, service *channelonboarding.Service, adapter *serveSessionBootstrap, ctx, join context.Context, op channelonboarding.Operation, attempt *serveSessionBootstrapAttempt, selections *atomic.Int64) {
	t.Helper()
	if attempt.err != nil || attempt.connection == nil {
		t.Fatal("original non-executable preparation failed", attempt.err)
	}
	if _, err := attempt.current(ctx); !errors.Is(err, channelonboarding.ErrSessionPending) {
		t.Fatal("completed storage preparation automatically launched SDK execution", err)
	}
	fresh, end := context.WithTimeout(ctx, 5*time.Second)
	continued, err := service.Retry(fresh, channelonboarding.RetryInput{OperationID: op.OperationID})
	end()
	if err != nil || !reflect.DeepEqual(continued.Operation, op) || continued.Pairing == nil ||
		continued.Operation.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || continued.Readiness != nil && continued.Readiness.Ready ||
		selections.Load() != 1 || adapter.connection(op.OperationID) != attempt.connection {
		t.Fatal("fresh explicit request failed to consume exactly the original preparation", continued, err, selections.Load())
	}
	if err := attempt.connection.Close(join); err != nil {
		t.Fatal("continued original SDK occurrence did not join", err)
	}
}

func requireSessionPrivateOpenFailure(t *testing.T, backend string) {
	t.Helper()
	ctx := context.Background()
	service, sessions := installedSessionBootstrapFixture(t, backend)
	adapter := sessions.(*serveSessionBootstrap)
	file, err := os.CreateTemp(t.TempDir(), "not-a-session-directory")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	adapter.directory = file.Name()
	caller, cancel := context.WithTimeout(ctx, 5*time.Second)
	started, startErr := service.Start(caller, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
		Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "private-open-failure"})
	cancel()
	if started.Operation.OperationID == "" {
		t.Fatal("failure lost its retained onboarding responsibility", startErr)
	}
	adapter.mu.Lock()
	attempt := adapter.connections[started.Operation.OperationID]
	adapter.mu.Unlock()
	if attempt == nil {
		t.Fatal("failure lost its original preparation owner", startErr)
	}
	join, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	select {
	case <-attempt.done:
	case <-join.Done():
		t.Fatal("failed private open did not join", context.Cause(join))
	}
	if attempt.err == nil {
		t.Fatal("invalid private directory became preparation success")
	}
	for range 2 {
		readback, err := service.Get(join, started.Operation.OperationID)
		if !errors.Is(err, attempt.err) || readback.Pairing != nil || readback.Readiness != nil && readback.Readiness.Ready ||
			readback.Operation.OperationID != started.Operation.OperationID || readback.Operation.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
			t.Fatal("readback discarded a construction error or fabricated successful authority", readback, err, attempt.err)
		}
	}
	adapter.mu.Lock()
	unchanged := adapter.connections[started.Operation.OperationID] == attempt
	adapter.mu.Unlock()
	if !unchanged {
		t.Fatal("failed preparation automatically started a successor")
	}
}

func requireSessionPendingRequestProgress(t *testing.T, backend string, retirement bool) {
	t.Helper()
	ctx := context.Background()
	service, sessions := installedSessionBootstrapFixture(t, backend)
	adapter := sessions.(*serveSessionBootstrap)
	cut, selections, release := installSessionConstructionCut(t, adapter)
	caller, cancel := context.WithTimeout(ctx, 5*time.Second)
	started, err := service.Start(caller, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
		Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "pending-private-preparation"})
	if err != nil || started.Operation.Phase != channelonboarding.PhaseActivatingProvider || started.Operation.SessionConnectionID == "" {
		cancel()
		t.Fatal("Start did not retain admitted pending responsibility", started, err)
	}
	requireSessionPendingResult(t, started, started.Operation, err)
	select {
	case <-cut.entered:
	case <-caller.Done():
		cancel()
		t.Fatal("owned construction never entered the barrier", context.Cause(caller))
	}
	cancel()
	requirePendingConcurrentSessionRequests(t, service, ctx, started.Operation)
	adapter.mu.Lock()
	attempt := adapter.connections[started.Operation.OperationID]
	adapter.mu.Unlock()
	if attempt == nil || selections.Load() != 1 {
		t.Fatal("pending requests substituted the original attempt", selections.Load())
	}
	if _, err := adapter.ChannelExecution(ctx, started.Operation); !errors.Is(err, channelonboarding.ErrSessionPending) {
		t.Fatal("pending preparation gained or lost its precise execution refusal", err)
	}
	if retirement {
		if _, err := adapter.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
			OperationID: started.Operation.OperationID, ExpectedRevision: started.Operation.Revision, Phase: channelonboarding.PhaseFailed,
			FailureCode: "pending_preparation_retired", FailureMessage: "bounded retirement proof", Now: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		join, done := context.WithTimeout(ctx, 50*time.Millisecond)
		err := adapter.RetireInactiveSessions(join)
		done()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("retirement released unresolved construction", err)
		}
	}
	release()
	join, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	select {
	case <-attempt.done:
	case <-join.Done():
		t.Fatal("private preparation failed to join", context.Cause(join))
	}
	if retirement {
		if attempt.err == nil || adapter.RetireInactiveSessions(join) != nil || selections.Load() != 1 {
			t.Fatal("retired preparation executed or replaced its original", attempt.err, selections.Load())
		}
		return
	}
	requireExplicitSessionContinuation(t, service, adapter, ctx, join, started.Operation, attempt, selections)
}

func requireSessionRetryPendingConstruction(t *testing.T, backend string) {
	t.Helper()
	ctx := context.Background()
	service, sessions := installedSessionBootstrapFixture(t, backend)
	adapter := sessions.(*serveSessionBootstrap)
	started, err := service.Start(ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
		Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "owned-construction-progress"})
	if err != nil || started.Operation.Phase != channelonboarding.PhaseActivatingProvider {
		t.Fatal("original onboarding did not establish its exact reservation", started, err)
	}
	if err := adapter.AwaitSessionBootstrap(ctx, started.Operation); err != nil {
		t.Fatal("initial fixture bootstrap did not complete through its owner", err)
	}
	original := adapter.connection(started.Operation.OperationID)
	if original == nil {
		t.Fatal("original SDK connection missing")
	}
	if err := original.Close(ctx); err != nil {
		t.Fatal("original connection did not join before explicit retry", err)
	}
	cut, selections, release := installSessionConstructionCut(t, adapter)
	caller, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	type retryResult struct {
		result channelonboarding.Result
		err    error
	}
	finished := make(chan retryResult, 1)
	requested := time.Now()
	go func() {
		result, err := service.Retry(caller, channelonboarding.RetryInput{OperationID: started.Operation.OperationID})
		finished <- retryResult{result: result, err: err}
	}()
	select {
	case <-cut.entered:
	case result := <-finished:
		t.Fatal("retry did not reach its admitted construction cut", result.result, result.err)
	case <-caller.Done():
		t.Fatal("retry never selected its owned construction", context.Cause(caller))
	}
	got := <-finished
	t.Logf("onboarding_retry call=1 phase=owned_open elapsed=%s private_open_released=false err=%v", time.Since(requested), got.err)
	if got.err != nil || caller.Err() != nil {
		t.Error("onboarding_retry waits for owned construction instead of projecting pending", got.err, context.Cause(caller))
	}
	if got.err == nil {
		requireSessionPendingResult(t, got.result, started.Operation, nil)
	}

	adapter.mu.Lock()
	attempt := adapter.connections[started.Operation.OperationID]
	adapter.mu.Unlock()
	if attempt == nil || selections.Load() != 1 {
		t.Fatal("retry lost or replaced its single admitted attempt", selections.Load())
	}
	select {
	case <-attempt.done:
		t.Error("blocked construction reported a completed attempt")
	default:
	}
	cancel()
	release()
	join, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	select {
	case <-attempt.done:
	case <-join.Done():
		t.Fatal("released construction did not join", context.Cause(join))
	}
	if connection := adapter.connection(started.Operation.OperationID); connection != nil {
		if err := connection.Close(join); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal("original attempt retained unresolved private work", err)
		}
	}
}
