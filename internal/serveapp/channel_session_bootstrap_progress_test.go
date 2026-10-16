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

func startHeldSessionRequest(t *testing.T, service *channelonboarding.Service, adapter *serveSessionBootstrap, cut *sessionConstructionCut, key string) (channelonboarding.Operation, func()) {
	t.Helper()
	caller, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	finished := make(chan error, 1)
	go func() {
		_, err := service.Start(caller, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
			Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: key})
		finished <- err
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Error("held original request lost its caller cancellation", err)
				}
			case <-time.After(time.Second):
				t.Error("held original request did not return promptly after cancellation")
			}
		})
	}
	t.Cleanup(stop)
	select {
	case <-cut.entered:
	case <-caller.Done():
		t.Fatal("original construction never entered its cut", context.Cause(caller))
	}
	adapter.mu.Lock()
	var op channelonboarding.Operation
	if len(adapter.connections) == 1 {
		for _, attempt := range adapter.connections {
			op = attempt.operation
		}
	}
	adapter.mu.Unlock()
	if op.OperationID == "" || op.Phase != channelonboarding.PhaseActivatingProvider || op.SessionConnectionID == "" {
		t.Fatal("held original request lost its exact admitted responsibility")
	}
	readback, err := service.Get(caller, op.OperationID)
	requireSessionPendingResult(t, readback, op, err)
	select {
	case err := <-finished:
		finished <- err
		t.Fatal("original request returned before its held constructor", err)
	default:
	}
	return op, stop
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
	if attempt.err == nil || !errors.Is(startErr, attempt.err) {
		t.Fatal("original request lost the private construction failure", startErr, attempt.err)
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
	op, cancel := startHeldSessionRequest(t, service, adapter, cut, "pending-private-preparation")
	requirePendingConcurrentSessionRequests(t, service, ctx, op)
	cancel()
	adapter.mu.Lock()
	attempt := adapter.connections[op.OperationID]
	adapter.mu.Unlock()
	if attempt == nil || selections.Load() != 1 {
		t.Fatal("pending requests substituted the original attempt", selections.Load())
	}
	if _, err := adapter.ChannelExecution(ctx, op); !errors.Is(err, channelonboarding.ErrSessionPending) {
		t.Fatal("pending preparation gained or lost its precise execution refusal", err)
	}
	if retirement {
		if _, err := adapter.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseFailed,
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
	requireExplicitSessionContinuation(t, service, adapter, ctx, join, op, attempt, selections)
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
	select {
	case got := <-finished:
		t.Fatal("original retry returned before its held construction", got.err)
	default:
	}
	requirePendingConcurrentSessionRequests(t, service, ctx, started.Operation)

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
	select {
	case got := <-finished:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatal("original retry lost its caller cancellation", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("original retry did not promptly cancel its construction wait")
	}
	release()
	join, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	select {
	case <-attempt.done:
	case <-join.Done():
		t.Fatal("released construction did not join", context.Cause(join))
	}
	requireExplicitSessionContinuation(t, service, adapter, ctx, join, started.Operation, attempt, selections)
}
