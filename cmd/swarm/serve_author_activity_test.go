package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type serveStoryReadResult struct {
	page runtimeauthoractivity.ListResult
	err  error
}

type scriptedServeStoryReader struct {
	mu        sync.Mutex
	responses []serveStoryReadResult
	calls     []runtimeauthoractivity.ListOptions
}

type mutableServeAuthorActivityScope struct {
	mu     sync.RWMutex
	hashes []string
}

func (s *mutableServeAuthorActivityScope) BundleHashes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.hashes...)
}

func (s *mutableServeAuthorActivityScope) replace(hashes ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hashes = append([]string(nil), hashes...)
}

func (r *scriptedServeStoryReader) HeadAuthorActivity(context.Context) (int64, error) { return 0, nil }

func (r *scriptedServeStoryReader) ListAuthorActivity(_ context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, opts)
	if len(r.responses) == 0 {
		return runtimeauthoractivity.ListResult{}, nil
	}
	response := r.responses[0]
	r.responses = r.responses[1:]
	return response.page, response.err
}

func (r *scriptedServeStoryReader) snapshotCalls() []runtimeauthoractivity.ListOptions {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtimeauthoractivity.ListOptions(nil), r.calls...)
}

type failOnceWriter struct {
	mu     sync.Mutex
	failed bool
	out    bytes.Buffer
}

type synchronizedBuffer struct {
	mu  sync.Mutex
	out bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.out.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.out.String()
}

func (w *failOnceWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.failed {
		w.failed = true
		return 0, io.ErrClosedPipe
	}
	return w.out.Write(p)
}

func (w *failOnceWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.out.String()
}

func TestServeAuthorActivityFollowerRetriesUnchangedCursorAndExactScope(t *testing.T) {
	withFastServeStoryPolling(t)
	valid := serveStoryOccurrence(t, 11, "delivered", time.Date(2026, 7, 14, 19, 9, 2, 0, time.UTC))
	invalid := valid
	invalid.Sequence = 11
	invalid.Kind = "unregistered"
	reader := &scriptedServeStoryReader{responses: []serveStoryReadResult{
		{err: errors.New("temporary read failure")},
		{err: errors.New("temporary read failure")},
		{page: runtimeauthoractivity.ListResult{Occurrences: []runtimeauthoractivity.Occurrence{invalid}, NextCursor: 11}},
		{page: runtimeauthoractivity.ListResult{Occurrences: []runtimeauthoractivity.Occurrence{valid}, NextCursor: 11}},
	}}
	var errOut bytes.Buffer
	out := &synchronizedBuffer{}
	presenter := newServeLifecyclePresenter(serveOptions{Output: out, ErrorOutput: &errOut, Dev: true})
	ctx, cancel := context.WithCancel(context.Background())
	scope := &mutableServeAuthorActivityScope{hashes: []string{"bundle-a", "bundle-b"}}
	follower := newServeAuthorActivityFollower(
		ctx, reader, presenter, "runtime-a", scope, 10,
		runtimeauthoractivity.NewHumanRenderer(runtimeauthoractivity.RenderOptions{Mode: runtimeauthoractivity.RenderPlain, Width: 120}),
	)
	waitForServeStory(t, func() bool { return strings.Contains(out.String(), "telegram-sender ✓ sent") })
	waitForServeStory(t, func() bool { return len(reader.snapshotCalls()) >= 5 })
	cancel()
	follower.StopAndWait()

	calls := reader.snapshotCalls()
	if len(calls) < 5 {
		t.Fatalf("list calls = %d, want retry and post-accept poll", len(calls))
	}
	for i := 0; i < 4; i++ {
		if calls[i].AfterSequence != 10 {
			t.Fatalf("call %d cursor = %d, want unchanged 10", i, calls[i].AfterSequence)
		}
	}
	if calls[4].AfterSequence != 11 {
		t.Fatalf("post-accept cursor = %d, want 11", calls[4].AfterSequence)
	}
	for i, call := range calls {
		if call.RuntimeInstanceID != "runtime-a" || strings.Join(call.BundleHashes, ",") != "bundle-a,bundle-b" || !call.IncludeRuntimeScope || call.Limit != serveAuthorActivityPageSize {
			t.Fatalf("call %d scope = %#v", i, call)
		}
	}
	if got := strings.Count(errOut.String(), "temporary read failure"); got != 1 {
		t.Fatalf("read warning count = %d, stderr=%q", got, errOut.String())
	}
	if !strings.Contains(errOut.String(), "unregistered") || strings.Count(errOut.String(), "swarm logs --follow") != 2 {
		t.Fatalf("warning classes/fallback = %q", errOut.String())
	}
}

func TestServeAuthorActivityFollowerRetriesWriteAndFlushesBeforeReturn(t *testing.T) {
	withFastServeStoryPolling(t)
	first := serveStoryOccurrence(t, 21, "failed", time.Date(2026, 7, 14, 19, 9, 5, 0, time.UTC))
	second := first
	second.OccurrenceID = uuid.NewString()
	second.Sequence = 22
	second.SourceIdentity = "delivery-22"
	second.DedupKey = "delivery-22:failed"
	second.OccurredAt = first.OccurredAt.Add(time.Second)
	third := second
	third.OccurrenceID = uuid.NewString()
	third.Sequence = 23
	third.SourceIdentity = "delivery-23"
	third.DedupKey = "delivery-23:failed"
	third.OccurredAt = first.OccurredAt.Add(2 * time.Second)
	reader := &scriptedServeStoryReader{responses: []serveStoryReadResult{
		{page: runtimeauthoractivity.ListResult{Occurrences: []runtimeauthoractivity.Occurrence{first, second, third}, NextCursor: 23}},
		{page: runtimeauthoractivity.ListResult{Occurrences: []runtimeauthoractivity.Occurrence{first, second, third}, NextCursor: 23}},
	}}
	out := &failOnceWriter{}
	var errOut bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: out, ErrorOutput: &errOut, Dev: true})
	scope := &mutableServeAuthorActivityScope{hashes: []string{"bundle-a"}}
	follower := newServeAuthorActivityFollower(
		context.Background(), reader, presenter, "runtime-a", scope, 20,
		runtimeauthoractivity.NewHumanRenderer(runtimeauthoractivity.RenderOptions{Mode: runtimeauthoractivity.RenderPlain, Width: 120}),
	)
	waitForServeStory(t, func() bool { return strings.Contains(out.String(), "(2nd time)") })
	follower.StopAndWait()

	if !strings.Contains(out.String(), "(×1 in 2s)") {
		t.Fatalf("shutdown did not flush pending repeat: %q", out.String())
	}
	if got := strings.Count(errOut.String(), io.ErrClosedPipe.Error()); got != 1 {
		t.Fatalf("write warning count = %d, stderr=%q", got, errOut.String())
	}
	calls := reader.snapshotCalls()
	if len(calls) < 2 || calls[0].AfterSequence != 20 || calls[1].AfterSequence != 20 {
		t.Fatalf("write retry cursors = %#v", calls)
	}
}

func TestServeAuthorActivityFollowerRefreshesExactScopeAfterRuntimeReload(t *testing.T) {
	withFastServeStoryPolling(t)
	initial := serveStoryOccurrence(t, 31, "delivered", time.Date(2026, 7, 14, 19, 9, 2, 0, time.UTC))
	replacement := initial
	replacement.OccurrenceID = uuid.NewString()
	replacement.Sequence = 32
	replacement.SourceIdentity = "delivery-replacement"
	replacement.DedupKey = "delivery-replacement:delivered"
	replacement.Scope = runtimeauthoractivity.BundleScope("runtime-a", "bundle-replacement")
	replacement.AuthorSafeSummary = "replacement activity"
	unrelated := replacement
	unrelated.OccurrenceID = uuid.NewString()
	unrelated.Sequence = 33
	unrelated.SourceIdentity = "delivery-unrelated"
	unrelated.DedupKey = "delivery-unrelated:delivered"
	unrelated.Scope = runtimeauthoractivity.BundleScope("runtime-a", "bundle-unrelated")
	unrelated.AuthorSafeSummary = "must stay hidden"

	reader := &scopeFilteringServeStoryReader{occurrences: []runtimeauthoractivity.Occurrence{initial, replacement, unrelated}}
	scope := &mutableServeAuthorActivityScope{hashes: []string{"bundle-a"}}
	out := &synchronizedBuffer{}
	var errOut bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: out, ErrorOutput: &errOut, Dev: true})
	follower := newServeAuthorActivityFollower(
		context.Background(), reader, presenter, "runtime-a", scope, 30,
		runtimeauthoractivity.NewHumanRenderer(runtimeauthoractivity.RenderOptions{Mode: runtimeauthoractivity.RenderPlain, Width: 120}),
	)
	waitForServeStory(t, func() bool { return strings.Contains(out.String(), "how are you") })
	scope.replace("bundle-replacement")
	waitForServeStory(t, func() bool { return strings.Contains(out.String(), "replacement activity") })
	follower.StopAndWait()

	if strings.Contains(out.String(), "must stay hidden") {
		t.Fatalf("unrelated bundle activity leaked into feed: %q", out.String())
	}
	calls := reader.snapshotCalls()
	if !serveStoryCallsContainExactScope(calls, "bundle-a") || !serveStoryCallsContainExactScope(calls, "bundle-replacement") {
		t.Fatalf("follower scopes = %#v, want initial and replacement bundle hashes", calls)
	}
	for _, call := range calls {
		if strings.Join(call.BundleHashes, ",") == "bundle-unrelated" {
			t.Fatalf("follower admitted unrelated scope: %#v", call)
		}
	}
}

type scopeFilteringServeStoryReader struct {
	mu          sync.Mutex
	occurrences []runtimeauthoractivity.Occurrence
	calls       []runtimeauthoractivity.ListOptions
}

func (r *scopeFilteringServeStoryReader) HeadAuthorActivity(context.Context) (int64, error) {
	return 0, nil
}

func (r *scopeFilteringServeStoryReader) ListAuthorActivity(_ context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, opts)
	allowed := make(map[string]struct{}, len(opts.BundleHashes))
	for _, hash := range opts.BundleHashes {
		allowed[hash] = struct{}{}
	}
	result := runtimeauthoractivity.ListResult{NextCursor: opts.AfterSequence}
	for _, occurrence := range r.occurrences {
		if occurrence.Sequence <= opts.AfterSequence || occurrence.Scope.RuntimeInstanceID != opts.RuntimeInstanceID {
			continue
		}
		if _, ok := allowed[occurrence.Scope.BundleHash]; !ok {
			continue
		}
		result.Occurrences = append(result.Occurrences, occurrence)
		result.NextCursor = occurrence.Sequence
	}
	return result, nil
}

func (r *scopeFilteringServeStoryReader) snapshotCalls() []runtimeauthoractivity.ListOptions {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtimeauthoractivity.ListOptions(nil), r.calls...)
}

func serveStoryCallsContainExactScope(calls []runtimeauthoractivity.ListOptions, hash string) bool {
	for _, call := range calls {
		if strings.Join(call.BundleHashes, ",") == hash {
			return true
		}
	}
	return false
}

func TestServeNoFeedRequiresDevBeforeSideEffects(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runServeRuntime(context.Background(), t.TempDir(), serveOptions{
		NoFeed: true, Output: &out, ErrorOutput: &errOut,
	})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	combined := out.String() + errOut.String()
	if !strings.Contains(combined, "--no-feed requires --dev") {
		t.Fatalf("output = %q", combined)
	}
	for _, forbidden := range []string{"config load", "store", "listener", "ready"} {
		if strings.Contains(strings.ToLower(combined), forbidden) {
			t.Fatalf("pre-admission failure crossed into %q side effect: %q", forbidden, combined)
		}
	}
}

func serveStoryOccurrence(t *testing.T, sequence int64, transition string, at time.Time) runtimeauthoractivity.Occurrence {
	t.Helper()
	occurrence := runtimeauthoractivity.Occurrence{
		OccurrenceID: uuid.NewString(), Sequence: sequence, Kind: runtimeauthoractivity.KindDeliveryLifecycle,
		Version: runtimeauthoractivity.Version, Transition: transition, SourceOwner: "event_deliveries",
		SourceIdentity: "delivery-" + uuid.NewString(), DedupKey: "delivery:" + uuid.NewString(), OccurredAt: at,
		Scope:             runtimeauthoractivity.BundleScope("runtime-a", "bundle-a"),
		Projection:        runtimeauthoractivity.Projection{SubjectType: "agent", SubjectID: "telegram-sender", EventType: "phrase.completed"},
		AuthorSafeSummary: "how are you",
	}
	if transition == "failed" {
		failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_unavailable", "test", "serve_story", nil))
		if !ok {
			t.Fatal("construct failure envelope")
		}
		occurrence.Projection.SubjectID = "telegram-normalizer"
		occurrence.Failure = &failure
		occurrence.RunID = "99e0d8c2-4e75-4e55-a17c-2b887c2a6f31"
	}
	return occurrence
}

func withFastServeStoryPolling(t *testing.T) {
	t.Helper()
	previous := serveAuthorActivityPollInterval
	serveAuthorActivityPollInterval = time.Millisecond
	t.Cleanup(func() { serveAuthorActivityPollInterval = previous })
}

func waitForServeStory(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for serve author story")
}
