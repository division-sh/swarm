package startupownership

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type agentRecoverySeedSessionProbe struct {
	*retainedSessionProbe
	read          func(context.Context) (AgentRecoverySeedRead, error)
	authorityRead func() (Authority, error)
}

func (s *agentRecoverySeedSessionProbe) Authority() (Authority, error) {
	if s.authorityRead != nil {
		return s.authorityRead()
	}
	return s.retainedSessionProbe.Authority()
}

func (s *agentRecoverySeedSessionProbe) ReadAgentRecoverySeed(ctx context.Context) (AgentRecoverySeedRead, error) {
	return s.read(ctx)
}

func newAgentRecoverySeedCapability(t *testing.T) (*processCapability, *agentRecoverySeedSessionProbe, AgentRecoverySeedRead) {
	t.Helper()
	session, plan := testRetainedSession(t)
	reader := &agentRecoverySeedSessionProbe{retainedSessionProbe: session}
	capability, err := newProcessCapability(reader, time.Hour, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	read := AgentRecoverySeedRead{Process: session.authority, SourceHead: plan, InProgressByProfile: map[string]int{"anthropic": 1}}
	reader.read = func(context.Context) (AgentRecoverySeedRead, error) { return read, nil }
	return capability.(*processCapability), reader, read
}

func TestAgentRecoverySeedIsPrivateImmutableAndOneShot(t *testing.T) {
	p, session, read := newAgentRecoverySeedCapability(t)
	var calls atomic.Int32
	session.read = func(context.Context) (AgentRecoverySeedRead, error) {
		calls.Add(1)
		return read, nil
	}
	if err := SeedAgentRecovery(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	wantHead, _ := json.Marshal(read.SourceHead)
	seed := p.agentRecoverySeed
	if seed == nil || seed.process != read.Process || seed.sourceHead != string(wantHead) || !reflect.DeepEqual(seed.inProgressByProfile, read.InProgressByProfile) {
		t.Fatalf("seed lost its full image binding: %#v", seed)
	}
	read.InProgressByProfile["anthropic"] = 10
	read.SourceHead.Sources[0].BundleHash = startupBundleHashB
	if seed.inProgressByProfile["anthropic"] != 1 || seed.sourceHead != string(wantHead) {
		t.Fatal("private seed aliases the session result")
	}
	if err := SeedAgentRecovery(context.Background(), p); err == nil || calls.Load() != 1 || p.agentRecoverySeed != seed {
		t.Fatalf("duplicate reseeded/re-read: err=%v calls=%d", err, calls.Load())
	}
	if err := p.Release(context.Background()); err != nil || p.agentRecoverySeed != nil {
		t.Fatalf("release retained seed: err=%v seed=%#v", err, p.agentRecoverySeed)
	}
	if err := SeedAgentRecovery(context.Background(), p); err == nil || calls.Load() != 1 {
		t.Fatal("released process re-read its seed")
	}
}

func TestAgentRecoverySeedRefusesIncompleteAndForeignResults(t *testing.T) {
	for _, name := range []string{"nil_counts", "bad_count", "source", "lease", "generation", "owner", "boot", "runtime", "backend", "read_error"} {
		t.Run(name, func(t *testing.T) {
			p, session, read := newAgentRecoverySeedCapability(t)
			failure := errors.New("canonical image refused")
			var readErr error
			switch name {
			case "nil_counts":
				read.InProgressByProfile = nil
			case "bad_count":
				read.InProgressByProfile["anthropic"] = -1
			case "source":
				read.SourceHead.Revision = "foreign"
			case "lease":
				read.Process.AuthorityID = uuid.NewString()
			case "generation":
				read.Process.AuthorityGeneration++
			case "owner":
				read.Process.OwnerID = "foreign"
			case "boot":
				read.Process.BootID = uuid.NewString()
			case "runtime":
				read.Process.RuntimeInstanceID = uuid.NewString()
			case "backend":
				read.Process.Backend = "postgres_retained_session"
			case "read_error":
				readErr = failure
			}
			session.read = func(context.Context) (AgentRecoverySeedRead, error) { return read, readErr }
			if err := SeedAgentRecovery(context.Background(), p); err == nil || (readErr != nil && !errors.Is(err, failure)) || p.agentRecoverySeed != nil {
				t.Fatalf("refusal published a seed/lost error: err=%v seed=%#v", err, p.agentRecoverySeed)
			}
			if err := p.ProveCurrent(context.Background()); err != nil {
				t.Fatalf("domain refusal fenced a healthy probe: %v", err)
			}
		})
	}
}

func TestAgentRecoverySeedRejectsLateReturnAndJoinsRelease(t *testing.T) {
	for _, cut := range []string{"cancel", "terminal", "release"} {
		t.Run(cut, func(t *testing.T) {
			p, session, read := newAgentRecoverySeedCapability(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, finish := make(chan struct{}), make(chan struct{})
			session.read = func(context.Context) (AgentRecoverySeedRead, error) {
				close(entered)
				<-finish
				return read, nil
			}
			result := make(chan error, 1)
			go func() { result <- SeedAgentRecovery(ctx, p) }()
			<-entered
			if p.opMu.TryLock() {
				p.opMu.Unlock()
				t.Fatal("read does not hold the original process operation boundary")
			}
			var release chan error
			switch cut {
			case "cancel":
				cancel()
			case "terminal":
				p.SelectedStoreSessionTerminal(TerminalResult{Cause: TerminalOwnershipUnprovable})
			case "release":
				release = make(chan error, 1)
				go func() { release <- p.Release(context.Background()) }()
			}
			select {
			case err := <-result:
				t.Fatalf("operation returned before its held read joined: %v", err)
			default:
			}
			close(finish)
			err := <-result
			if cut == "release" {
				if releaseErr := <-release; releaseErr != nil {
					t.Fatal(releaseErr)
				}
			} else if err == nil {
				t.Fatal("late successful read published after cancellation/loss")
			}
			if p.agentRecoverySeed != nil {
				t.Fatal("canceled/terminal/released process retained a seed")
			}
		})
	}
}

func TestAgentRecoverySeedRefusesLossAtPublication(t *testing.T) {
	for _, cut := range []string{"cancel", "terminal"} {
		t.Run(cut, func(t *testing.T) {
			p, session, read := newAgentRecoverySeedCapability(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, finish := make(chan struct{}), make(chan struct{})
			session.authorityRead = func() (Authority, error) {
				close(entered)
				<-finish
				return read.Process, nil
			}
			result := make(chan error, 1)
			go func() { result <- SeedAgentRecovery(ctx, p) }()
			<-entered
			if cut == "cancel" {
				cancel()
			} else {
				p.SelectedStoreSessionTerminal(TerminalResult{Cause: TerminalOwnershipUnprovable})
			}
			close(finish)
			if err := <-result; err == nil || p.agentRecoverySeed != nil {
				t.Fatalf("publication ignored the final loss/cancel cut: err=%v seed=%#v", err, p.agentRecoverySeed)
			}
		})
	}
}

func TestAgentRecoverySeedValidEmptyIsNotFailureAsEmpty(t *testing.T) {
	p, session, read := newAgentRecoverySeedCapability(t)
	read.InProgressByProfile = map[string]int{}
	session.read = func(context.Context) (AgentRecoverySeedRead, error) { return read, nil }
	if err := SeedAgentRecovery(context.Background(), p); err != nil || p.agentRecoverySeed == nil || p.agentRecoverySeed.inProgressByProfile == nil || len(p.agentRecoverySeed.inProgressByProfile) != 0 {
		t.Fatalf("valid empty complete census refused: err=%v seed=%#v", err, p.agentRecoverySeed)
	}
}

func TestAgentRecoverySeedRequiresOriginalProcessAndContext(t *testing.T) {
	p, _, _ := newAgentRecoverySeedCapability(t)
	for _, tc := range []struct {
		ctx context.Context
		cap ProcessCapability
	}{
		{nil, p}, {context.Background(), nil},
		{context.Background(), struct{ ProcessCapability }{p}},
	} {
		if err := SeedAgentRecovery(tc.ctx, tc.cap); err == nil || p.agentRecoverySeed != nil {
			t.Fatal("missing context or substituted process owner was accepted")
		}
	}
}

func TestAgentRecoverySeedConcurrentDuplicateCannotOverwrite(t *testing.T) {
	p, session, read := newAgentRecoverySeedCapability(t)
	entered, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	session.read = func(context.Context) (AgentRecoverySeedRead, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-finish
		return read, nil
	}
	results := make(chan error, 2)
	go func() { results <- SeedAgentRecovery(context.Background(), p) }()
	<-entered
	go func() { results <- SeedAgentRecovery(context.Background(), p) }()
	close(finish)
	one, two := <-results, <-results
	if (one == nil) == (two == nil) || calls.Load() != 1 || p.agentRecoverySeed == nil || p.agentRecoverySeed.inProgressByProfile["anthropic"] != 1 {
		t.Fatalf("concurrent duplicate changed the seed: errors=%v / %v reads=%d seed=%#v", one, two, calls.Load(), p.agentRecoverySeed)
	}
}

func TestAgentRecoverySeedTerminalLossRevokesInstalledObservation(t *testing.T) {
	p, session, read := newAgentRecoverySeedCapability(t)
	var calls atomic.Int32
	session.read = func(context.Context) (AgentRecoverySeedRead, error) {
		calls.Add(1)
		return read, nil
	}
	if err := SeedAgentRecovery(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	p.SelectedStoreSessionTerminal(TerminalResult{Cause: TerminalOwnershipUnprovable})
	if p.agentRecoverySeed != nil {
		t.Fatal("terminal process retained its formerly installed private observation")
	}
	if err := SeedAgentRecovery(context.Background(), p); err == nil || calls.Load() != 1 {
		t.Fatal("terminal process re-read/reseeded after revocation")
	}
}
