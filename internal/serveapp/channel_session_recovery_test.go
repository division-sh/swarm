//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/google/uuid"
)

type recoveryClockOwner struct {
	serveSessionBootstrapOwner
	op        channelonboarding.Operation
	at        time.Time
	connected bool
	failure   error
}

func (s *recoveryClockOwner) registeredSessionConnections(context.Context) ([]serveSessionDiagnostic, error) {
	return []serveSessionDiagnostic{{Operation: s.op, Account: "+100000000001", Fact: sessionprovider.SessionDiagnostic{ConnectionID: s.op.SessionConnectionID}}}, nil
}

func (s *recoveryClockOwner) sessionRecovery(context.Context, channelonboarding.Operation) (time.Time, bool, error) {
	return s.at, s.connected, s.failure
}

// Invariant: R158 intervals, completion anchors and reset; pure scheduler inputs, NOT native eligibility proof.
func TestServeSessionRecoveryClockSchedule(t *testing.T) {
	now := time.Unix(1000000, 0)
	owner := &recoveryClockOwner{at: now, op: channelonboarding.Operation{OperationID: uuid.NewString(), SessionConnectionID: uuid.NewString(), DeploymentRegistration: true}}
	var out bytes.Buffer
	d := &serveChannelDeliveryDispatcher{sessions: owner, onboarding: &channelonboarding.Service{}, output: &out,
		sessionStates: map[string]string{}, sessionAttempts: map[string]int{}, sessionNextAttempt: map[string]time.Time{}, now: func() time.Time { return now }}
	calls := 0
	d.deployment = &serveDeploymentTransport{resume: func(context.Context, channelonboarding.Operation) error {
		calls++
		now = now.Add(3 * time.Second)
		return channelonboarding.ErrSessionUnavailable
	}}
	poll := func() {
		t.Helper()
		if err := d.reconcileSessionConnections(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	}
	poll()
	if calls != 0 || d.sessionNextAttempt[owner.op.OperationID] != owner.at.Add(5*time.Second) {
		t.Fatal("first attempt was not drop+5s")
	}
	for i, seconds := range []int{5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, 7200, 7200, 7200} {
		due := d.sessionNextAttempt[owner.op.OperationID]
		now = due.Add(-time.Nanosecond)
		poll()
		if calls != i {
			t.Fatal("early or catch-up attempt", i, calls)
		}
		now = due
		poll()
		wait := min(seconds*2, 7200)
		if calls != i+1 || d.sessionNextAttempt[owner.op.OperationID] != now.Add(time.Duration(wait)*time.Second) {
			t.Fatal("wrong failed-completion interval", i)
		}
	}
	if strings.Count(out.String(), "reconnecting...") != 1 {
		t.Fatal("outage sentence repeated", out.String())
	}
	d.deployment.resume = func(context.Context, channelonboarding.Operation) error { calls++; owner.connected = true; return nil }
	now = d.sessionNextAttempt[owner.op.OperationID]
	poll()
	poll() // A fresh post-recovery presentation is observed on the following worker pass.
	if d.sessionAttempts[owner.op.OperationID] != 0 || d.sessionStates[owner.op.OperationID] != "connected" {
		t.Fatal("authenticated success did not end outage")
	}
	owner.connected, owner.at = false, now.Add(time.Second)
	now = owner.at
	before := calls
	poll()
	if calls != before || d.sessionNextAttempt[owner.op.OperationID] != owner.at.Add(5*time.Second) || strings.Count(out.String(), "reconnecting...") != 2 {
		t.Fatal("new outage did not reset at5s")
	}
	owner.failure = errors.Join(channelonboarding.ErrSessionUnavailable, errors.New("independent storage failure"))
	if err := d.reconcileSessionConnections(t.Context(), nil); err == nil || calls != before {
		t.Fatal("mixed read error permitted attempt")
	}
	owner.failure = channelonboarding.ErrSessionNotLinked
	poll()
	if calls != before || d.sessionStates[owner.op.OperationID] != "not_linked" {
		t.Fatal("terminal Not linked entered the retry schedule")
	}
}

// Invariant: the synchronous worker joins a held attempt on cancellation; unit lifetime, not SDK IO.
func TestServeSessionRecoveryClockHeldCancellation(t *testing.T) {
	now := time.Unix(1000000, 0)
	owner := &recoveryClockOwner{at: now, op: channelonboarding.Operation{OperationID: uuid.NewString(), SessionConnectionID: uuid.NewString(), DeploymentRegistration: true}}
	var out bytes.Buffer
	d := &serveChannelDeliveryDispatcher{sessions: owner, onboarding: &channelonboarding.Service{}, output: &out,
		sessionStates: map[string]string{}, sessionAttempts: map[string]int{}, sessionNextAttempt: map[string]time.Time{}, now: func() time.Time { return now }}
	entered := make(chan struct{})
	d.deployment = &serveDeploymentTransport{resume: func(ctx context.Context, _ channelonboarding.Operation) error {
		close(entered)
		<-ctx.Done()
		return context.Cause(ctx)
	}}
	if err := d.reconcileSessionConnections(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.reconcileSessionConnections(ctx, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("attempt not entered")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("held attempt did not join")
	}
}
