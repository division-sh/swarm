package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/tests/support/testutil"
)

func TestRegistrationHolderDiscoveryFailsClosedOnlyDuringRetainedStop(t *testing.T) {
	t.Run("shared_expiry", testRetainedHolderJoinsAtControlledExpiry)
	source, err := correlation.NewSourceArtifactFact(runtimeContextTestHashA)
	if err != nil {
		t.Fatal(err)
	}
	candidate := StandingActivation{BundleHash: source.BundleHash(), RunID: "unchecked", FlowPath: "chat"}
	manager := &RuntimeContextManager{
		availability: fakeRunBundleAvailability{rows: map[string]runbundle.Availability{candidate.RunID: {ErrorCode: "unavailable"}}},
		landingRuns: runtimeTestLandingRuns{candidate.FlowPath: {
			LiveRun: runlifecycle.LiveRun{RunID: candidate.RunID, Source: source}, State: runlifecycle.StateLive,
		}},
	}
	ref := channelonboarding.AccountReference{Provider: "telegram", Account: "420079"}
	if rows, err := manager.activationAccountHolders(t.Context(), candidate, ref, true); len(rows) != 0 || err != nil {
		t.Fatalf("ordinary currentness changed discovery: rows=%v err=%v", rows, err)
	}
	// Supply the retained phase directly to exercise only the failed read.
	manager.inboundStopObservation = &InboundRuntimeStop{manager: manager}
	if rows, err := manager.activationAccountHolders(t.Context(), candidate, ref, true); len(rows) != 0 || !errors.Is(err, channelonboarding.ErrRevisionConflict) {
		t.Fatalf("retained currentness skipped an unchecked live contender: rows=%v err=%v", rows, err)
	}
}

func testRetainedHolderJoinsAtControlledExpiry(t *testing.T) {
	def := capturedOrdinaryRuntimeContext(t, "holder.requested")
	grant, err := def.Runtime.CurrentStartupGrant()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(t.Context()); err != nil {
		t.Fatal(err)
	}
	m, err := NewRuntimeContextManager(fakeRunBundleAvailability{rows: map[string]runbundle.Availability{"run": {
		RunID: "run", BundleHash: def.BundleHash(), RootReference: def.RootAdmission.Reference(),
		SourceArtifactPresent: true, RootAdmissionPresent: true,
	}}}, def)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, expire := testutil.StopDeadline(time.Now().Add(time.Hour))
		stop := m.PrepareInboundRuntimeStop(ctx, time.Hour)
		entered, release := make(chan struct{}), make(chan struct{})
		seen, read, cutoff := make(chan error, 1), make(chan error, 1), make(chan error, 1)
		go func() {
			_, _, _, err := m.acquireRun(context.Background(), "run", true, func(ctx context.Context, _ *Runtime, _ *BundleContext, _ runbundle.Availability) error {
				close(entered)
				select {
				case <-ctx.Done():
					seen <- ctx.Err()
					<-release
				case <-release:
				}
				return ctx.Err()
			})
			read <- err
		}()
		select {
		case <-entered:
		case err := <-read:
			_ = m.CloseInboundRequests(context.Background())
			_ = stop.Finish()
			t.Fatalf("retained holder did not reach its rooted read: %v", err)
		}
		expire(context.DeadlineExceeded)
		synctest.Wait()
		select {
		case err := <-seen:
			if err != context.DeadlineExceeded {
				t.Errorf("held read observed %v, want stop expiry", err)
			}
		default:
			t.Error("retained holder read did not observe the shared stop expiry")
		}
		go func() { cutoff <- m.CloseInboundRequests(ctx) }()
		synctest.Wait()
		select {
		case err := <-cutoff:
			t.Errorf("cutoff returned before held read released: %v", err)
		default:
		}
		close(release)
		if err := <-read; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("holder result did not retain expiry: %v", err)
		}
		if err := <-cutoff; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("cutoff did not join held read after expiry: %v", err)
		}
		_ = stop.Finish() // The reserved runtime must finish even on the RED control.
	})
}

func TestStoredInboundCompletedCutoffRequiresPreparationAndJoin(t *testing.T) {
	failure := errors.New("independent stop preparation failure")
	for _, name := range []string{"complete", "active", "missing_seal", "missing_final_set", "seal_error", "prior_error", "setup_error"} {
		t.Run(name, func(t *testing.T) {
			m := &RuntimeContextManager{}
			if name == "active" {
				_, release, admitted := m.BeginInboundRequest(t.Context())
				if !admitted {
					t.Fatal("admit held callback")
				}
				t.Cleanup(release)
			}
			m.PrepareInboundStop()
			if name != "missing_seal" {
				if err := m.SealInboundExecution(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if name == "missing_final_set" {
				m.inboundFinalSet = false
			}
			if name == "seal_error" {
				m.inboundSealErr = failure
			}
			// Close without Wait only for the active negative; the owner must
			// distinguish a closed gate from an actually joined cutoff.
			m.inboundRequests.Close()
			stop := &InboundRuntimeStop{manager: m}
			var prior error
			if name == "prior_error" {
				prior = failure
			}
			if name == "setup_error" {
				stop.result = failure
			}
			err := stop.Open(prior, nil)
			if name == "complete" {
				if err != nil {
					t.Fatalf("completed cutoff was reopened: %v", err)
				}
			} else if name == "seal_error" || name == "prior_error" || name == "setup_error" {
				if !errors.Is(err, failure) {
					t.Fatalf("independent preparation failure lost: %v", err)
				}
			} else if !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatalf("incomplete cutoff accepted: %v", err)
			}
			if !m.inboundRequests.Closed() || m.inboundStoreOnly {
				t.Fatal("terminal cutoff reopened publication admission")
			}
			if err := m.OpenStoredInbound(); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatalf("global Open contract changed: %v", err)
			}
		})
	}
}

func TestStoredInboundStopPossessionVetoIsNotJoinFailure(t *testing.T) {
	veto := errors.New("independent possession loss")
	joinFailure := errors.New("independent preparation/join failure")
	for _, prior := range []error{nil, joinFailure} {
		t.Run(fmt.Sprint(prior), func(t *testing.T) {
			ctx := t.Context()
			m := &RuntimeContextManager{}
			if err := m.SealInboundExecution(ctx); err != nil {
				t.Fatal(err)
			}
			stop := &InboundRuntimeStop{manager: m}
			if err := stop.Open(prior, veto); !errors.Is(err, veto) || (prior != nil && !errors.Is(err, prior)) {
				t.Fatalf("stop opening lost refusal: %v", err)
			}
			if err := m.OpenStoredInbound(); !errors.Is(err, worklifetime.ErrAdmissionFenced) || !errors.Is(err, veto) {
				t.Fatalf("possession veto reopened intake: %v", err)
			}
			if stop.Drain() {
				t.Fatal("possession veto allowed final output")
			}
			if err := stop.Finish(); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatalf("stop finished without joined cutoff: %v", err)
			}
			if err := m.CloseInboundRequests(ctx); err != nil {
				t.Fatal(err)
			}
			if err := stop.Finish(); !errors.Is(err, prior) || errors.Is(err, veto) {
				t.Fatalf("projection-release eligibility lost its join-only result: %v", err)
			}
		})
	}
}
