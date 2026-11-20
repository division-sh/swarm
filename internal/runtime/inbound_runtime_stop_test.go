package runtime

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestRegistrationHolderDiscoveryFailsClosedOnlyDuringRetainedStop(t *testing.T) {
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
			err := stop.Open(prior)
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
