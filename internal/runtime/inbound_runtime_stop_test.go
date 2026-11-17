package runtime

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

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
