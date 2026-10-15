package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestChannelInputBindingFaultKeepsOrdinaryRunAndExactReceiptBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var selected any
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p.deps.EventStore })
			h, _, hash := startChannelAnchorJourney(t, backend, "binding-fault-control", false)
			seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "binding-fault-control",
			})
			card := waitChannelAnchorCard(t, channelAnchorStorageObservation(t, h), seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
			waitChannelAnchorReceipt(t, channelAnchorStorageObservation(t, h), card)
			if _, err := storetest.CaptureChannelInputBindingFaultForTest(context.Background(), nil, seed.RunID); err == nil {
				t.Fatal("foreign owner acquired the contradiction")
			}
			if _, err := storetest.CaptureChannelInputBindingFaultForTest(context.Background(), selected, uuid.NewString()); err == nil {
				t.Fatal("missing run acquired the contradiction")
			}
			first := captureChannelInputBindingFaultWithCleanup(t, selected, seed.RunID)
			stale := captureChannelInputBindingFaultWithCleanup(t, selected, seed.RunID)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := first.InstallForTest(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled binding fault mutated: %v", err)
			}
			assertChannelBindingFaultCounts(t, selected, 0)
			if err := first.InstallForTest(context.Background()); err != nil {
				t.Fatal(err)
			}
			binding := readChannelBindingFaultReceipt(t, selected, seed.RunID)
			if binding.SourceRunID != seed.RunID || binding.ForkRunID != seed.RunID || binding.ForkPoint.Kind != runfork.RunForkPointDeploymentRevision || binding.ForkPoint.Revision != 1 || binding.ContractSelection.Mode != "selected_contracts" || binding.ContractSelection.BundleHash != "" || binding.ForkEventID != "" {
				t.Fatalf("fixed contradiction changed its original fields: %+v", binding)
			}
			if err := stale.InstallForTest(context.Background()); err == nil {
				t.Fatal("stale capture replaced the current binding")
			}
			assertChannelBindingForeignCleanupRefused(t, stale, selected, seed.RunID, binding)
			if err := first.RestoreForTest(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled cleanup removed a receipt: %v", err)
			}
			if after := readChannelBindingFaultReceipt(t, selected, seed.RunID); after != binding {
				t.Fatalf("refusal changed complete binding identity: before=%+v after=%+v", binding, after)
			}
			assertChannelBindingFaultCounts(t, selected, 1)
			if err := first.RestoreForTest(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertChannelBindingFaultCounts(t, selected, 0)
			proveChannelBindingCleanupCannotRemoveReplacement(t, selected, seed.RunID, first, binding.BindingID)
		})
	}
}

func assertChannelBindingForeignCleanupRefused(t *testing.T, fault *storetest.ChannelInputBindingFault, selected any, run string, binding runfork.RunForkSelectedContractBinding) {
	t.Helper()
	if err := fault.RestoreForTest(context.Background()); err == nil {
		t.Fatal("foreign installed receipt was accepted as this capture's cleanup")
	}
	if after := readChannelBindingFaultReceipt(t, selected, run); after != binding {
		t.Fatal("uncertain cleanup changed a foreign installed receipt")
	}
}

func captureChannelInputBindingFaultWithCleanup(t *testing.T, selected any, run string) *storetest.ChannelInputBindingFault {
	t.Helper()
	fault, err := storetest.CaptureChannelInputBindingFaultForTest(context.Background(), selected, run)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fault.RestoreForTest(ctx); err != nil {
			t.Error(err)
		}
	})
	return fault
}

func readChannelBindingFaultReceipt(t *testing.T, selected any, run string) runfork.RunForkSelectedContractBinding {
	t.Helper()
	reader, ok := selected.(interface {
		LoadRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, bool, error)
	})
	if !ok {
		t.Fatalf("native selected owner lost its canonical binding reader: %T", selected)
	}
	row, found, err := reader.LoadRunForkSelectedContractBinding(context.Background(), run)
	if err != nil || !found {
		t.Fatalf("exact binding receipt missing: found=%t %v", found, err)
	}
	return row
}

func assertChannelBindingFaultCounts(t *testing.T, selected any, bindings int) {
	t.Helper()
	count, err := storetest.CountSelectedContractBindingStorage(context.Background(), selected)
	if err != nil || count != bindings {
		t.Fatalf("binding fault count=%d want=%d: %v", count, bindings, err)
	}
	executions, err := storetest.CountGlobalSelectedRuntimeExecutionStorage(context.Background(), selected)
	if err != nil || executions != 0 {
		t.Fatalf("binding fault fabricated selected execution authority: %d %v", executions, err)
	}
}

func proveChannelBindingCleanupCannotRemoveReplacement(t *testing.T, selected any, run string, first *storetest.ChannelInputBindingFault, oldID string) {
	t.Helper()
	successor := captureChannelInputBindingFaultWithCleanup(t, selected, run)
	if err := successor.InstallForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := readChannelBindingFaultReceipt(t, selected, run)
	if current.BindingID == oldID {
		t.Fatal("replacement reused the original fault receipt")
	}
	if err := first.RestoreForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := readChannelBindingFaultReceipt(t, selected, run); after != current {
		t.Fatal("historical cleanup overwrote the replacement")
	}
	if err := successor.RestoreForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertChannelBindingFaultCounts(t, selected, 0)
}
