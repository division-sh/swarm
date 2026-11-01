package serveapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func waitLifecycleCarrierSeedSettlement(ctx context.Context, probe *lifecycleprobe.Probe, seed servedEventPublishRPCResult) error {
	if probe == nil || seed.EventID == "" || seed.RunID == "" || len(seed.Deliveries) != 4 {
		return fmt.Errorf("selected-carrier seed requires its exact probe, event, run and four node deliveries")
	}
	ctx, cancel := context.WithTimeout(ctx, servedEventPublishLifecycleProbeWaitTimeout)
	defer cancel()
	if err := waitExactOriginPublications(ctx, probe, seed.EventID); err != nil {
		return err
	}
	seen := make(map[string]bool, len(seed.Deliveries))
	for _, delivery := range seed.Deliveries {
		if delivery.SubscriberType != "node" || delivery.SubscriberID == "" || seen[delivery.SubscriberID] {
			return fmt.Errorf("selected-carrier seed contains an invalid or duplicate node recipient")
		}
		seen[delivery.SubscriberID] = true
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := probe.WaitForDeliveryStatus(ctx, seed.EventID, delivery.SubscriberType, delivery.SubscriberID, "delivered"); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func TestServedCompiledTransitionSeedSettlementFenceBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := newExactOriginPublicationProbe(t)
			rootController := identitytest.RootNode(t, "controller").Key()
			entered, release := make(chan string, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			hook := func(ctx context.Context, node string, event events.Event) error {
				if node != rootController || event.Type() != "work.seeded" {
					return nil
				}
				entered <- event.ID()
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			backendName := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				backendName = "postgres"
			}
			opts, start := issue2564ServeHarness(t, backendName, canonicalrouting.CopyLifecycleSelectedCarriers(t, true), true)
			opts.TestWorkflowNodeHandlerStartHook = hook
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("held-carrier serve stop=%d", code)
				}
			})
			reader := requireReceiverProofStateReader(t, rt.selected)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"seed": true}, "idempotency_key": "held-carrier/seed",
			})
			ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
			defer cancel()
			select {
			case eventID := <-entered:
				if eventID != seed.EventID {
					t.Fatal("held handler is not the exact acknowledged seed")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			recipients := []string{"", "child/", "sibling/", "child/nested/"}
			requireCarrierNativeStages(t, reader, seed.RunID, recipients, "waiting")
			if err := waitExactOriginPublications(ctx, probe, seed.EventID); err != nil {
				t.Fatal(err)
			}
			if _, err := probe.WaitForDeliveryStatus(ctx, seed.EventID, "node", rootController, "in_progress"); err != nil {
				t.Fatal(err)
			}
			missing, cancelMissing := context.WithCancel(t.Context())
			cancelMissing()
			if _, err := probe.WaitForDeliveryStatus(missing, seed.EventID, "node", rootController, "delivered"); err == nil {
				t.Fatal("held seed already advertises successful delivery")
			}
			// Hold the real handler throughout this canceled observation. No
			// elapsed-time assertion or goroutine-scheduling assumption supplies the cut.
			missingSeed, cancelSeed := context.WithTimeout(ctx, time.Second)
			err := waitLifecycleCarrierSeedSettlement(missingSeed, probe, seed)
			cancelSeed()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("seed fence accepted completed publication with a held handler: %v", err)
			}
			unblock()
			if err := waitLifecycleCarrierSeedSettlement(ctx, probe, seed); err != nil {
				t.Fatal(err)
			}
			selected := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.second", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"choice": "alpha"}, "idempotency_key": "held-carrier/second-alpha",
			})
			if err := waitExactOriginPublications(ctx, probe, selected.EventID); err != nil {
				t.Fatal(err)
			}
			rt.waitDeliveries(t, seed.RunID)
			entities := requireCarrierNativeStages(t, reader, seed.RunID, recipients, "done")
			for _, recipient := range recipients {
				entity := entities[recipient]
				history := readLifecycleTransitionHistory(t, reader, seed.RunID, entity)
				if len(history) != 1 || history[0].From != "active" || history[0].To != "done" {
					t.Fatalf("history=%+v", history)
				}
				record := readLifecycleTransitionAtCut(t, reader, seed.RunID, entity, history[0].TriggerEventID)
				selection := record.Evidence.RuleSelection()
				if record.From != "waiting" || record.To != "active" || record.TriggerEventID != selected.EventID || selection.DisplayLabel() != "rules[0]: when payload.choice == 'alpha'" || selection.Ref().SemanticPath() != `nodes["controller"].handlers["work.second"].rules[0]` {
					t.Fatalf("selected immutable evidence changed: %+v", record)
				}
			}
		})
	}
}

func requireCarrierNativeStages(t *testing.T, reader receiverProofStateReader, runID string, scopes []string, stage string) map[string]string {
	t.Helper()
	entities := make(map[string]string, len(scopes))
	err := reader.InspectSnapshot(t.Context(), func(ctx context.Context) error {
		instances, err := reader.ListWorkflowInstances(ctx, runID)
		if err != nil {
			return err
		}
		if len(instances) != len(scopes) {
			return fmt.Errorf("constructed instance count=%d, want %d", len(instances), len(scopes))
		}
		for _, scope := range scopes {
			path := strings.TrimSuffix(scope, "/")
			if path == "" {
				path = runID
			}
			for _, instance := range instances {
				if instance.StorageRef == path && instance.CurrentState == stage && instance.EntityID != "" {
					entities[scope] = instance.EntityID
				}
			}
			if entities[scope] == "" {
				return fmt.Errorf("constructed instance %s lacks exact stage %s", path, stage)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return entities
}
