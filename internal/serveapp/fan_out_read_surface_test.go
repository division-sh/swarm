package serveapp

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestFanOutReadSelectedProcessCallback(t *testing.T) {
	ctx := context.Background()
	stores := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "fan-out-read.sqlite"), &config.Config{})
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	if _, err := initializeServePlatformStateStores(ctx, stores.Schema(), filepath.Join(repoRootForTest(), defaultPlatformSpecPath)); err != nil {
		t.Fatal(err)
	}
	offline, err := constructSelectedAPICapabilities(stores, selectedAPICapabilityRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if offline.FanOutRuntime != nil {
		t.Fatal("offline composition fabricated a runtime observer")
	}
	capability, err := stores.StartupOwnership().AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "fan-out-read", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(ctx) })
	caps, err := constructSelectedAPICapabilities(stores, selectedAPICapabilityRequest{ProcessCapability: capability})
	if err != nil {
		t.Fatal(err)
	}
	if caps.FanOutRuntime == nil {
		t.Fatal("selected process composition omitted runtime observer")
	}
	runID, at := uuid.NewString(), time.Now().UTC()
	page := fanoutobligation.ListPage{RunID: runID, RunStatus: "running", ObservedAt: at, Order: fanoutobligation.ListIdentityOrder, Intents: []fanoutobligation.IntentReadback{{
		Key:        fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString(), ElementRef: runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "read-test"}},
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), Status: fanoutobligation.StatusOpen, DurableState: "eligible",
		Cardinality: 1, Owed: 1, NextChunkSize: 32, CreatedAt: at, UpdatedAt: at, Runtime: fanoutobligation.UnavailableRuntimeReadback(),
	}}}
	if err := page.Validate(fanoutobligation.ListQuery{RunID: runID}); err != nil {
		t.Fatal(err)
	}
	observed, err := caps.FanOutRuntime(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(observed, page) {
		t.Fatalf("unregistered process invented runtime evidence: %+v", observed)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	observed, err = caps.FanOutRuntime(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	want := fanoutobligation.RuntimeReadback{Availability: "retired", Reason: "process_retired"}
	if observed.Intents[0].Runtime != want || observed.ObservedAt != page.ObservedAt {
		t.Fatalf("observer did not retain exact selected capability: %+v", observed)
	}
	if page.Intents[0].Runtime != fanoutobligation.UnavailableRuntimeReadback() {
		t.Fatal("runtime observer mutated caller-owned page")
	}
}
