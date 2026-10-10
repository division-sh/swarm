package cataloge2e

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogCausalDeliveryDiagnosticsUseExactNativeReadBothStores(t *testing.T) {
	fixture := catalogRuntimeFixture(t, "catalog.runtime.event_loop", "test-chain-depth-limit")
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			transcript := buildCatalogExecutionTranscript(t, fixture)
			h, _ := executeCatalogTranscript(t, fixture, backend, transcript)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext(context.Background())
			public, err := scatterGatherPublicEvents(h, ctx)
			if err != nil {
				t.Fatal(err)
			}
			rootID := h.publishedOrder[0]
			wantGroups, wantDeliveries, wantClaims := expectedCausalDeliveryWitness(public, rootID)
			if len(wantGroups) == 0 || wantDeliveries < 6 || wantClaims < 6 {
				t.Fatalf("actual chain did not construct descendant deliveries: groups=%v deliveries=%d claims=%d", wantGroups, wantDeliveries, wantClaims)
			}
			h.db = nil
			var selected any = h.sqlite
			if h.pg != nil {
				selected = h.pg
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			groups, err := storetest.ReadCausalDeliveryStatusCounts(ctx, reader, catalogRuntimeRunID, rootID)
			if err != nil || !reflect.DeepEqual(groups, wantGroups) {
				t.Fatalf("causal groups=%v want=%v err=%v", groups, wantGroups, err)
			}
			deliveries, claims, err := storetest.ReadNonLogRunDeliveryClaimTotals(ctx, reader, catalogRuntimeRunID)
			if err != nil || deliveries != wantDeliveries || claims != wantClaims {
				t.Fatalf("physical run totals=%d/%d want=%d/%d err=%v", deliveries, claims, wantDeliveries, wantClaims, err)
			}
			root := public[rootID]
			if len(root.Deliveries) != 1 {
				t.Fatalf("root deliveries=%v", root.Deliveries)
			}
			deliveryID := root.Deliveries[0].DeliveryID
			attempts, err := storetest.ReadDeliveryAttemptDiagnosticRows(ctx, reader, deliveryID)
			wantAttempt := storetest.DeliveryAttemptDiagnosticRow{Version: 1, Closure: "settled", Outcome: "delivered"}
			if err != nil || len(attempts) != 1 || attempts[0] != wantAttempt {
				t.Fatalf("physical root attempt=%v want=%+v err=%v", attempts, wantAttempt, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 3 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("delivery diagnostics escaped original read coordinator: %+v", proof)
			}
			logScatterGatherAttempts(t, h, deliveryID)
			requireEventDeliveryDiagnosticObservation(t, h, ctx, reader, probe, root)
			requireSubscriberManifestObservation(t, h, ctx, reader, probe, root)
			if rows, err := storetest.ReadCausalDeliveryStatusCounts(ctx, reader, uuid.NewString(), rootID); err != nil || rows != nil {
				t.Fatalf("foreign-run causal rows=%v err=%v", rows, err)
			}
			if rows, err := storetest.ReadDeliveryAttemptDiagnosticRows(ctx, reader, uuid.NewString()); err != nil || rows != nil {
				t.Fatalf("missing attempt rows=%v err=%v", rows, err)
			}
			requireCausalDeliveryReadRefusals(t, ctx, reader, rootID, deliveryID)
			if h.pg != nil {
				err = h.pg.Close()
			} else {
				err = h.sqlite.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := storetest.ReadCausalDeliveryStatusCounts(ctx, reader, catalogRuntimeRunID, rootID); err == nil || rows != nil {
				t.Fatalf("closed causal rows=%v err=%v", rows, err)
			}
			if names, err := storetest.ReadSubscriberEventNamesCreatedSince(ctx, reader, h.startedAt, root.Deliveries[0].SubscriberID); err == nil || names != nil {
				t.Fatalf("closed manifest names=%v err=%v", names, err)
			}
			if rows, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, reader, rootID); err == nil || rows != nil {
				t.Fatalf("closed event delivery diagnostic=%v err=%v", rows, err)
			}
		})
	}
}

func requireEventDeliveryDiagnosticObservation(t *testing.T, h *runtimeHarness, ctx context.Context, reader catalogOperatorEventLister, probe *storetest.TransactionCollector, root operatorread.OperatorEventFull) {
	t.Helper()
	delivery := root.Deliveries[0]
	want := storetest.EventDeliveryDiagnosticRow{
		SubscriberType: delivery.SubscriberType, SubscriberID: delivery.SubscriberID,
		Status: delivery.Status, Reason: delivery.ReasonCode,
		FlowInstance: delivery.Target.FlowInstance, EntityID: delivery.Target.EntityID,
	}
	if want.Status == "" || want.FlowInstance == "" || want.EntityID == "" {
		t.Fatalf("actual diagnostic control lacks stored route/status: %+v", want)
	}
	before := probe.Snapshot()
	rows, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, reader, root.EventID)
	if err != nil || !reflect.DeepEqual(rows, []storetest.EventDeliveryDiagnosticRow{want}) {
		t.Fatalf("event delivery diagnostic=%v want=%+v err=%v", rows, want, err)
	}
	if proof := probe.Snapshot(); proof.Total.ReadCommits-before.Total.ReadCommits != 1 || proof.Total.WriteCommits != before.Total.WriteCommits || proof.Active != 0 {
		t.Fatalf("event delivery diagnostic escaped original coordinator: %+v", proof)
	}
	wantText := want.SubscriberType + "/" + want.SubscriberID + " status=" + want.Status + " reason=" + want.Reason + " route=" + want.FlowInstance + "/" + want.EntityID
	if got := dumpEventDeliveries(t, h, root.EventID); got != wantText {
		t.Fatalf("diagnostic formatter changed: got=%q want=%q", got, wantText)
	}
	if got := dumpEventDeliveries(t, h, uuid.NewString()); got != "<none>" {
		t.Fatalf("missing event diagnostic=%q", got)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if rows, err := storetest.ReadEventDeliveryDiagnosticRows(canceled, reader, root.EventID); !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("canceled event diagnostic=%v err=%v", rows, err)
	}
	if rows, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, struct{}{}, root.EventID); err == nil || rows != nil {
		t.Fatalf("foreign event diagnostic=%v err=%v", rows, err)
	}
	if got := dumpEventDeliveries(t, &runtimeHarness{}, root.EventID); !strings.HasPrefix(got, "observation_error:") {
		t.Fatalf("missing owner diagnostic grants success: %q", got)
	}
}

func requireSubscriberManifestObservation(t *testing.T, h *runtimeHarness, ctx context.Context, reader catalogOperatorEventLister, probe *storetest.TransactionCollector, root operatorread.OperatorEventFull) {
	t.Helper()
	delivery := root.Deliveries[0]
	if delivery.CreatedAt == nil || delivery.SubscriberType != "node" {
		t.Fatalf("real physical manifest fixture=%+v", delivery)
	}
	cut := *delivery.CreatedAt
	before := probe.Snapshot()
	names, err := storetest.ReadSubscriberEventNamesCreatedSince(ctx, reader, cut, delivery.SubscriberID)
	if err != nil || !reflect.DeepEqual(names, []string{root.EventName}) {
		t.Fatalf("inclusive physical subscriber manifest=%v err=%v", names, err)
	}
	if proof := probe.Snapshot(); proof.Total.ReadCommits-before.Total.ReadCommits != 1 || proof.Total.WriteCommits != before.Total.WriteCommits || proof.Active != 0 {
		t.Fatalf("subscriber observation escaped original coordinator: %+v", proof)
	}
	// Historical vocabulary observes the manifest, not successful agent execution.
	assertAgentReceived(t, h, cut, map[string][]string{" " + delivery.SubscriberID + " ": {root.EventName}})
	assertAgentReceived(t, nil, cut, nil)
	assertAgentReceived(t, nil, cut, map[string][]string{" ": {}})
	if names, err := storetest.ReadSubscriberEventNamesCreatedSince(ctx, reader, cut.Add(time.Microsecond), delivery.SubscriberID); err != nil || names != nil {
		t.Fatalf("exclusive later cut names=%v err=%v", names, err)
	}
	if names, err := storetest.ReadSubscriberEventNamesCreatedSince(ctx, reader, cut, uuid.NewString()); err != nil || names != nil {
		t.Fatalf("foreign subscriber names=%v err=%v", names, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if names, err := storetest.ReadSubscriberEventNamesCreatedSince(canceled, reader, cut, delivery.SubscriberID); !errors.Is(err, context.Canceled) || names != nil {
		t.Fatalf("canceled manifest names=%v err=%v", names, err)
	}
	if _, err := storetest.ReadSubscriberEventNamesCreatedSince(ctx, struct{}{}, cut, delivery.SubscriberID); err == nil {
		t.Fatal("foreign subscriber read owner accepted")
	}
}

func expectedCausalDeliveryWitness(public map[string]operatorread.OperatorEventFull, rootID string) ([]storetest.CausalDeliveryStatusCount, int64, int64) {
	descendants := map[string]bool{rootID: true}
	for changed := true; changed; {
		changed = false
		for id, event := range public {
			if event.RunID == catalogRuntimeRunID && !descendants[id] && descendants[event.SourceEventID] {
				descendants[id], changed = true, true
			}
		}
	}
	counts := map[storetest.CausalDeliveryStatusCount]int{}
	var deliveries, claims int64
	for id, event := range public {
		if event.RunID != catalogRuntimeRunID {
			continue
		}
		for _, row := range event.Deliveries {
			if event.EventName != "platform.runtime_log" {
				deliveries++
				claims += row.ClaimVersion
			}
			if descendants[id] {
				counts[storetest.CausalDeliveryStatusCount{Class: row.SubscriberType, Subscriber: row.SubscriberID, Status: row.Status, Reason: row.ReasonCode}]++
			}
		}
	}
	var out []storetest.CausalDeliveryStatusCount
	for row, count := range counts {
		row.Count = count
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Class != b.Class {
			return a.Class < b.Class
		}
		if a.Subscriber != b.Subscriber {
			return a.Subscriber < b.Subscriber
		}
		if a.Status != b.Status {
			return a.Status < b.Status
		}
		return a.Reason < b.Reason
	})
	return out, deliveries, claims
}

func requireCausalDeliveryReadRefusals(t *testing.T, ctx context.Context, reader catalogOperatorEventLister, rootID, deliveryID string) {
	t.Helper()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if rows, err := storetest.ReadCausalDeliveryStatusCounts(canceled, reader, catalogRuntimeRunID, rootID); !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("canceled causal rows=%v err=%v", rows, err)
	}
	if deliveries, claims, err := storetest.ReadNonLogRunDeliveryClaimTotals(canceled, reader, catalogRuntimeRunID); !errors.Is(err, context.Canceled) || deliveries != 0 || claims != 0 {
		t.Fatalf("canceled physical totals=%d/%d err=%v", deliveries, claims, err)
	}
	if rows, err := storetest.ReadDeliveryAttemptDiagnosticRows(canceled, reader, deliveryID); !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("canceled attempt rows=%v err=%v", rows, err)
	}
	if _, err := storetest.ReadCausalDeliveryStatusCounts(ctx, struct{}{}, catalogRuntimeRunID, rootID); err == nil {
		t.Fatal("foreign causal owner accepted")
	}
	if _, _, err := storetest.ReadNonLogRunDeliveryClaimTotals(ctx, struct{}{}, catalogRuntimeRunID); err == nil {
		t.Fatal("foreign totals owner accepted")
	}
	if _, err := storetest.ReadDeliveryAttemptDiagnosticRows(ctx, struct{}{}, deliveryID); err == nil {
		t.Fatal("foreign attempt owner accepted")
	}
}
