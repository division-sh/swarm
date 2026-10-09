package bus

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestIndexedPubsubUsesNativeExistenceAndStoredIdentity(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	ctx := constructionIndexContext(t, source)
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("pubsub-event"), "account.ready", "", "", nil, 0, busInternalTestRunID, events.EventEnvelope{}, eventtest.ConcreteTemplateRoutingSource("account", "account/one", eventtest.UUID("actual-stored-account")), time.Now().UTC())
	instance := ConstructedFlowInstanceIdentityFixture(source, "account", "one", event.RunID())
	instance.EntityID = eventtest.UUID("actual-stored-account")
	reader := &unscopedConnectIndexTestReader{}
	resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
	keys := []string{instance.InstancePath + "/account.ready"}
	if err := table.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{Identity: flowidentity.RunScopedFlowInstance{RunID: event.RunID(), Route: instance.Route()}, Instance: instance}); err != nil {
		t.Fatal(err)
	}
	if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{}); err != nil || len(subscribers) != 0 {
		t.Fatalf("stale process membership substituted for native absence: subscribers=%+v err=%v", subscribers, err)
	}
	reader.observations = []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, event.RunID(), instance, "42")}
	first, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{})
	if err != nil || len(first) != 1 || first[0].Path != instance.InstancePath || first[0].Recipient.LocalID() != "account-node" || first[0].LocalizedEvent != "account.ready" {
		t.Fatalf("native receiver was not bound exactly: subscribers=%+v err=%v", first, err)
	}
	if len(reader.requested) != 2 || len(reader.requested[1].FlowIDs()) != 0 || len(reader.requested[1].Coordinates()) != 1 || reader.requested[1].Coordinates()[0].Key() != reader.observations[0].Owner().Key() {
		t.Fatalf("pubsub escaped its compiled declaration scope: %+v", reader.requested)
	}
	if err := table.RemoveFlowInstanceRoute(reader.observations[0].Owner()); err != nil {
		t.Fatal(err)
	}
	second, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{})
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("process deletion changed native recipient projection: first=%+v second=%+v err=%v", first, second, err)
	}
	if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, []string{"account/account.ready"}, ordinaryPublicationSource{}); err != nil || len(subscribers) != 0 {
		t.Fatalf("declaration alias inferred a keyed instance: subscribers=%+v err=%v", subscribers, err)
	}
	reader.observations = nil
	if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{}); err != nil || len(subscribers) != 0 {
		t.Fatalf("retired native receiver survived in a planning cache: subscribers=%+v err=%v", subscribers, err)
	}
}

func TestIndexedPubsubRejectsForeignDuplicateAndIndependentFailures(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	ctx := constructionIndexContext(t, source)
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("pubsub-errors"), "account.ready", "", "", nil, 0, busInternalTestRunID, events.EventEnvelope{}, eventtest.ConcreteTemplateRoutingSource("account", "account/one", eventtest.UUID("one")), time.Now().UTC())
	keys := []string{"account/one/account.ready"}
	foreign := ConstructedFlowInstanceIdentityFixture(source, "account", "one", eventtest.UUID("foreign-run"))
	local := ConstructedFlowInstanceIdentityFixture(source, "account", "one", event.RunID())
	observed := constructionIndexObservation(t, source, event.RunID(), local, "42")
	for _, observations := range [][]pipeline.FlowInstanceObservation{
		{constructionIndexObservation(t, source, eventtest.UUID("foreign-run"), foreign, "42")},
		{observed, observed},
	} {
		reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{observations: observations}}
		resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
		if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{}); err == nil || len(subscribers) != 0 {
			t.Fatalf("invalid inventory became recipients: subscribers=%+v err=%v", subscribers, err)
		}
	}
	storeFailure := errors.New("independent inventory failure")
	for _, failure := range []error{context.Canceled, storeFailure, errors.Join(context.Canceled, storeFailure)} {
		resolver := newConnectRoutePlanResolver(source, table, nil, constructionIndexTestReader{err: failure}, nil)
		if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{}); len(subscribers) != 0 || !errors.Is(err, failure) {
			t.Fatalf("inventory failure became absence: subscribers=%+v err=%v want=%v", subscribers, err, failure)
		}
	}
	resolver := newConnectRoutePlanResolver(source, table, nil, nil, nil)
	if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, ordinaryPublicationSource{}); err == nil || len(subscribers) != 0 {
		t.Fatalf("missing index admitted pubsub: subscribers=%+v err=%v", subscribers, err)
	}
	if subscribers, err := resolver.resolvePubsubSubscribers(context.Background(), event, keys, ordinaryPublicationSource{}); err == nil || len(subscribers) != 0 {
		t.Fatalf("missing admitted source admitted pubsub: subscribers=%+v err=%v", subscribers, err)
	}
}

func TestIndexedPubsubRootUsesGenerationCoordinate(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.RootIngress))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	instance := ConstructedFlowInstanceIdentityFixture(source, ".", "", busInternalTestRunID)
	reader := constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, instance, "")}}
	resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("pubsub-root"), "item.received", "", "", nil, 0, busInternalTestRunID, events.EventEnvelope{}, time.Now().UTC())
	subscribers, err := resolver.resolvePubsubSubscribers(constructionIndexContext(t, source), event, pubsubEventKeys(event, ordinaryPublicationSource{}), ordinaryPublicationSource{})
	if err != nil || len(subscribers) == 0 {
		t.Fatalf("root lost its native generation coordinate: subscribers=%+v err=%v", subscribers, err)
	}
	for _, subscriber := range subscribers {
		if subscriber.Path != event.RunID() {
			t.Fatalf("root subscriber borrowed a declaration or foreign coordinate: %+v", subscriber)
		}
	}
	intents := routedRootNodeDeliveryIntentsForNoTargetEvent(source, event, subscribers)
	if len(intents) != len(subscribers) {
		t.Fatalf("native root lost its ordinary delivery authority: subscribers=%+v intents=%+v", subscribers, intents)
	}
	for _, intent := range intents {
		if intent.TargetBlueprint.FlowInstance != event.RunID() || intent.TargetBlueprint.FlowID != "." {
			t.Fatalf("root intent changed its canonical coordinate: %+v", intent)
		}
	}
	for _, path := range []string{eventtest.UUID("other-root-run"), "private-child"} {
		foreign := subscribers[0]
		foreign.Path = path
		if intents := routedRootNodeDeliveryIntentsForNoTargetEvent(source, event, []Subscriber{foreign}); len(intents) != 0 {
			t.Fatalf("foreign root coordinate obtained delivery authority: %+v", intents)
		}
	}
}

func TestIndexedPubsubConcreteRootUsesGenerationCoordinate(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.RootIngress))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	instance := ConstructedFlowInstanceIdentityFixture(source, ".", "", busInternalTestRunID)
	reader := constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, instance, "")}}
	resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("pubsub-concrete-root"), "item.received", "", "", nil, 0, busInternalTestRunID, events.EventEnvelope{}, eventtest.ConcreteTemplateRoutingSource(".", instance.InstancePath, instance.EntityID), time.Now().UTC())
	publication := ordinaryPublicationSource{}
	keys := pubsubEventKeys(event, publication)
	subscribers, err := resolver.resolvePubsubSubscribers(constructionIndexContext(t, source), event, keys, publication)
	if err != nil || len(subscribers) == 0 {
		t.Fatalf("concrete root lost its indexed consumers: keys=%v subscribers=%+v err=%v", keys, subscribers, err)
	}
	for _, subscriber := range subscribers {
		if subscriber.Path != instance.InstancePath {
			t.Fatalf("concrete root subscriber changed the indexed coordinate: %+v", subscriber)
		}
	}
}

func TestIndexedPubsubDeploymentFeedUsesConstructedRoot(t *testing.T) {
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	path := filepath.Join(root, "schema.yaml")
	schema, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(schema, []byte("  outputs:\n    - item.received\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	source := loadConnectRoutePlanCanonicalSource(t, root)
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	instance := ConstructedFlowInstanceIdentityFixture(source, ".", "", busInternalTestRunID)
	reader := &unscopedConnectIndexTestReader{}
	resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
	feed, err := events.NewDeploymentFeedRoutingSource(".")
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("pubsub-root-feed"), "item.received", "deployment-feed", "", []byte(`{"item_id":"one"}`), 0, busInternalTestRunID, events.EventEnvelope{}, feed, time.Now().UTC())
	ctx := constructionIndexContext(t, source)
	publication := ordinaryPublicationSource{}
	keys := pubsubEventKeys(event, publication)
	if subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, publication); err != nil || len(subscribers) != 0 {
		t.Fatalf("declaration invented a root receiver: subscribers=%+v err=%v", subscribers, err)
	}
	reader.observations = []pipeline.FlowInstanceObservation{constructionIndexObservation(t, source, busInternalTestRunID, instance, "")}
	subscribers, err := resolver.resolvePubsubSubscribers(ctx, event, keys, publication)
	if err != nil || len(subscribers) != 1 || subscribers[0].Path != instance.InstancePath {
		t.Fatalf("root feed lost its constructed local consumer: keys=%v subscribers=%+v err=%v", keys, subscribers, err)
	}
	if len(reader.requested) != 2 || len(reader.requested[1].Coordinates()) != 1 || reader.requested[1].Coordinates()[0].Key() != reader.observations[0].Owner().Key() {
		t.Fatalf("root feed escaped its exact native lookup: %+v", reader.requested)
	}
	intents := publication.localNodeIntents(source, event, subscribers)
	if len(intents) != 1 || intents[0].TargetBlueprint.FlowID != "." || intents[0].TargetBlueprint.FlowInstance != event.RunID() || event.RoutingSource() != feed {
		t.Fatalf("root feed changed receiver or declaration source: intents=%+v source=%+v", intents, event.RoutingSource())
	}
	for _, test := range []struct {
		flow, event string
	}{
		{".", "item.processed"},
		{"foreign", "item.received"},
	} {
		invalid, err := events.NewDeploymentFeedRoutingSource(test.flow)
		if err != nil {
			t.Fatal(err)
		}
		bad := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("pubsub-invalid-feed-"+test.flow), events.EventType(test.event), "deployment-feed", "", nil, 0, busInternalTestRunID, events.EventEnvelope{}, invalid, time.Now().UTC())
		before := len(reader.requested)
		if subscribers, err := resolver.resolvePubsubSubscribers(ctx, bad, pubsubEventKeys(bad, publication), publication); err == nil || len(subscribers) != 0 || len(reader.requested) != before {
			t.Fatalf("unadmitted feed queried receiver evidence: subscribers=%+v err=%v reads=%d", subscribers, err, len(reader.requested)-before)
		}
	}
}
