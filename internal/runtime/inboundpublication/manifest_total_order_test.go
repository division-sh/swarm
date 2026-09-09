package inboundpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestRecipientManifestCompleteWirePermutations(t *testing.T) {
	node := identitytest.RootNode(t, "worker")
	base := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"})}
	claimed := base
	var err error
	claimed.ConnectClaim, err = events.AdmitConnectExecutionClaim(sha256.Sum256([]byte("edge")), sha256.Sum256([]byte("pin")), base.Recipient, node, "work.received")
	if err != nil {
		t.Fatal(err)
	}
	projected := base
	projected.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/two", EntityID: "entity-2"})
	projected.PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"work_id": "work-two"})
	if err != nil {
		t.Fatal(err)
	}
	reply := base
	reply.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply"}}
	routes := []events.DeliveryRoute{claimed, projected, reply, base}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		t.Fatal(err)
	}
	want, hash, count, err := CanonicalRecipientManifest(routes)
	if err != nil || count != len(routes) {
		t.Fatalf("manifest: %s %d %v", want, count, err)
	}
	var wires []json.RawMessage
	if err := json.Unmarshal(want, &wires); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(wires); i++ {
		if bytes.Compare(wires[i-1], wires[i]) >= 0 {
			t.Fatal("manifest is not in strict complete-wire order")
		}
	}
	permutations := 0
	var visit func(int)
	visit = func(index int) {
		if index < len(routes) {
			for i := index; i < len(routes); i++ {
				routes[index], routes[i] = routes[i], routes[index]
				visit(index + 1)
				routes[index], routes[i] = routes[i], routes[index]
			}
			return
		}
		permutations++
		input := append(append([]events.DeliveryRoute{}, routes...), routes[0], routes[2])
		before, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		got, gotHash, gotCount, err := CanonicalRecipientManifest(input)
		if err != nil || !bytes.Equal(got, want) || gotHash != hash || gotCount != count {
			t.Fatalf("permutation=%d count=%d hash=%s err=%v", permutations, gotCount, gotHash, err)
		}
		after, err := json.Marshal(input)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("caller input changed")
		}
		var hydrated []events.DeliveryRoute
		if err := json.Unmarshal(got, &hydrated); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < 2; n++ {
			hydrated = events.NormalizeDeliveryRoutes(hydrated)
		}
		again, againHash, againCount, err := CanonicalRecipientManifest(hydrated)
		if err != nil || !bytes.Equal(again, want) || againHash != hash || againCount != count {
			t.Fatalf("hydration/normalization changed manifest: %v", err)
		}
	}
	visit(0)
	if permutations != 24 {
		t.Fatalf("tested %d permutations, want 24", permutations)
	}
	changed := append([]events.DeliveryRoute{}, routes...)
	changed[1].PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"work_id": "different"})
	if err != nil {
		t.Fatal(err)
	}
	if err := events.ValidateDeliveryRoutes(changed); err != nil {
		t.Fatal(err)
	}
	_, changedHash, changedCount, err := CanonicalRecipientManifest(changed)
	if err != nil || changedCount != count || changedHash == hash {
		t.Fatalf("semantic change lost: %d %s %v", changedCount, changedHash, err)
	}
}

func TestRecipientManifestEmptyAndMarshalFailure(t *testing.T) {
	for _, routes := range [][]events.DeliveryRoute{nil, {}} {
		got, hash, count, err := CanonicalRecipientManifest(routes)
		if err != nil || string(got) != "[]" || count != 0 || hash != "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945" {
			t.Fatalf("empty identity: %s %s %d %v", got, hash, count, err)
		}
	}
	node := identitytest.RootNode(t, "worker")
	recipient := events.MustNodeDeliveryRecipient(node)
	claim, err := events.AdmitConnectExecutionClaim(sha256.Sum256([]byte("edge")), sha256.Sum256([]byte("pin")), recipient, node, "work.received")
	if err != nil {
		t.Fatal(err)
	}
	bad := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "other")), ConnectClaim: claim}
	if _, err := json.Marshal(bad); err == nil {
		t.Fatal("invalid fixture must fail through canonical route marshaler")
	}
	got, hash, count, err := CanonicalRecipientManifest([]events.DeliveryRoute{bad})
	if err == nil || got != nil || hash != "" || count != 0 {
		t.Fatalf("marshal failure produced evidence: %s %s %d %v", got, hash, count, err)
	}
}
