package fanoutobligation

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func a2AdmitFixtureSourceProjection(t *testing.T, r IntentRequest) IntentRequest {
	t.Helper()
	ref := rc.CatalogTypeReference{Type: "[text]"}
	p, err := rc.AdmitCollectionProjection(ref)
	if err != nil {
		t.Fatal(err)
	}
	r.Capsule.SourceProjection = rc.FanOutPlanSemantics{ElementRef: r.PlanRef.ElementRef, ItemsFrom: "payload." + r.Source.Field,
		CollectionType: ref, CollectionProjection: p, ItemType: p.ItemType(), ItemAlias: "entry", Identity: "entry", IdentityDerived: true,
		MaxItems: rc.DefaultFanOutMaxItems, Emit: rc.EmitSpec{Event: "item.ready"}}
	r.PlanRef.SemanticDigest, err = canonicaljson.Hash(r.Capsule.SourceProjection)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func a2SourceProjectionRequest(t *testing.T) IntentRequest {
	t.Helper()
	r := validIntentRequest(t)
	ref := rc.CatalogTypeReference{Type: "map[text][integer]"}
	p, err := rc.AdmitCollectionProjection(ref)
	if err != nil {
		t.Fatal(err)
	}
	r.Capsule.SourceProjection = rc.FanOutPlanSemantics{
		ElementRef: r.PlanRef.ElementRef, ItemsFrom: "payload.items", CollectionType: ref, CollectionProjection: p, ItemType: p.ItemType(), ItemAlias: "key", Identity: "key", IdentityDerived: true, MaxItems: 5, Emit: rc.EmitSpec{Event: "item.ready"},
	}
	r.PlanRef.SemanticDigest, err = canonicaljson.Hash(r.Capsule.SourceProjection)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestA2SourceProjectionStrictCodecAndPlanAgreement(t *testing.T) {
	r := a2SourceProjectionRequest(t)
	raw, err := MarshalCapsule(r.Capsule)
	if err != nil {
		t.Fatal(err)
	}
	var capsule Capsule
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &capsule); err != nil {
		t.Fatal(err)
	}
	if !capsule.Equal(r.Capsule) {
		t.Fatal("retained plan evidence changed in codec")
	}
	r.Capsule = capsule
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	items, err := r.ProjectSource(map[string]any{"z": []any{1}, " a ": []any{2}, "a": []any{3}})
	if err != nil || !reflect.DeepEqual(items, []any{" a ", "a", "z"}) {
		t.Fatalf("projected items: %#v %v", items, err)
	}
	for _, value := range []any{
		map[string]any{"z": []any{1}, " a ": []any{2}, "a": []any{"not an integer"}},
		map[string]any{"z": []any{1}, " a ": []any{2}, "a": 3},
	} {
		if _, err := r.ProjectSource(value); err == nil {
			t.Fatalf("admitted source catalog failed to reject invalid map values: %#v", value)
		}
	}
	plan := rc.FanOutCompiledPlan{
		Ref: r.PlanRef, ItemsFrom: r.Capsule.SourceProjection.ItemsFrom, CollectionType: r.Capsule.SourceProjection.CollectionType,
		CollectionProjection: r.Capsule.SourceProjection.CollectionProjection, ItemType: r.Capsule.SourceProjection.ItemType, ItemAlias: "key", Identity: "key", IdentityDerived: true, MaxItems: 5, Emit: rc.EmitSpec{Event: "item.ready"},
	}
	if err := r.ValidateCompiledPlan(plan); err != nil {
		t.Fatal(err)
	}
	plan.CollectionProjection = plan.CollectionProjection.Clone()
	plan.CollectionProjection.Type.Value.Element.Kind = rc.CatalogTypeText
	if err := r.ValidateCompiledPlan(plan); err == nil {
		t.Fatal("loaded plan interpretation drift accepted")
	}
	for _, change := range []func(*IntentRequest){
		func(r *IntentRequest) { r.Capsule.SourceProjection = rc.FanOutPlanSemantics{} },
		func(r *IntentRequest) { r.Capsule.SourceProjection.CollectionProjection.Kind = rc.CollectionListItems },
		func(r *IntentRequest) { r.Capsule.SourceProjection.MaxItems = 2 },
		func(r *IntentRequest) { r.Capsule.SourceProjection.SourceAfterWrites = true },
		func(r *IntentRequest) { r.Source.Field = "different" },
		func(r *IntentRequest) { r.PlanRef.SemanticDigest = "wrong" },
		func(r *IntentRequest) { r.Cardinality = 6 },
	} {
		bad := r
		bad.Capsule.SourceProjection = r.Capsule.SourceProjection.Clone()
		change(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatal("contradictory source evidence accepted")
		}
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	wire["source_projection"].(map[string]any)["unexpected"] = "must reject"
	badRaw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(badRaw, &capsule); err == nil {
		t.Fatal("unknown source witness field accepted")
	}
}

func TestA2SourceProjectionRetainsExactRevisionAndRangeFences(t *testing.T) {
	r := a2SourceProjectionRequest(t)
	r.Source = SourceRef{Kind: SourceEntityField, RunID: r.Key.RunID, EntityID: r.Key.RunID, Field: "items"}
	r.Capsule.EntityID = r.Key.RunID
	r.Capsule.SourceProjection.ItemsFrom = "entity.items"
	r.Capsule.SourceProjection.SourceAfterWrites = true
	var err error
	r.PlanRef.SemanticDigest, err = canonicaljson.Hash(r.Capsule.SourceProjection)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	i := Intent{Request: r, Source: r.Source, Cursor: 1, Status: StatusOpen, NextChunkSize: 2, CreatedAt: now, UpdatedAt: now}
	i.Source.MutationID = r.Key.TriggeringDeliveryID
	if err := i.Validate(); err != nil {
		t.Fatal(err)
	}
	if i.ChunkEndOrdinal() != 3 {
		t.Fatal("projection changed ordinal budget")
	}
	for _, change := range []func(*Intent){
		func(i *Intent) { i.Source.MutationID = "" },
		func(i *Intent) { i.Source.RunID = i.Source.MutationID },
		func(i *Intent) { i.Source.Field = "other" },
		func(i *Intent) { i.Source.Kind = SourceEventPayloadField },
		func(i *Intent) { i.Cursor = 4 },
		func(i *Intent) { i.NextChunkSize = MaxChunkSize + 1 },
	} {
		bad := i
		change(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatal("revision/progress contradiction accepted")
		}
	}
}
