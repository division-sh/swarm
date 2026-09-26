package bus

import (
	"fmt"
	"reflect"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// This is the former linear comparison, kept only as a differential test oracle.
func linearActiveTargetDescriptors(input []ActiveTargetDescriptor) []ActiveTargetDescriptor {
	var out []ActiveTargetDescriptor
	if input != nil {
		out = make([]ActiveTargetDescriptor, 0, len(input))
	}
	for _, descriptor := range input {
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" && descriptor.EntityID == "" {
			continue
		}
		duplicate := false
		for _, existing := range out {
			existing = existing.Normalized()
			if existing.ID == descriptor.ID && existing.EntityID == descriptor.EntityID && existing.FlowInstance == descriptor.FlowInstance && existing.Materializing == descriptor.Materializing && existing.Availability == descriptor.Availability {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, descriptor)
		}
	}
	return out
}

func TestOrderedActiveTargetDescriptorsPreservesLinearSemantics(t *testing.T) {
	base := ActiveTargetDescriptor{
		ID: " receiver ", EntityID: " entity ", FlowInstance: " /flow/one/ ",
		AddressFields: map[string]string{"site": "first"},
	}
	availability := runtimepipeline.NewDeliveryTargetAvailability("ready", "inactive", false)
	otherID := base
	otherID.ID = "other"
	otherEntity := base
	otherEntity.EntityID = "other"
	otherFlow := base
	otherFlow.FlowInstance = "flow/two"
	materializing := base
	materializing.Materializing = true
	unavailable := base
	unavailable.Availability = availability
	duplicate := base
	duplicate.ID = "receiver"
	duplicate.EntityID = "entity"
	duplicate.FlowInstance = "flow/one"
	duplicate.AddressFields = map[string]string{"site": "later"}

	cases := []struct {
		name  string
		input []ActiveTargetDescriptor
	}{
		{name: "nil"},
		{name: "empty", input: []ActiveTargetDescriptor{}},
		{name: "first address wins", input: []ActiveTargetDescriptor{base, duplicate}},
		{name: "reversed first address wins", input: []ActiveTargetDescriptor{duplicate, base}},
		{name: "all exact key fields", input: []ActiveTargetDescriptor{
			base, otherID, otherEntity, otherFlow, materializing, unavailable, duplicate,
		}},
		{name: "ownerless skipped", input: []ActiveTargetDescriptor{{ID: "ownerless"}, base}},
		{name: "empty flow with entity retained", input: []ActiveTargetDescriptor{{EntityID: "entity"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ordered := newOrderedActiveTargetDescriptors(nil)
			if tc.input != nil {
				ordered = newOrderedActiveTargetDescriptors([]ActiveTargetDescriptor{})
			}
			for _, descriptor := range tc.input {
				ordered.add(descriptor)
			}
			got := ordered.descriptors
			want := linearActiveTargetDescriptors(tc.input)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ordered descriptors = %#v, linear = %#v", got, want)
			}
		})
	}
	initial := []ActiveTargetDescriptor{base, duplicate}
	ordered := newOrderedActiveTargetDescriptors(initial)
	ordered.add(duplicate)
	ordered.add(otherFlow)
	wantInitial := append(append([]ActiveTargetDescriptor(nil), initial...), otherFlow.Normalized())
	if !reflect.DeepEqual(ordered.descriptors, wantInitial) {
		t.Fatalf("pre-existing descriptors were rewritten: got %#v, want %#v", ordered.descriptors, wantInitial)
	}

	input := make([]ActiveTargetDescriptor, 0, 2048)
	for i := range 1024 {
		descriptor := ActiveTargetDescriptor{
			ID: fmt.Sprintf("receiver-%d", i), EntityID: fmt.Sprintf("entity-%d", i),
			FlowInstance:  fmt.Sprintf("flow/instance-%d", i),
			AddressFields: map[string]string{"ordinal": fmt.Sprint(i)},
		}
		input = append(input, descriptor)
		if i%2 == 0 {
			duplicate := descriptor
			duplicate.AddressFields = map[string]string{"ordinal": "later"}
			input = append(input, duplicate)
		}
	}
	ordered = newOrderedActiveTargetDescriptors(nil)
	for _, descriptor := range input {
		ordered.add(descriptor)
	}
	got := ordered.descriptors
	want := linearActiveTargetDescriptors(input)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("large ordered descriptors differ from linear oracle: got %d, want %d", len(got), len(want))
	}
}

func BenchmarkActiveTargetDescriptorDedup(b *testing.B) {
	input := make([]ActiveTargetDescriptor, 0, 1024)
	for i := range 1024 {
		input = append(input, ActiveTargetDescriptor{
			ID: fmt.Sprintf("receiver-%d", i), EntityID: fmt.Sprintf("entity-%d", i),
			FlowInstance: fmt.Sprintf("flow/instance-%d", i),
		})
	}
	b.Run("linear", func(b *testing.B) {
		for range b.N {
			if got := linearActiveTargetDescriptors(input); len(got) != len(input) {
				b.Fatal("lost descriptors")
			}
		}
	})
	b.Run("ordered", func(b *testing.B) {
		for range b.N {
			ordered := newOrderedActiveTargetDescriptors(nil)
			for _, descriptor := range input {
				ordered.add(descriptor)
			}
			if got := ordered.descriptors; len(got) != len(input) {
				b.Fatal("lost descriptors")
			}
		}
	})
}
