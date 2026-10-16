package processbinding

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestActivityExecutionBindingExactReference(t *testing.T) {
	exact := ActivityExecutionBinding{Version: ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
	if err := exact.Validate(); err != nil || exact.IsZero() {
		t.Fatalf("exact reference: zero=%t err=%v", exact.IsZero(), err)
	}
	encoded, err := json.Marshal(exact)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ActivityExecutionBinding
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != exact {
		t.Fatalf("immutable attribution round trip: decoded=%+v err=%v", decoded, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 3 || fields["version"] == nil || fields["grant_id"] == nil || fields["state_version"] == nil {
		t.Fatalf("reference must contain only its version and exact historical keys: fields=%v err=%v", fields, err)
	}
	for _, version := range []uint64{1, math.MaxInt64} {
		copy := exact
		copy.StateVersion = version
		if err := copy.Validate(); err != nil || copy == exact {
			t.Fatalf("distinct historical version was normalized to another snapshot: reference=%+v err=%v", copy, err)
		}
	}
	for _, cell := range []struct {
		name   string
		mutate func(*ActivityExecutionBinding)
	}{
		{"missing_version", func(b *ActivityExecutionBinding) { b.Version = "" }},
		{"other_version", func(b *ActivityExecutionBinding) { b.Version = "activity-execution.v2" }},
		{"missing_grant", func(b *ActivityExecutionBinding) { b.GrantID = "" }},
		{"zero_grant", func(b *ActivityExecutionBinding) { b.GrantID = uuid.Nil.String() }},
		{"compact_grant", func(b *ActivityExecutionBinding) { b.GrantID = strings.ReplaceAll(b.GrantID, "-", "") }},
		{"whitespace_grant", func(b *ActivityExecutionBinding) { b.GrantID += " " }},
		{"zero_state", func(b *ActivityExecutionBinding) { b.StateVersion = 0 }},
		{"overflow_state", func(b *ActivityExecutionBinding) { b.StateVersion = math.MaxInt64 + 1 }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			copy := exact
			cell.mutate(&copy)
			if err := copy.Validate(); err == nil {
				t.Fatalf("invalid reference admitted: %+v", copy)
			}
		})
	}
	var legacy ActivityExecutionBinding
	if !legacy.IsZero() || legacy.Validate() == nil {
		t.Fatal("missing historical attribution became an admitted reference")
	}
}
