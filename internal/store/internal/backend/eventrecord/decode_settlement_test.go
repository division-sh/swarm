package eventrecord

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestRecordJointDecodeRetainsStrictAdmissionAndFreshEvidence(t *testing.T) {
	record := validRecord(t)
	before := record.Clone()
	admitted, settlement, err := record.DecodeWithSettlement()
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := FromAdmitted(admitted, settlement)
	if err != nil || !record.Equal(rebuilt) {
		t.Fatalf("joint decode changed canonical facts: %v", err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("joint decode mutated its input")
	}
	for name, mutate := range map[string]func(*Record){
		"payload null":          func(r *Record) { r.Payload = []byte(`{"bad":null}`) },
		"noncanonical identity": func(r *Record) { r.EventID += " " },
		"unknown settlement field": func(r *Record) {
			r.RouteSettlement = append(bytes.TrimSuffix(r.RouteSettlement, []byte(`}`)), []byte(`,"unknown":true}`)...)
		},
		"trailing settlement": func(r *Record) { r.RouteSettlement = append(r.RouteSettlement, []byte(` {}`)...) },
		"wrong settlement class": func(r *Record) {
			value := map[string]any{}
			if err := json.Unmarshal(r.RouteSettlement, &value); err != nil {
				t.Fatal(err)
			}
			value["write_class"] = "invented"
			r.RouteSettlement, _ = json.Marshal(value)
		},
		"source corruption": func(r *Record) { r.SourceRoute = []byte(`{"flow_id":"foreign"}`) },
	} {
		t.Run(name, func(t *testing.T) {
			hostile := record.Clone()
			mutate(&hostile)
			got, route, err := hostile.DecodeWithSettlement()
			if !errors.Is(err, ErrCorrupt) || got.ID() != "" || route.WriteClass().Code() != "" {
				t.Fatalf("corruption returned admitted partial evidence: event=%s class=%s err=%v", got.ID(), route.WriteClass().Code(), err)
			}
			if _, err := hostile.Decode(); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ordinary decoder disagrees with joint admission: %v", err)
			}
		})
	}
	// A later read of changed bytes must not reuse earlier validated evidence.
	record.Payload = []byte(`{"nested":{"a":2}}`)
	fresh, freshSettlement, err := record.DecodeWithSettlement()
	if err != nil || bytes.Equal(fresh.Event().Payload(), admitted.Event().Payload()) {
		t.Fatalf("joint decode reused stale event input: %v", err)
	}
	if freshSettlement.WriteClass() != settlement.WriteClass() {
		t.Fatal("payload-only mutation changed settlement identity")
	}
	if _, err := FromAdmitted(admitted, events.RouteSettlement{}); err == nil {
		t.Fatal("constructor accepted invalid typed settlement")
	}
}

func TestRecordJSONEqualityDoesNotGrantAdmission(t *testing.T) {
	for _, tc := range []struct {
		left, right string
		equal       bool
	}{
		{`{"items":[1,2]}`, `{"items":[1,2]}`, true},
		{`{"items":[1,2]}`, ` { "items": [1.0,2] } `, true},
		{`{"items":[1,2]}`, `{"items":[2,1]}`, false},
		{`{"a":1}`, `{"b":1}`, false},
		{`{"a":1}`, `{"a":1} {}`, false},
	} {
		if got := jsonEqual([]byte(tc.left), []byte(tc.right)); got != tc.equal {
			t.Fatalf("equality(%s,%s)=%v, want %v", tc.left, tc.right, got, tc.equal)
		}
	}
	invalid := validRecord(t)
	invalid.RouteSettlement = []byte(`{"write_class":"invented"}`)
	if !invalid.Equal(invalid.Clone()) {
		t.Fatal("identical records should compare equal independently of admission")
	}
	if _, _, err := invalid.DecodeWithSettlement(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("identical invalid bytes bypassed admission: %v", err)
	}
}

func TestRecordSettlementDecodeMatchesStrictWireAdmission(t *testing.T) {
	valid := `{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]}}`
	for _, raw := range []string{
		valid, " \n" + valid + "\t", "", "null", "[]", valid + ` {}`,
		`{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]},"unknown":true}`,
		`{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]},"evaluation":{}}`,
		`{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[{"plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","resolution":"no_registration","targets":[],"candidates":[]}],"plans":[{"plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","resolution":"no_registration"}]}}`,
	} {
		var expected events.RouteSettlement
		expectedErr := json.Unmarshal([]byte(raw), &expected)
		actual, err := (Record{RouteSettlement: []byte(raw)}).DecodeSettlement()
		if (expectedErr == nil) != (err == nil) || (err == nil && !reflect.DeepEqual(actual, expected)) {
			t.Fatalf("record settlement admission differs for %q: actual=%v expected=%v", raw, err, expectedErr)
		}
	}
}
