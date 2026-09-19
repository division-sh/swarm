package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func settlementRootFixture(t testing.TB, size int) RouteSettlement {
	t.Helper()
	var plans []ConnectPlanEvaluation
	if size > 0 {
		var targets []RouteIdentity
		var candidates []ConnectCandidateEvidence
		for i := 0; i < size; i++ {
			name := fmt.Sprintf("consumer-%02d", i)
			targets = append(targets, RouteIdentity{FlowID: name, FlowInstance: name})
			candidate, err := NewConnectCandidateEvidence(
				AdmitConnectReceiverIdentity(sha256.Sum256([]byte(name))),
				MustNodeDeliveryRecipient(identitytest.RootNode(t, name)), name,
				agentidentity.Plan{}, ConnectCandidateAccepted)
			if err != nil {
				t.Fatal(err)
			}
			candidates = append(candidates, candidate)
		}
		plan, err := NewConnectPlanEvaluation(AdmitConnectPlanIdentity(sha256.Sum256([]byte("root-wire-plan"))), ConnectPlanResolved, targets, candidates)
		if err != nil {
			t.Fatal(err)
		}
		plans = append(plans, plan)
	}
	ledger, err := NewConnectEvaluationLedger(plans)
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewDeliverySettlement(EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func checkSettlementRootCodec(t testing.TB, raw []byte, seed RouteSettlement, wrapped bool) {
	t.Helper()
	current, original := seed, routeSettlementRootBefore(seed)
	var gotErr, wantErr error
	if wrapped {
		gotErr, wantErr = json.Unmarshal(raw, &current), json.Unmarshal(raw, &original)
	} else {
		gotErr, wantErr = current.UnmarshalJSON(raw), original.UnmarshalJSON(raw)
	}
	if (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("whole-root admission changed wrapped=%v raw=%q: current=%v original=%v", wrapped, raw, gotErr, wantErr)
	}
	if !reflect.DeepEqual(current, RouteSettlement(original)) {
		t.Fatalf("whole-root value changed wrapped=%v raw=%q: current=%+v original=%+v", wrapped, raw, current, original)
	}
	if gotErr != nil {
		if !reflect.DeepEqual(current, seed) || !reflect.DeepEqual(RouteSettlement(original), seed) {
			t.Fatalf("root error mutated populated receiver for %q", raw)
		}
		return
	}
	if !bytes.Equal(settlementWireCanonical(t, current), settlementWireCanonical(t, RouteSettlement(original))) {
		t.Fatalf("whole-root canonical bytes changed for %q", raw)
	}
}

func TestRouteSettlementRootCodecDifferential(t *testing.T) {
	seed := settlementRootFixture(t, 1)
	var nilCurrent *RouteSettlement
	var nilOriginal *routeSettlementRootBefore
	if nilCurrent.UnmarshalJSON([]byte(`{}`)) == nil || nilOriginal.UnmarshalJSON([]byte(`{}`)) == nil {
		t.Fatal("nil receiver lost refusal")
	}
	var wire routeSettlementWire
	if err := json.Unmarshal(settlementWireCanonical(t, seed), &wire); err != nil {
		t.Fatal(err)
	}
	fullLedger := string(settlementWireCanonical(t, wire.Evaluation))
	prefix := `"write_class":"normal_publication","arm":"delivery",`
	inputs := []string{
		"", " ", "null", "{}", "[]", "true", "1", `"object"`, "{", "{]",
		`{"write_class":"directive_direct","arm":"no_delivery","reason":"no_subscriber_by_design"}`,
		`{"write_class":"normal_publication","arm":"delivery"}`,
		`{"write_class":"normal_publication","arm":"delivery","reason":"bad","evaluation":{"plans":[]}}`,
		`{"write_class":"invented","arm":"delivery","evaluation":{"plans":[]}}`,
		`{"write_class":"normal_publication","arm":"invented","evaluation":{"plans":[]}}`,
		`{"write_class":"normal_publication","arm":"no_delivery","reason":"invented","evaluation":{"plans":[]}}`,
		`{` + prefix + `"unknown":0,"evaluation":{"plans":[]}}`,
		`{` + prefix + `"evaluation":{"plans":[]},"unknown":0}`,
		`{` + prefix + `"evaluation":{"plans":[{"plan_sha256":"bad","resolution":"no_registration","targets":[],"candidates":[]}]}}`,
		`{` + prefix + `"evaluation":{"plans":[{"plan_sha256":"` + strings.Repeat("a", 64) + `","resolution":"resolved","targets":[],"candidates":[]}]}}`,
	}
	// Duplicate pointer fields must clear on null, reuse on objects, and invoke
	// a fresh ledger decode even when the previous object was complete.
	evaluations := []string{
		`null`, `{"plans":[]}`, fullLedger, `{}`, `{"Plans":[]}`, `{"plans":null}`,
		`{"plans":[],"Plans":null}`, `{"plans":null,"Plans":[]}`,
		`{"plans":[null]}`, `{"plans":[{"targets":[],"candidates":[]}]}`,
		`{"plans":[],"unknown":1}`, `[]`, `false`, `1`, `"ledger"`,
	}
	for _, key := range []string{`evaluation`, `EVALUATION`, `ev\u0061luation`} {
		for _, first := range evaluations {
			inputs = append(inputs, `{`+prefix+`"`+key+`":`+first+`}`)
			for _, second := range evaluations {
				inputs = append(inputs, `{`+prefix+`"evaluation":`+first+`,"`+key+`":`+second+`}`)
			}
		}
	}
	for _, tc := range []struct{ field, valid string }{
		{"write_class", `"normal_publication"`}, {"arm", `"delivery"`}, {"reason", `""`},
	} {
		for _, key := range []string{tc.field, strings.ToUpper(tc.field), strings.ReplaceAll(tc.field, "s", `\u017f`)} {
			for _, value := range []string{tc.valid, "null", `"bad"`, "0", "false", "{}", "[]"} {
				for _, reverse := range []bool{false, true} {
					left, right := `"`+tc.field+`":`+tc.valid, `"`+key+`":`+value
					if reverse {
						left, right = right, left
					}
					inputs = append(inputs, `{`+prefix+`"evaluation":{"plans":[]},`+left+`,`+right+`}`)
				}
			}
		}
	}
	for _, size := range []int{0, 1, 18} {
		raw := string(settlementWireCanonical(t, settlementRootFixture(t, size)))
		inputs = append(inputs, raw, " \n"+raw+"\t")
		for _, suffix := range []string{" null", " {}", " []", " true", " 1", " garbage", ",", "\x00", "\xff"} {
			inputs = append(inputs, raw+suffix)
		}
		for _, replacement := range []string{"bad", "consumer-\xff", `consumer-\ud800`, `consumer-\"\\\n\u2028<&>`} {
			inputs = append(inputs, strings.Replace(raw, "consumer-00", replacement, 1))
		}
		inputs = append(inputs, strings.Replace(raw, `"accepted"`, `"invented"`, 1))
		inputs = append(inputs, strings.Replace(raw, `"recipient_kind":"node"`, `"recipient_kind":"agent"`, 1))
		if size == 1 {
			for end := 0; end < len(raw); end++ {
				inputs = append(inputs, raw[:end])
			}
		}
	}
	for i, raw := range inputs {
		t.Run(fmt.Sprintf("case_%04d", i), func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				checkSettlementRootCodec(t, []byte(raw), seed, wrapped)
			}
		})
	}
}

func FuzzRouteSettlementRootCodecDifferential(f *testing.F) {
	seed := settlementRootFixture(f, 1)
	f.Add(settlementWireCanonical(f, seed))
	for _, raw := range []string{
		`null`, `{}`, `{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]}}`,
		`{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]},"evaluation":{}}`,
		`{"write_class":"normal_publication","arm":"delivery","evaluation":null,"EVALUATION":{"plans":[]}}`,
		`{"write_class":"normal_publication","arm":"delivery","evaluation":{"plans":[]},"evaluation":null}`,
	} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		for _, wrapped := range []bool{false, true} {
			checkSettlementRootCodec(t, raw, seed, wrapped)
		}
	})
}

func BenchmarkRouteSettlementRootCodec(b *testing.B) {
	for _, size := range []int{0, 1, 18} {
		value := settlementRootFixture(b, size)
		raw := settlementWireCanonical(b, value)
		checkSettlementRootCodec(b, raw, value, false)
		var baseline routeSettlementRootBefore
		if err := baseline.unmarshalJSON(raw, true); err != nil || !reflect.DeepEqual(value, RouteSettlement(baseline)) {
			b.Fatalf("root-only baseline changed canonical fixture: %v", err)
		}
		for _, version := range []string{"original", "before_root", "after"} {
			b.Run(fmt.Sprintf("recipients%d/%s", size, version), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				for b.Loop() {
					var err error
					if version == "after" {
						var decoded RouteSettlement
						err = decoded.UnmarshalJSON(raw)
					} else {
						var decoded routeSettlementRootBefore
						err = decoded.unmarshalJSON(raw, version == "before_root")
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
