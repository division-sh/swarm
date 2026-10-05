package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func receiverCodecRoutes(t testing.TB) []DeliveryRoute {
	_, node, agents := receiverMaterializationFixture(t)
	return append([]DeliveryRoute{node}, agents...)
}

// Accepted inputs must bind the destination and round-trip through the one
// construction-only codec. Failed inputs must not mutate either destination.
func checkReceiverCodecRoundTrip(t testing.TB, route DeliveryRoute, standalone bool, raw []byte) {
	t.Helper()
	before := route
	if standalone {
		got := route.Initialization
		err := got.UnmarshalJSON(raw)
		if err != nil {
			if got != before.Initialization {
				t.Fatal("failed decode mutated admitted receipt")
			}
			return
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		var restored ReceiverInitialization
		if err := restored.UnmarshalJSON(encoded); err != nil || restored != got {
			t.Fatalf("standalone roundtrip: %v", err)
		}
		return
	}
	got, err := RestoreReceiverMaterializationRecord(route, raw)
	if !reflect.DeepEqual(route, before) {
		t.Fatal("decode mutated input route")
	}
	if err != nil {
		if !reflect.DeepEqual(got, DeliveryRoute{}) {
			t.Fatal("failed restoration leaked partial route")
		}
		return
	}
	if !got.Initialization.Empty() {
		if err := got.Initialization.ValidateRoute(got); err != nil {
			t.Fatal(err)
		}
	}
	identity, err := got.Identity()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeReceiverMaterializationRecord(got)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreReceiverMaterializationRecord(got, encoded)
	if err != nil || !reflect.DeepEqual(restored, got) {
		t.Fatalf("record roundtrip: %v", err)
	}
	restoredID, err := restored.Identity()
	if err != nil || restoredID != identity {
		t.Fatalf("record route identity changed: %v", err)
	}
}

func receiverCodecHostileInputs(raw []byte) [][]byte {
	inputs := [][]byte{raw, nil, []byte("null"), []byte(" null "), []byte("{}"), []byte("[]"), []byte("true"), []byte("0"), []byte(`"object"`), append(append([]byte{}, raw...), []byte(" {}")...), append(append([]byte{}, raw...), 0xff)}
	var visit func([]byte, func([]byte) []byte)
	visit = func(value []byte, wrap func([]byte) []byte) {
		var object map[string]json.RawMessage
		if json.Unmarshal(value, &object) != nil || object == nil {
			return
		}
		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		encode := func(replace string, replacement []byte, omit bool) []byte {
			var out bytes.Buffer
			out.WriteByte('{')
			first := true
			for _, key := range keys {
				if omit && key == replace {
					continue
				}
				if !first {
					out.WriteByte(',')
				}
				first = false
				name, _ := json.Marshal(key)
				out.Write(name)
				out.WriteByte(':')
				if key == replace {
					out.Write(replacement)
				} else {
					out.Write(object[key])
				}
			}
			out.WriteByte('}')
			return out.Bytes()
		}
		for _, key := range keys {
			inputs = append(inputs, wrap(encode(key, nil, true)))
			for _, replacement := range []string{"null", "false", "0", "-0", "1.0", "1e999", "1e-999", "[]", "{}", `""`, `"\ud800"`, "\"\xff\""} {
				inputs = append(inputs, wrap(encode(key, []byte(replacement), false)))
			}
			name, _ := json.Marshal(key)
			alias, _ := json.Marshal(strings.ToUpper(key))
			base := encode("", nil, false)
			inputs = append(inputs, wrap(bytes.Replace(base, name, alias, 1)))
			// Exact/escaped duplicate keys reject; case aliases retain their
			// original wire-order semantics, including null resetting pointers.
			for _, spelling := range [][]byte{name, alias, []byte(fmt.Sprintf(`"\u%04x%s"`, key[0], key[1:]))} {
				for _, replacement := range [][]byte{object[key], []byte("null"), []byte("false")} {
					prefix := append(append(append([]byte("{"), spelling...), ':'), replacement...)
					prefix = append(prefix, ',')
					inputs = append(inputs, wrap(append(prefix, base[1:]...)))
					suffix := append(append(append(append([]byte{}, base[:len(base)-1]...), ','), spelling...), ':')
					suffix = append(append(suffix, replacement...), '}')
					inputs = append(inputs, wrap(suffix))
				}
			}
			visit(object[key], func(child []byte) []byte { return wrap(encode(key, child, false)) })
		}
		for _, extra := range []string{`"unknown":true`, `"unknown":1e999`, `"unknown":"\ud800"`} {
			base := encode("", nil, false)
			inputs = append(inputs, wrap(append([]byte("{"+extra+","), base[1:]...)))
		}
	}
	visit(raw, func(value []byte) []byte { return value })
	return inputs
}

func TestReceiverInitializationCodecClosedRecord(t *testing.T) {
	cases := 0
	for index, route := range receiverCodecRoutes(t) {
		t.Run(fmt.Sprintf("recipient-%d", index), func(t *testing.T) {
			record, err := EncodeReceiverMaterializationRecord(route)
			if err != nil {
				t.Fatal(err)
			}
			initialization, err := json.Marshal(route.Initialization)
			if err != nil {
				t.Fatal(err)
			}
			for mode, raw := range [][]byte{record, initialization} {
				for _, input := range receiverCodecHostileInputs(raw) {
					for _, admitted := range []bool{false, true} {
						base := route
						if !admitted {
							base.Initialization = ReceiverInitialization{}
						}
						checkReceiverCodecRoundTrip(t, base, mode == 1, input)
						cases++
					}
				}
			}
		})
	}
	t.Logf("checked %d hostile-input/admitted-destination roundtrip cells", cases)
}

func TestReceiverInitializationRequiredFieldsRejectIndividually(t *testing.T) {
	_, node, _ := receiverMaterializationFixture(t)
	raw, err := json.Marshal(node.Initialization)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"kind", "run_id", "event_id", "target"} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/remove=%v", field, remove), func(t *testing.T) {
				var object map[string]json.RawMessage
				if err := json.Unmarshal(raw, &object); err != nil {
					t.Fatal(err)
				}
				if remove {
					delete(object, field)
				} else {
					object[field] = json.RawMessage("null")
				}
				bad, err := json.Marshal(object)
				if err != nil {
					t.Fatal(err)
				}
				before := node.Initialization
				got := before
				if err := got.UnmarshalJSON(bad); err == nil || got != before {
					t.Fatalf("accepted or mutated on missing %s", field)
				}
			})
		}
	}
}

func FuzzReceiverInitializationCodecRoundTrip(f *testing.F) {
	routes := receiverCodecRoutes(f)
	for index, route := range routes {
		record, err := EncodeReceiverMaterializationRecord(route)
		if err != nil {
			f.Fatal(err)
		}
		initialization, err := json.Marshal(route.Initialization)
		if err != nil {
			f.Fatal(err)
		}
		for mode, value := range [][]byte{record, initialization} {
			f.Add(uint8(index), uint8(mode), value)
			f.Add(uint8(index), uint8(mode), append(append([]byte{}, value...), []byte(" {}")...))
		}
	}
	f.Fuzz(func(t *testing.T, index, mode uint8, raw []byte) {
		base := routes[int(index)%len(routes)]
		checkReceiverCodecRoundTrip(t, base, mode%2 == 1, raw)
		base.Initialization = ReceiverInitialization{}
		checkReceiverCodecRoundTrip(t, base, mode%2 == 1, raw)
	})
}

func BenchmarkReceiverInitializationRecordDecode(b *testing.B) {
	for index, route := range receiverCodecRoutes(b) {
		raw, err := EncodeReceiverMaterializationRecord(route)
		if err != nil {
			b.Fatal(err)
		}
		route.Initialization = ReceiverInitialization{}
		b.Run(fmt.Sprintf("recipient-%d", index), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := RestoreReceiverMaterializationRecord(route, raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
