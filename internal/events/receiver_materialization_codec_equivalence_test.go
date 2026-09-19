package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func receiverCodecFixture(t testing.TB) (Event, DeliveryRoute, []DeliveryRoute) {
	t.Helper()
	event, err := NewExistingRunRootIngressEvent(ExistingRunRootIngressEventInput{Facts: validFacts(), RunID: "11111111-1111-4111-8111-111111111111"})
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewMaterializingEntityTarget(RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: "22222222-2222-4222-8222-222222222222"})
	if err != nil {
		t.Fatal(err)
	}
	node := identitytest.FlowNode(t, "consumer", "materializer")
	materializer := DeliveryRoute{Recipient: MustNodeDeliveryRecipient(node), Target: target}
	materializer.Initialization, err = AdmitNodeReceiverInitialization(event, target, node)
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256([]byte("exact-compiled-receiver-pin"))
	materializer.ConnectClaim, err = AdmitConnectExecutionClaim(sha256.Sum256([]byte("node-edge-generation")), pin, materializer.Recipient, node, "item.received")
	if err != nil {
		t.Fatal(err)
	}
	var agents []DeliveryRoute
	for _, label := range []string{"materializer", "renamed-observer"} {
		name, err := agentidentity.DeclaredName(label, "consumer/agents.yaml")
		if err != nil {
			t.Fatal(err)
		}
		route, err := agentidentity.PresentRoute("consumer", "consumer", "consumer")
		if err != nil {
			t.Fatal(err)
		}
		actor, err := agentidentity.New(event.RunID(), name, route)
		if err != nil {
			t.Fatal(err)
		}
		agent := DeliveryRoute{Recipient: MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor, Target: target}
		agent.Initialization = materializer.Initialization
		agent.ConnectClaim, err = AdmitConnectExecutionClaim(sha256.Sum256([]byte(label)), pin, agent.Recipient, identity.ExecutableNode{}, "item.received")
		if err != nil {
			t.Fatal(err)
		}
		agents = append(agents, agent)
	}
	return event, materializer, agents
}

func receiverCodecRoutes(t testing.TB) []DeliveryRoute {
	t.Helper()
	event, node, agents := receiverCodecFixture(t)
	plan, err := AdmitReceiverMaterializationPlan(event, node, agents, append([]DeliveryRoute{node}, agents...))
	if err != nil {
		t.Fatal(err)
	}
	agent, err := plan.BindDependent(agents[0])
	if err != nil {
		t.Fatal(err)
	}
	flow := node
	flow.Initialization, err = AdmitFlowReceiverInitialization(event, node.Target)
	if err != nil {
		t.Fatal(err)
	}
	return []DeliveryRoute{node, agent, flow}
}

func checkReceiverCodecParity(t testing.TB, route DeliveryRoute, mode uint8, raw []byte) {
	t.Helper()
	before := route
	if mode%3 == 1 {
		got := route.Initialization
		want := receiverInitializationOracle(route.Initialization)
		gotErr := got.UnmarshalJSON(raw)
		wantErr := want.UnmarshalJSON(raw)
		checkReceiverCodecErrors(t, raw, gotErr, wantErr)
		if got != ReceiverInitialization(want) {
			t.Fatalf("initialization value or failure mutation differs for %q: got=%+v want=%+v", raw, got, want)
		}
		return
	}
	var got, want DeliveryRoute
	var gotErr, wantErr error
	if mode%3 == 0 {
		got, gotErr = RestoreReceiverMaterializationRecord(route, raw)
		want, wantErr = restoreReceiverMaterializationRecordOracle(route, raw)
	} else {
		got, gotErr = RestoreDeliveryMaterialization(route, raw)
		want, wantErr = restoreDeliveryMaterializationOracle(route, raw)
	}
	checkReceiverCodecErrors(t, raw, gotErr, wantErr)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(route, before) {
		t.Fatalf("route value/input mutation differs for %q: got=%+v want=%+v", raw, got, want)
	}
	if gotErr != nil {
		if !reflect.DeepEqual(got, DeliveryRoute{}) {
			t.Fatal("failed restoration leaked partial route")
		}
		return
	}
	gotID, gotIdentityErr := got.Identity()
	wantID, wantIdentityErr := want.Identity()
	if (gotIdentityErr == nil) != (wantIdentityErr == nil) || gotID != wantID {
		t.Fatalf("route byte identity changed for %q: %v / %v", raw, gotIdentityErr, wantIdentityErr)
	}
	gotBytes, gotEncodeErr := EncodeReceiverMaterializationRecord(got)
	wantBytes, wantEncodeErr := EncodeReceiverMaterializationRecord(want)
	if (gotEncodeErr == nil) != (wantEncodeErr == nil) || !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("durable bytes differ for %q: %s / %s", raw, gotBytes, wantBytes)
	}
}

func checkReceiverCodecErrors(t testing.TB, raw []byte, got, want error) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Fatalf("admission differs for %q: got=%v want=%v", raw, got, want)
	}
	var gotAdmission, wantAdmission *canonicaljson.AdmissionError
	if errors.As(got, &gotAdmission) != errors.As(want, &wantAdmission) {
		t.Fatalf("canonical admission error wrapping differs for %q: %v / %v", raw, got, want)
	}
}

// Mutate each object level, not just the outer record. Parent wrappers are
// re-encoded without changing the hostile bytes at the selected level.
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

func TestReceiverMaterializationCodecDifferential(t *testing.T) {
	cases := 0
	for index, route := range receiverCodecRoutes(t) {
		t.Run([]string{"node", "dependent_agent", "flow"}[index], func(t *testing.T) {
			record, err := EncodeReceiverMaterializationRecord(route)
			if err != nil {
				t.Fatal(err)
			}
			initialization, err := json.Marshal(route.Initialization)
			if err != nil {
				t.Fatal(err)
			}
			dependency := []byte("null")
			if !route.Materialization.Empty() {
				dependency, err = json.Marshal(route.Materialization)
				if err != nil {
					t.Fatal(err)
				}
			}
			for mode, raw := range [][]byte{record, initialization, dependency} {
				for _, input := range receiverCodecHostileInputs(raw) {
					for _, admitted := range []bool{false, true} {
						base := route
						if !admitted {
							base.Materialization = ReceiverMaterializationPlan{}
							if mode == 0 {
								base.Initialization = ReceiverInitialization{}
							}
						}
						checkReceiverCodecParity(t, base, uint8(mode), input)
						cases++
					}
				}
			}
		})
	}
	t.Logf("compared %d old/new admission and output cells", cases)
}

func TestReceiverMaterializationCodecBindingErrorsAndFreshInput(t *testing.T) {
	routes := receiverCodecRoutes(t)
	for _, source := range routes {
		raw, err := EncodeReceiverMaterializationRecord(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, destination := range routes {
			checkReceiverCodecParity(t, destination, 0, raw)
		}
		bare := source
		bare.Initialization, bare.Materialization = ReceiverInitialization{}, ReceiverMaterializationPlan{}
		first, err := RestoreReceiverMaterializationRecord(bare, raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Materialization.dependents) != 0 {
			first.Materialization.dependents[0] = DeliveryRouteIdentity{}
		}
		second, err := RestoreReceiverMaterializationRecord(bare, raw)
		if err != nil || !reflect.DeepEqual(second, source) {
			t.Fatalf("prior output mutated fresh admission: %v", err)
		}
		bad := bytes.Replace(raw, []byte(`"run_id":"11111111-1111-4111-8111-111111111111"`), []byte(`"run_id":"33333333-3333-4333-8333-333333333333"`), 1)
		if bytes.Equal(raw, bad) {
			t.Fatal("corruption did not alter selected bytes")
		}
		checkReceiverCodecParity(t, source, 0, bad)
		if got, err := RestoreReceiverMaterializationRecord(source, bad); err == nil || !reflect.DeepEqual(got, DeliveryRoute{}) {
			t.Fatal("fresh contradictory supplier did not return a bound error")
		}
	}
}

func FuzzReceiverMaterializationCodecDifferential(f *testing.F) {
	routes := receiverCodecRoutes(f)
	for index, route := range routes {
		raw, err := EncodeReceiverMaterializationRecord(route)
		if err != nil {
			f.Fatal(err)
		}
		initialization, _ := json.Marshal(route.Initialization)
		dependency := []byte("null")
		if !route.Materialization.Empty() {
			dependency, _ = json.Marshal(route.Materialization)
		}
		for mode, value := range [][]byte{raw, initialization, dependency} {
			f.Add(uint8(index), uint8(mode), value)
			f.Add(uint8(index), uint8(mode), append(append([]byte{}, value...), []byte(" {}")...))
		}
	}
	f.Fuzz(func(t *testing.T, index, mode uint8, raw []byte) {
		base := routes[int(index)%len(routes)]
		checkReceiverCodecParity(t, base, mode, raw)
		base.Materialization = ReceiverMaterializationPlan{}
		if mode%3 == 0 {
			base.Initialization = ReceiverInitialization{}
		}
		checkReceiverCodecParity(t, base, mode, raw)
	})
}

func BenchmarkReceiverMaterializationRecordDecode(b *testing.B) {
	for index, route := range receiverCodecRoutes(b) {
		raw, err := EncodeReceiverMaterializationRecord(route)
		if err != nil {
			b.Fatal(err)
		}
		route.Initialization, route.Materialization = ReceiverInitialization{}, ReceiverMaterializationPlan{}
		for _, old := range []bool{true, false} {
			name := []string{"node", "dependent_agent", "flow"}[index] + "/new"
			decode := RestoreReceiverMaterializationRecord
			if old {
				name = []string{"node", "dependent_agent", "flow"}[index] + "/old"
				decode = restoreReceiverMaterializationRecordOracle
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := decode(route, raw); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
