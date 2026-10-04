package main

import (
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

func TestRewriteGeneratedPositivePins(t *testing.T) {
	before := canonicalrouting.PinRewriteSyntaxSource(t, "RewriteGeneratedPositivePins-1")
	after, err := rewriteGoSource("producer.go", []byte("package fixture\nconst source = "+strconv.Quote(before)+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(before, "pins:\n  inputs:\n    events:\n      - {event: work.requested, source: harness}\n  outputs:\n    events:\n      - {event: work.completed, sink: harness}\n", "pins:\n  inputs:\n    - work.requested\n  outputs:\n    - work.completed\n", 1)
	if got := generatedSourceLiteral(t, after); got != want {
		t.Fatalf("rewritten source = %q, want %q", got, want)
	}
	second, err := rewriteGoSource("producer.go", after)
	if err != nil || string(second) != string(after) {
		t.Fatalf("second rewrite changed source: %s, %v", second, err)
	}
}

func TestRewriteGeneratedPinsSkipsMutationAndCompoundFragments(t *testing.T) {
	for _, source := range []string{
		"package fixture\nvar source = \"pins:\\n  inputs:\\n    events: \" + values\n",
		"package fixture\nvar source = strings.Replace(source, \"pins:\\n  inputs:\\n    events: [work.requested]\\n\", replacement, 1)\n",
	} {
		after, err := rewriteGoSource("producer.go", []byte(source))
		if err != nil || string(after) != source {
			t.Fatalf("non-source literal changed: %s, %v", after, err)
		}
	}
}

func TestRewriteGeneratedPinsRejectsUnratifiedOptions(t *testing.T) {
	for _, option := range []string{"source: external", "other: true", canonicalrouting.PinRewriteSyntaxSource(t, "RewriteGeneratedPinsRejectsUnratifiedOptions-2"), "initialize: {}"} {
		source := "pins:\n  inputs:\n    events:\n      - {event: work.requested, " + option + "}\n"
		if _, err := rewriteGoSource("producer.go", []byte("package fixture\nconst source = "+strconv.Quote(source)+"\n")); err == nil {
			t.Fatalf("rewrite accepted unratified option %s", option)
		}
	}
}

func TestRewriteKnownPinMutationMovesBothSides(t *testing.T) {
	name := "internal/runtime/testfixtures/canonicalrouting/fork_receiver_ownership.go"
	before := "      - receiver.closed\n"
	after := rewritePinMutationLiteral(name, before)
	if after != "    - receiver.closed\n" || rewritePinMutationLiteral(name, after) != after {
		t.Fatalf("pin mutation rewrite = %q", after)
	}
	if got := rewritePinMutationLiteral("other.go", before); got != before {
		t.Fatalf("unregistered fragment changed: %q", got)
	}
}

func TestRewritePinMutationConsumersTrackCurrentSequences(t *testing.T) {
	for _, tc := range []struct {
		file, before, after string
	}{
		{"internal/runtime/conformance/data_text_file_journey_2456_test.go", "events: [root.ready]", "    - root.ready\n"},
		{"internal/runtime/conformance/data_text_file_journey_2456_test.go", "events: [root.ready, root.ready.body]", "    - root.ready\n    - root.ready.body\n"},
		{"internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go", "      - %s\n", "    - %s\n"},
		{"internal/runtime/conformance/fan_out_b17_mixed_dependency_test.go", "      - account.task.completed\n", "    - account.task.completed\n"},
		{"internal/runtime/connector_schema_binding_test.go", "      - inbound.telegram.text_message\n", "    - inbound.telegram.text_message\n"},
		{"internal/store/internal/runtimepersistence/mutation_protocol_composed_journey_test.go", "events: [work.requested", "outputs: [work.requested"},
		{"internal/runtime/testfixtures/canonicalrouting/channel_delivery.go", "      - observer.requested\n", "    - observer.requested\n"},
		{"internal/runtime/testfixtures/canonicalrouting/fork_receiver_acquisition_effect_only.go", "  outputs: [receiver.finished]\n", "  outputs:\n    - receiver.finished\n"},
		{"internal/runtime/testfixtures/canonicalrouting/fork_receiver_notice_effect.go", "  outputs: [receiver.finished]\n", "  outputs:\n    - receiver.finished\n"},
		{"internal/runtime/testfixtures/canonicalrouting/fork_receiver_ownership.go", "  outputs:\n    [producer.closed]", "  outputs: [producer.closed]"},
		{"internal/runtime/testfixtures/canonicalrouting/receiver_agent_collision.go", "    - event: work.ready\n        initialize:\n", "    - event: work.ready\n      initialize:\n"},
		{"internal/runtime/testfixtures/canonicalrouting/lifecycle_emitters.go", "inputs: [loop.escaped]", "    - loop.escaped\n"},
		{"internal/runtime/testfixtures/canonicalrouting/lifecycle_emitters.go", "inputs: [loop.escaped, ordinary.repeated]", "    - loop.escaped\n    - ordinary.repeated\n"},
	} {
		t.Run(tc.file+"/"+tc.before, func(t *testing.T) {
			got := rewritePinMutationLiteral(tc.file, tc.before)
			if got != tc.after || rewritePinMutationLiteral(tc.file, got) != got {
				t.Fatalf("mutation rewrite = %q, want stable %q", got, tc.after)
			}
			if rewritePinMutationLiteral("unregistered.go", tc.before) != tc.before {
				t.Fatal("unregistered mutation changed")
			}
		})
	}
}

func generatedSourceLiteral(t testing.TB, source []byte) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "producer.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var literals []string
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			literals = append(literals, value)
		}
		return true
	})
	if len(literals) != 1 {
		t.Fatalf("got %d source literals, want one", len(literals))
	}
	return literals[0]
}
