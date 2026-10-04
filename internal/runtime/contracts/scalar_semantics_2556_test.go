package contracts

import (
	"reflect"
	"strings"
	"testing"
)

func TestContractScalarStyleClassification2556(t *testing.T) {
	for _, tc := range []struct {
		source  string
		kind    ExpressionKind
		literal any
		code    string
	}{
		{"payload.count", ExpressionKindCEL, nil, "payload.count"},
		{"|\n  payload.count\n", ExpressionKindCEL, nil, "payload.count"},
		{">\n  payload.count\n", ExpressionKindCEL, nil, "payload.count"},
		{`"payload.count"`, ExpressionKindLiteral, "payload.count", ""},
		{`'payload.count'`, ExpressionKindLiteral, "payload.count", ""},
		{`!!str payload.count`, ExpressionKindCEL, nil, "payload.count"},
		{`!!int "3"`, ExpressionKindLiteral, "3", ""},
		{`!!bool 'true'`, ExpressionKindLiteral, "true", ""},
		{`!!null "null"`, ExpressionKindLiteral, "null", ""},
		{"3", ExpressionKindLiteral, 3, ""},
		{"true", ExpressionKindLiteral, true, ""},
		{"null", ExpressionKindLiteral, nil, ""},
		{`""`, ExpressionKindLiteral, "", ""},
		{`{literal: "text"}`, ExpressionKindLiteral, map[string]any{"literal": "text"}, ""},
	} {
		t.Run(tc.source, func(t *testing.T) {
			var got ExpressionValue
			if err := decodeNodeTestYAML([]byte(tc.source), &got); err != nil {
				t.Fatal(err)
			}
			if got.Kind != tc.kind || got.CEL != tc.code || !reflect.DeepEqual(got.Literal, tc.literal) {
				t.Fatalf("got %#v, want kind=%s literal=%#v code=%q", got, tc.kind, tc.literal, tc.code)
			}
		})
	}
}

func TestQuotedInterpolationAlwaysReturnsText2556(t *testing.T) {
	for _, source := range []string{`"${payload.count}"`, `'${payload.count}'`} {
		var got ExpressionValue
		if err := decodeNodeTestYAML([]byte(source), &got); err != nil {
			t.Fatal(err)
		}
		if got.CEL != "__swarm_r2_format((payload.count\n))" {
			t.Fatalf("quoted interpolation bypassed text formatting: %#v", got)
		}
	}
}

func TestScalar2556PredicateConsumers(t *testing.T) {
	consumers := []struct {
		name   string
		decode func(string) (string, error)
	}{
		{"rule.when", func(s string) (string, error) {
			var handler SystemNodeEventHandler
			err := decodeNodeTestYAML([]byte("rules:\n  - when: "+s+"\n    emit: done\n  - else: true\n    emit: other\n"), &handler)
			if err != nil {
				return "", err
			}
			return handler.Rules[0].Condition, nil
		}},
		{"on_complete.condition", func(s string) (string, error) {
			var handler SystemNodeEventHandler
			err := decodeNodeTestYAML([]byte("on_complete:\n  - condition: "+s+"\n    emit: done\n"), &handler)
			if err != nil {
				return "", err
			}
			return handler.OnComplete[0].Condition, nil
		}},
		{"guard.check", func(s string) (string, error) {
			var guard GuardSpec
			err := decodeNodeTestYAML([]byte("check: "+s+"\n"), &guard)
			return guard.Check, err
		}},
		{"guard.checks.check", func(s string) (string, error) {
			var guard GuardSpec
			err := decodeNodeTestYAML([]byte("checks:\n  - check: "+s+"\n"), &guard)
			if err != nil {
				return "", err
			}
			return guard.Checks[0].Check, nil
		}},
		{"query.filter", func(s string) (string, error) {
			var query QuerySpec
			err := decodeNodeTestYAML([]byte("entities: account\nfilter: "+s+"\n"), &query)
			return query.Filter, err
		}},
		{"filter.condition", func(s string) (string, error) {
			var filter FilterSpec
			err := decodeNodeTestYAML([]byte("items_from: payload.items\ncondition: "+s+"\n"), &filter)
			return filter.Condition, err
		}},
		{"count.condition", func(s string) (string, error) {
			var count CountSpec
			err := decodeNodeTestYAML([]byte("items_from: payload.items\ncondition: "+s+"\n"), &count)
			return count.Condition, err
		}},
	}
	for _, consumer := range consumers {
		t.Run(consumer.name, func(t *testing.T) {
			for _, scalar := range []string{"payload.ok", "true", "false"} {
				got, err := consumer.decode(scalar)
				if err != nil || got != scalar {
					t.Fatalf("%s: got %q, %v", scalar, got, err)
				}
			}
			for _, scalar := range []string{`"payload.ok"`, `'payload.ok'`, `"${payload.ok}"`, `'true'`, `!!bool "true"`} {
				_, err := consumer.decode(scalar)
				if err == nil || !strings.Contains(err.Error(), consumer.name) || !strings.Contains(err.Error(), "remove YAML quotes") || !strings.Contains(err.Error(), "nodes.yaml:") {
					t.Fatalf("%s: missing source/slot/boolean diagnostic: %v", scalar, err)
				}
			}
		})
	}
}

func TestScalar2556CompletionFallbackSelectionAndAssignment(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := decodeNodeTestYAML([]byte("on_complete:\n  - emit: done\n"), &handler); err != nil {
		t.Fatal(err)
	}
	if len(handler.OnComplete) != 1 || handler.OnComplete[0].Condition != "" {
		t.Fatalf("fallback: %#v", handler.OnComplete)
	}
	for _, scalar := range []string{"else", `'else'`, `"else"`} {
		if err := decodeNodeTestYAML([]byte("on_complete:\n  - condition: "+scalar+"\n    emit: done\n"), &handler); err == nil {
			t.Fatalf("authored sentinel %s admitted", scalar)
		}
	}
}
