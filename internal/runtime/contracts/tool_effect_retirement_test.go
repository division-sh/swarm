package contracts

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestToolEffectClassRetirement(t *testing.T) {
	const teaching = "effect_class is not a tool key; the runtime attempts each HTTP call once, and an interrupted call is outcome-uncertain"
	for _, handler := range []string{"http", "mcp"} {
		for _, value := range []string{"read_only", "idempotent_write", "non_idempotent_write", "long_running", "write_or_unknown", "unknown", "null", "''", "false", "0", "[]", "{}", "[read_only]", "{class: read_only}"} {
			t.Run(handler+"/"+value, func(t *testing.T) {
				body := "worker:\n  handler_type: " + handler + "\n  effect_class: " + value + "\n"
				if handler == "http" {
					body += "  http: {method: GET, url: 'https://proof.invalid'}\n"
				}
				_, err := admitW5Tools(t, body)
				if err == nil || !strings.Contains(err.Error(), teaching) || !strings.Contains(err.Error(), "tools.yaml:3:") || !strings.Contains(err.Error(), `["worker"]["effect_class"]`) {
					t.Fatalf("retired declaration must teach at its source coordinate: %v", err)
				}
			})
		}
	}
	for _, value := range []string{"null", "read_only", "{}"} {
		t.Run("merged/"+value, func(t *testing.T) {
			_, err := admitW5Tools(t, "worker:\n  handler_type: mcp\n  <<: {effect_class: "+value+"}\n")
			if err == nil || !strings.Contains(err.Error(), teaching) || !strings.Contains(err.Error(), "tools.yaml:3:") {
				t.Fatalf("merged retired declaration did not preserve introduction evidence: %v", err)
			}
		})
	}
}

func TestKeyFreeHTTPToolRoundTripOmitsRetiredSelector(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		t.Run(method, func(t *testing.T) {
			entries, err := admitW5Tools(t, fmt.Sprintf("worker:\n  handler_type: http\n  http: {method: %s, url: 'https://proof.invalid', timeout_seconds: 120}\n", method))
			if err != nil {
				t.Fatal(err)
			}
			entry := entries["worker"]
			for _, field := range []string{"effect_class", "EffectClass"} {
				if _, present := entry.admissionProvenance[field]; present {
					t.Fatalf("key-free tool gained authored %s provenance", field)
				}
			}
			value, err := entry.CanonicalValue()
			if err != nil {
				t.Fatal(err)
			}
			if _, present := value["effect_class"]; present {
				t.Fatal("canonical tool declaration still exposes the retired selector")
			}
			raw, err := yaml.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "effect_class") {
				t.Fatalf("tool serialization resurrected the retired key: %s", raw)
			}
			reloaded, err := admitW5Tools(t, "worker:\n"+indentToolEffectProof(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			beforeHash, err := entry.CanonicalHash()
			if err != nil {
				t.Fatal(err)
			}
			afterHash, err := reloaded["worker"].CanonicalHash()
			if err != nil || beforeHash != afterHash {
				t.Fatalf("key-free tool changed through serialization: before=%s after=%s err=%v", beforeHash, afterHash, err)
			}
		})
	}
}

func indentToolEffectProof(body string) string {
	return "  " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n  ") + "\n"
}
