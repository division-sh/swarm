package httpresponsesuccess

import (
	"encoding/json"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestValidateClosedVocabulary(t *testing.T) {
	tests := []struct {
		name  string
		check runtimecontracts.HTTPResponseSuccess
		want  string
	}{
		{name: "http status", check: runtimecontracts.HTTPResponseSuccess{Kind: KindHTTPStatus2xx}},
		{name: "json field", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: true}},
		{name: "missing kind", check: runtimecontracts.HTTPResponseSuccess{}, want: "kind is required"},
		{name: "unknown kind", check: runtimecontracts.HTTPResponseSuccess{Kind: "provider_special"}, want: "is unsupported"},
		{name: "status path", check: runtimecontracts.HTTPResponseSuccess{Kind: KindHTTPStatus2xx, Path: "response.status"}, want: "path is forbidden"},
		{name: "status equals", check: runtimecontracts.HTTPResponseSuccess{Kind: KindHTTPStatus2xx, Equals: 200}, want: "equals is forbidden"},
		{name: "json missing path", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Equals: true}, want: "path is required"},
		{name: "json wrong root", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "body.ok", Equals: true}, want: "must start with response."},
		{name: "json malformed path", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body..ok", Equals: true}, want: "is invalid"},
		{name: "json malformed index", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body[latest]", Equals: true}, want: "is invalid"},
		{name: "json missing equals", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok"}, want: "equals is required"},
		{name: "json complex equals", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: map[string]any{"ok": true}}, want: "must be a scalar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.check)
			if tc.want == "" && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("Validate error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestEvaluateSharedSemantics(t *testing.T) {
	env := map[string]any{"response": map[string]any{
		"status": 201,
		"body": map[string]any{
			"ok":     true,
			"label":  "done",
			"count":  json.Number("2"),
			"secret": "token-value",
		},
	}}
	tests := []struct {
		name    string
		check   runtimecontracts.HTTPResponseSuccess
		wantErr string
	}{
		{name: "2xx", check: runtimecontracts.HTTPResponseSuccess{Kind: KindHTTPStatus2xx}},
		{name: "bool", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: true}},
		{name: "string", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.label", Equals: "done"}},
		{name: "numeric", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.count", Equals: 2.0}},
		{name: "unresolved", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.missing", Equals: true}, wantErr: "did not resolve"},
		{name: "provider failure", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: false}, wantErr: "want false"},
		{name: "redaction", check: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.secret", Equals: "other"}, wantErr: "[REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Evaluate("http surface", &tc.check, env, []string{"token-value"})
			if tc.wantErr == "" && err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("Evaluate error = %v, want containing %q", err, tc.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), "token-value") {
				t.Fatalf("Evaluate leaked secret: %v", err)
			}
		})
	}
}

func TestEvaluateHTTPStatusFailsClosed(t *testing.T) {
	check := runtimecontracts.HTTPResponseSuccess{Kind: KindHTTPStatus2xx}
	env := map[string]any{"response": map[string]any{"status": 302}}
	err := Evaluate("activity http tool sample", &check, env, nil)
	if err == nil || !strings.Contains(err.Error(), "want HTTP 2xx") {
		t.Fatalf("Evaluate error = %v, want HTTP 2xx failure", err)
	}
}

func TestEquivalentPreservesScalarKinds(t *testing.T) {
	tests := []struct {
		name string
		a    runtimecontracts.HTTPResponseSuccess
		b    runtimecontracts.HTTPResponseSuccess
		want bool
	}{
		{name: "same boolean", a: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: true}, b: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: true}, want: true},
		{name: "boolean and string differ", a: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: true}, b: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.ok", Equals: "true"}},
		{name: "numeric and string differ", a: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.count", Equals: 1}, b: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.count", Equals: "1"}},
		{name: "numeric representations agree", a: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.count", Equals: 1}, b: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.count", Equals: json.Number("1.0")}, want: true},
		{name: "different path", a: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.count", Equals: 1}, b: runtimecontracts.HTTPResponseSuccess{Kind: KindJSONFieldEquals, Path: "response.body.other", Equals: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Equivalent(tc.a, tc.b); got != tc.want {
				t.Fatalf("Equivalent = %t, want %t", got, tc.want)
			}
		})
	}
}
