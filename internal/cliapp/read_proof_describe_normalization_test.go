package cliapp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// Only declared presentation locations are normalized. In particular, arbitrary
// policy maps, event names, digests, topology IDs and authored selectors are not.
func normalizeDescribeProof(raw []byte, surface, identity, repo, embedded string) ([]byte, error) {
	if strings.HasSuffix(surface, "-json") {
		if _, err := canonicaljson.Decode(raw); err != nil {
			return nil, err
		}
		var output map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&output); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(surface, "routes-") {
			if output["source_hash"] != identity {
				return nil, fmt.Errorf("public source identity %v differs from independently admitted %s", output["source_hash"], identity)
			}
		}
		prefix := ""
		if !strings.HasPrefix(surface, "routes-") {
			prefix = "routing_topology."
		}
		decoder = json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var edits []describeProofEdit
		if err := collectDescribeProofEdits(decoder, raw, "", prefix, identity, repo, embedded, &edits); err != nil {
			return nil, err
		}
		var normalized bytes.Buffer
		previous := 0
		for _, edit := range edits {
			normalized.Write(raw[previous:edit.start])
			normalized.Write(edit.value)
			previous = edit.end
		}
		normalized.Write(raw[previous:])
		return normalized.Bytes(), nil
	}
	text := string(raw)
	if !strings.HasPrefix(surface, "routes-") {
		head := "describe: source=" + identity + "\n"
		if surface == "describe-quiet" {
			head = identity + "\n"
		}
		if !strings.HasPrefix(text, head) {
			return nil, fmt.Errorf("public text does not carry admitted source identity %s", identity)
		}
		// The current human label is semantic presentation, not volatile hash bytes.
		// Assert its independent admission above and retain it exactly in the corpus.
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "    connect: ") {
			if start := strings.LastIndex(line, " ("); start >= 0 && strings.HasSuffix(line, ")") {
				lines[i] = line[:start+2] + normalizeDescribeLocation(line[start+2:len(line)-1]) + ")"
			}
		}
		if strings.HasPrefix(line, "  - [") {
			if start := strings.Index(line, " @ "); start >= 0 {
				if end := strings.Index(line[start+3:], ": "); end >= 0 {
					end += start + 3
					lines[i] = line[:start+3] + normalizeDescribeLocation(line[start+3:end]) + line[end:]
				}
			}
		}
	}
	return []byte(strings.Join(lines, "\n")), nil
}

var describeLocationCoordinate = regexp.MustCompile(`(\.ya?ml:)[1-9][0-9]*(:[1-9][0-9]*)?$`)

func normalizeDescribeLocation(location string) string {
	return describeLocationCoordinate.ReplaceAllStringFunc(location, func(value string) string {
		parts := describeLocationCoordinate.FindStringSubmatch(value)
		column := ""
		if parts[2] != "" {
			column = ":<column>"
		}
		return parts[1] + "<line>" + column
	})
}

type describeProofEdit struct {
	start, end int
	value      []byte
}

// Token offsets preserve ordering, formatting, escaping and every unapproved byte.
func collectDescribeProofEdits(decoder *json.Decoder, raw []byte, path, prefix, identity, repo, embedded string, edits *[]describeProofEdit) error {
	start := int(decoder.InputOffset())
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); ok {
		for decoder.More() {
			child := path + "[]"
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				child = key.(string)
				if path != "" {
					child = path + "." + child
				}
			}
			if err := collectDescribeProofEdits(decoder, raw, child, prefix, identity, repo, embedded, edits); err != nil {
				return err
			}
		}
		_, err := decoder.Token()
		return err
	}
	value, changed := describeProofScalar(token, path, prefix, identity, repo, embedded)
	if changed {
		for strings.ContainsRune(" \t\r\n,:", rune(raw[start])) {
			start++
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		*edits = append(*edits, describeProofEdit{start: start, end: int(decoder.InputOffset()), value: encoded})
	}
	return nil
}

func describeProofScalar(value any, path, prefix, identity, repo, embedded string) (any, bool) {
	if prefix != "" && (path == "source_hash" || path == "workflow_version") && value == identity {
		return "<admitted-bundle>", true
	}
	key := path[strings.LastIndex(path, ".")+1:]
	if !describePresentationField(path, prefix, key) {
		return value, false
	}
	switch actual := value.(type) {
	case json.Number:
		if n, err := actual.Int64(); err == nil && n > 0 {
			return json.Number("1"), true
		}
	case string:
		if key != "source_file" {
			normalized := normalizeDescribeLocation(actual)
			return normalized, normalized != actual
		}
		if actual == embedded {
			return "embedded://swarm/platform-spec.yaml", true
		}
		if strings.HasPrefix(actual, repo+"/") {
			return "<repo>/" + strings.TrimPrefix(actual, repo+"/"), true
		}
	}
	return value, false
}

func describePresentationField(path, topologyPrefix, key string) bool {
	parent := strings.TrimSuffix(path, "."+key)
	switch parent {
	case "effective_provenance[].provenance", "flows[].input_pins[]", "flows[].output_pins[]":
		return key == "source_file" || key == "source_line" || key == "source_column"
	case "diagnostics[]":
		return key == "location" || key == "authored_location"
	}
	parent = strings.TrimPrefix(parent, topologyPrefix)
	switch parent {
	case "producers[]", "consumers[]", "input_pins[]", "output_pins[]", "boundary_exposures[].producer", "boundary_exposures[].output", "edges[].producer", "edges[].consumer":
		return key == "source_file" || key == "source_line"
	case "edges[].boundary", "root_input_sources[]":
		return key == "authored_location"
	case "issues[]":
		return key == "location" || key == "authored_location"
	}
	return false
}

func TestDescribeProofNormalization(t *testing.T) {
	const identity = "bundle-v2:sha256:admitted"
	const original = `{"source_hash":"bundle-v2:sha256:admitted","workflow_version":"bundle-v2:sha256:admitted","effective_provenance":[{"path":"flows.account","provenance":{"origin":"authored","source_file":"/cache/platform-spec-current.yaml","source_line":10,"source_column":3,"source_presence":"authored"}}],"routing_topology":{"edges":[{"id":"edge-unchanged","event":{"canonical":"account.ready"},"boundary":{"authored_location":"schema.yaml:10:3"},"producer_schema_digest":"sha256:producer","receiver_schema_digest":"sha256:receiver","derived_from":"instance.account_id + carries.account_id.from"}]},"policy":{"source_line":10,"source_file":"/cache/platform-spec-current.yaml"},"stages":{"active":{"initial":true},"complete":{"terminal":true}},"approvals":{"required":true},"rules":["account.amount > 10"],"diagnostics":[{"severity":"lint_evidence","message":"keep schema.yaml:10:3","location":"schema.yaml:10:3"}]}`
	normalize := func(raw string) []byte {
		t.Helper()
		got, err := normalizeDescribeProof([]byte(raw), "describe-json", identity, "/repo", "/cache/platform-spec-current.yaml")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	want := normalize(original)
	if !bytes.Contains(want, []byte(`"policy":{"source_line":10,"source_file":"/cache/platform-spec-current.yaml"}`)) || bytes.Contains(want, []byte("\n")) {
		t.Fatal("normalization changed unapproved bytes or formatting")
	}
	for _, row := range []struct{ name, old, replacement string }{
		{"line", `"source_line":10,"source_column"`, `"source_line":200,"source_column"`},
		{"column", `"source_column":3`, `"source_column":70`},
		{"authored_coordinate", `schema.yaml:10:3`, `schema.yaml:90:7`},
	} {
		t.Run(row.name, func(t *testing.T) {
			if !bytes.Equal(want, normalize(strings.Replace(original, row.old, row.replacement, 1))) {
				t.Fatal("presentation-only change altered corpus comparison")
			}
		})
	}
	for _, row := range []struct{ name, old, replacement string }{
		{"line_presence", `"source_line":10,`, ``},
		{"zero_line", `"source_line":10,`, `"source_line":0,`},
		{"column_presence", `"source_column":3,`, ``},
		{"file_identity", `"source_file":"/cache/platform-spec-current.yaml"`, `"source_file":"/cache/foreign.yaml"`},
		{"authored_file", `schema.yaml:10:3`, `other.yaml:10:3`},
		{"coordinate_presence", `schema.yaml:10:3`, `schema.yaml:10`},
		{"event", `account.ready`, `account.changed`},
		{"edge_id", `edge-unchanged`, `edge-changed`},
		{"producer_schema", `sha256:producer`, `sha256:other-producer`},
		{"receiver_schema", `sha256:receiver`, `sha256:other-receiver`},
		{"carries_derivation", `carries.account_id.from`, `carries.foreign_id.from`},
		{"stage_initial", `"initial":true`, `"initial":false`},
		{"stage_terminal", `"terminal":true`, `"terminal":false`},
		{"approval", `"approvals":{"required":true}`, `"approvals":{"required":false}`},
		{"rule", `account.amount > 10`, `account.amount > 20`},
		{"severity", `"severity":"lint_evidence"`, `"severity":"error"`},
		{"message_coordinate", `"message":"keep schema.yaml:10:3"`, `"message":"keep schema.yaml:90:7"`},
		{"provenance", `"origin":"authored"`, `"origin":"platform_default"`},
		{"policy_numeric_field", `"policy":{"source_line":10`, `"policy":{"source_line":200`},
	} {
		t.Run(row.name, func(t *testing.T) {
			mutated := strings.Replace(original, row.old, row.replacement, 1)
			if mutated == original {
				t.Fatal("mutation did not change its named input")
			}
			if bytes.Equal(want, normalize(mutated)) {
				t.Fatal("semantic/presence mutation was hidden")
			}
		})
	}
	for _, raw := range []string{strings.Replace(original, identity, "foreign", 1), strings.Replace(original, `"source_hash":"`+identity+`",`, "", 1), strings.Replace(original, `"source_hash":`, `"source_hash":"foreign","source_hash":`, 1), original + "{}", "{"} {
		if _, err := normalizeDescribeProof([]byte(raw), "describe-json", identity, "/repo", "/cache/platform-spec-current.yaml"); err == nil {
			t.Fatal("invalid/missing/foreign identity accepted")
		}
	}
	text := "describe: source=" + identity + "\n    connect: a -> b (schema.yaml:10:3)\n  - [INFO] check @ schema.yaml:10:3: keep schema.yaml:10:3\n"
	got, err := normalizeDescribeProof([]byte(text), "describe-text", identity, "/repo", "/cache/platform-spec-current.yaml")
	if err != nil || !bytes.Contains(got, []byte("schema.yaml:<line>:<column>: keep schema.yaml:10:3")) {
		t.Fatalf("text location must retain message bytes: %s, %v", got, err)
	}
}
