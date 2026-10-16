package formatproof

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func reviewedSource(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", Source))
	if err != nil {
		t.Fatal(err)
	}
	if Digest(body) != Origin {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", Successors))
		if err != nil {
			t.Fatal(err)
		}
		history, err := Read(raw)
		if err != nil || len(history.Records) == 0 {
			t.Fatalf("reviewed origin unavailable: %v", err)
		}
		body = []byte(history.Records[0].Before)
		if Digest(body) != Origin {
			t.Fatal("positive witness lost the immutable approved origin")
		}
	}
	return body
}

func spacedSource(t *testing.T, body []byte, spaces int) []byte {
	t.Helper()
	anchor := []byte("OmitOutputPin               bool")
	if bytes.Count(body, anchor) != 1 {
		t.Fatal("the reviewed formatting anchor changed")
	}
	return bytes.Replace(body, anchor, []byte("OmitOutputPin"+strings.Repeat(" ", spaces)+"bool"), 1)
}

func TestFiniteFormattingExactReviewedWitnessAndTwoCuts(t *testing.T) {
	before := reviewedSource(t)
	first := spacedSource(t, before, 16)
	if Digest(first) != "3287eddf98a03d17f58b926d27da8632965b2ad6b73849a4579e0115b8b109c8" {
		t.Fatal("the gate's independently checked witness changed")
	}
	history := History{Version: 1, Records: []Record{}}
	one, err := history.Append(before, first)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Edit{{Offset: bytes.Index(before, []byte("OmitOutputPin               bool")) + len("OmitOutputPin"), Before: "               ", After: "                "}}; !reflect.DeepEqual(one.Records[0].Edits, want) {
		t.Fatalf("diff-to-row provenance = %#v, want %#v", one.Records[0].Edits, want)
	}
	second := spacedSource(t, before, 17)
	two, err := one.Append(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Records) != 0 || !reflect.DeepEqual(one.Records[0], two.Records[0]) {
		t.Fatal("append mutated immutable history")
	}
	for _, cut := range []struct {
		history History
		body    []byte
	}{{one, first}, {two, second}} {
		raw, err := json.Marshal(cut.history)
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := Read(raw)
		if err != nil {
			t.Fatal(err)
		}
		endpoint, _, err := replayed.Endpoint()
		if err != nil || !bytes.Equal(endpoint, cut.body) {
			t.Fatalf("historical cut did not replay exactly: %v", err)
		}
	}
	if _, err := two.Append(second, first); err == nil {
		t.Fatal("a removed endpoint was resurrected")
	}
	if _, err := two.Append(first, second); err == nil {
		t.Fatal("stale predecessor accepted")
	}
}

func TestFiniteFormattingRejectsNonFormattingAndProtectedComments(t *testing.T) {
	before := []byte("//go:build linux\n\n// owner: preserve\npackage fixture\n\nvar value = `a b`\n\nfunc take() int {\n\treturn 1\n}\n")
	for name, after := range map[string][]byte{
		"literal":   bytes.Replace(before, []byte("`a b`"), []byte("`a  b`"), 1),
		"comment":   bytes.Replace(before, []byte("owner: preserve"), []byte("owner:  preserve"), 1),
		"anchor":    bytes.Replace(before, []byte("// owner:"), []byte(" // owner:"), 1),
		"build":     bytes.Replace(before, []byte("build linux"), []byte("build darwin"), 1),
		"token":     bytes.Replace(before, []byte("return 1"), []byte("return 2"), 1),
		"semicolon": bytes.Replace(before, []byte("return 1"), []byte("return\n1"), 1),
		"unicode":   bytes.Replace(before, []byte("var value"), []byte("var\u00a0value"), 1),
		"newline":   append(append([]byte{}, before...), '\n'),
		"noop":      before,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := FormattingEdits(before, after); err == nil {
				t.Fatal("protected source change accepted as formatting")
			}
		})
	}
	if _, err := FormattingEdits(before, bytes.Replace(before, []byte("var value ="), []byte("var value  ="), 1)); err != nil {
		t.Fatalf("unchanged comments and syntax must permit horizontal formatting: %v", err)
	}
}

func TestFiniteHistoryFailsClosedOnIdentityAndChainTampering(t *testing.T) {
	before := reviewedSource(t)
	first := spacedSource(t, before, 16)
	one, err := (History{Version: 1, Records: []Record{}}).Append(before, first)
	if err != nil {
		t.Fatal(err)
	}
	two, err := one.Append(first, spacedSource(t, before, 17))
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*History){
		"version":        func(h *History) { h.Version++ },
		"missing":        func(h *History) { h.Records = h.Records[1:] },
		"reordered":      func(h *History) { h.Records[0], h.Records[1] = h.Records[1], h.Records[0] },
		"fork":           func(h *History) { h.Records = append(h.Records, h.Records[0]) },
		"role":           func(h *History) { h.Records[0].Role = "registry" },
		"multiplicity":   func(h *History) { h.Records[0].Multiplicity++ },
		"file":           func(h *History) { h.Records[0].File = "another.go" },
		"origin":         func(h *History) { h.Records[0].Origin = strings.Repeat("0", 64) },
		"predecessor":    func(h *History) { h.Records[1].Previous = Origin },
		"missing_object": func(h *History) { h.Records[0].Before = "" },
		"hash_only":      func(h *History) { h.Records[0].AfterSHA256 = strings.Repeat("0", 64) },
		"edit":           func(h *History) { h.Records[0].Edits[0].Offset++ },
		"retire":         func(h *History) { h.Records[0].Rule = "retire" },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(two)
			var copy History
			if err := Decode(raw, &copy); err != nil {
				t.Fatal(err)
			}
			change(&copy)
			if _, _, err := copy.Endpoint(); err == nil {
				t.Fatal("unclassified chain change accepted")
			}
		})
	}
	for _, raw := range []string{
		`{"version":1,"version":1,"records":[]}`,
		`{"version":1,"records":[],"retire":true}`,
		`{"version":1,"records":null}`,
		`{"Version":1,"records":[]}`,
		`{"version":1,"records":[]} {}`,
		`{"version":1,"records":[{"file":"a","file":"a"}]}`,
	} {
		if _, err := Read([]byte(raw)); err == nil {
			t.Fatalf("ambiguous/incomplete schema accepted: %s", raw)
		}
	}
}
