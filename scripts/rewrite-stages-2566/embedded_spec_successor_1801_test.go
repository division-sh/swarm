package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const embeddedSpecFiniteFile = "internal/runtime/conformance/data_text_file_journey_2456_test.go"
const embeddedSpecFiniteLedgerHash = "3da9e2f24fb5126eede32087a4fbb2131152bcf4110901fd029c3626bf173b01"

func embeddedSpecFiniteSuccessorPlan() plan {
	return plan{
		Baseline: "fa5a180a213022b38c6f9ee0610ffe3546407919",
		Scope:    "https://github.com/division-sh/swarm/pull/2695#issuecomment-6201846244",
		Changes: []change{{
			File:       embeddedSpecFiniteFile,
			BeforeHash: "39785b6de6159b06c974cc36790de6067994476159b99fc4ba84f29f3b694b7f",
			AfterHash:  "fbe92820fc80d5eb5baef88f980f6052677fd243a15565446c93b933de240afc",
			Edits: []edit{{
				Offset: 33059,
				Before: "\t\tPlatformSpecPath: filepath.Join(conformanceRepoRoot(t), \"platform-spec.yaml\"),\n\t\tAuthTokens:       []string{apiv1.DefaultLoopbackAPIToken}, Handlers: methods,\n",
				After:  "\t\tAuthTokens: []string{apiv1.DefaultLoopbackAPIToken}, Handlers: methods,\n",
			}},
			Equivalence: []equivalence{},
			Review:      "Delete only apiv1.Options.PlatformSpecPath and gofmt-align AuthTokens. Preserve the original finite-fixtures.json decision, all three stage edits, equivalence/review text, assertions, cleanup, store cells and the other seven members.",
		}},
	}
}

func originalFiniteEmbeddedSpecOutput(ledger, successor, current []byte) ([]byte, error) {
	if digest(ledger) != embeddedSpecFiniteLedgerHash {
		return nil, fmt.Errorf("original eight-member finite-fixture history changed")
	}
	var p plan
	decoder := json.NewDecoder(bytes.NewReader(successor))
	decoder.DisallowUnknownFields()
	if !json.Valid(successor) {
		return nil, fmt.Errorf("missing or invalid embedded-spec successor")
	}
	if err := decoder.Decode(&p); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(p, embeddedSpecFiniteSuccessorPlan()) {
		return nil, fmt.Errorf("embedded-spec successor is not the exact reviewed file, key and edit")
	}
	c := p.Changes[0]
	if digest(current) != c.AfterHash {
		return nil, fmt.Errorf("embedded-spec successor current source changed")
	}
	reverse := change{
		File: c.File, BeforeHash: c.AfterHash, AfterHash: c.BeforeHash,
		Edits: []edit{{Offset: c.Edits[0].Offset, Before: c.Edits[0].After, After: c.Edits[0].Before}},
	}
	original, err := rewrite(reverse, current)
	if err != nil {
		return nil, err
	}
	forward, err := rewrite(c, original)
	if err != nil || !bytes.Equal(forward, current) {
		return nil, fmt.Errorf("embedded-spec successor did not round-trip: %v", err)
	}
	return original, nil
}

func finiteFixtureRootWithEmbeddedSpecSuccessor(t *testing.T, root string) string {
	t.Helper()
	ledger := readEmbeddedSpecFiniteEvidence(t, root, "scripts/rewrite-stages-2566/finite-fixtures.json")
	successor := readEmbeddedSpecFiniteEvidence(t, root, "scripts/rewrite-stages-2566/embedded-spec-successor-1801.json")
	current := readEmbeddedSpecFiniteEvidence(t, root, embeddedSpecFiniteFile)
	original, err := originalFiniteEmbeddedSpecOutput(ledger, successor, current)
	if err != nil {
		t.Fatal(err)
	}
	var p plan
	if err := json.Unmarshal(ledger, &p); err != nil {
		t.Fatal(err)
	}
	// Only the verified successor is reversed. The original apply/check owner
	// still checks every member against the unchanged historical ledger.
	mirror := t.TempDir()
	writeEmbeddedSpecFiniteEvidence(t, mirror, "finite-fixtures.json", ledger)
	for _, c := range p.Changes {
		body := readEmbeddedSpecFiniteEvidence(t, root, c.File)
		if c.File == embeddedSpecFiniteFile {
			body = original
		}
		writeEmbeddedSpecFiniteEvidence(t, mirror, c.File, body)
	}
	return mirror
}

func readEmbeddedSpecFiniteEvidence(t *testing.T, root, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func writeEmbeddedSpecFiniteEvidence(t *testing.T, root, name string, body []byte) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRewrite2566EmbeddedSpecFiniteSuccessorPreservesExactHistory(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	ledger := readEmbeddedSpecFiniteEvidence(t, root, "scripts/rewrite-stages-2566/finite-fixtures.json")
	successor := readEmbeddedSpecFiniteEvidence(t, root, "scripts/rewrite-stages-2566/embedded-spec-successor-1801.json")
	current := readEmbeddedSpecFiniteEvidence(t, root, embeddedSpecFiniteFile)
	original, err := originalFiniteEmbeddedSpecOutput(ledger, successor, current)
	if err != nil {
		t.Fatal(err)
	}
	if digest(original) != embeddedSpecFiniteSuccessorPlan().Changes[0].BeforeHash || bytes.Equal(original, current) {
		t.Fatal("successor did not restore the exact distinct reviewed output")
	}
	for _, tc := range []struct{ name, before, after string }{
		{"stage edit", "[]byte(declaration+canonicalrouting.DeploymentFixtureCloseDeclaration)", "[]byte(declaration)"},
		{"assertion", "t.Fatal(err)", "t.Log(err)"},
		{"cleanup", "t.Cleanup(proxy.Close)", "// t.Cleanup(proxy.Close)"},
		{"store cell", `[]string{"sqlite", "postgres"}`, `[]string{"sqlite"}`},
		{"unrelated byte", "package conformance", "package conformance\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := bytes.Replace(current, []byte(tc.before), []byte(tc.after), 1)
			if bytes.Equal(changed, current) {
				t.Fatal("negative control did not change the intended source boundary")
			}
			if _, err := originalFiniteEmbeddedSpecOutput(ledger, successor, changed); err == nil {
				t.Fatal("unrelated source edit acquired successor authority")
			}
		})
	}
}

func TestRewrite2566EmbeddedSpecFiniteSuccessorRefusesChangedDecisions(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	ledger := readEmbeddedSpecFiniteEvidence(t, root, "scripts/rewrite-stages-2566/finite-fixtures.json")
	successor := readEmbeddedSpecFiniteEvidence(t, root, "scripts/rewrite-stages-2566/embedded-spec-successor-1801.json")
	current := readEmbeddedSpecFiniteEvidence(t, root, embeddedSpecFiniteFile)
	for _, tc := range []struct {
		name   string
		mutate func(*plan)
	}{
		{"file", func(p *plan) { p.Changes[0].File = "different.go" }},
		{"key", func(p *plan) { p.Changes[0].BeforeHash = p.Changes[0].AfterHash }},
		{"extra member", func(p *plan) { p.Changes = append(p.Changes, p.Changes[0]) }},
		{"edit", func(p *plan) { p.Changes[0].Edits[0].After += "// unrelated\n" }},
		{"missing edit", func(p *plan) { p.Changes[0].Edits = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := embeddedSpecFiniteSuccessorPlan()
			tc.mutate(&p)
			changed, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := originalFiniteEmbeddedSpecOutput(ledger, changed, current); err == nil {
				t.Fatal("changed successor acquired authority")
			}
		})
	}
	for _, tc := range []struct {
		name      string
		ledger    []byte
		successor []byte
	}{
		{"missing successor", ledger, nil},
		{"changed stage edit", bytes.Replace(ledger, []byte("DeploymentFixtureCloseDeclaration"), []byte("ChangedStageEdit"), 1), successor},
		{"changed review", bytes.Replace(ledger, []byte("Routing-only root"), []byte("Changed review"), 1), successor},
		{"changed equivalence", bytes.Replace(ledger, []byte(`"equivalence": []`), []byte(`"equivalence": [{}]`), 1), successor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := originalFiniteEmbeddedSpecOutput(tc.ledger, tc.successor, current); err == nil {
				t.Fatal("missing successor or changed historical decision acquired authority")
			}
		})
	}
	var original plan
	if err := json.Unmarshal(ledger, &original); err != nil {
		t.Fatal(err)
	}
	for _, member := range original.Changes {
		t.Run("current member/"+member.File, func(t *testing.T) {
			mirror := finiteFixtureRootWithEmbeddedSpecSuccessor(t, root)
			body := readEmbeddedSpecFiniteEvidence(t, mirror, member.File)
			writeEmbeddedSpecFiniteEvidence(t, mirror, member.File, append(body, '\n'))
			if err := apply(mirror, "finite-fixtures.json", false, true); err == nil {
				t.Fatal("existing full-plan check accepted changed member bytes")
			}
		})
	}
}
