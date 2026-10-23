package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
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
	writeEmbeddedSpecFiniteEvidence(t, mirror, formatproof.Successors, readEmbeddedSpecFiniteEvidence(t, root, formatproof.Successors))
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
