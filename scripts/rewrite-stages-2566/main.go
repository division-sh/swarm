// A finite corpus edit, not a runtime parser or migration command.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type plan struct {
	Baseline string   `json:"baseline"`
	Scope    string   `json:"scope"`
	Changes  []change `json:"changes"`
}

type change struct {
	File        string        `json:"file"`
	BeforeHash  string        `json:"before_hash"`
	AfterHash   string        `json:"after_hash"`
	Edits       []edit        `json:"edits"`
	Equivalence []equivalence `json:"equivalence"`
}

type edit struct {
	Offset int    `json:"offset"`
	Before string `json:"before"`
	After  string `json:"after"`
}

type equivalence struct {
	Site        string   `json:"site"`
	Entry       string   `json:"entry"`
	BeforeOrder []string `json:"before_order"`
	AfterOrder  []string `json:"after_order"`
	Finals      []string `json:"finals"`
	Reordered   bool     `json:"reordered,omitempty"`
}

func digest(body []byte) string { return fmt.Sprintf("%x", sha256.Sum256(body)) }

func main() {
	root := flag.String("root", ".", "checkout root")
	ledger := flag.String("ledger", "scripts/rewrite-stages-2566/intent.json", "finite reviewed ledger")
	write := flag.Bool("write", false, "apply the complete prepared plan (only after the gate)")
	check := flag.Bool("check", false, "require reviewed outputs already present")
	flag.Parse()
	if err := apply(*root, *ledger, *write, *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rewrite(c change, body []byte) ([]byte, error) {
	if digest(body) == c.AfterHash {
		return body, nil
	}
	if digest(body) != c.BeforeHash {
		return nil, fmt.Errorf("%s: source hash drift; refresh the reviewed ledger", c.File)
	}
	var output bytes.Buffer
	end := 0
	for _, e := range c.Edits {
		if e.Offset < end || e.Offset > len(body) || len(e.Before) > len(body)-e.Offset || e.Before == e.After {
			return nil, fmt.Errorf("%s: overlapping or invalid edit", c.File)
		}
		if string(body[e.Offset:e.Offset+len(e.Before)]) != e.Before {
			return nil, fmt.Errorf("%s: edit anchor drift", c.File)
		}
		output.Write(body[end:e.Offset])
		output.WriteString(e.After)
		end = e.Offset + len(e.Before)
	}
	output.Write(body[end:])
	if digest(output.Bytes()) != c.AfterHash {
		return nil, fmt.Errorf("%s: reviewed output hash mismatch", c.File)
	}
	return output.Bytes(), nil
}

func apply(root, ledger string, write, check bool) error {
	if write && check {
		return fmt.Errorf("choose -write or -check, not both")
	}
	if !filepath.IsAbs(ledger) {
		ledger = filepath.Join(root, ledger)
	}
	body, err := os.ReadFile(ledger)
	if err != nil {
		return err
	}
	var p plan
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	if p.Baseline == "" || p.Scope == "" || len(p.Changes) == 0 {
		return fmt.Errorf("missing finite corpus decisions")
	}
	outputs := make(map[string][]byte, len(p.Changes))
	sites := 0
	// Validate the whole plan before changing any file.
	for _, c := range p.Changes {
		if filepath.IsAbs(c.File) || filepath.Clean(c.File) != c.File || c.File == "." || c.File == ".." || strings.HasPrefix(c.File, ".."+string(filepath.Separator)) {
			return fmt.Errorf("invalid ledger path %q", c.File)
		}
		if _, duplicate := outputs[c.File]; duplicate {
			return fmt.Errorf("duplicate ledger file %s", c.File)
		}
		if len(c.Equivalence) == 0 || len(c.Edits) == 0 || c.BeforeHash == c.AfterHash {
			return fmt.Errorf("%s: missing edit/equivalence decision", c.File)
		}
		body, err := os.ReadFile(filepath.Join(root, c.File))
		if err != nil {
			return err
		}
		output, err := rewrite(c, body)
		if err != nil {
			return err
		}
		outputs[c.File] = nil
		sites += len(c.Equivalence)
		if !bytes.Equal(output, body) {
			if check {
				return fmt.Errorf("%s: unapplied reviewed output", c.File)
			}
			outputs[c.File] = output
		}
	}
	changed := 0
	for _, c := range p.Changes {
		output := outputs[c.File]
		if output == nil {
			continue
		}
		changed++
		if write {
			info, err := os.Stat(filepath.Join(root, c.File))
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(root, c.File), output, info.Mode().Perm()); err != nil {
				return err
			}
		}
	}
	fmt.Printf("%d prepared source sites; %d files %s\n", sites, changed, map[bool]string{true: "written", false: "would change"}[write])
	return nil
}
