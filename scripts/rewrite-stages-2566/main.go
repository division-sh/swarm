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
	File                string        `json:"file"`
	EntrySourceRevision string        `json:"entry_source_revision,omitempty"`
	BeforeHash          string        `json:"before_hash"`
	AfterHash           string        `json:"after_hash"`
	Edits               []edit        `json:"edits"`
	Equivalence         []equivalence `json:"equivalence"`
	Review              string        `json:"review,omitempty"`
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
	prove := flag.Bool("prove", false, "one-time exact corpus replay from the recorded git baseline, including byte idempotence")
	typed := flag.Bool("prepare-typed", false, "prepare the inventoried typed Go stage fixture field cut")
	literals := flag.Bool("prepare-literals", false, "prepare stage-owned fields in complete Go YAML literals; fragments remain explicitly reviewed")
	entries := flag.Bool("prepare-entry-golden", false, "extract permanent disk entry/end goldens from the independently reviewed baseline ledger")
	baseline := flag.String("refresh-baseline", "", "record exact corpus edits against this reviewed git baseline; does not apply a rewrite")
	reviewedRevision := flag.String("reviewed-plan-revision", "", "original independently reviewed ledger revision for a post-rebase capture")
	flag.Parse()
	var err error
	if *prove {
		if *typed || *literals || *entries || *write || *check || *baseline != "" {
			err = fmt.Errorf("corpus proof is separate from preparation/application")
		} else {
			err = provePlan(*root, *ledger)
		}
	} else if *baseline != "" {
		if *typed || *literals || *entries || *write || *check {
			err = fmt.Errorf("ledger capture is separate from plan application")
		} else {
			err = refreshLedger(*root, *ledger, *baseline, *reviewedRevision)
		}
	} else if *entries {
		if *typed || *literals || *write || *check {
			err = fmt.Errorf("entry golden extraction is separate from plan application")
		} else {
			err = prepareEntryGoldens(*root, *ledger)
		}
	} else if *literals {
		if *typed || *write || *check {
			err = fmt.Errorf("literal preparation is separate from finite plan application")
		} else {
			err = prepareLiterals(*root)
		}
	} else if *typed {
		if *write || *check {
			err = fmt.Errorf("typed preparation is separate from finite plan application")
		} else {
			err = prepareTyped(*root)
		}
	} else {
		err = apply(*root, *ledger, *write, *check)
	}
	if err != nil {
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
		if _, duplicate := outputs[c.File]; duplicate {
			return fmt.Errorf("duplicate ledger file %s", c.File)
		}
		output, err := pendingChange(root, c, check)
		if err != nil {
			return err
		}
		outputs[c.File] = output
		sites += len(c.Equivalence)
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

func pendingChange(root string, c change, check bool) ([]byte, error) {
	if filepath.IsAbs(c.File) || filepath.Clean(c.File) != c.File || c.File == "." || c.File == ".." || strings.HasPrefix(c.File, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("invalid ledger path %q", c.File)
	}
	if (len(c.Equivalence) == 0 && c.Review == "") || len(c.Edits) == 0 || c.BeforeHash == c.AfterHash {
		return nil, fmt.Errorf("%s: missing edit/equivalence decision", c.File)
	}
	body, err := os.ReadFile(filepath.Join(root, c.File))
	if err != nil {
		return nil, err
	}
	output, err := rewrite(c, body)
	if err != nil || bytes.Equal(output, body) {
		return nil, err
	}
	if check {
		return nil, fmt.Errorf("%s: unapplied reviewed output", c.File)
	}
	return output, nil
}
