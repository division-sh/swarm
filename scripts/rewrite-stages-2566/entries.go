package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type entryGolden struct {
	File   string   `json:"file"`
	Source string   `json:"source"`
	Flow   string   `json:"flow"`
	Entry  string   `json:"entry"`
	Order  []string `json:"order"`
	Finals []string `json:"finals"`
}

// Expected entry comes from the pre-change review, never the current parser.
func baselineEntryGoldens(root string, p plan) []entryGolden {
	var entries []entryGolden
	for _, change := range p.Changes {
		if filepath.Base(change.File) != "schema.yaml" {
			continue
		}
		for _, proof := range change.Equivalence {
			if proof.Site != "whole-file" {
				continue
			}
			source := filepath.Dir(change.File)
			for parent := filepath.Dir(source); parent != "."; parent = filepath.Dir(parent) {
				if _, err := os.Stat(filepath.Join(root, parent, "schema.yaml")); err == nil {
					source = parent
				}
			}
			flow, err := filepath.Rel(source, filepath.Dir(change.File))
			if err != nil {
				panic(err)
			}
			entries = append(entries, entryGolden{File: change.File, Source: filepath.ToSlash(source), Flow: filepath.ToSlash(flow), Entry: proof.Entry, Order: proof.AfterOrder, Finals: proof.Finals})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].File < entries[j].File })
	return entries
}

func prepareEntryGoldens(root, ledger string) error {
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
	entries := baselineEntryGoldens(root, p)
	if len(entries) == 0 {
		return fmt.Errorf("missing reviewed baseline entry decisions")
	}
	body, err = json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "scripts/rewrite-stages-2566/entries.json"), append(body, '\n'), 0644); err != nil {
		return err
	}
	fmt.Printf("%d baseline-reviewed source/flow entry goldens written\n", len(entries))
	return nil
}
