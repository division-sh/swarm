package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

var toolModules = map[string]string{
	"cyclo":  "github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0",
	"cognit": "github.com/uudashr/gocognit/cmd/gocognit@v1.2.1",
}

type policy struct {
	Schema    int               `json:"schema"`
	Tools     map[string]string `json:"tools"`
	Threshold int               `json:"threshold"`
	Scope     string            `json:"scope"`
}

func currentPolicy() policy {
	return policy{1, toolModules, 30, "tracked-regular-go/all-build-variants/exclude-test-and-ast.IsGenerated/reject-suppression-and-line-directives/lexical-occurrence-v1"}
}

type fileRecord struct {
	Path  string `json:"path"`
	Class string `json:"class"`
}

// Identity does not include source lines. Ordinal distinguishes repeated init and variable literals.
type identity struct {
	File       string `json:"file"`
	Package    string `json:"package"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Occurrence int    `json:"occurrence"`
}
type score struct {
	Identity identity `json:"identity"`
	Value    int      `json:"value"`
}
type baseline struct {
	Policy  policy             `json:"policy"`
	Files   []fileRecord       `json:"files"`
	Metrics map[string][]score `json:"metrics"`
}
type summary struct {
	Callables int `json:"callables"`
	At30      int `json:"at_least_30"`
	At50      int `json:"at_least_50"`
	Maximum   int `json:"maximum"`
}
type scoreChange struct {
	Metric   string   `json:"metric"`
	Identity identity `json:"identity"`
	Before   *int     `json:"before"`
	After    *int     `json:"after"`
}
type delta struct {
	Base         string             `json:"base,omitempty"`
	Head         string             `json:"head"`
	BaseSummary  map[string]summary `json:"base_summary,omitempty"`
	HeadSummary  map[string]summary `json:"head_summary"`
	Increased    bool               `json:"hotspot_count_increased"`
	RemovedFiles []fileRecord       `json:"removed_file_facts"`
	AddedFiles   []fileRecord       `json:"added_file_facts"`
	Changes      []scoreChange      `json:"callable_changes"`
}

func summaries(b baseline) map[string]summary {
	out := map[string]summary{}
	for metric, rows := range b.Metrics {
		s := summary{Callables: len(rows)}
		for _, r := range rows {
			if r.Value >= 30 {
				s.At30++
			}
			if r.Value >= 50 {
				s.At50++
			}
			if r.Value > s.Maximum {
				s.Maximum = r.Value
			}
		}
		out[metric] = s
	}
	return out
}

func identityKey(id identity) string { b, _ := json.Marshal(id); return string(b) }

func compare(base, head baseline) delta {
	d := delta{BaseSummary: summaries(base), HeadSummary: summaries(head), Changes: []scoreChange{}, RemovedFiles: []fileRecord{}, AddedFiles: []fileRecord{}}
	for _, metric := range []string{"cyclo", "cognit"} {
		if d.HeadSummary[metric].At30 > d.BaseSummary[metric].At30 {
			d.Increased = true
		}
		d.Changes = append(d.Changes, changes(metric, base.Metrics[metric], head.Metrics[metric])...)
	}
	old, next := map[fileRecord]bool{}, map[fileRecord]bool{}
	for _, f := range base.Files {
		old[f] = true
	}
	for _, f := range head.Files {
		next[f] = true
		if !old[f] {
			d.AddedFiles = append(d.AddedFiles, f)
		}
	}
	for _, f := range base.Files {
		if !next[f] {
			d.RemovedFiles = append(d.RemovedFiles, f)
		}
	}
	return d
}

func changes(metric string, base, head []score) []scoreChange {
	old, next := map[identity]int{}, map[identity]int{}
	ids := map[identity]bool{}
	for _, r := range base {
		old[r.Identity] = r.Value
		ids[r.Identity] = true
	}
	for _, r := range head {
		next[r.Identity] = r.Value
		ids[r.Identity] = true
	}
	keys := make([]identity, 0, len(ids))
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool { return identityKey(keys[i]) < identityKey(keys[j]) })
	out := []scoreChange{}
	for _, id := range keys {
		before, had := old[id]
		after, has := next[id]
		if had && has && before == after {
			continue
		}
		c := scoreChange{Metric: metric, Identity: id}
		if had {
			c.Before = &before
		}
		if has {
			c.After = &after
		}
		out = append(out, c)
	}
	return out
}

func checkBaseline(ctx context.Context, repo, sha string, expected []byte) error {
	b, err := git(ctx, repo, "show", sha+":"+baselinePath)
	if err != nil {
		return fmt.Errorf("head baseline missing: %w", err)
	}
	if !bytes.Equal(b, expected) {
		return fmt.Errorf("head baseline differs from exact measured snapshot; regenerate with -update")
	}
	return nil
}

func checkBasePolicy(ctx context.Context, repo, sha string, measured baseline) error {
	// Only genuine absence is bootstrap, not an unreadable/malformed existing artifact.
	b, err := git(ctx, repo, "ls-tree", sha, "--", baselinePath)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	b, err = git(ctx, repo, "show", sha+":"+baselinePath)
	if err != nil {
		return err
	}
	var previous baseline
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&previous); err != nil {
		return fmt.Errorf("base baseline: %w", err)
	}
	if !reflect.DeepEqual(previous.Policy, measured.Policy) {
		return fmt.Errorf("complexity policy changed: explicit policy review and comparable evidence required")
	}
	expected, err := encode(measured)
	if err != nil {
		return err
	}
	if !bytes.Equal(b, expected) {
		return fmt.Errorf("base baseline does not match independent measurement")
	}
	return nil
}
