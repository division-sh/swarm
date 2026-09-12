// Command swarm-complexity measures pinned upstream complexity on exact Git snapshots.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const baselinePath = ".github/complexity-baseline.json"

type options struct {
	repo, head, base, event, eventFile, evidence string
	update                                       bool
}

func main() {
	var o options
	flag.StringVar(&o.repo, "repo", ".", "Git repository")
	flag.StringVar(&o.head, "head", "HEAD", "exact revision to measure (never the working tree)")
	flag.StringVar(&o.base, "base", "", "independent comparison revision")
	flag.StringVar(&o.event, "event", "", "CI event: pull_request, push, workflow_dispatch, schedule")
	flag.StringVar(&o.eventFile, "event-file", "", "GitHub event JSON")
	flag.StringVar(&o.evidence, "evidence", "", "directory for measured baseline and delta evidence")
	flag.BoolVar(&o.update, "update", false, "write measured head baseline; does not approve growth")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if err := run(context.Background(), o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options, out io.Writer) error {
	if err := applyEvent(&o); err != nil {
		return err
	}
	headSHA, err := revision(ctx, o.repo, o.head)
	if err != nil {
		return err
	}
	head, err := measure(ctx, o.repo, headSHA, upstream)
	if err != nil {
		return fmt.Errorf("head: %w", err)
	}
	data, err := encode(head)
	if err != nil {
		return err
	}
	report := delta{Head: headSHA, HeadSummary: summaries(head)}
	var admissionErr error
	if o.update {
		admissionErr = writeFile(filepath.Join(o.repo, baselinePath), data)
	} else {
		admissionErr = checkBaseline(ctx, o.repo, headSHA, data)
	}
	if o.base != "" {
		baseSHA, err := revision(ctx, o.repo, o.base)
		if err != nil {
			return err
		}
		base, err := measure(ctx, o.repo, baseSHA, upstream)
		if err != nil {
			return fmt.Errorf("base: %w", err)
		}
		if err := checkBasePolicy(ctx, o.repo, baseSHA, base); err != nil {
			return err
		}
		report = compare(base, head)
		report.Head, report.Base = headSHA, baseSHA
	}
	if err := emitEvidence(o.evidence, data, report, out); err != nil {
		return err
	}
	if admissionErr != nil {
		return admissionErr
	}
	if report.Increased {
		return fmt.Errorf("complexity hotspot count increased; updating the baseline cannot approve growth")
	}
	return nil
}

func encode(v any) ([]byte, error) {
	if b, ok := v.(baseline); ok {
		return encodeBaseline(b)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	return append(b, '\n'), err
}

// Keep the checked-in inventory diffable without ten lines of indentation per score.
func encodeBaseline(b baseline) ([]byte, error) {
	var out bytes.Buffer
	p, err := json.Marshal(b.Policy)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(&out, "{\n  \"policy\": %s,\n  \"files\": [\n", p)
	for i, f := range b.Files {
		r, err := json.Marshal(f)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			out.WriteString(",\n")
		}
		fmt.Fprintf(&out, "    %s", r)
	}
	out.WriteString("\n  ],\n  \"metrics\": {\n")
	for i, metric := range []string{"cyclo", "cognit"} {
		if i > 0 {
			out.WriteString(",\n")
		}
		fmt.Fprintf(&out, "    %q: [\n", metric)
		for j, row := range b.Metrics[metric] {
			r, err := json.Marshal(row)
			if err != nil {
				return nil, err
			}
			if j > 0 {
				out.WriteString(",\n")
			}
			fmt.Fprintf(&out, "      %s", r)
		}
		out.WriteString("\n    ]")
	}
	out.WriteString("\n  }\n}\n")
	return out.Bytes(), nil
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func emitEvidence(dir string, head []byte, report delta, out io.Writer) error {
	b, err := encode(report)
	if err != nil {
		return err
	}
	if dir != "" {
		if err := writeFile(filepath.Join(dir, "head.json"), head); err != nil {
			return err
		}
		if err := writeFile(filepath.Join(dir, "delta.json"), b); err != nil {
			return err
		}
	}
	_, err = out.Write(b)
	return err
}

func applyEvent(o *options) error {
	if o.event == "" {
		if o.eventFile != "" {
			return fmt.Errorf("event-file requires event")
		}
		return nil
	}
	if o.update || o.base != "" {
		return fmt.Errorf("CI events cannot update the baseline or override the comparison base")
	}
	if o.event == "workflow_dispatch" || o.event == "schedule" {
		return nil
	}
	data, err := os.ReadFile(o.eventFile)
	if err != nil {
		return err
	}
	head, base, err := eventRevisions(o.event, data)
	if err != nil {
		return err
	}
	o.head, o.base = head, base
	return nil
}

func eventRevisions(event string, data []byte) (string, string, error) {
	var payload struct {
		Before      string `json:"before"`
		After       string `json:"after"`
		PullRequest struct {
			Head struct {
				SHA string `json:"sha"`
			} `json:"head"`
			Base struct {
				SHA string `json:"sha"`
			} `json:"base"`
		} `json:"pull_request"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", "", err
	}
	var head, base string
	switch event {
	case "pull_request":
		head, base = payload.PullRequest.Head.SHA, payload.PullRequest.Base.SHA
	case "push":
		head, base = payload.After, payload.Before
	default:
		return "", "", fmt.Errorf("unsupported CI event %q", event)
	}
	if !objectID(head) || !objectID(base) {
		return "", "", fmt.Errorf("%s requires nonzero exact head and base SHAs", event)
	}
	return head, base, nil
}
