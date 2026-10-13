package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func bindFailureJobIdentity(ctx context.Context, plan testplanning.RunPlan, f *testtiming.ObservedFailure) {
	source, platform, err := failureJobIdentity(ctx, plan, *f)
	if err != nil {
		f.Problem = joinFailureProblem(f.Problem, "job execution identity unavailable: "+err.Error())
		return
	}
	f.Source, f.GOOS = source, platform
}

func failureJobIdentity(ctx context.Context, plan testplanning.RunPlan, f testtiming.ObservedFailure) (string, string, error) {
	// Job names select only the existing artifact route, never platform authority.
	platform := ""
	switch f.Job {
	case "Native Linux unused analysis":
		platform = "linux"
	case "Native Darwin unused analysis":
		platform = "darwin"
	}
	if platform != "" {
		name := fmt.Sprintf("unused-%s-%d-%d", platform, f.RunID, f.Attempt)
		raw, err := readFailureArtifact(ctx, "repos/division-sh/swarm", f.RunID, name)
		if err != nil {
			return "", "", err
		}
		return unusedFailureArchive(raw, plan.HeadSHA, platform)
	}
	for _, batch := range plan.Batches {
		if f.Job != "Go proof "+batch.ID {
			continue
		}
		name := fmt.Sprintf("proof-%d-%d-%s-timing", f.RunID, f.Attempt, batch.ID)
		raw, err := readFailureArtifact(ctx, "repos/division-sh/swarm", f.RunID, name)
		if err != nil {
			return "", "", err
		}
		return proofFailureArchive(raw, plan, batch, f.RunID, f.Attempt)
	}
	return "", "", fmt.Errorf("no job-specific execution artifact")
}

func proofFailureArchive(raw []byte, plan testplanning.RunPlan, batch testplanning.ProofBatch, runID int64, attempt int) (string, string, error) {
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", "", err
	}
	allowed := make(map[string]string)
	for _, id := range batch.Units {
		allowed[id+"-primary-evidence.json"] = id
	}
	seen := make(map[string]bool)
	for _, member := range archive.File {
		if !strings.HasSuffix(member.Name, "-primary-evidence.json") {
			continue
		}
		id, ok := allowed[member.Name]
		if !ok || seen[id] {
			return "", "", fmt.Errorf("foreign/duplicate job command evidence")
		}
		seen[id] = true
		var e testtiming.CommandEvidence
		if err := readFailureArchiveJSON(member, &e); err != nil {
			return "", "", err
		}
		if e.WorkflowRunID != runID || e.WorkflowAttempt != attempt || e.UnitID != id {
			return "", "", fmt.Errorf("foreign job command identity")
		}
		if problems := testtiming.ValidateCommandIdentity(e, plan); len(problems) != 0 {
			return "", "", fmt.Errorf("invalid job command identity: %s", strings.Join(problems, "; "))
		}
	}
	if len(seen) == 0 {
		return "", "", fmt.Errorf("missing job command identity")
	}
	return plan.HeadSHA, plan.BuildContext.GOOS, nil
}

func unusedFailureArchive(raw []byte, source, platform string) (string, string, error) {
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", "", err
	}
	members := make(map[string]*zip.File)
	for _, member := range archive.File {
		if member.Name != "metadata.json" && member.Name != "staticcheck.bin" {
			continue
		}
		if members[member.Name] != nil {
			return "", "", fmt.Errorf("duplicate native unused artifact member")
		}
		members[member.Name] = member
	}
	if members["metadata.json"] == nil || members["staticcheck.bin"] == nil {
		return "", "", fmt.Errorf("missing native unused artifact members")
	}
	metadata, err := members["metadata.json"].Open()
	if err != nil {
		return "", "", err
	}
	defer metadata.Close()
	result, err := members["staticcheck.bin"].Open()
	if err != nil {
		return "", "", err
	}
	defer result.Close()
	e, err := testtiming.ReadUnusedIdentity(io.LimitReader(metadata, 1<<20), io.LimitReader(result, 256<<20), source, platform)
	if err != nil {
		return "", "", err
	}
	return e.SourceSHA, e.Native.GOOS, nil
}

func readFailureArchiveJSON(member *zip.File, target any) error {
	reader, err := member.Open()
	if err != nil {
		return err
	}
	defer reader.Close()
	dec := json.NewDecoder(io.LimitReader(reader, 32<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing job command evidence")
	}
	return nil
}
