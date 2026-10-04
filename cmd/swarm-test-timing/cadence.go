package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func observeFullCadence(cfg config) error {
	if cfg.resultJSONPath == "" || cfg.markdownPath == "" || cfg.evidenceRoot == "" {
		return fmt.Errorf("cadence observation requires result-json, markdown and evidence-root")
	}
	plan, err := readPlan(cfg.planPath)
	if err != nil {
		return err
	}
	evidence, problems := readEvidenceTree(cfg.evidenceRoot)
	full := testtiming.CadenceAttempt{Plan: plan, RunID: cfg.workflowRunID, Attempt: cfg.workflowAttempt, Evidence: evidence, LoadProblems: problems}
	var classifications []testtiming.CadenceAttribution
	var inputErr error
	if cfg.classificationPath != "" {
		if err := readJSON(cfg.classificationPath, &classifications); err != nil {
			inputErr = err
			classifications = nil
			full.LoadProblems = append(full.LoadProblems, err.Error())
		}
	}
	core, err := readCadenceCore(cfg)
	if err != nil {
		inputErr = err
		full.LoadProblems = append(full.LoadProblems, err.Error())
	}
	lineage := cadenceMasterLineage(context.Background(), ".", plan.HeadSHA)
	observation, observeErr := testtiming.ObserveFullCadence(full, core, classifications, lineage)
	if observeErr == nil {
		observeErr = inputErr
	}
	if observeErr != nil {
		observation.Problems = append(observation.Problems, observeErr.Error())
	}
	if err := writeJSON(cfg.resultJSONPath, observation); err != nil {
		return err
	}
	out, closeOutput, err := openOutput(cfg.markdownPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	if err := testtiming.WriteCadenceMarkdown(out, observation); err != nil {
		return err
	}
	return observeErr
}

func cadenceMasterLineage(ctx context.Context, repo, head string) []string {
	shallow, err := exec.CommandContext(ctx, "git", "-C", repo, "rev-parse", "--is-shallow-repository").Output()
	if err != nil || strings.TrimSpace(string(shallow)) != "false" {
		return nil
	}
	data, err := exec.CommandContext(ctx, "git", "-C", repo, "rev-list", "--first-parent", "refs/remotes/origin/master").Output()
	if err != nil {
		return nil
	}
	lineage := strings.Fields(string(data))
	for i, sha := range lineage {
		if sha == head {
			return lineage[i:]
		}
	}
	return nil
}

func readCadenceCore(cfg config) (*testtiming.CadenceCoreProof, error) {
	if cfg.priorCorePlanPath == "" && cfg.priorCoreEvidenceRoot == "" && cfg.priorCoreRunPath == "" && cfg.priorCoreLandingSHA == "" {
		return nil, nil
	}
	if cfg.priorCorePlanPath == "" || cfg.priorCoreEvidenceRoot == "" || cfg.priorCoreRunPath == "" {
		return nil, fmt.Errorf("preceding core proof needs its actual plan, evidence and successful Actions run metadata together")
	}
	plan, err := readPlan(cfg.priorCorePlanPath)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(cfg.priorCoreRunPath)
	if err != nil {
		return nil, err
	}
	var run testplanning.QualifiedRun
	if err := json.Unmarshal(raw, &run); err != nil {
		return nil, err
	}
	if plan.Profile != testplanning.ProfileCore || plan.Venue != testplanning.VenueCI || run.ID < 1 || run.RunAttempt < 1 || run.Status != "completed" || run.Conclusion != "success" {
		return nil, fmt.Errorf("preceding core workflow was not a successful bound CI core attempt")
	}
	if cfg.priorCoreLandingSHA != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		observed, actual, err := observeMergedQualification(ctx, config{repository: "division-sh/swarm", branchRef: "refs/heads/master", headSHA: cfg.priorCoreLandingSHA})
		if err != nil {
			return nil, fmt.Errorf("preceding core master landing: %w", err)
		}
		if observed.Digest != plan.Digest || actual.ID != run.ID || actual.RunAttempt != run.RunAttempt || actual.HeadSHA != run.HeadSHA {
			return nil, fmt.Errorf("preceding core archive is not the exact observed merged qualification")
		}
	} else if run.HeadSHA != plan.HeadSHA {
		return nil, fmt.Errorf("preceding core workflow was not successfully completed on the plan source; source aliases require independent attribution, not guessed credit")
	}
	evidence, problems := readEvidenceTree(cfg.priorCoreEvidenceRoot)
	return &testtiming.CadenceCoreProof{CadenceAttempt: testtiming.CadenceAttempt{Plan: plan, RunID: run.ID, Attempt: run.RunAttempt, Evidence: evidence, LoadProblems: problems}, SuccessfulRun: true, LandedHeadSHA: cfg.priorCoreLandingSHA}, nil
}
