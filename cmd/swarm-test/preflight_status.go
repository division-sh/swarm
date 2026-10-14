package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/testplanning"
)

const preflightStatusContext = "preflight/local"

var preflightRepositoryName = regexp.MustCompile(`^[A-Za-z0-9-]+/[A-Za-z0-9_.-]+$`)

type preflightStatusIdentity struct {
	Repository       string `json:"repository"`
	RepositoryNodeID string `json:"repository_node_id"`
	AttemptID        string `json:"attempt_id"`
	ReceiptID        string `json:"receipt_id"`
	StartStatusID    int64  `json:"start_status_id"`
}

type preflightStatusAttempt struct {
	identity   preflightStatusIdentity
	head       string
	origin     string
	ended      atomic.Bool
	planDigest atomic.Pointer[string]
}

type preflightStatusRepository struct {
	Name string `json:"nameWithOwner"`
	URL  string `json:"url"`
	ID   string `json:"id"`
}

func beginPreflightStatus(ctx context.Context, head, receiptID, attemptID string) (*preflightStatusAttempt, error) {
	parsed, err := uuid.Parse(attemptID)
	if err != nil || parsed.Version() != 4 || parsed.String() != attemptID || !strings.HasSuffix(receiptID, "-"+attemptID) {
		return nil, fmt.Errorf("invalid local preflight attempt identity")
	}
	if _, err := completionSource(".", head, true); err != nil {
		return nil, err
	}
	origin, err := preflightOrigin(ctx)
	if err != nil {
		return nil, err
	}
	repository, err := resolvePreflightStatusRepository(ctx, origin)
	if err != nil {
		return nil, err
	}
	var commit struct {
		SHA string `json:"sha"`
	}
	if err := preflightStatusCommand(ctx, "verify uploaded candidate", nil, &commit,
		"api", "--hostname", "github.com", "repos/"+repository.Name+"/commits/"+head); err != nil {
		return nil, fmt.Errorf("upload the exact candidate to a non-PR staging ref and check gh authentication/read access: %w", err)
	}
	if commit.SHA != head {
		return nil, fmt.Errorf("uploaded preflight commit identity differs from the admitted source")
	}
	attempt := &preflightStatusAttempt{head: head, origin: origin, identity: preflightStatusIdentity{
		Repository: repository.Name, RepositoryNodeID: repository.ID, AttemptID: attemptID, ReceiptID: receiptID,
	}}
	if err := attempt.checkSource(ctx); err != nil {
		return nil, err
	}
	startID, err := attempt.post(ctx, "pending")
	if err != nil {
		return nil, err
	}
	attempt.identity.StartStatusID = startID
	return attempt, nil
}

func resolvePreflightStatusRepository(ctx context.Context, origin string) (preflightStatusRepository, error) {
	var repository preflightStatusRepository
	if err := preflightStatusCommand(ctx, "resolve origin repository", nil, &repository,
		"repo", "view", origin, "--json", "nameWithOwner,url,id"); err != nil {
		return repository, err
	}
	coordinate, err := url.Parse(repository.URL)
	if err != nil || !preflightRepositoryName.MatchString(repository.Name) || repository.ID == "" || coordinate.Scheme != "https" ||
		coordinate.Host != "github.com" || coordinate.User != nil || coordinate.RawQuery != "" || coordinate.Fragment != "" || coordinate.Path != "/"+repository.Name {
		return repository, fmt.Errorf("preflight origin is not one exact github.com repository")
	}
	return repository, nil
}

func (a *preflightStatusAttempt) finish(ctx context.Context, directory string, receipt preflightReceipt) (int64, error) {
	if a.identity.StartStatusID <= 0 || !a.ended.CompareAndSwap(false, true) {
		return 0, fmt.Errorf("preflight status attempt is not an active acknowledged start")
	}
	if receipt.Attestation != a.identity || receipt.Plan.HeadSHA != a.head {
		return 0, fmt.Errorf("preflight status cannot consume a foreign attempt or source receipt")
	}
	if err := a.checkSource(ctx); err != nil {
		return 0, err
	}
	state := "failure"
	if receipt.Complete {
		digest := a.planDigest.Load()
		if digest == nil || receipt.Plan.Digest != *digest {
			return 0, fmt.Errorf("preflight success requires this attempt's exact frozen plan")
		}
		if err := validatePreflightStatusReceipt(directory, receipt); err != nil {
			return 0, err
		}
		if err := checkPreflightSource(ctx, receipt.Plan); err != nil {
			return 0, err
		}
		state = "success"
	}
	repository, err := resolvePreflightStatusRepository(ctx, a.origin)
	if err != nil {
		return 0, err
	}
	if repository.Name != a.identity.Repository || repository.ID != a.identity.RepositoryNodeID {
		return 0, fmt.Errorf("preflight origin repository identity changed during the attempt")
	}
	if err := a.checkSource(ctx); err != nil {
		return 0, err
	}
	return a.post(ctx, state)
}

func (a *preflightStatusAttempt) bindPlan(plan testplanning.PreflightPlan) error {
	if a.ended.Load() || plan.HeadSHA != a.head {
		return fmt.Errorf("preflight attempt cannot bind a second or foreign plan")
	}
	if err := plan.Validate(); err != nil {
		return err
	}
	if !a.planDigest.CompareAndSwap(nil, &plan.Digest) || a.ended.Load() {
		return fmt.Errorf("preflight attempt cannot bind a second or ended plan")
	}
	return nil
}

func (a *preflightStatusAttempt) checkSource(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := completionSource(".", a.head, true); err != nil {
		return err
	}
	origin, err := preflightOrigin(ctx)
	if err != nil {
		return err
	}
	if origin != a.origin {
		return fmt.Errorf("preflight origin changed during the admitted attempt")
	}
	return nil
}

func (a *preflightStatusAttempt) post(ctx context.Context, state string) (int64, error) {
	description := "attempt:" + a.identity.AttemptID + " receipt:" + a.identity.ReceiptID
	body, err := json.Marshal(struct {
		State       string `json:"state"`
		Context     string `json:"context"`
		Description string `json:"description"`
	}{state, preflightStatusContext, description})
	if err != nil {
		return 0, err
	}
	var response struct {
		ID          int64  `json:"id"`
		State       string `json:"state"`
		Context     string `json:"context"`
		Description string `json:"description"`
	}
	if err := preflightStatusCommand(ctx, "publish attempt "+state, body, &response,
		"api", "--hostname", "github.com", "--method", "POST", "repos/"+a.identity.Repository+"/statuses/"+a.head, "--input", "-"); err != nil {
		return 0, err
	}
	if response.ID <= 0 || response.State != state || response.Context != preflightStatusContext || response.Description != description || response.ID == a.identity.StartStatusID {
		return 0, fmt.Errorf("preflight status publication has no exact acknowledged response")
	}
	return response.ID, nil
}

func validatePreflightStatusReceipt(directory string, receipt preflightReceipt) error {
	if err := receipt.Plan.Validate(); err != nil {
		return err
	}
	if !receipt.Complete || receipt.Problem != "" || len(receipt.Unstarted) != 0 || receipt.Started.IsZero() ||
		filepath.Base(directory) != receipt.Attestation.ReceiptID ||
		receipt.Attestation.ReceiptID != receipt.Started.UTC().Format("20060102T150405.000000000")+"-"+receipt.Attestation.AttemptID {
		return fmt.Errorf("preflight status requires a complete exact-identity receipt")
	}
	phases := preflightPhases(receipt.Plan)
	if len(receipt.Phases) != len(phases) {
		return fmt.Errorf("preflight status requires every planned phase")
	}
	for i, phase := range phases {
		proof := receipt.Phases[i]
		if !proof.Passed || proof.Name != phase.Name || !slices.Equal(proof.Args, phase.Args) || proof.Started.IsZero() || proof.Completed.Before(proof.Started) {
			return fmt.Errorf("preflight status requires the exact passed phase %s", phase.Name)
		}
	}
	raw, err := os.ReadFile(filepath.Join(directory, "receipt.json"))
	if err != nil {
		return fmt.Errorf("read completed preflight receipt: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return fmt.Errorf("invalid persisted preflight receipt: %w", err)
	}
	expected, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if !bytes.Equal(compact.Bytes(), expected) {
		return fmt.Errorf("persisted preflight receipt differs from completed evidence")
	}
	return nil
}

func preflightOrigin(ctx context.Context) (string, error) {
	if os.Getenv("GH_REPO") != "" || (os.Getenv("GH_HOST") != "" && os.Getenv("GH_HOST") != "github.com") {
		return "", fmt.Errorf("local preflight requires explicit github.com origin; unset GH_REPO and conflicting GH_HOST")
	}
	remote, err := exec.CommandContext(ctx, "git", "remote", "get-url", "origin").Output()
	if err != nil {
		return "", fmt.Errorf("preflight status requires an explicit origin remote: %w", errors.Join(ctx.Err(), err))
	}
	return strings.TrimSpace(string(remote)), nil
}

func preflightStatusCommand(ctx context.Context, operation string, input []byte, result any, args ...string) error {
	command := exec.CommandContext(ctx, "gh", args...)
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	raw, err := command.Output()
	if err != nil {
		// gh's authentication stderr may contain sensitive diagnostics.
		return fmt.Errorf("preflight GitHub %s failed: %w", operation, errors.Join(ctx.Err(), err))
	}
	if err := json.Unmarshal(raw, result); err != nil {
		return fmt.Errorf("invalid preflight GitHub %s response: %w", operation, err)
	}
	return nil
}
