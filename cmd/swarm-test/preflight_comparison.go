package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
)

func observePreflightComparison(ctx context.Context, head, origin string) (string, testplanning.PreflightComparison, error) {
	var comparison testplanning.PreflightComparison
	if os.Getenv("GITHUB_EVENT_PATH") != "" || os.Getenv("SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT") != "" || os.Getenv("SWARM_DEBT_CACHE_PUBLISH") != "" {
		return "", comparison, fmt.Errorf("local preflight refuses ambient debt event, refresh or publication authority")
	}
	git := func(args ...string) ([]byte, error) { return exec.CommandContext(ctx, "git", args...).Output() }
	current, err := git("rev-parse", "origin/master^{commit}")
	if err != nil {
		return "", comparison, fmt.Errorf("preflight trusted origin/master identity missing: %w", err)
	}
	if strings.TrimSpace(string(current)) != origin {
		return "", comparison, fmt.Errorf("preflight trusted origin/master identity changed")
	}
	base, err := git("merge-base", "--all", head, origin)
	if err != nil {
		return "", comparison, fmt.Errorf("preflight comparison missing: %w", err)
	}
	if len(strings.Fields(string(base))) != 1 {
		return "", comparison, fmt.Errorf("preflight comparison ambiguous")
	}
	baseSHA := strings.TrimSpace(string(base))
	if baseSHA == head {
		base, err = git("rev-parse", head+"^")
		if err != nil {
			return "", comparison, fmt.Errorf("preflight independent predecessor missing: %w", err)
		}
		baseSHA = strings.TrimSpace(string(base))
	}
	diff, err := git("diff", "--name-status", "-z", "--find-renames", "--find-copies", baseSHA, head, "--")
	if err != nil {
		return "", comparison, fmt.Errorf("preflight diff evidence unavailable: %w", err)
	}
	required, err := testplanning.PreflightDebtRequired(diff)
	if err != nil {
		return "", comparison, err
	}
	comparison = testplanning.PreflightComparison{OriginMasterSHA: origin, DiffSHA256: fmt.Sprintf("%x", sha256.Sum256(diff)), DebtRequired: required}
	return baseSHA, comparison, nil
}

// The frozen comparison is only an expectation; the unchanged native debt owner
// must report this exact source after its canonical ratchet has passed.
func validatePreflightDebtSource(log io.Reader, expected string) error {
	var count int
	source := regexp.MustCompile(`debt source=([0-9a-f]{40}) collector=[0-9a-f]{64} `)
	scanner := bufio.NewScanner(log)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		var event struct{ Action, Package, Test, Output string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return err
		}
		if event.Action != "output" || event.Package != "github.com/division-sh/swarm/internal/store" || event.Test != "TestPersistenceAuthorityDebtRatchet" {
			continue
		}
		if match := source.FindStringSubmatch(event.Output); match != nil {
			if match[1] != expected {
				return fmt.Errorf("native trusted debt comparator differs from frozen preflight comparison")
			}
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("preflight needs one native debt-owner source receipt, found %d", count)
	}
	return nil
}
