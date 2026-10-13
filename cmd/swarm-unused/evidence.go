package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/testtiming"
)

type configuration = testtiming.UnusedConfiguration

func pinnedConfig() configuration {
	return configuration{
		Tool: staticcheck, Toolchain: toolchain, Checks: "U1000", Matrix: buildMatrix,
		Packages: "./...", TargetGo: "module", Tests: true, ShowIgnored: true,
		GOWORK: "off", GOENV: "off", U1000Directives: "reject",
	}
}

type evidence = testtiming.UnusedEvidence

func validateNative(n native, platform string) error {
	return testtiming.ValidateUnusedNative(n, platform, toolchain)
}

func validateEvidence(e evidence, head, platform string) error {
	if err := testtiming.ValidateUnusedIdentity(e, head, platform); err != nil {
		return err
	}
	if e.Config != pinnedConfig() {
		return errors.New("evidence configuration does not match pinned guard")
	}
	return nil
}

func hashResult(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	if n == 0 {
		return "", 0, errors.New("empty native result")
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func gitOutput(root string, args ...string) (string, error) {
	c := exec.Command("git", args...)
	c.Dir = root
	b, err := c.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, b)
	}
	return strings.TrimSpace(string(b)), nil
}

func cleanHead(root string, results ...string) (string, error) {
	head, err := gitOutput(root, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	if _, err := gitOutput(root, "diff", "--exit-code", "HEAD", "--"); err != nil {
		return "", errors.New("exact-source evidence requires no tracked changes")
	}
	// Deliberately omit Git ignore rules: ignored Go, C, assembly and embedded
	// files can still change analysis. Only the exact evidence files are allowed.
	untracked, err := gitOutput(root, "ls-files", "--others", "-z")
	if err != nil {
		return "", err
	}
	allowed := make(map[string]bool, len(results))
	for _, path := range results {
		allowed[filepath.Clean(path)] = true
	}
	for _, name := range strings.Split(untracked, "\x00") {
		if name != "" && !allowed[filepath.Join(root, filepath.FromSlash(name))] {
			return "", fmt.Errorf("exact-source evidence rejects untracked file (including ignored files): %s", name)
		}
	}
	return head, nil
}

func (g guard) collect(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// Invalidate old success before any operation that can fail.
	if err := os.Remove(filepath.Join(dir, metaFile)); err != nil && !os.IsNotExist(err) {
		return err
	}
	path := filepath.Join(dir, resultFile)
	metadata := filepath.Join(dir, metaFile)
	head, err := cleanHead(g.root, path, metadata)
	if err != nil {
		return err
	}
	n, err := g.analyze(path)
	if err != nil {
		return err
	}
	after, err := cleanHead(g.root, path, metadata)
	if err != nil || after != head {
		return errors.New("source changed during collection")
	}
	hash, size, err := hashResult(path)
	if err != nil {
		return err
	}
	e := evidence{Schema: 1, State: "complete", SourceSHA: head, Config: pinnedConfig(), Native: n, ResultSHA256: hash, ResultBytes: size}
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, metaFile), append(b, '\n'), 0644); err != nil {
		return err
	}
	fmt.Fprintf(g.stdout, "Collected native %s/%s default/race/issue2413 evidence at %s for %s; U1000 decision deferred to Linux/Darwin merge (issue2438 parked)\n", n.GOOS, n.GOARCH, dir, head)
	return nil
}

func readEvidence(dir, head, platform string) (string, error) {
	f, err := os.Open(filepath.Join(dir, metaFile))
	if err != nil {
		return "", fmt.Errorf("missing %s evidence: %w", platform, err)
	}
	defer f.Close()
	path := filepath.Join(dir, resultFile)
	result, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer result.Close()
	e, err := testtiming.ReadUnusedIdentity(f, result, head, platform)
	if err != nil {
		return "", fmt.Errorf("invalid %s evidence: %w", platform, err)
	}
	if err := validateEvidence(e, head, platform); err != nil {
		return "", fmt.Errorf("%s: %w", platform, err)
	}
	return path, nil
}

func (g guard) merge(dir string) error {
	var results []string
	for _, platform := range []string{"linux", "darwin"} {
		results = append(results, filepath.Join(dir, platform, resultFile), filepath.Join(dir, platform, metaFile))
	}
	head, err := cleanHead(g.root, results...)
	if err != nil {
		return err
	}
	if err := g.rejectSuppressions(); err != nil {
		return err
	}
	var paths []string
	for _, platform := range []string{"linux", "darwin"} {
		path, err := readEvidence(filepath.Join(dir, platform), head, platform)
		if err != nil {
			return err
		}
		// Validate every native binary separately for load errors, never for U1000.
		if err := g.validateDiagnostics(path); err != nil {
			return err
		}
		paths = append(paths, path)
	}
	if err := g.decide(paths); err != nil {
		return err
	}
	after, err := cleanHead(g.root, results...)
	if err != nil || after != head {
		return errors.New("source changed during merge")
	}
	fmt.Fprintf(g.stdout, "Linux/Darwin native default/race/issue2413 union: zero U1000 for %s (issue2438 parked)\n", head)
	return nil
}
