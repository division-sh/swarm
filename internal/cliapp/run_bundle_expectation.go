package cliapp

import (
	"fmt"
	"strings"
)

type runBundleExpectationMismatch struct {
	expected  string
	selected  string
	server    string
	directory string
	bound     bool
}

func (e *runBundleExpectationMismatch) Error() string {
	from := ""
	expectation := "the expected hash is the release"
	if e.directory != "" {
		directory := fmt.Sprintf("%q", cliSanitizeOneLineValue(e.directory))
		from = " (from " + directory + ")"
		expectation = directory + " is the release"
	}
	if e.bound {
		return fmt.Sprintf("nothing was sent: this request is already bound to bundle %s, not the expected %s%s; check that %s this request is bound to", e.selected, e.expected, from, expectation)
	}
	return fmt.Sprintf("nothing was sent: the serve at %s holds bundle %s, not the expected %s%s; check that you are connected to the right serve and that %s it should hold", e.server, e.selected, e.expected, from, expectation)
}

func (o runCommandOptions) hasBundleExpectation() bool {
	return o.changedFlags["expect-bundle"] || o.expectBundle != ""
}

func (o runCommandOptions) validateBundleExpectation() error {
	connected := strings.TrimSpace(o.connectURL) != ""
	reattach := strings.TrimSpace(o.reattachRunID) != ""
	if connected && !reattach && (o.sourceRootSet || o.sourceRoot != "") {
		directory := fmt.Sprintf("%q", cliSanitizeOneLineValue(o.sourceRoot))
		return fmt.Errorf("--connect runs the flow the serve already holds, so it takes no directory: remove %s, or use --expect-bundle %s to check that the serve holds it", directory, directory)
	}
	if !o.hasBundleExpectation() {
		return nil
	}
	if !connected || reattach {
		return fmt.Errorf("--expect-bundle requires --connect publication mode and cannot be used with --reattach")
	}
	_, err := runExpectedBundleHash(o.expectBundle)
	return err
}

func runExpectedBundleHash(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("--expect-bundle must be a nonempty directory or canonical bundle hash")
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "sha256:") || strings.HasPrefix(lower, "bundle-") && strings.Contains(value, ":") {
		return validateBundleHashArg("--expect-bundle", value)
	}
	return "", nil
}

func (o runCommandOptions) resolveBundleExpectation(root InvocationRoot) (string, error) {
	if !o.hasBundleExpectation() {
		return "", nil
	}
	hash, err := runExpectedBundleHash(o.expectBundle)
	if err != nil || hash != "" {
		return hash, err
	}
	hash, err = admitCLIIdentitySource(root, o.expectBundle)
	if err != nil {
		return "", fmt.Errorf("--expect-bundle: %w", err)
	}
	return hash, nil
}
