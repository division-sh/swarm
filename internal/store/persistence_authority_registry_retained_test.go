package store_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

const (
	registryRetainedPath          = "internal/store/testdata/persistence_authority_registry_2542_retained.tsv"
	registryRetainedOrigin        = "4a7e8b8f66c5f536c1f96719e7e4f4fb5a82a12a"
	registryRetainedOriginDigest  = "5f5c9cf2ea361070e0bf3dba00f2029bf9205d7befcfe5ba793bf00fafc823cf"
	registryRetainedDecision      = "https://github.com/division-sh/swarm/issues/2542#issuecomment-6203866523"
	registryRetainedContract      = "https://github.com/division-sh/swarm/issues/2542#issuecomment-6203773156"
	registryRetainedQualification = "https://github.com/division-sh/swarm/issues/2542#issuecomment-6203881861"
)

type registryRetainedAuthorization struct{ Origin, Decision string }

type registryRetainedAdmission struct {
	Active, Origin, Previous           map[string]bool
	Digest, OriginTree, PreviousSource string
}

func parseRegistryRetainedRows(raw []byte) (map[string]bool, error) {
	rows := map[string]bool{}
	if len(raw) == 0 {
		return rows, nil
	}
	if raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw, '\r') {
		return nil, fmt.Errorf("retained registry must use exact LF-terminated rows")
	}
	previous := ""
	for _, line := range strings.Split(string(raw[:len(raw)-1]), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 7 || parts[0] != "unclassified" || (parts[2] != "typed" && parts[2] != "raw-sql") {
			return nil, fmt.Errorf("invalid retained registry row %q", line)
		}
		for _, part := range parts {
			if part == "" {
				return nil, fmt.Errorf("empty retained identity field")
			}
		}
		if previous != "" && line <= previous {
			return nil, fmt.Errorf("duplicate or non-bytewise-sorted retained row")
		}
		rows[line] = true
		previous = line
	}
	return rows, nil
}

func validateRegistryRetainedOrigin(authorization registryRetainedAuthorization, raw []byte) (map[string]bool, error) {
	if authorization.Origin != registryRetainedOrigin || authorization.Decision != registryRetainedDecision {
		return nil, fmt.Errorf("retained registry origin or user decision changed")
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != registryRetainedOriginDigest {
		return nil, fmt.Errorf("immutable retained registry origin digest differs")
	}
	rows, err := parseRegistryRetainedRows(raw)
	if err != nil {
		return nil, err
	}
	if len(rows) != 4813 {
		return nil, fmt.Errorf("immutable retained registry origin count differs")
	}
	return rows, nil
}

func loadRegistryRetainedAdmission(root string, authorization registryRetainedAuthorization) (registryRetainedAdmission, error) {
	var admission registryRetainedAdmission
	original, err := debtGit(root, "show", authorization.Origin+":internal/store/testdata/persistence_authority_findings.tsv")
	if err != nil {
		return admission, err
	}
	var lines []string
	for _, line := range strings.Split(string(original), "\n") {
		if strings.HasPrefix(line, "unclassified\t") {
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	origin := []byte(strings.Join(lines, "\n") + "\n")
	admission.Origin, err = validateRegistryRetainedOrigin(authorization, origin)
	if err != nil {
		return admission, err
	}
	tree, err := debtGit(root, "rev-parse", authorization.Origin+"^{tree}")
	if err != nil {
		return admission, err
	}
	admission.OriginTree = strings.TrimSpace(string(tree))
	active, err := os.ReadFile(filepath.Join(root, registryRetainedPath))
	if err != nil {
		return admission, err
	}
	admission.Active, err = parseRegistryRetainedRows(active)
	if err != nil {
		return admission, err
	}
	admission.Digest = fmt.Sprintf("%x", sha256.Sum256(active))
	admission.PreviousSource, err = debtTrustedBase(root)
	if err != nil {
		return admission, err
	}
	previous, err := registryRetainedPrevious(root, admission.PreviousSource, origin)
	if err != nil {
		return admission, err
	}
	admission.Previous, err = parseRegistryRetainedRows(previous)
	return admission, err
}

func registryRetainedPrevious(root, source string, origin []byte) ([]byte, error) {
	entry, err := debtGit(root, "ls-tree", source, "--", registryRetainedPath)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(entry)) != 0 {
		return debtGit(root, "show", source+":"+registryRetainedPath)
	}
	history, err := debtGit(root, "log", "-1", "--format=%H", source, "--", registryRetainedPath)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(history)) != 0 {
		return nil, fmt.Errorf("landed retained registry disappeared; reseed refused")
	}
	active, err := os.ReadFile(filepath.Join(root, registryRetainedPath))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(active, origin) {
		return nil, fmt.Errorf("first retained registry must equal the immutable origin exactly")
	}
	return origin, nil
}

func validateRegistryRetainedAdmission(admission registryRetainedAdmission, actual map[string]authorityFinding, registered map[string]string, unknown, stale, invalid []string) error {
	if len(unknown)+len(stale) != 0 {
		return fmt.Errorf("unknown or stale findings are never completion debt")
	}
	for row := range admission.Previous {
		if !admission.Origin[row] {
			return fmt.Errorf("trusted previous registry exceeds immutable origin")
		}
	}
	var allowed []string
	for row := range admission.Active {
		key := strings.TrimPrefix(row, "unclassified\t")
		finding, exists := actual[key]
		if !admission.Origin[row] || !admission.Previous[row] || !exists || registered[key] != "unclassified" {
			return fmt.Errorf("retained key is new, resurrected, absent or no longer unclassified: %s", row)
		}
		allowed = append(allowed, row) // The original completion reason.
		if finding.RawSQL {
			allowed = append(allowed, row) // Original raw-disallowed reason caused ONLY by unclassified.
		}
	}
	for key, disposition := range registered {
		if disposition == "unclassified" && !admission.Active["unclassified\t"+key] {
			return fmt.Errorf("unclassified registry key has no exact retained authority: %s", key)
		}
	}
	sort.Strings(allowed)
	diagnostics := slices.Clone(invalid)
	sort.Strings(diagnostics)
	if !slices.Equal(allowed, diagnostics) {
		return fmt.Errorf("strict diagnostic reason/key/multiplicity differs from exact retained completion debt")
	}
	return nil
}
