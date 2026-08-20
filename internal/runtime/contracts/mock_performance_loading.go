package contracts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
)

var windowsAbsoluteAgentMockModulePath = regexp.MustCompile(`^[A-Za-z]:`)

type agentMockMaterializationSource struct {
	ContractsRoot string
	PackageRoot   string
	Declaration   ContractItemSource
}

type resolvedAgentMockMaterializationSource struct {
	contractsRoot  string
	packageRoot    string
	declaration    ContractItemSource
	declarationRel string
}

func materializeAgentMockPerformances(source agentMockMaterializationSource, entries map[string]AgentRegistryEntry) (map[string]AgentRegistryEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	resolvedSource, err := resolveAgentMockMaterializationSource(source)
	if err != nil {
		return nil, err
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	for key, entry := range entries {
		performance, err := materializeAgentMockPerformance(resolvedSource, key, entry.Mock)
		if err != nil {
			return nil, err
		}
		entry.Mock = performance
		out[key] = entry
	}
	return out, nil
}

func resolveAgentMockMaterializationSource(source agentMockMaterializationSource) (resolvedAgentMockMaterializationSource, error) {
	contractsRoot, err := canonicalAbsDir(source.ContractsRoot, "contracts root for agent mock performances")
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, err
	}
	packageKey, err := validateAgentMockPackageKey(source.Declaration.PackageKey)
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, err
	}
	packageRoot, err := filepath.Abs(strings.TrimSpace(source.PackageRoot))
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("resolve declaring package root for agent mock performances: %w", err)
	}
	packageRoot = filepath.Clean(packageRoot)
	expectedPackageRoot := contractsRoot
	if packageKey != "." {
		expectedPackageRoot = filepath.Join(contractsRoot, filepath.FromSlash(packageKey))
	}
	if packageRoot != expectedPackageRoot {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf(
			"agent mock declaration package %q root %q disagrees with contracts-root location %q",
			packageKey,
			packageRoot,
			expectedPackageRoot,
		)
	}
	if packageKey != "." {
		info, statErr := lstatNoSymlinkPath(contractsRoot, filepath.FromSlash(packageKey), packageKey)
		if statErr != nil {
			return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock declaring package %q cannot be admitted: %w", packageKey, statErr)
		}
		if !info.IsDir() {
			return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock declaring package %q root must be a directory", packageKey)
		}
	}
	declarationFile := strings.TrimSpace(source.Declaration.File)
	if declarationFile == "" {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock materialization requires the exact declaring agents.yaml source")
	}
	declarationFile, err = filepath.Abs(declarationFile)
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("resolve agent mock declaration source %q: %w", source.Declaration.File, err)
	}
	declarationFile = filepath.Clean(declarationFile)
	declarationRel, err := pathRelativeToRoot(packageRoot, declarationFile)
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock declaration source %q is outside declaring package %q: %w", source.Declaration.File, packageKey, err)
	}
	declarationInfo, err := lstatNoSymlinkPath(packageRoot, declarationRel, source.Declaration.File)
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock declaration source %q cannot be admitted: %w", source.Declaration.File, err)
	}
	if !declarationInfo.Mode().IsRegular() {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock declaration source %q must be a regular file", source.Declaration.File)
	}
	outerDeclarationRel, err := pathRelativeToRoot(contractsRoot, declarationFile)
	if err != nil {
		return resolvedAgentMockMaterializationSource{}, fmt.Errorf("agent mock declaration source %q is outside contracts root: %w", source.Declaration.File, err)
	}
	resolvedDeclaration := source.Declaration
	resolvedDeclaration.PackageKey = packageKey
	resolvedDeclaration.File = declarationFile
	return resolvedAgentMockMaterializationSource{
		contractsRoot:  contractsRoot,
		packageRoot:    packageRoot,
		declaration:    resolvedDeclaration,
		declarationRel: filepath.ToSlash(outerDeclarationRel),
	}, nil
}

func validateAgentMockPackageKey(raw string) (string, error) {
	key := strings.TrimSpace(raw)
	if key == "." {
		return key, nil
	}
	if key == "" {
		return "", fmt.Errorf("agent mock declaration package key is required")
	}
	if strings.ContainsRune(key, '\x00') || strings.Contains(key, `\`) || filepath.IsAbs(key) || strings.HasPrefix(key, "/") || windowsAbsoluteAgentMockModulePath.MatchString(key) {
		return "", fmt.Errorf("agent mock declaration package key %q is not a canonical contracts-root-relative path", raw)
	}
	segments := strings.Split(key, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("agent mock declaration package key %q contains an empty, dot, or traversal segment", raw)
		}
	}
	return strings.Join(segments, "/"), nil
}

func materializeAgentMockPerformance(source resolvedAgentMockMaterializationSource, agentID string, performance mockperformance.Performance) (mockperformance.Performance, error) {
	if !performance.Configured() {
		return mockperformance.Performance{}, nil
	}
	if strings.TrimSpace(performance.Kind) != mockperformance.KindPython {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.kind %q is unsupported; use %q", agentID, performance.Kind, mockperformance.KindPython)
	}
	if performance.PostToolTailLatencyMS < 0 {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.post_tool_tail_latency_ms must be non-negative", agentID)
	}
	module, err := validateAgentMockModulePath(performance.Module)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q: %w", agentID, performance.Module, err)
	}
	path := filepath.Join(source.packageRoot, filepath.FromSlash(module))
	info, err := lstatNoSymlinkPath(source.packageRoot, filepath.FromSlash(module), performance.Module)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q cannot be read: %w", agentID, performance.Module, err)
	}
	if !info.Mode().IsRegular() {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q must be a regular file", agentID, performance.Module)
	}
	if info.Mode().Perm()&0o444 == 0 {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q is not readable", agentID, performance.Module)
	}
	if _, err := pathRelativeToRoot(source.packageRoot, path); err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q resolves outside declaring package %q: %w", agentID, performance.Module, source.declaration.PackageKey, err)
	}
	sourcePath, err := pathRelativeToRoot(source.contractsRoot, path)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q resolves outside the contracts root: %w", agentID, performance.Module, err)
	}
	moduleSource, err := os.ReadFile(path)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q cannot be read: %w", agentID, performance.Module, err)
	}
	sum := sha256.Sum256(moduleSource)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pythonmodule.ValidateSource(ctx, pythonmodule.Request{
		ModuleID:    "agent.mock." + strings.TrimSpace(agentID),
		RowID:       source.declarationRel,
		Digest:      digest,
		Entry:       mockperformance.EntryHandle,
		Source:      moduleSource,
		MemoryPages: mockperformance.ValidationMemoryPages,
		OutputBytes: mockperformance.ValidationOutputBytes,
	}); err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q is invalid: %w", agentID, performance.Module, err)
	}
	return mockperformance.Performance{
		Kind:                  mockperformance.KindPython,
		Module:                module,
		PostToolTailLatencyMS: performance.PostToolTailLatencyMS,
		Source:                append([]byte(nil), moduleSource...),
		Digest:                digest,
		SourcePath:            filepath.ToSlash(sourcePath),
	}, nil
}

func validateAgentMockModulePath(raw string) (string, error) {
	module := strings.TrimSpace(raw)
	if module == "" {
		return "", fmt.Errorf("path is required")
	}
	if module != raw {
		return "", fmt.Errorf("path must not contain leading or trailing whitespace")
	}
	if strings.ContainsRune(module, '\x00') {
		return "", fmt.Errorf("path contains a NUL byte")
	}
	if strings.Contains(module, `\`) {
		return "", fmt.Errorf("path must use forward separators and cannot use backslashes")
	}
	if filepath.IsAbs(module) || strings.HasPrefix(module, "/") || windowsAbsoluteAgentMockModulePath.MatchString(module) {
		return "", fmt.Errorf("path must be relative and canonical")
	}
	segments := strings.Split(module, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("path contains an empty, dot, or traversal segment")
		}
	}
	return strings.Join(segments, "/"), nil
}
