package contracts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
)

func materializeAgentMockPerformances(contractsRoot, sourceFile string, entries map[string]AgentRegistryEntry) (map[string]AgentRegistryEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	root, err := filepath.Abs(strings.TrimSpace(contractsRoot))
	if err != nil {
		return nil, fmt.Errorf("resolve contracts root for mock performances: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve contracts root for mock performances: %w", err)
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	for key, entry := range entries {
		performance, err := materializeAgentMockPerformance(root, sourceFile, key, entry.Mock)
		if err != nil {
			return nil, err
		}
		entry.Mock = performance
		out[key] = entry
	}
	return out, nil
}

func materializeAgentMockPerformance(contractsRoot, sourceFile, agentID string, performance mockperformance.Performance) (mockperformance.Performance, error) {
	if !performance.Configured() {
		return mockperformance.Performance{}, nil
	}
	if strings.TrimSpace(performance.Kind) != mockperformance.KindPython {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.kind %q is unsupported; use %q", agentID, performance.Kind, mockperformance.KindPython)
	}
	module := filepath.Clean(strings.TrimSpace(performance.Module))
	if module == "." || filepath.IsAbs(module) || module == ".." || strings.HasPrefix(module, ".."+string(filepath.Separator)) {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q must be a file below the contracts root", agentID, performance.Module)
	}
	path := filepath.Join(contractsRoot, module)
	info, err := os.Lstat(path)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q cannot be read: %w", agentID, performance.Module, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q must not be a symlink", agentID, performance.Module)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q cannot be resolved: %w", agentID, performance.Module, err)
	}
	if !pathWithinRoot(contractsRoot, resolved) {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q resolves outside the contracts root", agentID, performance.Module)
	}
	resolvedInfo, err := os.Stat(resolved)
	if err != nil || !resolvedInfo.Mode().IsRegular() {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q must be a regular file", agentID, performance.Module)
	}
	source, err := os.ReadFile(resolved)
	if err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q cannot be read: %w", agentID, performance.Module, err)
	}
	sum := sha256.Sum256(source)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pythonmodule.ValidateSource(ctx, pythonmodule.Request{
		ModuleID:    "agent.mock." + strings.TrimSpace(agentID),
		RowID:       strings.TrimSpace(sourceFile),
		Digest:      digest,
		Entry:       mockperformance.EntryHandle,
		Source:      source,
		MemoryPages: mockperformance.ValidationMemoryPages,
		OutputBytes: mockperformance.ValidationOutputBytes,
	}); err != nil {
		return mockperformance.Performance{}, fmt.Errorf("agent %s mock.module %q is invalid: %w", agentID, performance.Module, err)
	}
	return mockperformance.Performance{
		Kind:       mockperformance.KindPython,
		Module:     filepath.ToSlash(module),
		Source:     append([]byte(nil), source...),
		Digest:     digest,
		SourcePath: filepath.ToSlash(module),
	}, nil
}

func pathWithinRoot(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
