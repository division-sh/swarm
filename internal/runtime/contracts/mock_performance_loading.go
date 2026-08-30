package contracts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

var windowsAbsoluteAgentMockModulePath = regexp.MustCompile(`^[A-Za-z]:`)

func materializeAgentMockPerformancesFromSource(artifact *sourceartifact.AdmittedSourceArtifact, source ContractItemSource, entries map[string]AgentRegistryEntry) (map[string]AgentRegistryEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	declaration := strings.TrimSpace(source.File)
	if artifact == nil || declaration == "" {
		return nil, fmt.Errorf("agent mock materialization requires an admitted artifact and exact agents.yaml label")
	}
	flowDir := path.Dir(declaration)
	if flowDir == "." {
		flowDir = ""
	}
	out := make(map[string]AgentRegistryEntry, len(entries))
	for _, key := range sortedContractKeys(entries) {
		entry := entries[key]
		performance := entry.Mock
		if !performance.Configured() {
			entry.Mock = mockperformance.Performance{}
			out[key] = entry
			continue
		}
		if strings.TrimSpace(performance.Kind) != mockperformance.KindPython {
			return nil, fmt.Errorf("agent %s mock.kind %q is unsupported; use %q", key, performance.Kind, mockperformance.KindPython)
		}
		if performance.PostToolTailLatencyMS < 0 {
			return nil, fmt.Errorf("agent %s mock.post_tool_tail_latency_ms must be non-negative", key)
		}
		module, err := validateAgentMockModulePath(performance.Module)
		if err != nil {
			return nil, fmt.Errorf("agent %s mock.module %q: %w", key, performance.Module, err)
		}
		label := path.Join(flowDir, module)
		admitted, ok := artifact.Entry(label)
		if !ok {
			return nil, fmt.Errorf("agent %s mock.module %q is not an admitted source member", key, performance.Module)
		}
		moduleSource := admitted.Bytes()
		sum := sha256.Sum256(moduleSource)
		digest := "sha256:" + hex.EncodeToString(sum[:])
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = pythonmodule.ValidateSource(ctx, pythonmodule.Request{
			ModuleID: "agent.mock." + strings.TrimSpace(key), RowID: declaration,
			Digest: digest, Entry: mockperformance.EntryHandle, Source: moduleSource,
			MemoryPages: mockperformance.ValidationMemoryPages, OutputBytes: mockperformance.ValidationOutputBytes,
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("agent %s mock.module %q is invalid: %w", key, performance.Module, err)
		}
		entry.Mock = mockperformance.Performance{
			Kind: mockperformance.KindPython, Module: module,
			PostToolTailLatencyMS: performance.PostToolTailLatencyMS,
			Source:                append([]byte(nil), moduleSource...), Digest: digest, SourcePath: label,
		}
		out[key] = entry
	}
	return out, nil
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
