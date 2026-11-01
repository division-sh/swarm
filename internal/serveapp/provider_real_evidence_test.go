package serveapp

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/servedparity"
)

const (
	realModelSHAEnv       = "SWARM_REAL_MODEL_EVIDENCE_SHA"
	realModelCellsEnv     = "SWARM_REAL_MODEL_EVIDENCE_CELLS"
	realModelTransportEnv = "SWARM_REAL_MODEL_EVIDENCE_TRANSPORT"
)

var realModelCells = []string{"memory", "tool_round", "empty_content", "refusal"}

func TestProviderDeepSeekRealModelEvidence(t *testing.T) {
	selected, reason := realModelSelection(os.Getenv, func() (string, bool) {
		head := exec.Command("git", "rev-parse", "HEAD")
		head.Dir = repoRootForTest()
		sha, err := head.Output()
		if err != nil {
			return "", false
		}
		status := exec.Command("git", "status", "--porcelain", "--untracked-files=normal")
		status.Dir = repoRootForTest()
		dirty, err := status.Output()
		return strings.TrimSpace(string(sha)), err == nil && len(dirty) == 0
	})
	if reason != "" {
		t.Skip("NOT RUN: " + reason)
	}
	// Inspection uses the same file-tier owner as the adapter, not an env key or
	// a credential copied into the disposable project.
	live := os.Getenv(realModelTransportEnv) != "mock"
	if live {
		store, err := cliapp.BuildProviderCredentialStore()
		if err != nil {
			t.Skip("NOT RUN: credential store unavailable (details withheld)")
		}
		profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
		credential, err := runtimellm.NewProviderCredentialResolver(store).Inspect(context.Background(), profile)
		if err != nil || credential.Value == "" {
			t.Skip("NOT RUN: DeepSeek credential unavailable; no mock fallback")
		}
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			for _, cell := range realModelCells {
				if selected[cell] {
					t.Run(cell, func(t *testing.T) { runProviderEvidenceJourney(t, backend, cell, live) })
				}
			}
		})
	}
}

func realModelSelection(env func(string) string, source func() (string, bool)) (map[string]bool, string) {
	if env("CI") != "" || env("GITHUB_ACTIONS") != "" {
		return nil, "real-model evidence never runs in CI"
	}
	if mode := env(realModelTransportEnv); mode != "" && mode != "mock" && mode != "deepseek" {
		return nil, "transport must be deepseek or explicitly selected mock"
	}
	requested, cells := env(realModelSHAEnv), env(realModelCellsEnv)
	if requested == "" || cells == "" {
		return nil, "reviewer-selected SHA and cells are required"
	}
	if len(requested) != 40 || strings.Trim(requested, "0123456789abcdef") != "" {
		return nil, "reviewer SHA must be an exact lowercase commit"
	}
	selected := map[string]bool{}
	for _, cell := range strings.Split(cells, ",") {
		known := false
		for _, allowed := range realModelCells {
			known = known || cell == allowed
		}
		if !known || selected[cell] {
			return nil, "cells must name distinct exact supported journeys"
		}
		selected[cell] = true
	}
	actual, clean := source()
	if actual != requested || !clean {
		return nil, "reviewer SHA must match a clean checkout"
	}
	return selected, ""
}

func TestProviderRealModelEvidenceSelection(t *testing.T) {
	const sha = "1234567890123456789012345678901234567890"
	for _, test := range []struct {
		name, requested, cells, ci string
		clean                      bool
		selected                   bool
	}{
		{"not_selected", "", "", "", true, false},
		{"sha_alone", sha, "", "", true, false},
		{"cells_alone", "", "memory", "", true, false},
		{"short_sha", sha[:10], "memory", "", true, false},
		{"unknown_cell", sha, "memory,other", "", true, false},
		{"duplicate_cell", sha, "memory,memory", "", true, false},
		{"dirty", sha, "memory", "", false, false},
		{"ci", sha, "memory", "true", true, false},
		{"explicit", sha, "memory,tool_round", "", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := map[string]string{realModelSHAEnv: test.requested, realModelCellsEnv: test.cells, "CI": test.ci}
			selected, reason := realModelSelection(func(key string) string { return env[key] }, func() (string, bool) { return sha, test.clean })
			if (reason == "") != test.selected || (test.selected && (!selected["memory"] || !selected["tool_round"])) {
				t.Fatalf("selection=%t reason=%q", reason == "", reason)
			}
		})
	}
	t.Run("github_actions", func(t *testing.T) {
		_, reason := realModelSelection(func(key string) string {
			if key == "GITHUB_ACTIONS" {
				return "true"
			}
			return ""
		}, func() (string, bool) { t.Fatal("CI admission reached source or credentials"); return "", false })
		if reason == "" {
			t.Fatal("GitHub execution was admitted")
		}
	})
	t.Run("invalid_transport", func(t *testing.T) {
		_, reason := realModelSelection(func(key string) string {
			if key == realModelTransportEnv {
				return "other"
			}
			return ""
		}, func() (string, bool) { t.Fatal("invalid transport reached source or credentials"); return "", false })
		if reason == "" {
			t.Fatal("unknown transport was admitted")
		}
	})
	t.Run("observation_is_allowlisted", func(t *testing.T) {
		o := &providerEvidenceObservation{}
		o.observe(200, []byte(`{"choices":[{"message":{"content":"","reasoning_content":"synthetic-private-reasoning","tool_calls":[{"id":"read-1","function":{"name":"read_flow_data"}}]}}],"usage":{"prompt_tokens":17,"completion_tokens":3,"cost":0.125}}`))
		o.observeRequest(strings.NewReader(`{"messages":[{"role":"assistant","content":"","reasoning_content":"synthetic-private-reasoning","tool_calls":[{"id":"read-1"}]},{"role":"tool","tool_call_id":"read-1","content":"synthetic tool result"}]}`))
		if !o.empty || !o.toolAck || !o.reasoningAck || o.input != 17 || o.output != 3 || o.usageResponses != 1 || o.cost["cost"] != 0.125 {
			t.Fatal("observation lost exact shape, continuation or usage facts")
		}
		o.observe(400, []byte("synthetic-private-provider-error"))
		if summary := o.summary(); strings.Contains(summary, "private") || !strings.Contains(summary, "0.125") {
			t.Fatal("summary leaked provider content or lost provider cost")
		}
		o.observe(429, nil)
		if !o.inconclusive {
			t.Fatal("rate limit was not inconclusive")
		}
		o.observe(200, []byte(`{"usage":{"cost_usd":null}}`))
		if _, present := o.cost["cost_usd"]; present {
			t.Fatal("null provider cost became a zero-cost claim")
		}
		missing := &providerEvidenceObservation{}
		missing.observe(400, []byte(`{"error":{"message":"synthetic refusal"}}`))
		missing.observe(200, []byte(`{"usage":{"prompt_tokens":null,"completion_tokens":null}}`))
		missing.observe(200, []byte(`{"usage":{"prompt_tokens":"unknown","completion_tokens":3}}`))
		if summary := missing.summary(); !strings.Contains(summary, "usage=NOT_REPORTED") || strings.Contains(summary, "input_tokens=0") {
			t.Fatal("absent provider usage became a zero-token claim")
		}
	})
	t.Run("observation_body_is_transparent", func(t *testing.T) {
		const raw = `{"usage":{"prompt_tokens":3,"completion_tokens":2}}`
		o := &providerEvidenceObservation{}
		body := &providerEvidenceBody{ReadCloser: io.NopCloser(strings.NewReader(raw)), observation: o, status: 200}
		observed, err := io.ReadAll(body)
		if err != nil || string(observed) != raw || o.input != 3 || o.output != 2 {
			t.Fatal("observation changed the adapter response or usage")
		}
		if err := body.Close(); err != nil {
			t.Fatal("observation changed response close")
		}
	})
	t.Run("unobserved_tool_round_is_not_exercised", func(t *testing.T) {
		o := &providerEvidenceObservation{tools: true}
		o.requireShape(t, "tool_round")
	})
}
