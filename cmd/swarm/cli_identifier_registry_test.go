package main

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

type cliIdentifierSpecRow struct {
	Command   string
	Selector  string
	Family    string
	Mode      string
	Safety    string
	ScopeRule string
}

type cliIdentifierSpecFamily struct {
	CandidateSource           string
	ScopeMode                 string
	ScopeRule                 string
	NormalizationMode         string
	NormalizationRule         string
	DisplayProjection         string
	DisplayShorteningEligible bool
}

func TestCLIIdentifierRegistryMatchesAuthoritativeSpec(t *testing.T) {
	specRows, specFamilies := cliIdentifierSpecRows(t)
	if err := validateCLIIdentifierRegistry(); err != nil {
		t.Fatalf("production identifier registry is invalid: %v", err)
	}
	productionRows := map[string]cliIdentifierInputRegistration{}
	for _, row := range cliIdentifierInputRegistry {
		key := cliIdentifierRegistryKey(row.Command, row.Selector)
		if _, ok := productionRows[key]; ok {
			t.Fatalf("duplicate production identifier row %s %s", row.Command, row.Selector)
		}
		productionRows[key] = row
	}
	if len(specRows) != len(productionRows) {
		t.Fatalf("identifier registry row count: spec=%d production=%d", len(specRows), len(productionRows))
	}
	for key, row := range productionRows {
		specRow, ok := specRows[key]
		if !ok {
			t.Errorf("production identifier row missing from spec: %s %s", row.Command, row.Selector)
			continue
		}
		if specRow.Family != string(row.Family) || specRow.Mode != string(row.Mode) || specRow.Safety != string(row.Safety) || specRow.ScopeRule != row.ScopeRule {
			t.Errorf("identifier row %s %s differs: spec=%+v production=%+v", row.Command, row.Selector, specRow, row)
		}
	}
	if len(specFamilies) != len(cliIdentifierFamilyRegistry) {
		t.Fatalf("identifier family count: spec=%d production=%d", len(specFamilies), len(cliIdentifierFamilyRegistry))
	}
	for family, policy := range cliIdentifierFamilyRegistry {
		specFamily, ok := specFamilies[string(family)]
		if !ok {
			t.Errorf("production identifier family missing from spec: %s", family)
			continue
		}
		if specFamily.CandidateSource != string(policy.CandidateSource) ||
			specFamily.ScopeMode != string(policy.ScopeMode) ||
			specFamily.ScopeRule != policy.ScopeRule ||
			specFamily.NormalizationMode != string(policy.NormalizationMode) ||
			specFamily.NormalizationRule != policy.NormalizationRule ||
			specFamily.DisplayProjection != string(policy.DisplayProjection) {
			t.Errorf("identifier family %s differs: spec=%+v production=%+v", family, specFamily, policy)
		}
		if got := cliIdentifierFamilyDisplayEligible(family); got != specFamily.DisplayShorteningEligible {
			t.Errorf("family %s display eligibility=%t, spec=%t", family, got, specFamily.DisplayShorteningEligible)
		}
	}
}

func TestCLIIdentifierRegistryRejectsUnsupportedFamilyPolicy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*cliIdentifierFamilyPolicy)
		want   string
	}{
		{
			name: "unknown candidate source",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.CandidateSource = cliIdentifierCandidateSource("unknown")
			},
			want: "unsupported source/scope/normalization combination",
		},
		{
			name: "unknown scope mode",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.ScopeMode = cliIdentifierScopeMode("unknown")
			},
			want: "unsupported source/scope/normalization combination",
		},
		{
			name: "unknown normalization mode",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.NormalizationMode = cliIdentifierNormalizationMode("unknown")
			},
			want: "unsupported source/scope/normalization combination",
		},
		{
			name: "unknown display projection",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.DisplayProjection = cliIdentifierDisplayProjection("unknown")
			},
			want: "unsupported display projection",
		},
		{
			name: "known source from another family",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.CandidateSource = cliIdentifierSourceBundleList
			},
			want: "unsupported source/scope/normalization combination",
		},
		{
			name: "known scope from another family",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.ScopeMode = cliIdentifierScopeBoundedCatalog
			},
			want: "unsupported source/scope/normalization combination",
		},
		{
			name: "known normalization from another family",
			mutate: func(policy *cliIdentifierFamilyPolicy) {
				policy.NormalizationMode = cliIdentifierNormalizeBundleDigest
			},
			want: "unsupported source/scope/normalization combination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := cliIdentifierFamilyRegistry[cliIdentifierFamilyAgent]
			mutated := original
			tt.mutate(&mutated)
			cliIdentifierFamilyRegistry[cliIdentifierFamilyAgent] = mutated
			t.Cleanup(func() { cliIdentifierFamilyRegistry[cliIdentifierFamilyAgent] = original })

			err := validateCLIIdentifierRegistry()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateCLIIdentifierRegistry() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestCLIIdentifierRegistryRejectsUnknownFamily(t *testing.T) {
	unknown := cliIdentifierFamily("unknown")
	cliIdentifierFamilyRegistry[unknown] = cliIdentifierFamilyPolicy{
		Family:            unknown,
		CandidateSource:   cliIdentifierSourceAgentList,
		ScopeMode:         cliIdentifierScopeGlobalBounded,
		ScopeRule:         "test",
		NormalizationMode: cliIdentifierNormalizeCaseSensitive,
		NormalizationRule: "test",
		DisplayProjection: cliIdentifierDisplayFull,
	}
	t.Cleanup(func() { delete(cliIdentifierFamilyRegistry, unknown) })

	err := validateCLIIdentifierRegistry()
	if err == nil || !strings.Contains(err.Error(), "unsupported family") {
		t.Fatalf("validateCLIIdentifierRegistry() error = %v, want unsupported family", err)
	}
}

func TestCLIIdentifierMatchingRejectsUnsupportedNormalizationMode(t *testing.T) {
	policy := cliIdentifierFamilyRegistry[cliIdentifierFamilyAgent]
	policy.NormalizationMode = cliIdentifierNormalizationMode("unknown")

	_, _, err := matchCLIIdentifierCandidates(policy, "agent", []cliIdentifierCandidate{{ID: "agent-one"}})
	if err == nil || !strings.Contains(err.Error(), "unsupported normalization mode") {
		t.Fatalf("matchCLIIdentifierCandidates() error = %v, want unsupported normalization mode", err)
	}
}

func TestCLIIdentifierRegistryCoversVisiblePositionalsAndStringFlags(t *testing.T) {
	paths := visibleCLICommandPaths(t)
	for command, cmd := range paths {
		for _, selector := range cliUsePositionals(cmd.Use) {
			key := cliIdentifierRegistryKey(command, "arg:"+selector)
			if cliIdentifierStructuralRowCovers(command, "arg:"+selector) || cliIdentifierNonResourcePositionals[key] {
				continue
			}
			t.Errorf("%s positional %q is not classified by the identifier registry or explicit non-resource ledger", command, selector)
		}
		cmd.Flags().VisitAll(func(flag *pflag.Flag) {
			if flag.Value.Type() != "string" && flag.Value.Type() != "stringSlice" && flag.Value.Type() != "stringArray" {
				return
			}
			selector := "flag:" + flag.Name
			key := cliIdentifierRegistryKey(command, selector)
			if cliIdentifierStructuralRowCovers(command, selector) || cliIdentifierGlobalNonResourceStringFlags[flag.Name] || cliIdentifierNonResourceStringFlags[key] {
				return
			}
			t.Errorf("%s --%s (%s) is not classified by the identifier registry or explicit non-resource ledger", command, flag.Name, flag.Value.Type())
		})
	}
}

func TestCLIIdentifierRegistryRowsReferToLiveInputs(t *testing.T) {
	paths := visibleCLICommandPaths(t)
	assertLive := func(command, selector, ledger string) {
		t.Helper()
		if strings.Contains(command, "<") {
			return
		}
		cmd, ok := paths[command]
		if !ok {
			t.Errorf("%s row refers to missing command %s", ledger, command)
			return
		}
		switch {
		case strings.HasPrefix(selector, "arg:"):
			name := strings.TrimPrefix(selector, "arg:")
			for _, positional := range cliUsePositionals(cmd.Use) {
				if positional == name {
					return
				}
			}
			t.Errorf("%s row refers to missing positional %s %s", ledger, command, selector)
		case strings.HasPrefix(selector, "flag:"):
			if cmd.Flags().Lookup(strings.TrimPrefix(selector, "flag:")) == nil {
				t.Errorf("%s row refers to missing flag %s %s", ledger, command, selector)
			}
		default:
			t.Errorf("%s row has unsupported selector %q", ledger, selector)
		}
	}

	for _, row := range cliIdentifierInputRegistry {
		assertLive(row.Command, row.Selector, "identifier registry")
	}
	for key := range cliIdentifierNonResourcePositionals {
		parts := strings.SplitN(key, "\x00", 2)
		assertLive(parts[0], parts[1], "non-resource positional ledger")
	}
	for key := range cliIdentifierNonResourceStringFlags {
		parts := strings.SplitN(key, "\x00", 2)
		assertLive(parts[0], parts[1], "non-resource flag ledger")
	}
}

func TestCLIIdentifierResolverCallsitesUseRegisteredReadRows(t *testing.T) {
	count := 0
	for _, path := range productionCLIGoFiles(t) {
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || (name.Name != "resolveCLIIdentifier" && name.Name != "resolveCLIIdentifierAfterNotFound") || len(call.Args) < 3 {
				return true
			}
			request, ok := call.Args[2].(*ast.CompositeLit)
			if !ok {
				return true // shared helper forwarding its already-checked request
			}
			command, selector := cliIdentifierRequestLiteral(request)
			if command == "" || selector == "" {
				t.Errorf("%s resolver call must use literal Command and Selector", fileSet.Position(call.Pos()))
				return true
			}
			row, ok := cliIdentifierRegistration(command, selector)
			if !ok {
				t.Errorf("%s resolver call is not registered: %s %s", fileSet.Position(call.Pos()), command, selector)
				return true
			}
			if row.Mode != cliIdentifierModeResolverBounded && row.Mode != cliIdentifierModeResolverScoped {
				t.Errorf("%s resolver call uses non-resolver mode %s: %s %s", fileSet.Position(call.Pos()), row.Mode, command, selector)
			}
			if row.Safety != "" {
				t.Errorf("%s resolver call uses safety-classified row %s: %s %s", fileSet.Position(call.Pos()), row.Safety, command, selector)
			}
			count++
			return true
		})
	}
	if count != 6 {
		t.Fatalf("literal production resolver callsites=%d, want 6", count)
	}
}

func TestCLIIdentifierDisplayColumnsUseFamilyAwareOwner(t *testing.T) {
	expected := map[string]cliIdentifierFamily{
		"agents.go\x00AGENT_ID":            cliIdentifierFamilyAgent,
		"agents.go\x00EVENT ID":            cliIdentifierFamilyEvent,
		"agents.go\x00RUN":                 cliIdentifierFamilyRun,
		"agents.go\x00ENTITY":              cliIdentifierFamilyEntity,
		"bundle.go\x00BUNDLE":              cliIdentifierFamilyBundle,
		"bundle.go\x00AGENT":               cliIdentifierFamilyAgent,
		"bundle.go\x00FLOW_INSTANCE":       cliIdentifierFamilyFlowInstance,
		"context_command.go\x00NAME":       cliIdentifierFamilyContext,
		"control_mailbox.go\x00MAILBOX_ID": cliIdentifierFamilyMailbox,
		"conversations.go\x00SESSION_ID":   cliIdentifierFamilySession,
		"conversations.go\x00AGENT":        cliIdentifierFamilyAgent,
		"conversations.go\x00RUN":          cliIdentifierFamilyRun,
		"conversations.go\x00EVENT_ID":     cliIdentifierFamilyEvent,
		"diagnostics.go\x00RUN ID":         cliIdentifierFamilyRun,
		"diagnostics.go\x00ID":             cliIdentifierFamilyEvent,
		"diagnostics.go\x00SUBSCRIBER":     cliIdentifierFamilySubscriber,
		"diagnostics.go\x00SESSION":        cliIdentifierFamilySession,
		"diagnostics.go\x00EVENT ID":       cliIdentifierFamilyEvent,
		"entities.go\x00ENTITY_ID":         cliIdentifierFamilyEntity,
		"entities.go\x00RUN_ID":            cliIdentifierFamilyRun,
		"entities.go\x00FLOW":              cliIdentifierFamilyFlowInstance,
		"events.go\x00EVENT ID":            cliIdentifierFamilyEvent,
		"events.go\x00RUN":                 cliIdentifierFamilyRun,
		"events.go\x00ENTITY":              cliIdentifierFamilyEntity,
		"forkchat.go\x00FORK_ID":           cliIdentifierFamilyFork,
		"forkchat.go\x00SOURCE_SESSION":    cliIdentifierFamilySession,
		"forkchat.go\x00SOURCE_AGENT":      cliIdentifierFamilyAgent,
		"logs.go\x00RUN":                   cliIdentifierFamilyRun,
		"logs.go\x00ENTITY":                cliIdentifierFamilyEntity,
		"logs.go\x00SESSION":               cliIdentifierFamilySession,
	}
	seen := map[string]int{}
	keyColumnExceptions := map[string]bool{
		"agents.go\x00DELIVERY ID":      true,
		"connections.go\x00KEY":         true,
		"conversations.go\x00TURN_ID":   true,
		"diagnostics.go\x00DELIVERY ID": true,
		"forkchat.go\x00TURN_ID":        true,
		"incidents.go\x00INCIDENT ID":   true,
		"secrets.go\x00KEY":             true,
	}

	for _, path := range productionCLIGoFiles(t) {
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		base := filepath.Base(path)
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			header, family, keyColumn := cliIdentifierTableColumnLiteral(literal)
			if header == "" {
				return true
			}
			key := base + "\x00" + header
			if want, ok := expected[key]; ok {
				seen[key]++
				if family != want {
					t.Errorf("%s table column %q family=%s, want %s", base, header, family, want)
				}
			} else if family != cliIdentifierFamilyNone {
				t.Errorf("%s table column %q declares unaccounted identifier family %s", base, header, family)
			}
			if keyColumn && family == cliIdentifierFamilyNone && cliIdentifierLikeHeader(header) && !keyColumnExceptions[key] {
				t.Errorf("%s key column %q must declare IdentifierFamily or an explicit non-resource/result-only exception", base, header)
			}
			return true
		})
	}
	for key := range expected {
		if seen[key] == 0 {
			t.Errorf("expected identifier display column %q was not found", key)
		}
	}
}

func TestCLIIdentifierDisplayHasNoDirectShortSliceBypass(t *testing.T) {
	for _, path := range productionCLIGoFiles(t) {
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			slice, ok := node.(*ast.SliceExpr)
			if !ok || slice.High == nil || !cliIdentifierLikeExpression(slice.X) {
				return true
			}
			high, ok := slice.High.(*ast.BasicLit)
			if !ok || high.Kind != token.INT {
				return true
			}
			limit, err := strconv.Atoi(high.Value)
			if err == nil && limit <= 16 {
				t.Errorf("%s directly short-slices an identifier-like expression; route display through formatCLIIdentifierForDisplay", fileSet.Position(slice.Pos()))
			}
			return true
		})
	}
}

func TestCLIIdentifierSafetyRowsCannotBecomeResolverBacked(t *testing.T) {
	original := cliIdentifierInputRegistry
	defer func() { cliIdentifierInputRegistry = original }()
	cliIdentifierInputRegistry = append(append([]cliIdentifierInputRegistration{}, original...), cliIdentifierInputRegistration{
		Command: "swarm unsafe-test", Selector: "arg:agent-id", Family: cliIdentifierFamilyAgent,
		Mode: cliIdentifierModeResolverBounded, Safety: "mutating",
	})
	if err := validateCLIIdentifierRegistry(); err == nil || !strings.Contains(err.Error(), "must be full_only") {
		t.Fatalf("unsafe resolver registry validation error=%v", err)
	}
	if cliIdentifierFamilyDisplayEligible(cliIdentifierFamilyAgent) {
		t.Fatal("invalid safety row must fail display eligibility closed")
	}
	if got := formatCLIIdentifierForDisplay(cliIdentifierFamilyAgent, "agent-alpha"); got != "agent-alpha" {
		t.Fatalf("invalid registry display=%q, want full identifier", got)
	}
}

func TestCLIIdentifierResolverRejectsFullOnlyAndSplitRows(t *testing.T) {
	for _, request := range []cliIdentifierResolveRequest{
		{Command: "swarm agent restart", Selector: "arg:agent-id", Value: "agent-a"},
		{Command: "swarm forkchat view", Selector: "arg:fork-id", Value: "fork-a"},
	} {
		if _, err := resolveCLIIdentifier(context.Background(), nil, request); err == nil || !strings.Contains(err.Error(), "is not resolver-backed") {
			t.Errorf("%s %s error=%v, want non-resolver rejection", request.Command, request.Selector, err)
		}
	}
}

func TestCLIIdentifierFamiliesWithFullOrSplitRowsCannotShorten(t *testing.T) {
	for family := range cliIdentifierFamilyRegistry {
		if cliIdentifierFamilyDisplayEligible(family) {
			t.Errorf("family %s unexpectedly permits short display", family)
		}
	}
}

func TestCLIIdentifierGateFoundSurfacesRemainFullOnly(t *testing.T) {
	tests := []struct {
		command  string
		selector string
	}{
		{command: "swarm serve", selector: "flag:bundle-hash"},
		{command: "swarm event publish", selector: "flag:bundle-hash"},
		{command: "swarm event publish", selector: "flag:bundle-fingerprint"},
		{command: "swarm event publish", selector: "flag:run-id"},
		{command: "swarm event publish", selector: "flag:source-event-id"},
		{command: "swarm event publish", selector: "flag:target-entity-id"},
		{command: "swarm event publish", selector: "flag:target-flow-instance"},
		{command: "swarm agent directive", selector: "arg:agent-id"},
		{command: "swarm agent directive", selector: "flag:run-id"},
		{command: "swarm logs", selector: "flag:run-id"},
		{command: "swarm logs", selector: "flag:entity-id"},
		{command: "swarm logs", selector: "flag:session-id"},
	}

	for _, test := range tests {
		row, ok := cliIdentifierRegistration(test.command, test.selector)
		if !ok {
			t.Errorf("%s %s is not registered", test.command, test.selector)
			continue
		}
		if row.Mode != cliIdentifierModeFullOnly {
			t.Errorf("%s %s mode=%s, want %s", test.command, test.selector, row.Mode, cliIdentifierModeFullOnly)
		}
		if cliIdentifierFamilyDisplayEligible(row.Family) {
			t.Errorf("%s %s family %s unexpectedly permits display shortening", test.command, test.selector, row.Family)
		}
	}
}

func TestCLIIdentifierAgentPrefixResolvesAfterExactMiss(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		methods = append(methods, request.Method)
		switch request.Method {
		case "agent.get":
			if request.Params["agent_id"] == "agent-al" {
				writeIdentifierRPCError(t, w, request.ID, "AGENT_NOT_FOUND")
				return
			}
			writeJSONRPCResult(t, w, request.ID, map[string]any{
				"agent": agentSummaryResult("agent-alpha", "reviewer", "running"),
			})
		case "agent.list":
			writeJSONRPCResult(t, w, request.ID, map[string]any{
				"agents": []map[string]any{
					agentSummaryResult("agent-alpha", "reviewer", "running"),
					agentSummaryResult("agent-beta", "researcher", "idle"),
				},
			})
		default:
			t.Fatalf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "view", "agent-al"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got, want := strings.Join(methods, ","), "agent.get,agent.list,agent.get"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
	if !strings.Contains(stdout.String(), "Agent agent-alpha") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestCLIIdentifierAdditionalAgentReadConsumersResolvePrefix(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		targetMethod string
		result       func() map[string]any
	}{
		{name: "diagnose", args: []string{"agent", "diagnose", "agent-o"}, targetMethod: "agent.diagnose", result: validAgentDiagnosisResult},
		{name: "deliveries", args: []string{"agent", "deliveries", "agent-o"}, targetMethod: "agent.delivery_lifecycle", result: validAgentDeliveryLifecycleResult},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			methods := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&request)
				methods = append(methods, request.Method)
				switch request.Method {
				case test.targetMethod:
					if request.Params["agent_id"] == "agent-o" {
						writeIdentifierRPCError(t, w, request.ID, "AGENT_NOT_FOUND")
						return
					}
					if request.Params["agent_id"] != "agent-one" {
						t.Fatalf("agent_id=%v, want agent-one", request.Params["agent_id"])
					}
					result := test.result()
					result["agent_id"] = "agent-one"
					writeJSONRPCResult(t, w, request.ID, result)
				case "agent.list":
					writeJSONRPCResult(t, w, request.ID, map[string]any{"agents": []map[string]any{agentSummaryResult("agent-one", "reviewer", "running")}})
				default:
					t.Fatalf("unexpected method %q", request.Method)
				}
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), test.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			want := test.targetMethod + ",agent.list," + test.targetMethod
			if got := strings.Join(methods, ","); got != want {
				t.Fatalf("methods=%s, want %s", got, want)
			}
		})
	}
}

func TestCLIIdentifierNoMatchTeachesDiscoveryAndStops(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		switch request.Method {
		case "agent.get":
			writeIdentifierRPCError(t, w, request.ID, "AGENT_NOT_FOUND")
		case "agent.list":
			writeJSONRPCResult(t, w, request.ID, map[string]any{"agents": []map[string]any{agentSummaryResult("agent-alpha", "reviewer", "running")}})
		default:
			t.Fatalf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "view", "missing"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitValidation {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	for _, want := range []string{`no agent ID matches "missing"`, "`swarm agent list`"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, stderr.String())
		}
	}
	if got, want := strings.Join(methods, ","), "agent.get,agent.list"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierCandidateListFailureStopsBeforeRetry(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		switch request.Method {
		case "agent.get":
			writeIdentifierRPCError(t, w, request.ID, "AGENT_NOT_FOUND")
		case "agent.list":
			writeIdentifierRPCError(t, w, request.ID, "RUNTIME_UNAVAILABLE")
		default:
			t.Fatalf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "view", "agent-o"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitRuntime {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if got, want := strings.Join(methods, ","), "agent.get,agent.list"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierAmbiguityListsCandidatesAndStops(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		if request.Method == "agent.get" {
			writeIdentifierRPCError(t, w, request.ID, "AGENT_NOT_FOUND")
			return
		}
		writeJSONRPCResult(t, w, request.ID, map[string]any{
			"agents": []map[string]any{
				agentSummaryResult("agent-alpha", "reviewer", "running"),
				agentSummaryResult("agent-alpine", "reviewer", "idle"),
			},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"agent", "view", "agent-al"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitValidation {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	for _, want := range []string{"prefix \"agent-al\" is ambiguous", "agent-alpha", "status=running", "agent-alpine", "status=idle"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q: %s", want, stderr.String())
		}
	}
	if got, want := strings.Join(methods, ","), "agent.get,agent.list"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierBundleDigestPrefixPagesToCompletion(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	wantHash := validBundleHash("a")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		switch request.Method {
		case bundleListMethod:
			if request.Params["cursor"] == nil {
				writeJSONRPCResult(t, w, request.ID, map[string]any{"bundles": []map[string]any{validBundleSummary(validBundleHash("b"))}, "next_cursor": "page-2"})
				return
			}
			writeJSONRPCResult(t, w, request.ID, map[string]any{"bundles": []map[string]any{validBundleSummary(wantHash)}})
		case bundleGetMethod:
			if request.Params["bundle_hash"] != wantHash {
				t.Fatalf("bundle_hash=%v, want %s", request.Params["bundle_hash"], wantHash)
			}
			writeJSONRPCResult(t, w, request.ID, validBundleDetail(wantHash))
		default:
			t.Fatalf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "show", "AAAA"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got, want := strings.Join(methods, ","), "bundle.list,bundle.list,bundle.get"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierBundleAcceptsRegisteredPrefixForms(t *testing.T) {
	for _, input := range []string{"bundle-v1:sha256:CCCC", "sha256:CCCC"} {
		t.Run(input, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			wantHash := validBundleHash("c")
			methods := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&request)
				methods = append(methods, request.Method)
				switch request.Method {
				case bundleListMethod:
					writeJSONRPCResult(t, w, request.ID, map[string]any{"bundles": []map[string]any{validBundleSummary(wantHash)}})
				case bundleGetMethod:
					if request.Params["bundle_hash"] != wantHash {
						t.Fatalf("bundle_hash=%v, want %s", request.Params["bundle_hash"], wantHash)
					}
					writeJSONRPCResult(t, w, request.ID, validBundleDetail(wantHash))
				default:
					t.Fatalf("unexpected method %q", request.Method)
				}
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "show", input}, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 {
				t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if got, want := strings.Join(methods, ","), "bundle.list,bundle.get"; got != want {
				t.Fatalf("methods=%s, want %s", got, want)
			}
		})
	}
}

func TestCLIIdentifierBundleAgentsConsumesResolvedHash(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	wantHash := validBundleHash("c")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		switch request.Method {
		case bundleListMethod:
			writeJSONRPCResult(t, w, request.ID, map[string]any{"bundles": []map[string]any{validBundleSummary(wantHash)}})
		case bundleAgentsMethod:
			if request.Params["bundle_hash"] != wantHash {
				t.Fatalf("bundle_hash=%v, want %s", request.Params["bundle_hash"], wantHash)
			}
			writeJSONRPCResult(t, w, request.ID, map[string]any{"agents": []map[string]any{validBundleAgent("agent-alpha")}})
		default:
			t.Fatalf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "agents", "cccc"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got, want := strings.Join(methods, ","), "bundle.list,bundle.agents"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierBundlePagingFailsClosedOnRepeatedCursor(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		if request.Method != bundleListMethod {
			t.Fatalf("unexpected method %q", request.Method)
		}
		writeJSONRPCResult(t, w, request.ID, map[string]any{
			"bundles":     []map[string]any{validBundleSummary(validBundleHash("b"))},
			"next_cursor": "same-page",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"bundle", "show", "aaaa"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitRuntime {
		t.Fatalf("code=%d, want %d; stderr=%s", code, cliExitRuntime, stderr.String())
	}
	if !strings.Contains(stderr.String(), `repeated next_cursor "same-page"`) {
		t.Fatalf("stderr=%q", stderr.String())
	}
	if got, want := strings.Join(methods, ","), "bundle.list,bundle.list"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierScopedEntityPrefixUsesRunScope(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		switch request.Method {
		case entityGetMethod:
			if request.Params["entity_id"] == "entity-al" {
				writeIdentifierRPCError(t, w, request.ID, "ENTITY_NOT_FOUND")
				return
			}
			writeJSONRPCResult(t, w, request.ID, validEntityFullResult("entity-alpha"))
		case entityListMethod:
			if request.Params["run_id"] != "run-full" {
				t.Fatalf("entity.list run_id=%v", request.Params["run_id"])
			}
			if request.Params["cursor"] == nil {
				writeJSONRPCResult(t, w, request.ID, map[string]any{
					"entities":    []map[string]any{validEntitySummary("entity-beta")},
					"next_cursor": "page-2",
				})
				return
			}
			writeJSONRPCResult(t, w, request.ID, map[string]any{"entities": []map[string]any{validEntitySummary("entity-alpha")}})
		default:
			t.Fatalf("unexpected method %q", request.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-al", "--run-id", "run-full"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got, want := strings.Join(methods, ","), "entity.get,entity.list,entity.list,entity.get"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func TestCLIIdentifierUnscopedEntityDoesNotEnumerate(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		methods = append(methods, request.Method)
		if request.Method != entityGetMethod {
			t.Fatalf("unexpected method %q", request.Method)
		}
		writeIdentifierRPCError(t, w, request.ID, "ENTITY_NOT_FOUND")
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-al"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != cliExitNotFound {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if got, want := strings.Join(methods, ","), "entity.get"; got != want {
		t.Fatalf("methods=%s, want %s", got, want)
	}
}

func cliIdentifierSpecRows(t *testing.T) (map[string]cliIdentifierSpecRow, map[string]cliIdentifierSpecFamily) {
	t.Helper()
	spec := loadCLISpecification(t)
	identifierResolution := driftMappingValue(driftMappingValue(driftMappingValue(spec, "foundations"), "output_contract"), "identifier_resolution")
	if identifierResolution == nil {
		t.Fatal("output_contract.identifier_resolution is missing")
	}
	rows := driftMappingValue(identifierResolution, "input_rows")
	if rows == nil || rows.Kind != yaml.MappingNode {
		t.Fatal("identifier_resolution.input_rows is missing")
	}
	out := map[string]cliIdentifierSpecRow{}
	for i := 0; i+1 < len(rows.Content); i += 2 {
		node := rows.Content[i+1]
		row := cliIdentifierSpecRow{
			Command: cliOutputRegistryScalar(node, "command"), Selector: cliOutputRegistryScalar(node, "selector"),
			Family: cliOutputRegistryScalar(node, "family"), Mode: cliOutputRegistryScalar(node, "mode"),
			Safety: cliOutputRegistryScalar(node, "safety"), ScopeRule: cliOutputRegistryScalar(node, "scope_rule"),
		}
		key := cliIdentifierRegistryKey(row.Command, row.Selector)
		if _, ok := out[key]; ok {
			t.Fatalf("duplicate spec identifier row %s %s", row.Command, row.Selector)
		}
		out[key] = row
	}
	specFamilies := map[string]cliIdentifierSpecFamily{}
	families := driftMappingValue(identifierResolution, "family_registry")
	for i := 0; i+1 < len(families.Content); i += 2 {
		family := families.Content[i].Value
		node := families.Content[i+1]
		specFamilies[family] = cliIdentifierSpecFamily{
			CandidateSource:           cliOutputRegistryScalar(node, "candidate_source"),
			ScopeMode:                 cliOutputRegistryScalar(node, "scope_mode"),
			ScopeRule:                 cliOutputRegistryScalar(node, "scope_rule"),
			NormalizationMode:         cliOutputRegistryScalar(node, "normalization_mode"),
			NormalizationRule:         cliOutputRegistryScalar(node, "normalization"),
			DisplayProjection:         cliOutputRegistryScalar(node, "display_projection"),
			DisplayShorteningEligible: cliOutputRegistryScalar(node, "display_shortening_eligible") == "true",
		}
	}
	return out, specFamilies
}

var cliUseAnglePositionals = regexp.MustCompile(`<([a-z0-9-|]+)>`)
var cliUseBarePositionals = regexp.MustCompile(`\[([a-z0-9-]+)(?: \.\.\.)?\]`)

func cliUsePositionals(use string) []string {
	first := strings.Fields(use)
	if len(first) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, match := range cliUseAnglePositionals.FindAllStringSubmatchIndex(use, -1) {
		name := use[match[2]:match[3]]
		before := strings.Fields(strings.TrimSpace(use[:match[0]]))
		if strings.Contains(name, "|") || (len(before) > 0 && strings.Contains(before[len(before)-1], "--")) {
			continue
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	for _, match := range cliUseBarePositionals.FindAllStringSubmatch(use, -1) {
		name := match[1]
		if strings.HasPrefix(name, "--") {
			continue
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func cliIdentifierStructuralRowCovers(command, selector string) bool {
	if _, ok := cliIdentifierRegistration(command, selector); ok {
		return true
	}
	if selector == "flag:context" && command != "swarm serve" {
		_, ok := cliIdentifierRegistration("swarm <api-backed>", selector)
		return ok
	}
	if selector == "flag:idempotency-key" {
		_, ok := cliIdentifierRegistration("swarm <mutating>", selector)
		return ok
	}
	if selector == "arg:key" && strings.HasPrefix(command, "swarm connections ") {
		_, ok := cliIdentifierRegistration("swarm connections <key>", selector)
		return ok
	}
	if selector == "arg:key" && strings.HasPrefix(command, "swarm secrets ") {
		_, ok := cliIdentifierRegistration("swarm secrets <key>", selector)
		return ok
	}
	return false
}

var cliIdentifierNonResourcePositionals = map[string]bool{
	cliIdentifierRegistryKey("swarm agent directive", "arg:message"):                    true,
	cliIdentifierRegistryKey("swarm bundle register", "arg:registration-envelope-yaml"): true,
	cliIdentifierRegistryKey("swarm conversation turn", "arg:turn-index"):               true,
	cliIdentifierRegistryKey("swarm event publish", "arg:event-name"):                   true,
	cliIdentifierRegistryKey("swarm help", "arg:command"):                               true,
	cliIdentifierRegistryKey("swarm incidents", "arg:filters"):                          true,
	cliIdentifierRegistryKey("swarm logs", "arg:filters"):                               true,
	cliIdentifierRegistryKey("swarm test", "arg:scenario-file"):                         true,
}

var cliIdentifierGlobalNonResourceStringFlags = map[string]bool{
	"api-server": true, "api-token-file": true, "config": true, "contracts": true,
	"data": true, "log-level": true, "platform-spec": true,
}

var cliIdentifierNonResourceStringFlags = map[string]bool{
	cliIdentifierRegistryKey("swarm agent deliveries", "flag:cursor"):               true,
	cliIdentifierRegistryKey("swarm agent deliveries", "flag:delivery-status"):      true,
	cliIdentifierRegistryKey("swarm agent diagnose", "flag:queue-cursor"):           true,
	cliIdentifierRegistryKey("swarm agent list", "flag:role"):                       true,
	cliIdentifierRegistryKey("swarm bundle build", "flag:output"):                   true,
	cliIdentifierRegistryKey("swarm bundle build", "flag:report"):                   true,
	cliIdentifierRegistryKey("swarm bundle list", "flag:cursor"):                    true,
	cliIdentifierRegistryKey("swarm bundle register", "flag:data-blob"):             true,
	cliIdentifierRegistryKey("swarm connections callback", "flag:state"):            true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:account"):           true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:api-base-url"):      true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:auth-url"):          true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:grant"):             true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:grant-model"):       true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:provider"):          true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:redirect-url"):      true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:scope"):             true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:token-body"):        true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:token-client-auth"): true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:token-header"):      true,
	cliIdentifierRegistryKey("swarm connections connect", "flag:token-url"):         true,
	cliIdentifierRegistryKey("swarm conversation list", "flag:cursor"):              true,
	cliIdentifierRegistryKey("swarm doctor", "flag:api-listen-addr"):                true,
	cliIdentifierRegistryKey("swarm doctor", "flag:backend"):                        true,
	cliIdentifierRegistryKey("swarm doctor", "flag:mcp-listen-addr"):                true,
	cliIdentifierRegistryKey("swarm doctor", "flag:workspace-backend"):              true,
	cliIdentifierRegistryKey("swarm entity aggregate", "flag:group-by"):             true,
	cliIdentifierRegistryKey("swarm entity aggregate", "flag:type"):                 true,
	cliIdentifierRegistryKey("swarm entity list", "flag:current-state"):             true,
	cliIdentifierRegistryKey("swarm entity list", "flag:cursor"):                    true,
	cliIdentifierRegistryKey("swarm entity list", "flag:type"):                      true,
	cliIdentifierRegistryKey("swarm event follow", "flag:delivery-status"):          true,
	cliIdentifierRegistryKey("swarm event follow", "flag:event-name"):               true,
	cliIdentifierRegistryKey("swarm event follow", "flag:reason-code"):              true,
	cliIdentifierRegistryKey("swarm event follow", "flag:replay-since"):             true,
	cliIdentifierRegistryKey("swarm event follow", "flag:subscriber-type"):          true,
	cliIdentifierRegistryKey("swarm event list", "flag:cursor"):                     true,
	cliIdentifierRegistryKey("swarm event list", "flag:delivery-status"):            true,
	cliIdentifierRegistryKey("swarm event list", "flag:event-name"):                 true,
	cliIdentifierRegistryKey("swarm event list", "flag:reason-code"):                true,
	cliIdentifierRegistryKey("swarm event list", "flag:since"):                      true,
	cliIdentifierRegistryKey("swarm event list", "flag:subscriber-type"):            true,
	cliIdentifierRegistryKey("swarm event list", "flag:until"):                      true,
	cliIdentifierRegistryKey("swarm event publish", "flag:payload-json"):            true,
	cliIdentifierRegistryKey("swarm forkchat list", "flag:cursor"):                  true,
	cliIdentifierRegistryKey("swarm forkchat new", "flag:at"):                       true,
	cliIdentifierRegistryKey("swarm forkchat new", "flag:message"):                  true,
	cliIdentifierRegistryKey("swarm forkchat resume", "flag:message"):               true,
	cliIdentifierRegistryKey("swarm incidents", "flag:component"):                   true,
	cliIdentifierRegistryKey("swarm incidents", "flag:cursor"):                      true,
	cliIdentifierRegistryKey("swarm incidents", "flag:level"):                       true,
	cliIdentifierRegistryKey("swarm logs", "flag:component"):                        true,
	cliIdentifierRegistryKey("swarm logs", "flag:cursor"):                           true,
	cliIdentifierRegistryKey("swarm logs", "flag:error-code"):                       true,
	cliIdentifierRegistryKey("swarm logs", "flag:level"):                            true,
	cliIdentifierRegistryKey("swarm logs", "flag:order"):                            true,
	cliIdentifierRegistryKey("swarm logs", "flag:replay-since"):                     true,
	cliIdentifierRegistryKey("swarm logs", "flag:since"):                            true,
	cliIdentifierRegistryKey("swarm logs", "flag:source"):                           true,
	cliIdentifierRegistryKey("swarm logs", "flag:until"):                            true,
	cliIdentifierRegistryKey("swarm mailbox defer", "flag:until"):                   true,
	cliIdentifierRegistryKey("swarm mailbox list", "flag:cursor"):                   true,
	cliIdentifierRegistryKey("swarm mailbox list", "flag:priority"):                 true,
	cliIdentifierRegistryKey("swarm mailbox list", "flag:status"):                   true,
	cliIdentifierRegistryKey("swarm mailbox list", "flag:type"):                     true,
	cliIdentifierRegistryKey("swarm run list", "flag:cursor"):                       true,
	cliIdentifierRegistryKey("swarm run list", "flag:since"):                        true,
	cliIdentifierRegistryKey("swarm run list", "flag:status"):                       true,
	cliIdentifierRegistryKey("swarm run list", "flag:until"):                        true,
	cliIdentifierRegistryKey("swarm run start", "flag:backend"):                     true,
	cliIdentifierRegistryKey("swarm run start", "flag:connect"):                     true,
	cliIdentifierRegistryKey("swarm run start", "flag:event"):                       true,
	cliIdentifierRegistryKey("swarm run start", "flag:payload"):                     true,
	cliIdentifierRegistryKey("swarm run trace", "flag:cursor"):                      true,
	cliIdentifierRegistryKey("swarm run trace", "flag:delivery-status"):             true,
	cliIdentifierRegistryKey("swarm run trace", "flag:event-name"):                  true,
	cliIdentifierRegistryKey("swarm run trace", "flag:since"):                       true,
	cliIdentifierRegistryKey("swarm run trace", "flag:subscriber-type"):             true,
	cliIdentifierRegistryKey("swarm run trace", "flag:until"):                       true,
	cliIdentifierRegistryKey("swarm secrets list", "flag:source"):                   true,
	cliIdentifierRegistryKey("swarm serve", "flag:api-listen-addr"):                 true,
	cliIdentifierRegistryKey("swarm serve", "flag:backend"):                         true,
	cliIdentifierRegistryKey("swarm serve", "flag:mcp-listen-addr"):                 true,
	cliIdentifierRegistryKey("swarm serve", "flag:store"):                           true,
	cliIdentifierRegistryKey("swarm serve", "flag:workspace-backend"):               true,
	cliIdentifierRegistryKey("swarm workspace build", "flag:backend"):               true,
	cliIdentifierRegistryKey("swarm workspace build", "flag:docker-bin"):            true,
	cliIdentifierRegistryKey("swarm workspace build", "flag:image"):                 true,
}

func writeIdentifierRPCError(t *testing.T, w http.ResponseWriter, id, code string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": -32010, "message": "application error", "data": map[string]any{"code": code}},
	}); err != nil {
		t.Fatalf("write error response: %v", err)
	}
}

func productionCLIGoFiles(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(repoRoot(), "cmd", "swarm", "*.go"))
	if err != nil {
		t.Fatalf("glob production CLI files: %v", err)
	}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if !strings.HasSuffix(path, "_test.go") {
			out = append(out, path)
		}
	}
	return out
}

func cliIdentifierRequestLiteral(literal *ast.CompositeLit) (string, string) {
	values := map[string]string{}
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		name, ok := field.Key.(*ast.Ident)
		if !ok || (name.Name != "Command" && name.Name != "Selector") {
			continue
		}
		value, ok := field.Value.(*ast.BasicLit)
		if !ok || value.Kind != token.STRING {
			continue
		}
		values[name.Name], _ = strconv.Unquote(value.Value)
	}
	return values["Command"], values["Selector"]
}

func cliIdentifierTableColumnLiteral(literal *ast.CompositeLit) (string, cliIdentifierFamily, bool) {
	header := ""
	family := cliIdentifierFamilyNone
	keyColumn := false
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		name, ok := field.Key.(*ast.Ident)
		if !ok {
			continue
		}
		switch name.Name {
		case "Header":
			value, ok := field.Value.(*ast.BasicLit)
			if ok && value.Kind == token.STRING {
				header, _ = strconv.Unquote(value.Value)
			}
		case "IdentifierFamily":
			value, ok := field.Value.(*ast.Ident)
			if ok {
				family = cliIdentifierFamily(strings.TrimPrefix(value.Name, "cliIdentifierFamily"))
				family = cliIdentifierFamily(strings.ToLower(string(family)))
				if family == "flowinstance" {
					family = cliIdentifierFamilyFlowInstance
				}
			}
		case "KeyColumn":
			value, ok := field.Value.(*ast.Ident)
			keyColumn = ok && value.Name == "true"
		}
	}
	return header, family, keyColumn
}

func cliIdentifierLikeHeader(header string) bool {
	header = strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(header), "_", " "))
	return header == "ID" || header == "KEY" || header == "BUNDLE" || strings.HasSuffix(header, " ID")
}

func cliIdentifierLikeExpression(expression ast.Expr) bool {
	var names []string
	ast.Inspect(expression, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.Ident:
			names = append(names, strings.ToLower(value.Name))
		case *ast.SelectorExpr:
			names = append(names, strings.ToLower(value.Sel.Name))
		}
		return true
	})
	for _, name := range names {
		for _, marker := range []string{"id", "hash", "agent", "bundle", "context", "entity", "event", "fork", "mailbox", "run", "session", "subscriber"} {
			if strings.Contains(name, marker) {
				return true
			}
		}
	}
	return false
}
