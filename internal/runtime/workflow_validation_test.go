package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"swarm/internal/config"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func testRuntimeWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func TestEnsureWorkflowBootWiring_RejectsTouchedValidationDriftThroughSharedPath(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	cases := []struct {
		name        string
		source      semanticview.Source
		errContains string
		wantErr     bool
	}{
		{
			name: "tool resolution warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"missing_tool"}},
				}
				return semanticview.Wrap(bundle)
			}(),
			wantErr: false,
		},
		{
			name: "missing emitted event schema warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", EmitEvents: []string{"missing.event"}},
				}
				return semanticview.Wrap(bundle)
			}(),
			errContains: "'missing.event' emitted but no schema in events.yaml",
			wantErr:     true,
		},
		{
			name: "tool implementation warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
					"legacy_call": {
						HandlerType: "api_call",
					},
				}
				return semanticview.Wrap(bundle)
			}(),
			errContains: "tool implementation warnings",
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ensureWorkflowBootWiring(RuntimeOptions{
				WorkflowModule: semanticOnlyWorkflowRuntime{source: tc.source},
			})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("ensureWorkflowBootWiring error = %v, want substring %q", err, tc.errContains)
				}
			} else if err != nil {
				t.Fatalf("ensureWorkflowBootWiring error = %v, want nil", err)
			}
		})
	}
}

func TestRuntimeDepsValidateOwnsRequiredBootInputs(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	validModule := semanticOnlyWorkflowRuntime{source: semanticview.Wrap(testRuntimeWorkflowValidationBundle())}

	cases := []struct {
		name        string
		deps        RuntimeDeps
		errContains string
	}{
		{
			name:        "nil config",
			deps:        RuntimeDeps{Options: RuntimeOptions{WorkflowModule: validModule}},
			errContains: "runtime config is required",
		},
		{
			name:        "missing workflow module",
			deps:        RuntimeDeps{Config: &config.Config{}},
			errContains: "workflow contract validation failed: workflow module is required",
		},
		{
			name: "retired llm runtime mode",
			deps: RuntimeDeps{
				Config: &config.Config{
					LLM: config.LLMConfig{RuntimeMode: "cli_test"},
				},
				Options: RuntimeOptions{WorkflowModule: validModule},
			},
			errContains: "llm.runtime_mode is retired",
		},
		{
			name: "valid dependency graph",
			deps: RuntimeDeps{
				Config:  &config.Config{},
				Options: RuntimeOptions{WorkflowModule: validModule},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.deps.Validate()
			if tc.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("RuntimeDeps.Validate error = %v, want substring %q", err, tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("RuntimeDeps.Validate: %v", err)
			}
		})
	}
}

func TestRuntimeDepsValidatedDerivesCanonicalBootGraph(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	module := semanticOnlyWorkflowRuntime{source: semanticview.Wrap(testRuntimeWorkflowValidationBundle())}

	boot, err := (RuntimeDeps{
		Config: &config.Config{},
		Options: RuntimeOptions{
			WorkflowModule:    module,
			BundleFingerprint: "  fingerprint-1  ",
		},
	}).validated()
	if err != nil {
		t.Fatalf("RuntimeDeps.validated: %v", err)
	}
	if boot.Source == nil {
		t.Fatal("validated RuntimeDeps Source = nil")
	}
	if boot.PromptResolver == nil {
		t.Fatal("validated RuntimeDeps PromptResolver = nil")
	}
	if boot.Credentials == nil {
		t.Fatal("validated RuntimeDeps Credentials = nil")
	}
	if boot.Authority == nil {
		t.Fatal("validated RuntimeDeps Authority = nil")
	}
	if boot.EmitRegistry == nil {
		t.Fatal("validated RuntimeDeps EmitRegistry = nil")
	}
	if boot.TrimmedBundleFingerprint != "fingerprint-1" {
		t.Fatalf("TrimmedBundleFingerprint = %q, want fingerprint-1", boot.TrimmedBundleFingerprint)
	}
}

func TestValidateWorkflowContractSurface_AllowsExplicitEventSchemas(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", EmitEvents: []string{"ready.event"}},
		"agent-2": {ID: "agent-2", Subscriptions: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"id": {Type: "string"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface: %v", err)
	}
	if len(result.MissingEmitSchemaEventTypes) != 0 {
		t.Fatalf("MissingEmitSchemaEventTypes = %#v, want none", result.MissingEmitSchemaEventTypes)
	}
	if len(result.BootReport.Warnings()) != 0 {
		t.Fatalf("BootReport warnings = %#v, want none", result.BootReport.Warnings())
	}
}

func TestValidateWorkflowContractSurfaceRejectsInvalidGeneratedEmitToolSchema(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", Role: "agent", EmitEvents: []string{"ready.event"}},
		"agent-2": {ID: "agent-2", Role: "consumer", Subscriptions: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"unsupported": {Type: "NotDeclared"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err == nil || !strings.Contains(err.Error(), "generated_tool_schema_closure") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want boot generated schema closure failure", err)
	}
	if len(result.BootReport.Errors()) != 1 {
		t.Fatalf("BootReport errors = %#v, want one error", result.BootReport.Errors())
	}
	if got := result.BootReport.Errors()[0].Message; !strings.Contains(got, "unsupported JSON Schema type \"NotDeclared\"") {
		t.Fatalf("BootReport error = %q, want unsupported type", got)
	}
}

func TestValidateWorkflowContractSurfaceAllowsPrecisionQualifiedGeneratedEmitToolSchema(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.RootTypes = runtimecontracts.TypeCatalogDocument{
		Types: map[string]runtimecontracts.NamedTypeDecl{
			"RequiredCapabilities": {
				Fields: map[string]runtimecontracts.TypeFieldSpec{
					"automation_with_unlock": {Type: "numeric(5,2)"},
				},
			},
		},
	}
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", Role: "agent", EmitEvents: []string{"ready.event"}},
		"agent-2": {ID: "agent-2", Role: "consumer", Subscriptions: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"capabilities": {Type: "RequiredCapabilities"},
					"amounts":      {Type: "[numeric(10,2)]"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface: %v", err)
	}
	if len(result.GeneratedEmitSchemaErrors) != 0 {
		t.Fatalf("GeneratedEmitSchemaErrors = %#v, want none", result.GeneratedEmitSchemaErrors)
	}
}

func TestValidateWorkflowContractSurface_FatalToolImplementationWarningsFollowSharedOptions(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"legacy_call": {
			HandlerType: "api_call",
		},
	}
	source := semanticview.Wrap(bundle)

	_, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err == nil || !strings.Contains(err.Error(), "tool implementation warnings") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want tool implementation warning failure", err)
	}
}

func TestValidateWorkflowContractSurface_RejectsCreateEntityWithAccumulate(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadRuntimeWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate")))

	_, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want create_entity/accumulate boot error", err)
	}
}

func loadRuntimeWorkflowValidationFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	fixtureRoot := filepath.Join(repoRoot, relativeRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}
