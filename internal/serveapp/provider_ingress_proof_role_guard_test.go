package serveapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
)

type providerIngressProofRole struct {
	disposition string
	reason      string
}

func TestProviderIngressProofRoleRegistryRejectsHiddenAuthorityFromSupportedProof(t *testing.T) {
	root := cliapp.RepoRoot()
	affected := map[string]providerIngressProofRole{
		"internal/serveapp/provider_trigger_smoke_helpers_test.go":                                {"demoted_bounded", "seeded smoke helper; capture utility only"},
		"internal/cliapp/provider_trigger_packs_test.go":                                          {"demoted_bounded", "release-layout pack integration; not standing-ingress E2E"},
		"internal/serveapp/provider_trigger_shopify_smoke_test.go":                                {"demoted_bounded", "optional live-provider smoke; not standing-ingress E2E"},
		"internal/serveapp/provider_trigger_typeform_smoke_test.go":                               {"demoted_bounded", "optional live-provider smoke; not standing-ingress E2E"},
		"internal/runtime/inbound_postgres_test.go":                                               {"demoted_bounded", "provider/store persistence integration"},
		"internal/runtime/telegram_connector_supported_surface_test.go":                           {"superseded", "served Telegram proof moved to cmd/swarm standing-ingress E2E"},
		"internal/runtime/slack_connector_managed_credential_supported_surface_test.go":           {"demoted_bounded", "connector and managed-credential integration"},
		"internal/runtime/notion_connector_managed_credential_supported_surface_test.go":          {"demoted_bounded", "connector and managed-credential integration"},
		"internal/runtime/microsoft_graph_connector_client_credentials_supported_surface_test.go": {"demoted_bounded", "connector and managed-credential integration"},
		"internal/runtime/github_app_issue_workflow_supported_surface_test.go":                    {"demoted_bounded", "GitHub workflow integration with explicit fixtures"},
		"internal/runtime/github_app_issue_comment_supported_surface_test.go":                     {"demoted_bounded", "GitHub comment integration with explicit fixtures"},
		"internal/runtime/pipeline/activity_engine_test.go":                                       {"demoted_bounded", "accepted delivery to journaled activity integration"},
	}
	if len(affected) != 12 {
		t.Fatalf("affected proof-role inventory = %d, want 12", len(affected))
	}
	for path, role := range affected {
		if strings.TrimSpace(role.disposition) == "" || strings.TrimSpace(role.reason) == "" {
			t.Fatalf("proof role for %s is incomplete: %#v", path, role)
		}
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("proof-role inventory path %s: %v", path, err)
		}
	}

	supported := map[string][]string{
		"internal/serveapp/standing_ingress_supported_surface_test.go": {
			"TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies",
			"TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies",
		},
		"internal/serveapp/builder_project_supervisor_test.go": {
			"TestRuntimeProcessInboundHandlerSelectsExactLoadedContext",
			"TestRuntimeProcessInboundHandlerTeachesUnknownStandingAlias",
		},
		"internal/runtime/standing_targets_test.go": {
			"TestInboundGatewayConsumesCompiledGitHubRouteWithoutReinterpretingDynamicPins",
		},
	}
	for path, tests := range supported {
		body, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatalf("read supported proof %s: %v", path, err)
		}
		text := string(body)
		for _, test := range tests {
			if !strings.Contains(text, "func "+test+"(") {
				t.Fatalf("supported proof %s is missing %s", path, test)
			}
		}
		for _, forbidden := range []string{
			"seedProviderTriggerSmokeRuntime",
			"test.setup_entities",
			"runtimecorrelation.WithRunID(",
			".WithContext(",
			"seedActivityRun",
			"acceptedTelegramInboundDeliveryEvent",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("supported proof %s contains hidden-authority marker %q", path, forbidden)
			}
		}
	}

	activityBody, err := os.ReadFile(filepath.Join(root, "internal/runtime/pipeline/activity_engine_test.go"))
	if err != nil {
		t.Fatalf("read bounded activity proof: %v", err)
	}
	for _, marker := range []string{"seedActivityRun", "acceptedTelegramInboundDeliveryEvent"} {
		if !strings.Contains(string(activityBody), marker) {
			t.Fatalf("bounded activity proof no longer contains tracked marker %q; update the proof-role disposition", marker)
		}
	}
}
