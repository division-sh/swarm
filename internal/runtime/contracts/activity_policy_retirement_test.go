package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestActivityEventSchemasOmitRetiredPolicy(t *testing.T) {
	site := ActivitySite{Node: identitytest.RootNode(t, "requester"), HandlerEventKey: "request", Kind: ActivitySiteHandler, RuleIndex: -1, Spec: ActivitySpec{ID: "once", Tool: "provider.http"}, RevisionField: "revision"}
	tool := MustToolSchemaEntry(WithToolSchemas(MustToolInputSchema(ToolSchemaObject), MustToolInputSchema(ToolSchemaObject)))
	for _, status := range []string{ActivityResultStatusSucceeded, ActivityResultStatusFailed} {
		entry := ActivityResultEventCatalogEntry(site, tool, status)
		if _, present := entry.Payload.Properties["revision"]; !present {
			t.Fatal("generated result lost its loop coordinate")
		}
		for _, key := range []string{"effect_class", "fork_policy", "retry_max_attempts", "retry_backoff", "attempt"} {
			if _, present := entry.Payload.Properties[key]; present {
				t.Errorf("%s catalog still exposes retired activity policy %s", status, key)
			}
		}
	}
	schemas, err := ActivityResultEventSchemasForSite(site, tool)
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range []bool{false, true} {
		entry := ActivityApprovalEventCatalogEntry(site, revision)
		if _, present := entry.Payload.Properties["effect_class"]; present {
			t.Error("approval catalog still exposes retired activity policy")
		}
		schema := activityApprovalEventSchema(revision)
		if revision {
			schemas["approval_revision"] = schema
		} else {
			schemas["approval_rejection"] = schema
		}
	}
	for name, schema := range schemas {
		t.Run(name, func(t *testing.T) {
			properties := schema.Schema["properties"].(map[string]any)
			for _, key := range []string{"effect_class", "fork_policy", "retry_max_attempts", "retry_backoff", "attempt"} {
				if _, present := properties[key]; present {
					t.Errorf("compiled schema still exposes retired activity policy %s", key)
				}
				for _, required := range schema.Schema["required"].([]string) {
					if required == key {
						t.Errorf("compiled schema still requires retired activity policy %s", key)
					}
				}
			}
			if properties["activity_id"] == nil || properties["tool"] == nil {
				t.Fatal("generated activity identity fields were removed")
			}
		})
	}
}
