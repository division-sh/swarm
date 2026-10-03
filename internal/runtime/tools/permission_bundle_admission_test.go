package tools

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestPermissionBundleMalformedDeclarationsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"null_root", nil},
		{"scalar_root", "operators"},
		{"list_root", []any{"operators"}},
		{"null_bundle", map[string]any{"operators": nil}},
		{"scalar_bundle", map[string]any{"operators": "message_flow"}},
		{"list_bundle", map[string]any{"operators": []any{"message_flow"}}},
		{"missing_permissions", map[string]any{"operators": map[string]any{}}},
		{"null_permissions", map[string]any{"operators": map[string]any{"permissions": nil}}},
		{"scalar_permissions", map[string]any{"operators": map[string]any{"permissions": "message_flow"}}},
		{"mixed_integer", map[string]any{"operators": map[string]any{"permissions": []any{"message_flow", 7}}}},
		{"mixed_null", map[string]any{"operators": map[string]any{"permissions": []any{"message_peers", nil}}}},
		{"mixed_object", map[string]any{"operators": map[string]any{"permissions": []any{"ask_human", map[string]any{}}}}},
		{"valid_and_malformed", map[string]any{"valid": map[string]any{"permissions": []any{"custom_access"}}, "operators": map[string]any{}}},
	} {
		for _, selected := range []bool{false, true} {
			name := tc.name + "/unused"
			if selected {
				name = tc.name + "/selected"
			}
			t.Run(name, func(t *testing.T) {
				policy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
					"permission_bundles": {Value: tc.value},
				}}
				source := retiredToolSourceForScope("root", nil, nil, policy)
				if _, err := permissionBundles(policy); err == nil {
					t.Fatal("shared reader suppressed a declaration error")
				}
				known := map[string]struct{}{"existing": {}}
				if err := collectPermissionBundleExtensions(known, policy); err == nil || len(known) != 1 {
					t.Fatalf("vocabulary must fail without partial mutation: %v, %v", known, err)
				}
				if _, err := knownPermissionNames(source); err == nil {
					t.Fatal("known-permission consumer suppressed a declaration error")
				}
				for _, validate := range []func() []error{
					func() []error { return ValidateHITLIdentityLifecycleReferences(source) },
					func() []error { return ValidateRetiredDynamicAgentToolReferences(source) },
					func() []error { _, errs := ValidateAgentPermissions(source); return errs },
				} {
					if errs := validate(); len(errs) == 0 || !strings.Contains(errs[0].Error(), "permission_bundles") {
						t.Fatalf("malformed declaration became absence: %v", errs)
					}
				}
				entry := runtimecontracts.AgentRegistryEntry{Permissions: []string{"ask_human"}}
				if selected {
					entry.PermissionsBundle = "operators"
				}
				if perms, err := ResolveAgentPermissions(source, "", entry); err == nil || perms != nil {
					t.Fatalf("resolver accepted malformed policy: %v, %v", perms, err)
				}
			})
		}
	}
}

func TestPermissionBundleDeclarationControls(t *testing.T) {
	for _, permissions := range []any{[]any{}, []string{}, []any{" custom_access ", "ask_human", "custom_access"}, []string{"custom_access", "ask_human"}} {
		policy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"permission_bundles": {Value: map[string]any{"operators": map[string]any{"permissions": permissions}}},
		}}
		source := retiredToolSourceForScope("root", nil, nil, policy)
		if _, errs := ValidateAgentPermissions(source); len(errs) != 0 {
			t.Fatalf("valid unused declaration rejected: %v", errs)
		}
		entry := runtimecontracts.AgentRegistryEntry{PermissionsBundle: "operators", Permissions: []string{"ask_human"}}
		got, err := ResolveAgentPermissions(source, "", entry)
		want := []string{"custom_access", "ask_human"}
		if reflect.ValueOf(permissions).Len() == 0 {
			want = []string{"ask_human"}
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("valid expansion/dedup: %v, %v, want %v", got, err, want)
		}
	}
	for _, policy := range []runtimecontracts.PolicyDocument{
		{}, {Values: map[string]runtimecontracts.PolicyValue{"permission_bundles": {Value: map[string]any{}}}},
	} {
		source := retiredToolSourceForScope("root", nil, nil, policy)
		if _, errs := ValidateAgentPermissions(source); len(errs) != 0 {
			t.Fatalf("absent/empty bundle map rejected: %v", errs)
		}
	}
}
