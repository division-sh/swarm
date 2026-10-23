package runtime

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestA9IngressLookupUsesExactAlias(t *testing.T) {
	catalog, source := runtimeAdmissionTestCatalog(t, "a", "primary")
	contextDef := runtimeAdmissionTestContext(t, source, catalog, uuid.NewString())
	alias := contextDef.StandingTargets[0].Alias
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	if !manager.LookupIngress(alias, "acme").Loaded() {
		t.Fatal("exact admitted ingress is not discoverable")
	}
	for _, alias := range []string{" " + alias, alias + " ", "/" + alias, alias + "/"} {
		if strings.HasPrefix(alias, " ") || strings.HasSuffix(alias, " ") {
			if parsed, _, _ := parseWebhookPath("/webhooks/" + alias + "/acme"); parsed != alias {
				t.Fatalf("lower gateway parser normalized unadmitted alias %q into %q", alias, parsed)
			}
		}
		if lookup := manager.LookupIngress(alias, "acme"); lookup.Found || lookup.AliasFound {
			t.Fatalf("unadmitted alias %q selected an incumbent: %+v", alias, lookup)
		}
		use, lookup, err := manager.AcquireIngress(t.Context(), alias, "acme")
		if use != nil {
			_ = use.Done()
			t.Fatalf("unadmitted alias %q acquired execution", alias)
		}
		if err != nil || lookup.Found || lookup.AliasFound {
			t.Fatalf("unadmitted lookup returned authority: %+v err=%v", lookup, err)
		}
	}
}
