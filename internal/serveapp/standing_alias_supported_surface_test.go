package serveapp

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestA9RootedAliasesServeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveA9ServedAliases(t, backend)
		})
	}
}

func proveA9ServedAliases(t *testing.T, backend string) {
	t.Helper()
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	writeChannelOnboardingLiveToolCredentials(t, credentialPath)
	file, err := credentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	root := canonicalrouting.CopyStandingRootTreePublic(t)
	selected := filepath.Join(filepath.Dir(root), "shop")
	if err := os.Rename(root, selected); err != nil {
		t.Fatal(err)
	}
	_, start := clockDeploymentHarness(t, backend, selected)
	for repetition := 0; repetition < 2; repetition++ {
		process, served := start()
		rt := servedTestProcessRuntime(t, process)
		statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
		if err != nil || len(statuses) != 2 {
			t.Fatalf("binding readback=%+v err=%v", statuses, err)
		}
		for flow, alias := range map[string]string{".": "shop", "beta": "shop.beta"} {
			proveA9AliasPublication(t, file, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), flow, alias, repetition != 0)
		}
		if code := process.stop(); code != 0 {
			t.Fatalf("joined shutdown=%d", code)
		}
	}
}

func proveA9AliasPublication(t *testing.T, file *credentials.FileStore, base, flow, alias string, restarted bool) {
	t.Helper()
	const key = "telegram.webhook_signing_secret"
	secret, present, err := file.Get(t.Context(), key)
	if err != nil || !present {
		t.Fatalf("signing credential %s: present=%t err=%v", key, present, err)
	}
	body := `{"update_id":9701,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"rooted"}}`
	status, receipt := postProviderAliasUpdate(t, base, alias, secret, body)
	want := http.StatusAccepted
	if restarted {
		want = http.StatusOK
	}
	if status != want {
		t.Fatalf("%s source %s status=%d receipt=%s, want %d", alias, flow, status, receipt, want)
	}
	status, duplicate := postProviderAliasUpdate(t, base, alias, secret, body)
	if status != http.StatusOK || !reflect.DeepEqual(a9AliasReceipt(t, receipt), a9AliasReceipt(t, duplicate)) {
		t.Fatalf("exact retry changed receipt: %s -> %s status=%d", receipt, duplicate, status)
	}
	for _, unadmitted := range []struct {
		alias  string
		status int
	}{{" " + alias, http.StatusNotFound}, {alias + " ", http.StatusNotFound}, {alias + "%2Fextra", http.StatusBadRequest}} {
		if status, response := postProviderAliasUpdate(t, base, unadmitted.alias, secret, body); status != unadmitted.status {
			t.Fatalf("unadmitted endpoint %q selected alias %q: status=%d want=%d response=%s", unadmitted.alias, alias, status, unadmitted.status, response)
		}
	}
}

func TestA9AliasChannelRegistrationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			proveA9AliasChannelRegistration(t, backend)
		})
	}
}

func proveA9AliasChannelRegistration(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	h := newChannelOnboardingE2EHarness(t, backend, true)
	writeChannelOnboardingLiveToolCredentials(t, h.credentialPath)
	const alias = "shop"
	selected := filepath.Join(filepath.Dir(h.opts.SourceRoot), alias)
	if err := os.Rename(h.opts.SourceRoot, selected); err != nil {
		t.Fatal(err)
	}
	h.opts.SourceRoot = selected
	h.start(t)
	t.Cleanup(func() { h.stop(t) })
	begun := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "rooted-registration-token", nil)
	if begun.IdentityOperation == nil || begun.Candidate == nil || begun.Candidate.Target.Alias != alias {
		t.Fatalf("compiled alias not selected by public registration: %+v", begun)
	}
	callback, signing, _ := h.provider.Registration()
	if !strings.Contains(callback, "/webhooks/"+alias+"/telegram") {
		t.Fatalf("provider registration recomputed the alias: %q", callback)
	}
	requireChannelClaimDisposition(t, "rooted registration", submitChannelOnboardingClaim(t, callback, signing, begun.IdentityOperation.Challenge, 9791, "rooted_operator"), "consumed_by_binding")
	claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
		t.Fatalf("registration callback failed real authentication: %+v", claimed)
	}
	var confirmed map[string]any
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true}, &confirmed)
	ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
	if ready.Readiness == nil || !ready.Readiness.Ready || ready.Candidate == nil || ready.Candidate.Target.Alias != alias {
		t.Fatalf("registered directory alias not READY: %+v", ready)
	}
	registrations, _ := h.provider.Counts()
	_ = retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
	if count, _ := h.provider.Counts(); count != registrations {
		t.Fatalf("same-intent retry resent registration: %d -> %d", registrations, count)
	}
	h.stop(t)
	h.start(t)
	restored := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if restored.Readiness == nil || !restored.Readiness.Ready || restored.Candidate == nil || restored.Candidate.Target.Alias != alias {
		t.Fatalf("restart lost registered alias: %+v", restored)
	}
	// A new process admits a fresh exposure/token, not replay of the old intent.
	currentCallback, _, _ := h.provider.Registration()
	if currentCallback == callback || !strings.Contains(currentCallback, "/webhooks/"+alias+"/telegram?") {
		t.Fatalf("new exposure lost alias or reused the old callback: %q -> %q", callback, currentCallback)
	}
	if count, _ := h.provider.Counts(); count != registrations+1 {
		t.Fatalf("fresh exposure registration count: %d -> %d", registrations, count)
	}
	if old := submitChannelOnboardingClaim(t, callback, signing, begun.IdentityOperation.Challenge, 9792, "retired_operator"); old.StatusCode != http.StatusNotFound {
		t.Fatalf("old exposure callback was still admitted: %+v", old)
	}
	_ = retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
	if count, _ := h.provider.Counts(); count != registrations+1 {
		t.Fatalf("restored same-intent retry resent registration: %d -> %d", registrations+1, count)
	}
}

func TestA9CrossContextAliasPromotionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveA9CrossContextAliasPromotion(t, backend)
		})
	}
}

func proveA9CrossContextAliasPromotion(t *testing.T, backend string) {
	t.Helper()
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	writeChannelOnboardingLiveToolCredentials(t, credentialPath)
	managers := make(chan *runtimepkg.RuntimeContextManager, 2)
	firstOpts, firstStart := clockDeploymentHarness(t, backend, a9ServedAliasSource(t))
	secondRoot := a9ServedAliasSource(t)
	path := filepath.Join(secondRoot, "schema.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkflowValidationFixtureFile(t, path, string(body)+"\n# distinct admitted source\n")
	secondOpts, secondStart := clockDeploymentHarness(t, backend, secondRoot)
	firstOpts.TestRuntimeContextsReadyHook = func(manager *runtimepkg.RuntimeContextManager) { managers <- manager }
	secondOpts.TestRuntimeContextsReadyHook = firstOpts.TestRuntimeContextsReadyHook
	first, served := firstStart()
	incumbent := <-managers
	second, _ := secondStart()
	incoming := <-managers
	contextDef, present := incoming.LookupBundleHash(incoming.BundleHashes()[0])
	if !present || contextDef.BundleHash() == served.BundleHash {
		t.Fatalf("cross-context proof did not admit distinct sources: %+v", contextDef)
	}
	rt := servedTestProcessRuntime(t, first)
	before, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
	if err != nil || len(before) != 1 {
		t.Fatalf("incumbent status=%+v err=%v", before, err)
	}
	definition := *contextDef
	definition.Runtime = servedTestProcessRuntime(t, second)
	definition.WorkOwner = definition.Runtime.WorkOccurrence()
	err = incumbent.Register(definition)
	if err == nil || !strings.Contains(err.Error(), "duplicate enabled ingress alias") || !strings.Contains(err.Error(), served.BundleHash) || !strings.Contains(err.Error(), contextDef.BundleHash()) {
		t.Fatalf("cross-context collision did not name both owners: %v", err)
	}
	if incumbent.Len() != 1 || !reflect.DeepEqual(incumbent.BundleHashes(), []string{served.BundleHash}) {
		t.Fatalf("failed collision published another context: %+v", incumbent.BundleHashes())
	}
	after, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed cross-context admission mutated incumbent status: %+v -> %+v err=%v", before, after, err)
	}
	status, receipt := postProviderAliasUpdate(t, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), "shared", "boot-signing-secret", `{"update_id":9901,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
	if status != http.StatusAccepted {
		t.Fatalf("cross-context refusal lost incumbent delivery: status=%d receipt=%s", status, receipt)
	}
	for _, process := range []*serveRuntimeTestProcess{second, first} {
		if code := process.stop(); code != 0 {
			t.Fatalf("joined cross-context shutdown=%d", code)
		}
	}
}

func a9AliasReceipt(t *testing.T, body json.RawMessage) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	for _, key := range []string{"publication_id", "service_id", "run_id", "generation", "flow_path", "event_ids"} {
		value, present := decoded[key]
		if !present {
			t.Fatalf("receipt omitted %q: %s", key, body)
		}
		out[key] = value
	}
	return out
}

func a9ServedAliasSource(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	disableChannelOnboardingBusinessConsumers(t, root)
	selected := filepath.Join(filepath.Dir(root), "shared")
	if err := os.Rename(root, selected); err != nil {
		t.Fatal(err)
	}
	return selected
}
