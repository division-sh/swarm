package serveapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
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
	h := newChannelOnboardingE2EHarness(t, map[string]servedparity.Backend{"sqlite": servedparity.BackendDefaultSQLite, "postgres": servedparity.BackendExplicitPostgres}[backend], true)
	writeChannelOnboardingLiveToolCredentials(t, h.credentialPath)
	root := canonicalrouting.CopyStandingRootTreePublic(t)
	selected := filepath.Join(filepath.Dir(root), "shop")
	if err := os.Rename(root, selected); err != nil {
		t.Fatal(err)
	}
	h.opts.SourceRoot, h.opts.AbandonActiveRuns = selected, false
	for _, account := range []struct {
		id    int64
		token string
	}{{420079, "root-bot-token"}, {420080, "beta-bot-token"}} {
		h.provider.SetResourceID(account.token, account.id)
		registerChannelFixtureAccount(t, h.opts.ConfigPath, account.token, fmt.Sprint(account.id))
	}
	for repetition := 0; repetition < 2; repetition++ {
		h.start(t)
		rt := servedTestProcessRuntime(t, h.process)
		statuses, err := readServedLandingRuns(t.Context(), rt)
		if err != nil || len(statuses) != 2 {
			t.Fatalf("binding readback=%+v err=%v", statuses, err)
		}
		for flow, alias := range map[string]string{".": "shop", "beta": "shop.beta"} {
			token := "root-bot-token"
			if flow == "beta" {
				token = "beta-bot-token"
			}
			callback, secret, count := h.provider.RegistrationForCredential(token)
			if count == 0 || callback == "" || secret == "" {
				t.Fatal("rooted alias has no exact native registration")
			}
			proveA9AliasPublication(t, callback, secret, h.endpoint, flow, alias, repetition != 0)
		}
		h.stop(t)
	}
}

func proveA9AliasPublication(t *testing.T, callback, secret, base, flow, alias string, restarted bool) {
	t.Helper()
	if !strings.Contains(callback, "/webhooks/"+alias+"/telegram?") {
		t.Fatal("native callback changed its exact admitted alias")
	}
	body := `{"update_id":9701,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"rooted"}}`
	status, receipt := postProviderAliasUpdate(t, callback, "", secret, body)
	want := http.StatusAccepted
	if restarted {
		want = http.StatusOK
	}
	if status != want {
		t.Fatalf("%s source %s status=%d receipt=%s, want %d", alias, flow, status, receipt, want)
	}
	if a9AliasReceipt(t, receipt)["flow_path"] != flow {
		t.Fatalf("native callback changed its declaring flow: %s", receipt)
	}
	status, duplicate := postProviderAliasUpdate(t, callback, "", secret, body)
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
	writeStandingCandidateFile(t, filepath.Join(secondRoot, "policy.yaml"), "operator_account: '420080'\n")
	secondOpts, secondStart := clockDeploymentHarness(t, backend, secondRoot)
	provider := &channelOnboardingTelegramProvider{}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	routes := map[string]string{"api.telegram.org": telegram.URL}
	for index, entry := range []struct {
		opts    *cliapp.ServeOptions
		account int64
		token   string
	}{{firstOpts, 420079, "first-alias-bot-token"}, {secondOpts, 420080, "second-alias-bot-token"}} {
		listener := reserveChannelOnboardingListener(t)
		entry.opts.PublicWebhookListener = listener
		entry.opts.PublicWebhookListen = listener.Addr().String()
		host := fmt.Sprintf("hooks.alias-%d.test", index)
		entry.opts.PublicWebhookBaseURL = "https://" + host
		routes[host] = "http://" + listener.Addr().String()
		provider.SetResourceID(entry.token, entry.account)
	}
	redirectExternalHosts(t, routes)
	registerChannelFixtureAccount(t, firstOpts.ConfigPath, "first-alias-bot-token", "420079")
	registerChannelFixtureAccount(t, secondOpts.ConfigPath, "second-alias-bot-token", "420080")
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
	before, err := readServedLandingRuns(t.Context(), rt)
	if err != nil || len(before) != 1 || before[0].Scope.FlowPath != "." || before[0].State != "live" {
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
	after, err := readServedLandingRuns(t.Context(), rt)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed cross-context admission mutated incumbent status: %+v -> %+v err=%v", before, after, err)
	}
	callback, signing, count := provider.RegistrationForCredential("first-alias-bot-token")
	if count == 0 || callback == "" || signing == "" {
		t.Fatal("incumbent source has no exact native callback")
	}
	status, receipt := postProviderAliasUpdate(t, callback, "", signing, `{"update_id":9901,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
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
	for _, key := range []string{"publication_id", "run_id", "flow_path", "event_ids"} {
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
	writeStandingCandidateFile(t, filepath.Join(root, "policy.yaml"), "operator_account: '420079'\n")
	selected := filepath.Join(filepath.Dir(root), "shared")
	if err := os.Rename(root, selected); err != nil {
		t.Fatal(err)
	}
	return selected
}
