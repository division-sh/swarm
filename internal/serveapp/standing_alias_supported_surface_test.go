package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"gopkg.in/yaml.v3"
)

func TestA9RootedAliasesServeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/override=%t", backend, override), func(t *testing.T) {
				proveA9ServedAliases(t, backend, override)
			})
		}
	}
}

func proveA9ServedAliases(t *testing.T, backend string, override bool) {
	t.Helper()
	file := a9ServedAliasCredentials(t, true)
	_, start := clockDeploymentHarness(t, backend, a9ServedAliasSource(t, override, false))
	for repetition := 0; repetition < 2; repetition++ {
		process, served := start()
		rt := servedTestProcessRuntime(t, process)
		statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
		if err != nil || len(statuses) != 2 {
			t.Fatalf("binding readback=%+v err=%v", statuses, err)
		}
		for flow, alias := range a9ServedAliasExpected(override, false) {
			proveA9AliasPublication(t, file, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), flow, alias, repetition != 0)
		}
		if code := process.stop(); code != 0 {
			t.Fatalf("joined shutdown=%d", code)
		}
	}
}

func proveA9AliasPublication(t *testing.T, file *credentials.FileStore, base, flow, alias string, restarted bool) {
	t.Helper()
	key := "webhook_signing.alpha"
	if flow == "beta" {
		key = "webhook_signing.beta"
	}
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
	for _, unadmitted := range []string{" " + alias, alias + " ", alias + "%2Fextra"} {
		if status, response := postProviderAliasUpdate(t, base, unadmitted, secret, body); status != http.StatusNotFound {
			t.Fatalf("unadmitted endpoint %q selected alias %q: status=%d response=%s", unadmitted, alias, status, response)
		}
	}
}

func TestA9AliasChannelRegistrationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/override=%t", backend, override), func(t *testing.T) {
				proveA9AliasChannelRegistration(t, backend, override)
			})
		}
	}
}

func proveA9AliasChannelRegistration(t *testing.T, backend servedparity.Backend, override bool) {
	t.Helper()
	h := newChannelOnboardingE2EHarness(t, backend, true)
	alias := "shop.telegram-ingress"
	if override {
		alias = "registered-override"
	}
	a9RewriteAliasSchema(t, filepath.Join(h.opts.SourceRoot, "schema.yaml"), "shop", "")
	a9RewriteAliasSchema(t, filepath.Join(h.opts.SourceRoot, "telegram-ingress", "schema.yaml"), "", map[bool]string{false: "", true: alias}[override])
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
		t.Fatalf("registered default/override not READY: %+v", ready)
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

func a9RewriteAliasSchema(t *testing.T, path, name, alias string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := yaml.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	if name != "" {
		schema["name"] = name
	}
	if ingress, ok := schema["ingress"].(map[string]any); ok {
		delete(ingress, "alias")
		if alias != "" {
			ingress["alias"] = alias
		}
	}
	updated, err := yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkflowValidationFixtureFile(t, path, string(updated))
}

func TestA9EnabledAliasAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveA9EnabledAliasAdmission(t, backend)
		})
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
	a9ServedAliasCredentials(t, false)
	managers := make(chan *runtimepkg.RuntimeContextManager, 2)
	firstOpts, firstStart := clockDeploymentHarness(t, backend, a9ServedAliasSource(t, true, true))
	secondRoot := a9ServedAliasSource(t, true, true)
	a9RewriteAliasSchema(t, filepath.Join(secondRoot, "schema.yaml"), "other-root", "shared")
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
	status, receipt := postProviderAliasUpdate(t, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), "shared", "alias-secret-webhook_signing.alpha", `{"update_id":9901,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
	if status != http.StatusAccepted {
		t.Fatalf("cross-context refusal lost incumbent delivery: status=%d receipt=%s", status, receipt)
	}
	for _, process := range []*serveRuntimeTestProcess{second, first} {
		if code := process.stop(); code != 0 {
			t.Fatalf("joined cross-context shutdown=%d", code)
		}
	}
}

func proveA9EnabledAliasAdmission(t *testing.T, backend string) {
	t.Helper()
	file := a9ServedAliasCredentials(t, false)
	opts, start := clockDeploymentHarness(t, backend, a9ServedAliasSource(t, true, true))
	managers := make(chan *runtimepkg.RuntimeContextManager, 1)
	opts.TestRuntimeContextsReadyHook = func(manager *runtimepkg.RuntimeContextManager) { managers <- manager }
	// Both declarations have the same alias, but only the root is enabled.
	process, served := start()
	rt := servedTestProcessRuntime(t, process)
	manager := <-managers
	before, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
	if err != nil || len(before) != 1 {
		t.Fatalf("dormant sibling acquired a generation: %+v err=%v", before, err)
	}
	owner, err := credentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	readback, err := manager.EvaluatedCapabilitySubjects(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	effective := 0
	for _, subject := range readback {
		if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "effective" {
			effective++
			wantReady := subject.TriggerAdmission.FlowPath == "."
			if subject.TriggerAdmission.BindingEnabled == nil || *subject.TriggerAdmission.BindingEnabled != wantReady || (subject.Status == packs.StatusReady) != wantReady {
				t.Fatalf("declared/active readback collapsed: %+v", subject)
			}
		}
	}
	if effective != 2 {
		t.Fatalf("duplicate dormant declaration lost from inventory: %+v", readback)
	}
	catalog, err := serveChannelOnboardingCatalog(manager)
	if err != nil {
		t.Fatal(err)
	}
	var dormant channelonboarding.Candidate
	for _, candidate := range catalog.Candidates() {
		if candidate.Provider == "telegram" && candidate.Target.FlowPath == "beta" {
			dormant = candidate
		}
	}
	if dormant.Target.Alias != "shared" || dormant.Target.Generation != 0 {
		t.Fatalf("dormant discovery fabricated a receiver/generation: %+v", dormant)
	}
	err = manager.AdmitChannelStandingTarget(t.Context(), channelonboarding.Operation{}, dormant, nil)
	if err == nil || !strings.Contains(err.Error(), `flow "."`) || !strings.Contains(err.Error(), `flow "beta"`) || !strings.Contains(err.Error(), "duplicate enabled ingress alias") {
		t.Fatalf("promotion did not refuse both exact owners before mutation: %v", err)
	}
	after, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed promotion changed standing state: before=%+v after=%+v err=%v", before, after, err)
	}
	currentReadback, err := manager.EvaluatedCapabilitySubjects(t.Context(), owner)
	if err != nil || !reflect.DeepEqual(readback, currentReadback) {
		t.Fatalf("failed promotion changed capability readback: %v", err)
	}
	status, receipt := postProviderAliasUpdate(t, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), "shared", "alias-secret-webhook_signing.alpha", `{"update_id":9801,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
	if status != http.StatusAccepted {
		t.Fatalf("failed promotion lost incumbent delivery: status=%d receipt=%s", status, receipt)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("joined shutdown=%d", code)
	}
}

func a9AliasReceipt(t *testing.T, body json.RawMessage) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	for _, key := range []string{"publication_id", "entity_id", "event_ids"} {
		value, present := decoded[key]
		if !present {
			t.Fatalf("receipt omitted %q: %s", key, body)
		}
		out[key] = value
	}
	return out
}

func a9ServedAliasCredentials(t *testing.T, child bool) *credentials.FileStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	file, err := credentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for key, present := range map[string]bool{"telegram_bot_token": true, "webhook_signing.alpha": true, "webhook_signing.beta": child} {
		if present {
			if err := file.Set(context.Background(), key, "alias-secret-"+key); err != nil {
				t.Fatal(err)
			}
		}
	}
	return file
}

func a9ServedAliasExpected(override, collide bool) map[string]string {
	if !override {
		return map[string]string{".": "shop", "beta": "shop.beta"}
	}
	if collide {
		return map[string]string{".": "shared", "beta": "shared"}
	}
	return map[string]string{".": "shared", "beta": "other"}
}

func a9ServedAliasSource(t *testing.T, override, collide bool) string {
	t.Helper()
	root := canonicalrouting.CopyStandingRootTreePublic(t)
	for flow, alias := range a9ServedAliasExpected(override, collide) {
		path := filepath.Join(root, flow, "schema.yaml")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var schema map[string]any
		if err := yaml.Unmarshal(body, &schema); err != nil {
			t.Fatal(err)
		}
		if flow == "." {
			schema["name"] = "shop"
		}
		ingress := schema["ingress"].(map[string]any)
		delete(ingress, "alias")
		if override {
			ingress["alias"] = alias
		}
		updated, err := yaml.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		writeWorkflowValidationFixtureFile(t, path, string(updated))
	}
	return root
}
