package runtime

import (
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
)

func TestRuntimeContextManagerPublishesOneAdmissionGenerationAcrossAllContexts(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	oldState := runtimeAdmissionTestState(t, oldCatalog)
	newState := runtimeAdmissionTestState(t, newCatalog)

	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := NewRuntimeContextManagerWithAdmission(nil, oldState, primary, survivor)
	if err != nil {
		t.Fatalf("NewRuntimeContextManagerWithAdmission: %v", err)
	}
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", newCatalog)
	survivingTargets := map[string][]StandingTarget{
		runtimeContextTestHashB: runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog).StandingTargets,
	}
	if err := manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidate, survivingTargets, newState); err != nil {
		t.Fatalf("ValidateProcessAdmissionReplacement: %v", err)
	}

	stop := make(chan struct{})
	errCh := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			subjects := manager.CapabilitySubjects()
			lookup := manager.LookupIngress("survivor", "acme")
			if !lookup.Loaded() {
				select {
				case errCh <- &mixedAdmissionGenerationError{first: "loaded", second: "missing lookup"}:
				default:
				}
				return
			}
			lookupGeneration := lookup.Target.AdmissionPlan.GenerationID()
			if lookupGeneration != oldCatalog.GenerationID() && lookupGeneration != newCatalog.GenerationID() {
				select {
				case errCh <- &mixedAdmissionGenerationError{first: oldCatalog.GenerationID() + " or " + newCatalog.GenerationID(), second: lookupGeneration}:
				default:
				}
				return
			}
			generation := ""
			for _, subject := range subjects {
				if subject.TriggerAdmission == nil {
					continue
				}
				got := subject.TriggerAdmission.CatalogGeneration
				if generation == "" {
					generation = got
				}
				if got != generation {
					select {
					case errCh <- &mixedAdmissionGenerationError{first: generation, second: got}:
					default:
					}
					return
				}
			}
		}
	}()

	if _, err := manager.BeginBundleHashReplacement(runtimeContextTestHashA, candidate); err != nil {
		t.Fatalf("BeginBundleHashReplacement: %v", err)
	}
	if err := manager.PublishBundleHashReplacementWithAdmission(runtimeContextTestHashA, candidate, survivingTargets, newState); err != nil {
		t.Fatalf("PublishBundleHashReplacementWithAdmission: %v", err)
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	if got := manager.AdmissionState().GenerationID; got != newCatalog.GenerationID() {
		t.Fatalf("process generation = %q, want %q", got, newCatalog.GenerationID())
	}
	for _, alias := range []string{"primary", "survivor"} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Target.AdmissionPlan.GenerationID() != newCatalog.GenerationID() {
			t.Fatalf("lookup %q = %#v, want loaded new generation", alias, lookup)
		}
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.CapabilitySubjects(), newCatalog.GenerationID(), 2)
}

func TestRuntimeContextManagerRejectsIncompleteAdmissionGenerationWithoutMutation(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := NewRuntimeContextManagerWithAdmission(nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", newCatalog)
	err = manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidate, nil, runtimeAdmissionTestState(t, newCatalog))
	if err == nil || !strings.Contains(err.Error(), "did not recompile loaded runtime context") {
		t.Fatalf("validation error = %v", err)
	}
	if got := manager.AdmissionState().GenerationID; got != oldCatalog.GenerationID() {
		t.Fatalf("failed candidate changed generation to %q", got)
	}
	for _, alias := range []string{"primary", "survivor"} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Target.AdmissionPlan.GenerationID() != oldCatalog.GenerationID() {
			t.Fatalf("failed candidate changed lookup %q: %#v", alias, lookup)
		}
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.CapabilitySubjects(), oldCatalog.GenerationID(), 2)
}

func TestRuntimeContextManagerAdmissionGenerationDoesNotDependOnPrimaryPackUse(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	primary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := NewRuntimeContextManagerWithAdmission(nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidatePrimary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	newSurvivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog)
	updates := map[string][]StandingTarget{runtimeContextTestHashB: newSurvivor.StandingTargets}
	state := runtimeAdmissionTestState(t, newCatalog)
	if err := manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatalf("primary-without-pack validation: %v", err)
	}
	if _, err := manager.BeginBundleHashReplacement(runtimeContextTestHashA, candidatePrimary); err != nil {
		t.Fatal(err)
	}
	if err := manager.PublishBundleHashReplacementWithAdmission(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatal(err)
	}
	if got := manager.LookupIngress("survivor", "acme"); !got.Loaded() || got.Target.AdmissionPlan.GenerationID() != newCatalog.GenerationID() {
		t.Fatalf("surviving pack target = %#v", got)
	}
	if primary, ok := manager.LookupBundleHash(runtimeContextTestHashA); !ok || len(primary.StandingTargets) != 0 {
		t.Fatalf("primary context unexpectedly acquired pack target: %#v/%t", primary, ok)
	}
}

func TestRuntimeContextManagerRejectsCandidatePackRemovalAcrossContexts(t *testing.T) {
	source, oldCatalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	emptyCatalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	primary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	survivor := testBundleContext(t, runtimeContextTestHashB, "inbound.telegram")
	survivor.Source = source
	plan, err := oldCatalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatal(err)
	}
	survivor.StandingTargets = []StandingTarget{{
		BundleHash: runtimeContextTestHashB, FlowID: "coordinator", Alias: "chat", Provider: "telegram",
		RunID: "run-chat", FlowInstance: "coordinator/chat", EntityID: "entity-chat",
		SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
	}}
	manager, err := NewRuntimeContextManagerWithAdmission(nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidatePrimary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	if _, err = RecompileStandingTargetAdmissions(survivor.Source, emptyCatalog, survivor.StandingTargets); err == nil || !strings.Contains(err.Error(), `provider "telegram" is pack-required`) {
		t.Fatalf("actual pack removal recompile error = %v", err)
	}
	if got := manager.AdmissionState().GenerationID; got != oldCatalog.GenerationID() {
		t.Fatalf("pack removal changed process generation to %q", got)
	}
	if got := manager.LookupIngress("chat", "telegram"); !got.Loaded() || got.Target.AdmissionPlan.GenerationID() != oldCatalog.GenerationID() {
		t.Fatalf("pack removal changed survivor: %#v", got)
	}
	if _, ok := manager.LookupBundleHash(candidatePrimary.BundleHash); !ok {
		t.Fatal("pack removal failure withdrew unchanged primary context")
	}
}

func TestRuntimeContextManagerRejectsTwoContextIngressCollisionWithoutMutation(t *testing.T) {
	oldCatalog := runtimeAdmissionTestCatalog(t, "a")
	newCatalog := runtimeAdmissionTestCatalog(t, "b")
	primary := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", oldCatalog)
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", oldCatalog)
	manager, err := NewRuntimeContextManagerWithAdmission(nil, runtimeAdmissionTestState(t, oldCatalog), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	candidate := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "survivor", newCatalog)
	updates := map[string][]StandingTarget{
		runtimeContextTestHashB: runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", newCatalog).StandingTargets,
	}
	err = manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidate, updates, runtimeAdmissionTestState(t, newCatalog))
	if err == nil || !strings.Contains(err.Error(), `duplicate standing ingress alias "survivor"`) {
		t.Fatalf("collision validation error = %v", err)
	}
	if got := manager.AdmissionState().GenerationID; got != oldCatalog.GenerationID() {
		t.Fatalf("collision changed generation to %q", got)
	}
	for alias, hash := range map[string]string{"primary": runtimeContextTestHashA, "survivor": runtimeContextTestHashB} {
		lookup := manager.LookupIngress(alias, "acme")
		if !lookup.Loaded() || lookup.Context.BundleHash != hash || lookup.Target.AdmissionPlan.GenerationID() != oldCatalog.GenerationID() {
			t.Fatalf("collision changed %s lookup: %#v", alias, lookup)
		}
	}
}

func TestRuntimeContextManagerSignedToUnsignedTransitionRequiresAcknowledgedRecompileAcrossContexts(t *testing.T) {
	signed := runtimeAdmissionTestCatalog(t, "a")
	unsigned := runtimeAdmissionUnsignedTestCatalog(t, "b")
	primary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	survivor := runtimeAdmissionTestContext(t, runtimeContextTestHashB, "survivor", signed)
	manager, err := NewRuntimeContextManagerWithAdmission(nil, runtimeAdmissionTestState(t, signed), primary, survivor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unsigned.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "survivor", Provider: "acme"}); err == nil || !strings.Contains(err.Error(), "admission.acknowledge: unsigned_webhook") {
		t.Fatalf("unacknowledged transition compile error = %v", err)
	}
	if got := manager.LookupIngress("survivor", "acme"); !got.Loaded() || got.Target.AdmissionPlan.RequestAuthentication() != providertriggers.RequestAuthenticationTokenEquality {
		t.Fatalf("failed transition changed predecessor: %#v", got)
	}

	unsignedPlan, err := unsigned.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "survivor", Provider: "acme",
		Declaration: providertriggers.AdmissionDeclaration{Acknowledge: providertriggers.UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatal(err)
	}
	newSurvivor := survivor
	newSurvivor.StandingTargets = append([]StandingTarget(nil), survivor.StandingTargets...)
	newSurvivor.StandingTargets[0].SigningSecret = ""
	newSurvivor.StandingTargets[0].AdmissionPlan = unsignedPlan
	candidatePrimary := testBundleContext(t, runtimeContextTestHashA, "primary.event")
	updates := map[string][]StandingTarget{runtimeContextTestHashB: newSurvivor.StandingTargets}
	state := runtimeAdmissionTestState(t, unsigned)
	if err := manager.ValidateProcessAdmissionReplacement(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginBundleHashReplacement(runtimeContextTestHashA, candidatePrimary); err != nil {
		t.Fatal(err)
	}
	if err := manager.PublishBundleHashReplacementWithAdmission(runtimeContextTestHashA, candidatePrimary, updates, state); err != nil {
		t.Fatal(err)
	}
	lookup := manager.LookupIngress("survivor", "acme")
	if !lookup.Loaded() || lookup.Target.AdmissionPlan.RequestAuthentication() != providertriggers.RequestAuthenticationNone || lookup.Target.AdmissionPlan.GenerationID() != unsigned.GenerationID() {
		t.Fatalf("acknowledged transition lookup = %#v", lookup)
	}
	for _, subject := range manager.CapabilitySubjects() {
		if subject.TriggerAdmission != nil && subject.Applicability == "effective" && subject.TriggerAdmission.RequestAuthentication != "UNAUTHENTICATED" {
			t.Fatalf("transition readback retained stale authentication: %#v", subject)
		}
	}
}

func TestRuntimeContextManagerRejectsAdmissionTargetsOnLegacyPublishAndRestoresPredecessor(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	predecessor := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := NewRuntimeContextManagerWithAdmission(nil, runtimeAdmissionTestState(t, catalog), predecessor)
	if err != nil {
		t.Fatal(err)
	}
	restored := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	if _, err := manager.BeginBundleHashReplacement(runtimeContextTestHashA, restored); err != nil {
		t.Fatal(err)
	}
	err = manager.PublishBundleHashReplacement(runtimeContextTestHashA, restored)
	if err == nil || !strings.Contains(err.Error(), "PublishBundleHashReplacementWithAdmission") {
		t.Fatalf("legacy publish error = %v", err)
	}
	if err := manager.PublishRestoredBundleHashReplacement(runtimeContextTestHashA, restored); err != nil {
		t.Fatalf("PublishRestoredBundleHashReplacement: %v", err)
	}
	lookup := manager.LookupIngress("primary", "acme")
	if !lookup.Loaded() || lookup.Target.AdmissionPlan.GenerationID() != catalog.GenerationID() {
		t.Fatalf("restored lookup = %#v", lookup)
	}
	assertRuntimeAdmissionSubjectGeneration(t, manager.CapabilitySubjects(), catalog.GenerationID(), 1)
}

type mixedAdmissionGenerationError struct{ first, second string }

func (e *mixedAdmissionGenerationError) Error() string {
	return "capability snapshot mixed admission generations " + e.first + " and " + e.second
}

func runtimeAdmissionTestCatalog(t *testing.T, hashToken string) *providertriggers.CatalogSnapshot {
	t.Helper()
	manifest := providertriggers.Manifest{
		Provider: "acme", Secret: providertriggers.SecretManifest{Required: true},
		Signature:  providertriggers.SignatureManifest{Type: "token_equality", Header: "X-Acme-Token"},
		DeliveryID: providertriggers.ValueSource{Header: "X-Acme-Delivery", Required: true},
		EventType:  providertriggers.ValueSource{Literal: "event", Required: true},
		EventName:  providertriggers.EventNameManifest{Literal: "inbound.acme"},
		Ack:        providertriggers.AckManifest{Mode: "after_publish"},
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Identity: providertriggers.PackIdentity{
			ID: "provider.acme", Version: "1.0.0", ManifestHash: strings.Repeat(hashToken, 64), Provenance: packs.ProvenanceExternal,
		},
		Manifest: manifest, Source: "test", SourcePath: "/packs/acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func runtimeAdmissionUnsignedTestCatalog(t *testing.T, hashToken string) *providertriggers.CatalogSnapshot {
	t.Helper()
	manifest := providertriggers.Manifest{
		Provider: "acme", Secret: providertriggers.SecretManifest{Required: false},
		DeliveryID: providertriggers.ValueSource{Header: "X-Acme-Delivery", Required: true},
		EventType:  providertriggers.ValueSource{Literal: "event", Required: true},
		EventName:  providertriggers.EventNameManifest{Literal: "inbound.acme"},
		Ack:        providertriggers.AckManifest{Mode: "after_publish"},
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Identity: providertriggers.PackIdentity{
			ID: "provider.acme", Version: "1.0.0", ManifestHash: strings.Repeat(hashToken, 64), Provenance: packs.ProvenanceExternal,
		},
		Manifest: manifest, Source: "test", SourcePath: "/packs/acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func runtimeAdmissionTestState(t *testing.T, catalog *providertriggers.CatalogSnapshot) ProcessAdmissionState {
	t.Helper()
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	return ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed}
}

func runtimeAdmissionTestContext(t *testing.T, hash, alias string, catalog *providertriggers.CatalogSnapshot) BundleContext {
	t.Helper()
	contextDef := testBundleContext(t, hash, "inbound.acme")
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: alias, Provider: "acme", SigningSecret: "webhook_signing.acme",
	})
	if err != nil {
		t.Fatal(err)
	}
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: hash, FlowID: "acme-flow", Alias: alias, Provider: "acme", RunID: "run-" + alias,
		FlowInstance: "acme-flow/" + alias, EntityID: "entity-" + alias, SigningSecret: "webhook_signing.acme", AdmissionPlan: plan,
	}}
	return contextDef
}

func assertRuntimeAdmissionSubjectGeneration(t *testing.T, subjects []packs.Subject, generation string, wantEffective int) {
	t.Helper()
	effective := 0
	for _, subject := range subjects {
		if subject.TriggerAdmission == nil {
			continue
		}
		effective++
		if subject.TriggerAdmission.CatalogGeneration != generation {
			t.Fatalf("subject %q generation = %q, want %q", subject.ID, subject.TriggerAdmission.CatalogGeneration, generation)
		}
	}
	if effective != wantEffective {
		t.Fatalf("effective trigger subjects = %d, want %d", effective, wantEffective)
	}
}
