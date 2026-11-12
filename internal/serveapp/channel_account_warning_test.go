package serveapp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

const whatsAppAccountMismatchWarning = "This flow's operator_account does not match the registered WhatsApp account; customer messages on the unmatched account will not reach this flow. Check the configured account."

func TestWhatsAppBootAccountMismatchWarningOnly(t *testing.T) {
	for _, account := range []string{"+15551234567", "+1 (555) 123-4567"} {
		t.Run(account, func(t *testing.T) { requireWhatsAppBootAccountMismatchWarning(t, account) })
	}
}

func requireWhatsAppBootAccountMismatchWarning(t *testing.T, account string) {
	t.Helper()
	catalog, plan, rooted := sessionDiscoveryCapturedProvider(t, ".", account)
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repoRootForTest(), rooted, contracts.DefaultPlatformSpecFile(repoRootForTest()), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := contracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	definition := runtime.BundleContext{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
		RootAdmission: rooted.Admission(), RuntimeInstanceID: uuid.NewString(), PublicationGeneration: 1,
		PackInventoryDigest: "sha256:discovery-inventory", ProviderTriggerCatalog: catalog, ChannelPlans: []packs.SatisfactionPlan{plan}, BundleIdentity: identity}
	binding, _ := installedChannelFactsFor("whatsapp")
	for _, test := range []struct {
		name, account, provider string
		phase                   channelonboarding.Phase
		confirmed, mismatch     bool
	}{
		{"matching", "+15551234567", "whatsapp", channelonboarding.PhaseSucceeded, true, false},
		{"mismatch", "+15551234568", "whatsapp", channelonboarding.PhaseSucceeded, true, true},
		{"pending", "+15551234568", "whatsapp", channelonboarding.PhaseAwaitingExternalIdentity, false, false},
		{"retired", "+15551234568", "whatsapp", channelonboarding.PhaseRetired, true, false},
		{"other provider", "420079", "telegram", channelonboarding.PhaseSucceeded, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			operations := []channelonboarding.Operation{{DeploymentRegistration: true, Provider: test.provider, AccountReference: test.account, AccountConfirmed: test.confirmed, Phase: test.phase}}
			mismatch, err := registeredAccountMismatch(binding, []runtime.BundleContext{definition}, operations)
			if err != nil || mismatch != test.mismatch {
				t.Fatalf("mismatch=%t want=%t error=%v", mismatch, test.mismatch, err)
			}
			var out, diagnostic bytes.Buffer
			presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out, ErrorOutput: &diagnostic})
			if mismatch {
				presenter.recordAccountMismatch(binding)
			}
			presenter.readyPresentation(serveLifecycleReadyFacts{})
			presenter.finish()
			if presenter.failed || strings.Contains(diagnostic.String(), whatsAppAccountMismatchWarning) != test.mismatch {
				t.Fatal("warning blocked startup or lost its text", out.String(), diagnostic.String())
			}
		})
	}
}
