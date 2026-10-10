//go:build linux || darwin

package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func installedSessionBootstrapFixture(t *testing.T, backend string) (*channelonboarding.Service, serveSessionBootstrapOwner) {
	t.Helper()
	var selected serveBootstrapTestStore
	if backend == "sqlite" {
		selected = storetest.StartSQLiteRuntimeStore(t)
	} else {
		selected = storetest.StartPostgresRuntimeStore(t)
	}
	ctx := context.Background()
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repoRootForTest(), filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session"),
		runtimePlatformSpecPath(t), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fact := sourceartifactfixture.RequireArtifact(t, ctx, selected, bundle.SourceArtifact)
	owner := newSupervisorTestRuntimeOccurrence(t, fact.BundleHash())
	serveBootstrapWireFixture(t, "connected", owner)
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(&processIngressEventStore{}, runtimebus.EventBusOptions{WorkOwner: owner,
		ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: fact, ReceiverExecution: eventreceiver.NormalExecution()})
	if err != nil {
		t.Fatal(err)
	}
	definition := completeServeTestPackContext(t, runtimepkg.BundleContext{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact, WorkOwner: owner,
		BundleIdentity: contracts.BundleIdentity{WorkflowName: "whatsapp-session", WorkflowVersion: "1.0.0", BundleHash: fact.BundleHash()},
		ChannelPlans:   projection.ChannelPlans,
		Runtime: &runtimepkg.Runtime{Bus: bus, ExecutionPosture: executionposture.Live,
			Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: owner.Identity().RuntimeInstanceID, ChannelPlans: projection.ChannelPlans}}})
	manager, err := runtimepkg.NewRuntimeContextManager(nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.QuiesceAllRuntimeContexts(ctx); err != nil {
			t.Error(err)
		}
	})
	catalog, err := serveChannelOnboardingCatalog(manager)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := catalog.Resolve(channelonboarding.CandidateSelection{Provider: "whatsapp"})
	if err != nil || candidate.ValidateDeclaration() != nil || candidate.Validate() == nil || candidate.Target.Alias != "reception" ||
		candidate.Coordinate.BundleHash != fact.BundleHash() || candidate.Target.Generation != 0 || candidate.Plan.HasNativeInbox() {
		t.Fatal("artifact-derived catalog invented authority or lost source ownership", candidate, err)
	}
	files, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := credentials.NewSnapshotOwner(files)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	sessions, err := newServeSessionBootstrap(serveSessionBootstrapRuntimeSelector(manager), selected, current, directory)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identities, err := operatorchannel.NewService(selected.(operatorchannel.Store), proofs, sessions, []operatorchannel.InterfaceIdentity{candidate.Interface}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identities.PreparePrincipal(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	writer, err := channelonboarding.NewCredentialWriter(files)
	if err != nil {
		t.Fatal(err)
	}
	effects := selected.(channelConfirmationEffectStore)
	confirmation, err := newServeChannelConfirmationDispatcher(effects, current, executionposture.Live, owner.Identity().RuntimeInstanceID, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{Store: selected, SourceArtifacts: selected, Identities: identities,
		Credentials: writer, Sessions: sessions, Catalog: func() (*channelonboarding.CandidateCatalog, error) { return serveChannelOnboardingCatalog(manager) },
		Activations: &serveChannelActivationRefresher{manager: manager, store: selected, identities: identities, credentials: current}, Confirmation: confirmation,
		Readiness: &serveConnectedChannelReadiness{manager: manager, store: selected, identities: identities, credentials: current, effects: effects, ingress: &publicingress.ReadinessOwner{}}})
	if err != nil {
		t.Fatal(err)
	}
	return service, sessions
}

func TestServeInstalledWhatsAppArtifactSelectsBootstrapWithoutExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			service, sessions := installedSessionBootstrapFixture(t, backend)
			started, err := service.Start(ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
				Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "installed-artifact-bootstrap"})
			if err != nil || started.Operation.Phase != channelonboarding.PhaseActivatingProvider || started.Operation.SessionConnectionID == "" ||
				started.Operation.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || started.IdentityOperation != nil || started.Binding != nil ||
				started.Operation.ActivationRevision != 0 || started.Operation.Coordinate.TargetGeneration != 0 {
				t.Fatal("installed artifact did not reach pairing-only bootstrap", started, err)
			}
			if err := sessions.(*serveSessionBootstrap).connection(started.Operation.OperationID).ReconcileIncoming(ctx); err != nil {
				t.Fatal("artifact-derived bootstrap omitted incoming ownership", err)
			}
		})
	}
}
