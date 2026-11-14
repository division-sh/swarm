package channelonboarding_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/serveapp"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/tests/support/testutil/packfixture"
	"github.com/google/uuid"
)

type deploymentLockKey struct{}
type deploymentLockRole struct{}
type deploymentLockCut struct {
	sync.Mutex
	parent                             string
	queries                            []string
	guardReady, mutationReady, release chan struct{}
	guardOnce, mutationOnce            sync.Once
}

func observeDeploymentLockCut(ctx context.Context, query string, args []driver.NamedValue) {
	cut, ok := ctx.Value(deploymentLockKey{}).(*deploymentLockCut)
	if !ok {
		return
	}
	if ctx.Value(deploymentLockRole{}) == "guard" {
		cut.Lock()
		cut.queries = append(cut.queries, query)
		cut.Unlock()
		if principalFenceQuery(query) {
			cut.guardOnce.Do(func() { close(cut.guardReady); <-cut.release })
		}
	} else if strings.Contains(query, "FROM deployment_channel_operations") && strings.Contains(query, "FOR UPDATE") &&
		len(args) > 0 && args[0].Value == cut.parent {
		cut.mutationOnce.Do(func() { close(cut.mutationReady) })
	}
}

type deploymentLockFixture struct {
	callbackLockFixture
	plan      packs.SatisfactionPlan
	trigger   providertriggers.InboundAdmissionPlan
	candidate domain.Candidate
	writer    *domain.CredentialWriter
	current   *credentials.SnapshotOwner
	service   *domain.Service
}

func newDeploymentLockFixture(t *testing.T) deploymentLockFixture {
	t.Helper()
	f := deploymentLockFixture{callbackLockFixture: newCallbackLockFixture(t, "postgres")}
	work := worklifetime.NewProcess()
	original, err := startupownership.CaptureMutationAuthority(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := work.BindOriginalProcess(original.ProcessAuthority().LeaseStamp()); err != nil {
		t.Fatal(err)
	}
	f.ctx = worklifetime.WithProcess(f.ctx, work)
	t.Cleanup(func() {
		work.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	spec, err := runtimecontracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	triggers := packfixture.TriggerCatalog(t)
	for _, channel := range packfixture.ChannelPacks(t) {
		if channel.Key() != "telegram" {
			continue
		}
		trigger, present := triggers.PackDescriptorForProvider(channel.Key())
		if !present {
			t.Fatal("missing admitted Telegram trigger")
		}
		for _, connector := range packfixture.ConnectorRegistry(t).PackDescriptors() {
			if connector.Identity.ID() == "telegram" {
				f.plan, err = packs.CompileProviderChannel(registry, channel, trigger, connector)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	identity, err := f.plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	profile, present := f.plan.OnboardingProfile()
	if !present {
		t.Fatal("missing admitted Telegram onboarding")
	}
	f.candidate = domain.Candidate{DeploymentRegistration: true, Provider: "telegram", Interface: identity, Plan: f.plan,
		Posture: domain.ActivationWebhookRegistration, Ceremony: domain.CeremonyAccountLink,
		ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential()}
	f.trigger, err = triggers.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "telegram", Provider: "telegram", SigningSecret: profile.SigningCredential()})
	if err != nil {
		t.Fatal(err)
	}
	file, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Set(f.ctx, "deployment.token", "synthetic-token"); err != nil {
		t.Fatal(err)
	}
	f.writer, err = domain.NewCredentialWriter(file)
	if err != nil {
		t.Fatal(err)
	}
	f.current, err = credentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := f.selected.EnsureOperatorPrincipal(f.ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	process, present := worklifetime.ProcessFromContext(f.ctx)
	if !present {
		t.Fatal("native fixture lost its original process")
	}
	native, err := serveapp.OpenChannelRegistration(f.ctx, cliapp.ChannelRegistrationOptions{Process: process, Principal: principal,
		Operations: f.deployment, Store: f.selected, Identities: f.selected, Credentials: f.current, Plan: f.plan, Trigger: f.trigger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := native.Close(context.WithoutCancel(f.ctx)); err != nil {
			t.Error(err)
		}
	})
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identities, err := operatorchannel.NewService(f.selected, proofs, native, []operatorchannel.InterfaceIdentity{identity}, f.principal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identities.PreparePrincipal(f.ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	f.service, err = domain.NewService(domain.ServiceOptions{Deployment: true, Store: f.selected, Operations: f.deployment,
		Identities: identities, Credentials: f.writer, Sessions: native, Catalog: func() (*domain.CandidateCatalog, error) {
			return domain.NewCandidateCatalog([]domain.Candidate{f.candidate})
		}})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f deploymentLockFixture) pending(ctx context.Context, now time.Time, eventID int64) (domain.Operation, operatorchannel.Operation, error) {
	var child operatorchannel.Operation
	id := uuid.NewString()
	op, err := f.deployment.ReserveChannelOnboarding(ctx, domain.StartRequest{DeploymentRegistration: true,
		OperationID: id, RequestKeyHash: id, RequestHash: id, PrincipalID: f.principal, Verb: domain.VerbConnect,
		Provider: "telegram", Interface: f.candidate.Interface, Posture: f.candidate.Posture, Ceremony: f.candidate.Ceremony,
		CredentialReservations: []domain.CredentialReservation{{Role: f.candidate.ProviderCredentialRole, StoreKey: "deployment.token"}}, RequestedAt: now})
	if err != nil {
		return op, child, err
	}
	observed, err := f.writer.Observe(ctx, "deployment.token")
	if err != nil {
		return op, child, err
	}
	for _, phase := range []domain.Phase{domain.PhaseCredentialsAdmitted, domain.PhaseActivatingProvider, domain.PhaseAwaitingExternalIdentity} {
		req := domain.AdvanceRequest{OperationID: id, ExpectedRevision: op.Revision, Phase: phase, Now: now}
		if phase == domain.PhaseCredentialsAdmitted {
			req.ReplaceCredentialAdmissions = true
			req.CredentialAdmissions = []domain.CredentialAdmission{{Role: f.candidate.ProviderCredentialRole,
				StoreKey: observed.StoreKey, Kind: domain.CredentialAdmissionObserved, ValueSeal: observed.ValueSeal}}
		}
		if phase == domain.PhaseAwaitingExternalIdentity {
			req.AccountReference = "420079"
		}
		op, err = f.deployment.AdvanceChannelOnboarding(ctx, req)
		if err != nil {
			return op, child, err
		}
	}
	op, err = f.deployment.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: id, ExpectedRevision: op.Revision, Phase: op.Phase, ConfirmAccount: true, Now: now})
	if err != nil {
		return op, child, err
	}
	bindings, err := f.selected.ListOperatorChannelBindings(ctx, f.principal)
	if err != nil {
		return op, child, err
	}
	var revision int64
	for _, binding := range bindings {
		if binding.Interface == op.Interface {
			revision = binding.Revision
		}
	}
	childID := uuid.NewString()
	child, err = f.selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{OperationID: childID, Kind: operatorchannel.OperationConnect,
		PrincipalID: f.principal, Interface: op.Interface, ExpectedRevision: revision, RequestKeyHash: childID, RequestHash: childID,
		OnboardingOperationID: id, ProviderAuthority: operatorchannel.ProviderAuthority{Scope: authority.DeploymentParent,
			Kind: operatorchannel.ProviderAuthorityCredential, Credential: credentials.ValueEvidence{Key: observed.StoreKey, Seal: observed.ValueSeal}},
		RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL)})
	if err != nil {
		return op, child, err
	}
	op, err = f.deployment.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: id, ExpectedRevision: op.Revision, Phase: op.Phase, IdentityOperationID: childID, Now: now})
	if err != nil {
		return op, child, err
	}
	account := domain.AccountReference{Provider: "telegram", Account: op.AccountReference}
	consumed, err := sessionprovider.ConsumeDeploymentWebhookClaim(ctx, sessionprovider.DeploymentConnectionOptions{
		Operations: f.deployment, Principal: operatorchannel.Principal{ID: f.principal}, Plan: f.plan, Trigger: f.trigger}, f.selected,
		account, func(admission domain.CredentialAdmission) bool {
			current, err := f.current.CurrentValueMatchesSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
			return err == nil && current
		}, func(providertriggers.InboundAdmissionPlan) (providertriggers.Delivery, error) {
			body, err := json.Marshal(map[string]any{"update_id": eventID, "message": map[string]any{"message_id": eventID,
				"from": map[string]any{"id": 1001}, "chat": map[string]any{"id": 1001, "type": "private"}, "text": child.Challenge}})
			if err != nil {
				return providertriggers.Delivery{}, err
			}
			var payload any
			if err := json.Unmarshal(body, &payload); err != nil {
				return providertriggers.Delivery{}, err
			}
			return f.trigger.Accept(providertriggers.Request{Provider: "telegram", Method: http.MethodPost, Body: body, Payload: payload,
				Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"fixture-secret"}}, Target: providertriggers.Target{WebhookSecret: "fixture-secret"}})
		}, func() bool { return true })
	if err != nil || !consumed {
		return op, child, fmt.Errorf("native deployment claim consumed=%t: %w", consumed, err)
	}
	children, err := f.selected.ListOperatorChannelOperations(ctx, f.principal)
	if err != nil {
		return op, child, err
	}
	for _, current := range children {
		if current.OperationID == childID {
			child = current
		}
	}
	op, err = f.deployment.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: id, ExpectedRevision: op.Revision,
		Phase: domain.PhaseAwaitingOperatorConfirmation, Now: now})
	return op, child, err
}

func (f deploymentLockFixture) confirm(ctx context.Context, op domain.Operation, child operatorchannel.Operation, now time.Time) (domain.Operation, operatorchannel.Operation, error) {
	bound, binding, err := f.selected.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{OperationID: child.OperationID,
		PrincipalID: f.principal, ExpectedRevision: child.Revision, Approve: true, ProviderAuthorityCurrent: true,
		ProviderAuthorityAdmission: child.ProviderAuthority, ConfirmedAt: now})
	if err != nil {
		return op, bound, err
	}
	op, err = f.deployment.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID,
		ExpectedRevision: op.Revision, Phase: domain.PhaseSucceeded, BindingRevision: binding.Revision, Now: now})
	return op, bound, err
}

func deploymentFixtureParent(op domain.Operation, child operatorchannel.Operation) channelnative.TransportParent {
	return channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration, ID: op.OperationID, Revision: op.Revision,
		BindingOperationID: child.OperationID, BindingOperationRevision: child.Revision}
}

func proveDeploymentParentInterleavings(t *testing.T) {
	for _, mutation := range []string{"confirmation", "logout", "logout_then_connect"} {
		t.Run(mutation, func(t *testing.T) {
			proveDeploymentParentInterleaving(t, mutation)
		})
	}
}

func proveDeploymentParentInterleaving(t *testing.T, mutation string) {
	f := newDeploymentLockFixture(t)
	now := time.Now().UTC()
	op, child, err := f.pending(f.ctx, now, 28731)
	if err != nil || child.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatal("genuine pending deployment identity", child.State, err)
	}
	if mutation != "confirmation" {
		op, child, err = f.confirm(f.ctx, op, child, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.run(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
			return channeldelivery.LockDeploymentTransportTx(ctx, tx, deploymentFixtureParent(op, child), true)
		}); err != nil {
			t.Fatal("current deployment parent positive", err)
		}
	}
	parent := deploymentFixtureParent(op, child)
	beforeChild := child
	cut := &deploymentLockCut{parent: op.OperationID, guardReady: make(chan struct{}), mutationReady: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	var release sync.Once
	unblock := func() { release.Do(func() { close(cut.release) }) }
	var workers sync.WaitGroup
	defer func() { unblock(); cancel(); workers.Wait() }()
	guardDone, mutationDone := make(chan error, 1), make(chan error, 1)
	work := context.WithValue(ctx, deploymentLockKey{}, cut)
	workers.Add(1)
	go func() {
		defer workers.Done()
		guardDone <- f.run(context.WithValue(work, deploymentLockRole{}, "guard"), func(ctx context.Context, tx *sql.Tx) error {
			return channeldelivery.LockDeploymentTransportTx(ctx, tx, parent, true)
		})
	}()
	select {
	case <-cut.guardReady:
	case err := <-guardDone:
		t.Fatal("guard missed parent-before-principal barrier", err)
	case <-ctx.Done():
		t.Fatal("guard missed parent-before-principal barrier")
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		mutating := context.WithValue(work, deploymentLockRole{}, "mutation")
		mutationDone <- f.compete(mutating, mutation, op, child, now)
	}()
	select {
	case <-cut.mutationReady:
	case err := <-mutationDone:
		t.Fatal("mutation missed its exact parent lock", err)
	case <-ctx.Done():
		t.Fatal("mutation missed its exact parent lock")
	}
	unblock()
	if err := <-guardDone; err == nil || !strings.Contains(err.Error(), "deployment transport parent is no longer current") {
		t.Fatal("pending/withdrawn deployment parent was not refused", err)
	}
	if err := <-mutationDone; err != nil {
		t.Fatal("contending lifecycle owner did not finish", err)
	}
	workers.Wait()
	if len(cut.queries) < 3 || !strings.Contains(cut.queries[0], "FROM deployment_channel_operations") ||
		!strings.Contains(cut.queries[0], "FOR UPDATE") || !principalFenceQuery(cut.queries[2]) {
		t.Fatal("deployment parent/principal lock prefix changed", cut.queries)
	}
	children, err := f.selected.ListOperatorChannelOperations(f.ctx, f.principal)
	if err != nil {
		t.Fatal(err)
	}
	for _, retained := range children {
		if mutation != "confirmation" && retained.OperationID == beforeChild.OperationID && !reflect.DeepEqual(retained, beforeChild) {
			t.Fatal("retirement changed immutable original child")
		}
	}
	if mutation != "confirmation" {
		if err := f.run(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
			return channeldelivery.LockDeploymentTransportTx(ctx, tx, parent, true)
		}); err == nil {
			t.Fatal("retired parent recovered authority after lifecycle completion")
		}
	}
}

func (f deploymentLockFixture) compete(ctx context.Context, mutation string, op domain.Operation, child operatorchannel.Operation, now time.Time) error {
	if mutation == "confirmation" {
		_, _, err := f.confirm(ctx, op, child, now)
		return err
	}
	retired, err := f.service.LogoutDeployment(ctx, op.OperationID, op.Revision)
	if err != nil {
		return err
	}
	if !retired.Operation.LogoutCompleted || retired.Operation.Phase != domain.PhaseRetired {
		return fmt.Errorf("logout did not retire its actual deployment parent")
	}
	if mutation != "logout_then_connect" {
		return nil
	}
	next, nextChild, err := f.pending(ctx, now.Add(time.Second), 28732)
	if err != nil {
		return err
	}
	next, nextChild, err = f.confirm(ctx, next, nextChild, now.Add(time.Second))
	if err != nil {
		return err
	}
	if next.OperationID == op.OperationID || nextChild.OperationID == child.OperationID {
		return fmt.Errorf("replacement reused retired parent/child")
	}
	return f.run(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return channeldelivery.LockDeploymentTransportTx(ctx, tx, deploymentFixtureParent(next, nextChild), true)
	})
}
