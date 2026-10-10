//go:build linux || darwin

package sessionprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
)

type runtimeBootstrapFixture struct {
	t          *testing.T
	ctx        context.Context
	store      sessionInputSelectedStore
	peer       *sdkPeer
	owner      *worklifetime.RuntimeOccurrence
	directory  string
	candidate  channelonboarding.Candidate
	principal  operatorchannel.Principal
	service    *channelonboarding.Service
	channels   *operatorchannel.Service
	connection *RuntimeConnection
}

func newRuntimeBootstrapFixture(t *testing.T, backend string) *runtimeBootstrapFixture {
	t.Helper()
	f := &runtimeBootstrapFixture{t: t, directory: t.TempDir()}
	if err := os.Chmod(f.directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if backend == "sqlite" {
		f.store = storetest.StartSQLiteRuntimeStore(t)
	} else {
		f.store = storetest.StartPostgresRuntimeStore(t)
	}
	source := sourceartifactfixture.Require(t, context.Background(), f.store)
	manifest := sessionInputManifestFixture(t)
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "1.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	plan := activeInputChannelFixture(t, catalog)
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	f.candidate = channelonboarding.Candidate{Provider: "whatsapp", Interface: identity, Plan: plan,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: source.BundleHash(), BundleIdentity: "bootstrap-fixture",
			PackInventoryGeneration: "sha256:bootstrap-fixture", RuntimeInstanceID: runtimeID, ContextPublicationGeneration: 1, PlanGeneration: generation},
		Target: channelonboarding.CandidateTarget{Selector: "ingress:.:whatsapp", FlowPath: ".", Alias: "whatsapp", Provider: "whatsapp",
			ServiceID: flowidentity.StandingServiceID("."), AdmissionGeneration: catalog.Generation()},
		Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
		ConfirmationOperation: "deliver", ConnectionHealth: "provider_connection"}
	// Database/spec construction is not part of the peer's bounded socket
	// exchange. Start its existing deadline only once those fixtures are ready.
	f.peer = newSDKPeerMode(t, false)
	process := worklifetime.NewProcess()
	f.owner, err = process.NewRuntime(f.peer.ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: source.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	f.ctx = worklifetime.WithOccurrence(correlation.WithSourceArtifactFact(f.peer.ctx, source), f.owner)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := f.owner.RetireAndWait(ctx); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	credentialStore, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	currentness, err := credentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.channels, err = operatorchannel.NewService(f.store, proofs, currentness, []operatorchannel.InterfaceIdentity{identity}, runtimeID)
	if err != nil {
		t.Fatal(err)
	}
	f.principal, _, err = f.channels.Bootstrap(f.ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := channelonboarding.NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{f.candidate})
	if err != nil {
		t.Fatal(err)
	}
	f.service, err = channelonboarding.NewService(channelonboarding.ServiceOptions{Store: f.store,
		SourceArtifacts: f.store.(sourceartifact.Reader), Identities: f.channels, Credentials: writer,
		Catalog:  func() (*channelonboarding.CandidateCatalog, error) { return candidates, nil },
		Sessions: f, Activations: f, Confirmation: f, Readiness: f})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Only runtime selection and unavailable later-effect ports are fixture inputs.
// Selected reservation, public onboarding, SDK/QR and principal admission are real.
func (f *runtimeBootstrapFixture) QualifySessionPlan(c channelonboarding.Candidate) error {
	return QualifyBootstrapPlan(c.Plan)
}
func (f *runtimeBootstrapFixture) BootstrapSession(_ context.Context, op channelonboarding.Operation, c channelonboarding.Candidate) error {
	if f.connection == nil {
		var err error
		f.connection, err = OpenRuntimeBootstrap(f.ctx, RuntimeConnectionOptions{Directory: f.directory,
			OperationID: op.OperationID, Store: f.store, Plan: c.Plan})
		if err != nil {
			return err
		}
		f.peer.attach(f.t, f.connection.state.currentOccurrence().client)
		if err := f.connection.Connect(f.ctx); err != nil {
			return err
		}
	}
	return nil
}
func (f *runtimeBootstrapFixture) ReadSessionPairing(ctx context.Context, op channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	if f.connection == nil {
		return channelonboarding.PairingReadback{Status: "not_started"}, nil
	}
	return f.connection.PairingReadback(ctx, principal)
}
func (*runtimeBootstrapFixture) RefreshChannelActivations(context.Context) error {
	return errors.New("bootstrap cannot enable a channel activation")
}
func (*runtimeBootstrapFixture) AdmitChannelTarget(context.Context, channelonboarding.Operation, channelonboarding.Candidate) (channelonboarding.Candidate, error) {
	return channelonboarding.Candidate{}, errors.New("bootstrap cannot admit a standing target")
}
func (*runtimeBootstrapFixture) PreflightChannelActivation(context.Context, channelonboarding.Operation, channelonboarding.Candidate) error {
	return errors.New("bootstrap cannot authorize executable activation")
}
func (*runtimeBootstrapFixture) RefreshChannelActivationCandidates(context.Context) error {
	return errors.New("bootstrap cannot refresh business activation")
}
func (*runtimeBootstrapFixture) PublishChannelActivation(context.Context, channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) error {
	return errors.New("bootstrap cannot publish business activation")
}
func (*runtimeBootstrapFixture) PromoteChannelRegistration(context.Context, channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) error {
	return errors.New("bootstrap cannot promote provider registration")
}
func (*runtimeBootstrapFixture) DispatchChannelConfirmation(context.Context, channelonboarding.ConfirmationRequest) (channelonboarding.ConfirmationResult, error) {
	return channelonboarding.ConfirmationResult{}, errors.New("bootstrap cannot dispatch confirmation")
}
func (*runtimeBootstrapFixture) ReconcileChannelEffectsBeforeRebind(context.Context, channelonboarding.Operation) (channelonboarding.EffectRebindDisposition, error) {
	return channelonboarding.EffectRebindDisposition{RetryAllowed: true}, nil
}
func (*runtimeBootstrapFixture) ProjectConnectedChannelReadiness(context.Context, channelonboarding.Operation, channelonboarding.Candidate) (channelonboarding.ConnectedChannelReadiness, bool, error) {
	return channelonboarding.ConnectedChannelReadiness{}, false, nil
}

func TestWhatsAppPublicBootstrapReservesBeforeQRBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			const token = "authenticated-bootstrap-token"
			handler, err := apiv1.NewHandler(apiv1.Options{PlatformSpecPath: filepath.Join("..", "..", "platform-spec.yaml"),
				AuthTokens: []string{token}, OperatorPrincipalID: f.principal.ID,
				Handlers: apiv1.ChannelOnboardingHandlers(apiv1.ChannelOnboardingHandlerOptions{Onboarding: f.service, Channels: f.channels})})
			if err != nil {
				t.Fatal(err)
			}
			call := func(method string, params map[string]any, authenticated bool) (*httptest.ResponseRecorder, map[string]any) {
				t.Helper()
				body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
				request := httptest.NewRequest(http.MethodPost, "/v1/rpc", bytes.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				if authenticated {
					request.Header.Set("Authorization", "Bearer "+token)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				var result map[string]any
				_ = json.Unmarshal(response.Body.Bytes(), &result)
				return response, result
			}
			start := map[string]any{"provider": "whatsapp", "verb": "connect", "idempotency_key": "one-bootstrap", "save_proof": false}
			if response, _ := call("channel.onboarding_start", start, false); response.Code != http.StatusUnauthorized || f.connection != nil {
				t.Fatal("unauthenticated bootstrap opened provider state", response.Body.String())
			}
			response, initial := call("channel.onboarding_start", start, true)
			if initial["error"] != nil || f.connection == nil {
				t.Fatal("public bootstrap failed", response.Body.String())
			}
			rows, err := f.store.ListChannelOnboardingOperations(f.ctx)
			if err != nil || len(rows) != 1 || rows[0].Phase != channelonboarding.PhaseActivatingProvider || rows[0].Coordinate.TargetGeneration != 0 ||
				len(rows[0].CredentialReservations) != 0 || rows[0].IdentityOperationID != "" || rows[0].ActivationRevision != 0 {
				t.Fatal("public bootstrap invented credential, identity or business authority", rows, err)
			}
			op := rows[0]
			occurrence := f.connection.state.currentOccurrence()
			if f.connection.state.directory.connectionID != op.SessionConnectionID || occurrence.pairing == nil {
				t.Fatal("bootstrap SDK did not consume the durable original connection reservation")
			}
			observed := make(chan []string, 1)
			occurrence.client.AddEventHandler(func(raw any) {
				if qr, ok := raw.(*events.QR); ok {
					observed <- qr.Codes
				}
			})
			socket := pairingSocketFixture(t, f.peer)
			if err := socket.send(f.ctx, pairingIQFixture(waBinary.Node{Tag: "pair-device", Content: []waBinary.Node{{Tag: "ref", Content: []byte("explicit-bootstrap-ref")}}})); err != nil {
				t.Fatal(err)
			}
			var codes []string
			select {
			case codes = <-observed:
			case <-f.peer.ctx.Done():
				t.Fatal("public SDK QR event did not arrive")
			}
			get := map[string]any{"operation_id": op.OperationID}
			response, readback := call("channel.onboarding_get", get, true)
			if readback["error"] != nil {
				t.Fatal("authenticated public pairing readback failed", response.Body.String())
			}
			result, ok := readback["result"].(map[string]any)
			pairing, paired := result["pairing"].(map[string]any)
			if !ok || !paired || pairing["code"] != codes[0] || pairing["paired"] != false || pairing["connected"] != false {
				t.Fatal("public readback changed QR material or granted ready identity", response.Body.String())
			}
			if response, _ := call("channel.onboarding_get", get, false); response.Code != http.StatusUnauthorized || bytes.Contains(response.Body.Bytes(), []byte(codes[0])) {
				t.Fatal("unauthenticated readback disclosed QR material")
			}
			foreign := f.principal
			foreign.ID = uuid.NewString()
			if view, err := f.connection.PairingReadback(f.ctx, foreign); err == nil || view != (channelonboarding.PairingReadback{}) {
				t.Fatal("foreign principal obtained pairing material", view, err)
			}
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			if view, err := f.connection.PairingReadback(canceled, f.principal); err == nil || view != (channelonboarding.PairingReadback{}) {
				t.Fatal("canceled caller obtained pairing material", view, err)
			}
			response, again := call("channel.onboarding_start", start, true)
			if again["error"] != nil || f.connection.state.currentOccurrence() != occurrence {
				t.Fatal("public request replay repeated provider connection", response.Body.String())
			}
			if _, err := f.connection.ChannelExecution(f.ctx); err == nil {
				t.Fatal("QR bootstrap granted native channel execution")
			}
			if _, err := f.connection.AdmitSessionAccount(f.ctx, rows[0].SessionAccount); err == nil {
				t.Fatal("QR bootstrap granted native account admission")
			}
		})
	}
}

func TestWhatsAppBootstrapRequiresReservationBeforeStateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			id, now := uuid.NewString(), time.Now().UTC()
			op, err := f.store.ReserveChannelOnboarding(f.ctx, channelonboarding.StartRequest{
				OperationID: id, RequestKeyHash: id, RequestHash: id, PrincipalID: f.principal.ID,
				Verb: channelonboarding.VerbConnect, Provider: f.candidate.Provider, Interface: f.candidate.Interface,
				Coordinate: f.candidate.Coordinate, TargetSelector: f.candidate.Target.Selector,
				Posture: f.candidate.Posture, Ceremony: f.candidate.Ceremony, RequestedAt: now})
			if err != nil {
				t.Fatal(err)
			}
			opts := RuntimeConnectionOptions{Directory: f.directory, OperationID: op.OperationID, Store: f.store, Plan: f.candidate.Plan}
			assertNoState := func() {
				t.Helper()
				entries, err := os.ReadDir(f.directory)
				if err != nil || len(entries) != 0 {
					t.Fatal("rejected constructor opened private state", entries, err)
				}
			}
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePreparing, channelonboarding.PhaseCredentialsAdmitted} {
				if phase != op.Phase {
					op, err = f.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
						ExpectedRevision: op.Revision, Phase: phase, Now: now})
					if err != nil {
						t.Fatal(err)
					}
				}
				if c, err := OpenRuntimeBootstrap(f.ctx, opts); !errors.Is(err, errRuntimeConnection) || c != nil {
					t.Fatal("bootstrap opened before selected pairing phase", c, err)
				}
				assertNoState()
			}
			op, err = f.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
				ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseActivatingProvider, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			for _, cell := range []string{"missing_source", "missing_runtime", "canceled", "missing_operation", "missing_plan", "paired_constructor"} {
				t.Run(cell, func(t *testing.T) {
					ctx, candidate := f.ctx, opts
					switch cell {
					case "missing_source":
						ctx = worklifetime.WithRuntimeOccurrence(f.peer.ctx, f.owner)
					case "missing_runtime":
						fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
						ctx = correlation.WithSourceArtifactFact(f.peer.ctx, fact)
					case "canceled":
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					case "missing_operation":
						candidate.OperationID = uuid.NewString()
					case "missing_plan":
						candidate.Plan = packs.SatisfactionPlan{}
					}
					open := OpenRuntimeBootstrap
					if cell == "paired_constructor" {
						open = OpenRuntimeConnection
					}
					if c, err := open(ctx, candidate); err == nil || c != nil {
						t.Fatal("incomplete owner opened session state", c, err)
					}
					assertNoState()
				})
			}
			connection, err := OpenRuntimeBootstrap(f.ctx, opts)
			if err != nil {
				t.Fatal("retained pairing responsibility could not open", err)
			}
			if duplicate, err := OpenRuntimeBootstrap(f.ctx, opts); !errors.Is(err, errSessionPossession) {
				if duplicate != nil {
					_ = duplicate.Close(f.ctx)
				}
				t.Fatal("duplicate bootstrap acquired the same private state", err)
			}
			if err := connection.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWhatsAppBootstrapReopenRetainsReservationWithoutAccountAdoptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			result, err := f.service.Start(f.ctx, channelonboarding.StartInput{Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"},
				Verb: channelonboarding.VerbConnect, IdempotencyKey: "reopen-bootstrap"})
			if err != nil {
				t.Fatal(err)
			}
			original := f.connection.state.currentOccurrence()
			headerPath := filepath.Join(f.directory, result.Operation.SessionConnectionID, "session.json")
			header, err := os.ReadFile(headerPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.connection.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if view, err := f.connection.PairingReadback(f.ctx, f.principal); err == nil || view.Code != "" {
				t.Fatal("retired bootstrap still disclosed pairing material", err)
			}
			reopened, err := OpenRuntimeBootstrap(f.ctx, RuntimeConnectionOptions{Directory: f.directory,
				OperationID: result.Operation.OperationID, Store: f.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(f.ctx); err != nil {
					t.Error(err)
				}
			}()
			successor := reopened.state.currentOccurrence()
			retainedHeader, headerErr := os.ReadFile(headerPath)
			if successor.occurrenceID == original.occurrenceID || successor.pairing == nil || successor.client.Store.ID != nil ||
				headerErr != nil || !bytes.Equal(header, retainedHeader) ||
				reopened.state.directory.connectionID != result.Operation.SessionConnectionID {
				t.Fatal("reopen replaced reservation, private header or account evidence", headerErr)
			}
			if _, err := reopened.ChannelExecution(f.ctx); err == nil {
				t.Fatal("reopened unpaired state granted execution")
			}
			retained, err := f.store.GetChannelOnboarding(f.ctx, result.Operation.OperationID)
			if err != nil || retained.Revision != result.Operation.Revision || retained.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
				t.Fatal("reopen rewrote retained account evidence", err)
			}
		})
	}
}
