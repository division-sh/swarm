//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/division-sh/swarm/internal/testutil/whatsappfixture"
	"github.com/google/uuid"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/socket"
)

type serveBootstrapTestStore interface {
	channelonboarding.Store
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
	sourceartifact.Reader
	sourceartifactfixture.Writer
}

type serveBootstrapTestFixture struct {
	ctx        context.Context
	owned      context.Context
	owner      *worklifetime.RuntimeOccurrence
	adapter    *serveSessionBootstrap
	op         channelonboarding.Operation
	candidate  channelonboarding.Candidate
	principal  operatorchannel.Principal
	selections atomic.Int64
}

func newServeBootstrapTestFixture(t *testing.T, backend string) *serveBootstrapTestFixture {
	t.Helper()
	var selected serveBootstrapTestStore
	if backend == "sqlite" {
		selected = storetest.StartSQLiteRuntimeStore(t)
	} else {
		selected = storetest.StartPostgresRuntimeStore(t)
	}
	ctx, now := context.Background(), time.Now().UTC()
	source := sourceartifactfixture.Require(t, ctx, selected)
	catalog := sessionDiscoveryCatalog(t)
	plan := packfixture.WhatsAppSessionChannel(t, filepath.Join(repoRootForTest(), defaultPlatformSpecPath), catalog)
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	runtimeID := uuid.NewString()
	owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: source.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	owned := worklifetime.WithOccurrence(correlation.WithSourceArtifactFact(ctx, source), owner)
	t.Cleanup(func() {
		wait, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(wait); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(wait); err != nil {
			t.Error(err)
		}
	})
	candidate := channelonboarding.Candidate{Provider: "whatsapp", Interface: identity, Plan: plan,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: source.BundleHash(), BundleIdentity: "serve-bootstrap-test",
			PackInventoryGeneration: "sha256:bootstrap-test", RuntimeInstanceID: runtimeID, ContextPublicationGeneration: 1, PlanGeneration: generation},
		Target: channelonboarding.CandidateTarget{Selector: "ingress:.:whatsapp", FlowPath: ".", Alias: "whatsapp", Provider: "whatsapp",
			ServiceID: flowidentity.StandingServiceID("."), AdmissionGeneration: catalog.Generation()},
		Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
		ConfirmationOperation: "deliver", ConnectionHealth: "provider_connection"}
	principal, err := selected.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	op, err := selected.ReserveChannelOnboarding(ctx, channelonboarding.StartRequest{OperationID: id,
		RequestKeyHash: id, RequestHash: id, PrincipalID: principal.ID, Verb: channelonboarding.VerbConnect,
		Provider: candidate.Provider, Interface: identity, Coordinate: candidate.Coordinate, TargetSelector: candidate.Target.Selector,
		Posture: candidate.Posture, Ceremony: candidate.Ceremony, RequestedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider} {
		op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: id,
			ExpectedRevision: op.Revision, Phase: phase, Now: now})
		if err != nil {
			t.Fatal(err)
		}
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
	f := &serveBootstrapTestFixture{ctx: ctx, owned: owned, owner: owner, op: op, candidate: candidate, principal: principal}
	f.adapter = &serveSessionBootstrap{connections: make(map[string]*serveSessionBootstrapAttempt),
		store: selected, credentials: current, directory: directory,
		selectRuntime: func(context.Context, channelonboarding.ChannelRuntimeContextCoordinate) (context.Context, func() error, error) {
			f.selections.Add(1)
			return owned, func() error { return nil }, nil
		}}
	return f
}

// Only the SDK's external TLS/Noise peer is supplied. The actual production
// adapter constructs and owns its SDK, selected reservation and runtime work.
func serveBootstrapWireFixture(t *testing.T, mode string, owner *worklifetime.RuntimeOccurrence) <-chan struct{} {
	t.Helper()
	noise := whatsappfixture.NewNoiseServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{}, 1)
	var mu sync.Mutex
	var peers []*websocket.Conn
	var workers sync.WaitGroup
	closing := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if closing {
			mu.Unlock()
			http.Error(w, "closed", http.StatusServiceUnavailable)
			return
		}
		workers.Add(1)
		mu.Unlock()
		defer workers.Done()
		if mode == "dial_failure" {
			entered <- struct{}{}
			http.Error(w, "deliberate test refusal", http.StatusServiceUnavailable)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"web.whatsapp.com"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(socket.FrameMaxSize)
		mu.Lock()
		peers = append(peers, conn)
		mu.Unlock()
		if mode == "handshake_failure" || mode == "blocked_handshake" {
			if _, err := whatsappfixture.ReadFrame(ctx, conn, socket.WAConnHeader); err != nil {
				t.Error(err)
				return
			}
			entered <- struct{}{}
			if mode == "handshake_failure" {
				_ = whatsappfixture.WriteFrame(ctx, conn, []byte{0xff})
			} else {
				<-ctx.Done()
			}
			return
		}
		wire, err := noise.Handshake(ctx, conn)
		if err != nil {
			t.Error(err)
			return
		}
		entered <- struct{}{}
		for {
			node, err := wire.Read(ctx)
			if err != nil {
				if errors.Is(err, whatsappfixture.ErrMalformedNode) {
					t.Error(err)
				}
				return
			}
			if node.Tag == "iq" {
				_ = wire.Send(ctx, waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": node.Attrs["id"], "type": "result"}})
			}
		}
	}))
	base := http.DefaultTransport
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig.ServerName = "example.com"
	dial := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "web.whatsapp.com:443" {
			return nil, fmt.Errorf("SDK test refuses external destination %q", address)
		}
		return dial.DialContext(ctx, network, server.Listener.Addr().String())
	}
	// The SDK clones the default *http.Transport at construction. Restore it
	// after all concrete clients and server workers have joined; tests are serial.
	http.DefaultTransport = transport
	t.Cleanup(func() {
		join, stop := context.WithTimeout(context.Background(), 30*time.Second)
		if _, err := owner.RetireAndWait(join); err != nil {
			t.Error("native connection join before restoring SDK trust:", err)
		}
		stop()
		mu.Lock()
		closing = true
		owned := append([]*websocket.Conn(nil), peers...)
		mu.Unlock()
		cancel()
		for _, peer := range owned {
			_ = peer.CloseNow()
		}
		server.Close()
		workers.Wait()
		transport.CloseIdleConnections()
		http.DefaultTransport = base
	})
	return entered
}

func TestServeSessionBootstrapCanceledAttemptCacheBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			selectRuntime := f.adapter.selectRuntime
			f.adapter.selectRuntime = func(ctx context.Context, coordinate channelonboarding.ChannelRuntimeContextCoordinate) (context.Context, func() error, error) {
				owned, release, err := selectRuntime(ctx, coordinate)
				cancel()
				return owned, release, err
			}
			if err := f.adapter.BootstrapSession(caller, f.op, f.candidate); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled handoff succeeded", err)
			}
			if f.adapter.connection(f.op.OperationID) != nil {
				t.Fatal("pre-construction cancellation stranded a concrete owner")
			}
			if entries, err := os.ReadDir(f.adapter.directory); err != nil || len(entries) != 0 {
				t.Fatal("canceled construction opened provider files", entries, err)
			}
			for index := 0; index < 2; index++ {
				if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); !errors.Is(err, context.Canceled) {
					t.Fatal("failed cached attempt became success or a retry", err)
				}
			}
			if _, paired, err := f.adapter.CheckpointSessionPairing(f.ctx, f.op); err == nil || paired {
				t.Fatal("failed attempt supplied a paired checkpoint", err)
			}
			if view, err := f.adapter.ReadSessionPairing(f.ctx, f.op, f.principal); err == nil || view.Code != "" {
				t.Fatal("failed attempt was read back as successful bootstrap", view, err)
			}
			if f.selections.Load() != 1 {
				t.Fatal("failed bootstrap automatically selected another attempt")
			}
		})
	}
}

func TestServeSessionBootstrapAttemptOutcomesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"dial_failure", "handshake_failure", "healthy_close", "healthy_retirement"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				f := newServeBootstrapTestFixture(t, backend)
				serveBootstrapWireFixture(t, mode, f.owner)
				first := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate)
				connection := f.adapter.connection(f.op.OperationID)
				if connection == nil {
					t.Fatal("actual adapter did not retain its concrete lifetime")
				}
				if mode == "dial_failure" || mode == "handshake_failure" {
					if first == nil {
						t.Fatal("failed SDK attempt reported success")
					}
				} else {
					if first != nil {
						t.Fatal("genuine authenticated bootstrap failed", first)
					}
					for index := 0; index < 3; index++ {
						if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err != nil || f.selections.Load() != 1 {
							t.Fatal("healthy idempotent reuse lost its original attempt", err)
						}
					}
				}
				if mode == "healthy_retirement" {
					if _, err := f.owner.RetireAndWait(f.ctx); err != nil {
						t.Fatal(err)
					}
				} else if err := connection.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				for index := 0; index < 2; index++ {
					if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err == nil {
						t.Fatal("failed or closed cached pointer reported success")
					}
				}
				if f.selections.Load() != 1 || connection.ConnectionID() != f.op.SessionConnectionID {
					t.Fatal("cache reuse retried Connect or replaced the reserved identity")
				}
				if _, paired, err := f.adapter.CheckpointSessionPairing(f.ctx, f.op); err == nil || paired {
					t.Fatal("failed or retired attempt supplied pairing authority", err)
				}
				if view, err := f.adapter.ReadSessionPairing(f.ctx, f.op, f.principal); err == nil || view.Code != "" {
					t.Fatal("failed or retired attempt supplied QR readback", view, err)
				}
			})
		}
	}
}

func TestServeSessionBootstrapPendingAttemptRetainsOwnershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			entered := serveBootstrapWireFixture(t, "blocked_handshake", f.owner)
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- f.adapter.BootstrapSession(caller, f.op, f.candidate) }()
			select {
			case <-entered:
			case err := <-finished:
				t.Fatal("SDK did not reach its pending handshake", err)
			}
			cancel()
			if err := f.adapter.BootstrapSession(caller, f.op, f.candidate); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled overlapping waiter became successful bootstrap", err)
			}
			if err := f.owner.WaitForQuiescence(caller); !errors.Is(err, context.Canceled) {
				t.Fatal("pending SDK handshake disappeared from runtime work", err)
			}
			other, err := sessionprovider.OpenRuntimeBootstrap(f.owned, sessionprovider.RuntimeConnectionOptions{
				Directory: f.adapter.directory, OperationID: f.op.OperationID, Store: f.adapter.store, Plan: f.candidate.Plan})
			if other != nil {
				if closeErr := other.Close(f.ctx); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if err == nil {
				t.Fatal("successor acquired pairing before the pending handshake joined")
			}
			// The pinned SDK's existing Noise wait is 20s; the outer safety bound
			// includes joining and never changes that SDK deadline or a test ceiling.
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("canceled original attempt lost its failure", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("SDK attempt failed to complete its bounded join")
			}
			if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err == nil || f.selections.Load() != 1 {
				t.Fatal("joined failed attempt silently retried", err)
			}
			if err := f.owner.WaitForQuiescence(f.ctx); err != nil {
				t.Fatal("joined attempt retained transient work", err)
			}
		})
	}
}
