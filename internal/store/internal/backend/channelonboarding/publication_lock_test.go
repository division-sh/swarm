package channelonboarding

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// The native driver barrier observes real publisher SQL. With the old ordering,
// PostgreSQL publication holds the activation when it reaches the INSERT's
// implicit principal FK lock, while delivery already holds that principal.
// Releasing both barriers then forces the cycle, rather than hoping to race it.
func TestConnectedChannelPublicationPrincipalFirstBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, verb := range []domain.Verb{domain.VerbReconnect, domain.VerbRebind} {
			t.Run(backend+"/"+string(verb), func(t *testing.T) {
				r, principalID, probe := publicationLockFixture(t, backend)
				now := time.Date(2026, 10, 6, 15, 0, 0, 0, time.UTC)
				predecessor := preparePublication(t, r, principalID, domain.VerbConnect, now)
				op, old, err := publishActivation(context.Background(), r, publicationRequest(predecessor, now))
				if err != nil {
					t.Fatal(err)
				}
				for _, phase := range []domain.Phase{domain.PhasePromotingRegistration, domain.PhaseRetiringPredecessor, domain.PhaseDeliveringConfirmation, domain.PhaseSucceeded} {
					op, err = advance(context.Background(), r, domain.AdvanceRequest{
						OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now,
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				successor := preparePublication(t, r, principalID, verb, now.Add(time.Minute))
				request := publicationRequest(successor, now.Add(time.Minute))
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				deliveryReady, releaseDelivery := make(chan struct{}), make(chan struct{})
				var deliveryOnce, publicationOnce sync.Once
				releaseDeliveryLock := func() { deliveryOnce.Do(func() { close(releaseDelivery) }) }
				releasePublication := func() { publicationOnce.Do(func() { close(probe.release) }) }
				release := func() {
					releaseDeliveryLock()
					releasePublication()
				}
				defer release()
				deliveryDone := make(chan error, 1)
				go func() {
					deliveryDone <- r.mutate(ctx, "channel delivery lock ordering proof", func(txctx context.Context, tx *sql.Tx) error {
						if err := channeldelivery.LockPrincipalTx(txctx, tx, principalID, r.dialect() == dialectPostgres); err != nil {
							return err
						}
						close(deliveryReady)
						<-releaseDelivery
						activation, found, err := loadActivationBySlot(txctx, tx, r.dialect(), old.SlotKey, true)
						if err == nil && (!found || activation.ActivationID != old.ActivationID) {
							return fmt.Errorf("delivery lost predecessor before releasing principal: %#v", activation)
						}
						return err
					})
				}()
				select {
				case <-deliveryReady:
				case err := <-deliveryDone:
					t.Fatalf("delivery principal fence: %v", err)
				case <-ctx.Done():
					t.Fatal("delivery did not acquire principal")
				}
				publicationDone := make(chan error, 1)
				publicationStarted := make(chan struct{})
				go func() {
					close(publicationStarted)
					_, _, err := publishActivation(context.WithValue(ctx, publicationProbeKey{}, true), r, request)
					publicationDone <- err
				}()
				<-publicationStarted
				if backend == "sqlite" {
					// SQLite's writer admission serializes the transactions before SQL.
					// Release that writer; the same driver oracle still checks its order.
					releaseDeliveryLock()
					if err := <-deliveryDone; err != nil {
						t.Fatal(err)
					}
				}
				var principalFirst bool
				select {
				case principalFirst = <-probe.arrived:
				case err := <-publicationDone:
					t.Fatalf("publication did not reach lock barrier: %v", err)
				case <-ctx.Done():
					t.Fatal("publication did not reach lock barrier")
				}
				if backend == "sqlite" {
					releasePublication()
				} else {
					release()
					if err := <-deliveryDone; err != nil {
						t.Errorf("concurrent delivery transaction: %v", err)
					}
				}
				if err := <-publicationDone; err != nil {
					t.Errorf("concurrent %s publication: %v", verb, err)
				}
				if !principalFirst {
					t.Error("publication acquired an activation before the principal fence")
				}
				current, err := listActivations(context.Background(), r)
				if err != nil || len(current) != 1 || current[0].ActivationID != request.ActivationID || current[0].Revision != old.Revision+1 {
					t.Fatalf("exact successor = %#v, err=%v", current, err)
				}
			})
		}
	}
}

type publicationProbeKey struct{}

type publicationLockConnector struct {
	driver  driver.Driver
	dsn     string
	arrived chan bool
	release chan struct{}
	once    sync.Once
}

func (p *publicationLockConnector) Driver() driver.Driver { return p.driver }
func (p *publicationLockConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := p.driver.Open(p.dsn)
	if err != nil {
		return nil, err
	}
	return &publicationLockConn{Conn: conn, probe: p}, nil
}

type publicationLockConn struct {
	driver.Conn
	probe      *publicationLockConnector
	descendant bool
}

func (c *publicationLockConn) observe(ctx context.Context, query string) {
	if ctx.Value(publicationProbeKey{}) != true {
		return
	}
	principal := strings.Contains(query, "operator_principal_singleton") || strings.Contains(query, "SELECT principal_id FROM operator_principals")
	insert := strings.Contains(query, "INSERT INTO connected_channel_activations")
	if principal || insert {
		c.probe.once.Do(func() {
			c.probe.arrived <- principal && !c.descendant
			<-c.probe.release
		})
	}
	if strings.Contains(query, "connected_channel_activations") {
		c.descendant = true
	}
}

func (c *publicationLockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.observe(ctx, query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *publicationLockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.observe(ctx, query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *publicationLockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.descendant = false
	if native, ok := c.Conn.(driver.ConnBeginTx); ok {
		return native.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}

func publicationLockFixture(t *testing.T, backend string) (runner, string, *publicationLockConnector) {
	t.Helper()
	var original *sql.DB
	var dsn string
	var err error
	if backend == "postgres" {
		dsn, original, _ = testutil.StartPostgres(t)
	} else {
		dsn = "file:" + filepath.Join(t.TempDir(), "channel.db") + "?_pragma=foreign_keys(ON)&_pragma=journal_mode(WAL)"
		original, err = sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = original.Close() })
	}
	probe := &publicationLockConnector{driver: original.Driver(), dsn: dsn, arrived: make(chan bool, 1), release: make(chan struct{})}
	db := sql.OpenDB(probe)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	var r runner
	var ensure func(context.Context, time.Time) (operatorchannel.Principal, error)
	if backend == "postgres" {
		b, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewPostgres(b)
		if err != nil {
			t.Fatal(err)
		}
		bootstrapPublicationSchema(t, schema)
		r = postgresRunner{&PostgresOwner{backend: b, requireCurrent: schema.RequireCurrent}}
		identity, err := identityowner.NewPostgres(b, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		ensure = identity.EnsureOperatorPrincipal
	} else {
		b, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewSQLiteWithBackend(b, dsn)
		if err != nil {
			t.Fatal(err)
		}
		bootstrapPublicationSchema(t, schema)
		r = sqliteRunner{&SQLiteOwner{backend: b, requireCurrent: schema.RequireCurrent}}
		identity, err := identityowner.NewSQLite(b, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		ensure = identity.EnsureOperatorPrincipal
	}
	principal, err := ensure(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return r, principal.ID, probe
}

func bootstrapPublicationSchema(t *testing.T, schema schemastore.SchemaBootstrapper) {
	t.Helper()
	spec, err := runtimecontracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.BootstrapSchema(context.Background(), schemastore.SchemaBootstrapRequest{
		PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{SwarmVersion: "reconnect-lock-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
}

func preparePublication(t *testing.T, r runner, principalID string, verb domain.Verb, now time.Time) domain.Operation {
	t.Helper()
	generation, err := plangeneration.FromCanonicalValue(map[string]string{"test": "publication-lock"})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	op, err := reserve(context.Background(), r, domain.StartRequest{
		OperationID: id, PrincipalID: principalID, RequestKeyHash: id, RequestHash: id, Verb: verb, Provider: "telegram",
		Interface: operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel", ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan"},
		Coordinate: domain.ChannelRuntimeContextCoordinate{
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "channel-test", PackInventoryGeneration: "sha256:inventory",
			RuntimeInstanceID: "d0df13de-f14d-44cb-b180-17d7b812da2a", ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1,
		},
		TargetSelector: "ingress:channel/flow:telegram", Posture: domain.ActivationWebhookRegistration,
		Ceremony: domain.CeremonyAuthenticatedTextChallenge, RequestedAt: now,
		CredentialReservations: []domain.CredentialReservation{{Role: "bot_token", StoreKey: "channel.token"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []domain.Phase{domain.PhaseCredentialsAdmitted, domain.PhaseActivatingProvider, domain.PhaseAwaitingExternalIdentity, domain.PhaseAwaitingOperatorConfirmation, domain.PhasePublishingActivation} {
		request := domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now}
		if phase == domain.PhaseCredentialsAdmitted {
			credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "channel.token", "fixture-provider-secret"); err != nil {
				t.Fatal(err)
			}
			evidence, err := runtimecredentials.SealCurrentValue(context.Background(), credentials, "channel.token")
			if err != nil {
				t.Fatal(err)
			}
			request.CredentialAdmissions = []domain.CredentialAdmission{{Role: "bot_token", StoreKey: "channel.token", Kind: domain.CredentialAdmissionWritten, Receipt: id, ValueSeal: evidence.Seal}}
			request.ReplaceCredentialAdmissions = true
		}
		if phase == domain.PhaseAwaitingOperatorConfirmation || phase == domain.PhasePublishingActivation {
			request.BindingRevision = 1
		}
		op, err = advance(context.Background(), r, request)
		if err != nil {
			t.Fatal(err)
		}
	}
	return op
}

func publicationRequest(op domain.Operation, now time.Time) domain.PublishActivationRequest {
	return domain.PublishActivationRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(), BindingRevision: 1, ConversationRef: "channel-conversation", Now: now}
}
