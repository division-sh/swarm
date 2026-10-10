package channelonboarding_test

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
	"github.com/division-sh/swarm/internal/packs"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	onboardingowner "github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Native SQL fixtures deliberately retain a chooser whose draft has ceased to
// be authoritative. Its authentic current receipt still enters the real
// callback mutation guard: stale draft selection must refuse, not deadlock.
func TestChannelCallbackPublicationLockOrderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, verb := range []domain.Verb{domain.VerbReconnect, domain.VerbRebind} {
			t.Run(backend+"/"+string(verb), func(t *testing.T) {
				f := newCallbackLockFixture(t, backend)
				now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
				predecessor := f.prepare(t, domain.VerbConnect, now)
				_, activation, err := f.selected.PublishConnectedChannelActivation(context.Background(), publishCallbackRequest(predecessor, now))
				if err != nil {
					t.Fatal(err)
				}
				f.bind(t, predecessor, now)
				f.finish(t, predecessor.OperationID, now)
				action, _ := f.chooser(t, predecessor, activation, now)
				if err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
					resolved, found, err := channeldelivery.ResolveActionFactTx(ctx, tx, action.ActionFact, f.postgres)
					if err != nil {
						return err
					}
					if !found || !resolved.CurrentRender || resolved.Action.Kind != "select_draft" || resolved.ActivationID != activation.ActivationID {
						return fmt.Errorf("fixture does not reach current callback authority: %#v", resolved)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				successor := f.prepare(t, verb, now.Add(time.Minute))
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				var release sync.Once
				unblock := func() { release.Do(func() { close(f.probe.release) }) }
				var workers sync.WaitGroup
				defer func() { unblock(); cancel(); workers.Wait() }()
				callbackDone := make(chan error, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					callbackDone <- f.run(context.WithValue(ctx, callbackLockRole{}, "callback"), func(ctx context.Context, tx *sql.Tx) error {
						_, _, _, err := channeldelivery.RequireChosenInputDraftTx(ctx, tx, action, now, true, f.postgres)
						return err
					})
				}()
				var principalFirst bool
				select {
				case principalFirst = <-f.probe.callbackReady:
				case err := <-callbackDone:
					t.Fatalf("callback missed native barrier: %v", err)
				case <-ctx.Done():
					t.Fatal("callback missed native barrier")
				}
				publicationDone := make(chan error, 1)
				request := publishCallbackRequest(successor, now.Add(time.Minute))
				if !f.postgres {
					unblock()
					assertCallbackRefusal(t, <-callbackDone)
				}
				workers.Add(1)
				go func() {
					defer workers.Done()
					_, _, err := f.selected.PublishConnectedChannelActivation(context.WithValue(ctx, callbackLockRole{}, "publisher"), request)
					publicationDone <- err
				}()
				select {
				case <-f.probe.publisherReady:
				case err := <-publicationDone:
					t.Fatalf("publisher missed principal barrier: %v", err)
				case <-ctx.Done():
					t.Fatal("publisher missed principal barrier")
				}
				unblock()
				if f.postgres {
					assertCallbackRefusal(t, <-callbackDone)
				}
				if err := <-publicationDone; err != nil {
					t.Errorf("publication: %v", err)
				}
				if !principalFirst {
					t.Error("callback locked activation before principal")
				}
				current, err := f.selected.ListCurrentConnectedChannelActivations(context.Background())
				if err != nil || len(current) != 1 || current[0].ActivationID != request.ActivationID || current[0].Revision != activation.Revision+1 {
					t.Fatalf("exact successor = %#v, err=%v", current, err)
				}
				if err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
					state, err := channeldelivery.RequireActionIntentTx(ctx, tx, action, f.postgres, false)
					if err == nil && state != "pending" {
						return fmt.Errorf("refused callback mutated its intent: %s", state)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func assertCallbackRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil || (!strings.Contains(err.Error(), "draft choice is no longer current") && !strings.Contains(err.Error(), "draft choice no longer names a current draft")) {
		t.Errorf("expected semantic stale callback refusal, got %v", err)
	}
}

func TestChannelActionMutationFencesAndReadOnlyPreviewsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"card", "skip", "choice"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				f := newCallbackLockFixture(t, backend)
				now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
				op := f.prepare(t, domain.VerbConnect, now)
				_, activation, err := f.selected.PublishConnectedChannelActivation(context.Background(), publishCallbackRequest(op, now))
				if err != nil {
					t.Fatal(err)
				}
				f.bind(t, op, now)
				f.finish(t, op.OperationID, now)
				action, _ := f.chooser(t, op, activation, now)
				for _, lock := range []bool{false, true} {
					observed := make(chan bool, 1)
					ctx := context.WithValue(context.Background(), callbackOrderObservation{}, observed)
					err := f.run(ctx, func(ctx context.Context, tx *sql.Tx) error {
						switch kind {
						case "card":
							return channeldelivery.RequireCardActionTx(ctx, tx, action.ActionFact,
								render.CardActionDemand{CardID: uuid.NewString(), PrincipalID: f.principal, Method: "mailbox.decide", Verdict: "accept", ReceiptOperationID: uuid.NewString(), RenderHash: "hash"}, f.postgres, lock)
						case "skip":
							_, _, _, err := channeldelivery.RequireCurrentSkipActionTx(ctx, tx, action, now, lock, f.postgres)
							return err
						default:
							_, _, _, err := channeldelivery.RequireChosenInputDraftTx(ctx, tx, action, now, lock, f.postgres)
							return err
						}
					})
					// A chooser is not a card/skip control and its retained choice no
					// longer names a draft. All three remain precise refusals.
					if err == nil {
						t.Fatal("invalid action was admitted")
					}
					select {
					case principalFirst := <-observed:
						if principalFirst != lock {
							t.Errorf("mutation=%t principal-before-action=%t", lock, principalFirst)
						}
					default:
						t.Fatal("guard did not reach its real native query")
					}
				}
			})
		}
	}
}

func TestNeutralChannelMutationPrincipalFirstBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, path := range []string{"quoted_control", "inbox", "text_response"} {
			t.Run(backend+"/"+path, func(t *testing.T) {
				f := newCallbackLockFixture(t, backend)
				now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
				op := f.prepare(t, domain.VerbConnect, now)
				_, activation, err := f.selected.PublishConnectedChannelActivation(context.Background(), publishCallbackRequest(op, now))
				if err != nil {
					t.Fatal(err)
				}
				f.bind(t, op, now)
				f.finish(t, op.OperationID, now)
				text := operatorchannel.InboundText{TextFact: operatorchannel.TextFact{
					Interface: op.Interface, ExternalAccountRef: "account", ConversationRef: "callback-chat",
					ConversationScope: operatorchannel.ConversationScopeDirect, Text: "Open inbox", MessageReference: `{"id":94}`,
				}, Provider: "telegram", ProviderEventID: uuid.NewString(), PublicationID: uuid.NewString(), ProviderAuthorization: "verified-pack"}
				if path == "quoted_control" {
					action, label := f.chooser(t, op, activation, now)
					text.Text, text.ReplyToReference = label, action.MessageReference
				} else if path == "inbox" {
					text.EntryReference = render.TextReplyInboxReference
				}
				var entry render.ResolvedInboxEntry
				if err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
					if err := channeldelivery.InsertTextIntentTx(ctx, tx, text, now, f.postgres); err != nil {
						return err
					}
					if path == "inbox" {
						var found bool
						entry, found, err = channeldelivery.ResolveCurrentInboxEntryTx(ctx, tx, text, f.postgres)
						if err == nil && (!found || entry.Kind != render.InboxEntryTextReply) {
							return fmt.Errorf("fixture does not reach neutral inbox authority")
						}
						return err
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				queries := &neutralQueryObservation{}
				ctx := context.WithValue(context.Background(), neutralLockOrderObservation{}, queries)
				if err := f.run(ctx, func(ctx context.Context, tx *sql.Tx) error {
					switch path {
					case "quoted_control":
						action, found, _, err := channeldelivery.AdmitReplyActionTx(ctx, tx, text, f.postgres)
						if err == nil && (!found || action.Fact.Kind != operatorchannel.ActionSourceReply || action.Fact.TextSource != text.TextFact) {
							return fmt.Errorf("fixture did not transfer its exact reply control")
						}
						return err
					case "inbox":
						_, _, err := channeldelivery.PlanInboxResponseTx(ctx, tx, text, entry, "Inbox", f.postgres)
						return err
					default:
						_, _, err := channeldelivery.PlanTextResponseTx(ctx, tx, text, "Inbox", "teaching", f.postgres)
						return err
					}
				}); err != nil {
					t.Fatal(err)
				}
				principal, mutation := false, false
				queries.Lock()
				defer queries.Unlock()
				for _, query := range queries.queries {
					if strings.Contains(query, "operator_principal_singleton") || strings.Contains(query, "SELECT principal_id FROM operator_principals") {
						principal = true
						continue
					}
					trimmed := strings.TrimSpace(query)
					if strings.Contains(query, "FOR UPDATE") || strings.HasPrefix(trimmed, "INSERT ") || strings.HasPrefix(trimmed, "UPDATE ") || strings.HasPrefix(trimmed, "DELETE ") {
						mutation = true
						if !principal {
							t.Fatalf("neutral mutation locked or wrote before principal fence: %s", query)
						}
					}
				}
				if !principal || !mutation {
					t.Fatalf("native ordering proof did not reach both fences: principal=%t mutation=%t", principal, mutation)
				}
			})
		}
	}
}

func TestNativeInboxAttachmentPublicationLockOrderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, verb := range []domain.Verb{domain.VerbReconnect, domain.VerbRebind} {
			t.Run(backend+"/"+string(verb), func(t *testing.T) {
				f := newCallbackLockFixture(t, backend)
				now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
				predecessor := f.prepare(t, domain.VerbConnect, now)
				_, activation, err := f.selected.PublishConnectedChannelActivation(context.Background(), publishCallbackRequest(predecessor, now))
				if err != nil {
					t.Fatal(err)
				}
				f.bind(t, predecessor, now)
				f.finish(t, predecessor.OperationID, now)
				hash, err := channelnative.EntryContractHash(activation.Coordinate.PlanGeneration)
				if err != nil {
					t.Fatal(err)
				}
				admission := channelnative.Admission{
					Provider: activation.Provider, ResourceSlotID: "telegram:lock-proof", ConversationReference: activation.ConversationRef,
					PrincipalID: activation.PrincipalID, InterfaceKey: activation.Interface.Key(), BindingRevision: activation.BindingRevision,
					ActivationID: activation.ActivationID, ActivationRevision: activation.Revision,
					ContextPublicationGeneration: int64(activation.Coordinate.ContextPublicationGeneration),
					PackID:                       activation.Interface.ChannelPackID, PackVersion: activation.Interface.ChannelPackVersion,
					PackManifestHash: activation.Interface.ChannelManifestHash, PlanGeneration: activation.Coordinate.PlanGeneration, EntryContractHash: hash,
				}
				successor := f.prepare(t, verb, now.Add(time.Minute))
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				var release sync.Once
				unblock := func() { release.Do(func() { close(f.probe.release) }) }
				var workers sync.WaitGroup
				defer func() { unblock(); cancel(); workers.Wait() }()
				attachmentDone := make(chan error, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					attachmentDone <- f.run(context.WithValue(ctx, callbackLockRole{}, "callback"), func(ctx context.Context, tx *sql.Tx) error {
						_, _, err := channeldelivery.AttachNativeInboxSettingTx(ctx, tx, admission, f.postgres)
						return err
					})
				}()
				var principalFirst bool
				select {
				case principalFirst = <-f.probe.callbackReady:
				case err := <-attachmentDone:
					t.Fatalf("attachment missed native barrier: %v", err)
				case <-ctx.Done():
					t.Fatal("attachment missed native barrier")
				}
				if !f.postgres {
					unblock()
					if err := <-attachmentDone; err != nil {
						t.Errorf("current SQLite attachment: %v", err)
					}
				}
				publicationDone := make(chan error, 1)
				request := publishCallbackRequest(successor, now.Add(time.Minute))
				workers.Add(1)
				go func() {
					defer workers.Done()
					_, _, err := f.selected.PublishConnectedChannelActivation(context.WithValue(ctx, callbackLockRole{}, "publisher"), request)
					publicationDone <- err
				}()
				select {
				case <-f.probe.publisherReady:
				case err := <-publicationDone:
					t.Fatalf("publisher missed principal barrier: %v", err)
				case <-ctx.Done():
					t.Fatal("publisher missed principal barrier")
				}
				unblock()
				if f.postgres {
					if err := <-attachmentDone; err == nil || !strings.Contains(err.Error(), "native inbox activation is not exact-current") {
						t.Errorf("stale attachment refusal: %v", err)
					}
				}
				if err := <-publicationDone; err != nil {
					t.Errorf("publication: %v", err)
				}
				if !principalFirst {
					t.Error("native attachment locked activation before principal")
				}
				current, err := f.selected.ListCurrentConnectedChannelActivations(context.Background())
				if err != nil || len(current) != 1 || current[0].ActivationID != request.ActivationID {
					t.Fatalf("successor: %#v, %v", current, err)
				}
			})
		}
	}
}

type callbackLockRole struct{}
type callbackOrderObservation struct{}
type neutralLockOrderObservation struct{}
type neutralQueryObservation struct {
	sync.Mutex
	queries []string
}
type callbackLockProbe struct {
	driver                      driver.Driver
	dsn                         string
	callbackReady               chan bool
	publisherReady              chan struct{}
	release                     chan struct{}
	callbackOnce, publisherOnce sync.Once
}

func (p *callbackLockProbe) Driver() driver.Driver { return p.driver }
func (p *callbackLockProbe) Connect(context.Context) (driver.Conn, error) {
	c, err := p.driver.Open(p.dsn)
	if err != nil {
		return nil, err
	}
	return &callbackLockConn{Conn: c, probe: p}, nil
}

type callbackLockConn struct {
	driver.Conn
	probe *callbackLockProbe
}

func (c *callbackLockConn) before(ctx context.Context, query string) {
	if observed, ok := ctx.Value(neutralLockOrderObservation{}).(*neutralQueryObservation); ok {
		observed.Lock()
		observed.queries = append(observed.queries, query)
		observed.Unlock()
	}
	if observed, ok := ctx.Value(callbackOrderObservation{}).(chan bool); ok {
		principal := strings.Contains(query, "operator_principal_singleton") || strings.Contains(query, "SELECT principal_id FROM operator_principals")
		if principal || strings.Contains(query, "FROM channel_delivery_actions action") {
			select {
			case observed <- principal:
			default:
			}
		}
	}
	if ctx.Value(callbackLockRole{}) == "callback" && strings.Contains(query, "operator_principal_singleton") {
		c.probe.callbackOnce.Do(func() { c.probe.callbackReady <- true; <-c.probe.release })
	}
	if ctx.Value(callbackLockRole{}) == "callback" && strings.Contains(query, "SELECT principal_id FROM operator_principals") {
		c.probe.callbackOnce.Do(func() { c.probe.callbackReady <- true; <-c.probe.release })
	}
}
func (c *callbackLockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.before(ctx, query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *callbackLockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.before(ctx, query)
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return rows, err
	}
	if ctx.Value(callbackLockRole{}) == "callback" && (strings.Contains(query, "FROM channel_delivery_actions action") || strings.Contains(query, "FROM connected_channel_activations a")) {
		c.probe.callbackOnce.Do(func() { c.probe.callbackReady <- false; <-c.probe.release })
	}
	if ctx.Value(callbackLockRole{}) == "publisher" && strings.Contains(query, "SELECT principal_id FROM operator_principals") {
		c.probe.publisherOnce.Do(func() { close(c.probe.publisherReady); <-c.probe.release })
	}
	return rows, nil
}
func (c *callbackLockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if native, ok := c.Conn.(driver.ConnBeginTx); ok {
		return native.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}

type callbackSelected interface {
	domain.Store
	operatorchannel.Store
}
type callbackOnboardingStore interface{ domain.Store }
type callbackIdentityStore interface{ operatorchannel.Store }
type callbackSelectedOwners struct {
	callbackOnboardingStore
	callbackIdentityStore
}
type callbackLockFixture struct {
	selected  callbackSelected
	run       func(context.Context, func(context.Context, *sql.Tx) error) error
	settle    func(context.Context, *sql.Tx, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
	postgres  bool
	principal string
	probe     *callbackLockProbe
}

func newCallbackLockFixture(t *testing.T, backend string) callbackLockFixture {
	t.Helper()
	var original *sql.DB
	var dsn string
	var err error
	if backend == "postgres" {
		dsn, original, _ = testutil.StartPostgres(t)
	} else {
		dsn = "file:" + filepath.Join(t.TempDir(), "callback.db") + "?_pragma=foreign_keys(ON)&_pragma=journal_mode(WAL)"
		original, err = sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = original.Close() })
	}
	probe := &callbackLockProbe{driver: original.Driver(), dsn: dsn, callbackReady: make(chan bool, 1), publisherReady: make(chan struct{}), release: make(chan struct{})}
	db := sql.OpenDB(probe)
	t.Cleanup(func() { _ = db.Close() })
	f := callbackLockFixture{postgres: backend == "postgres", probe: probe}
	if f.postgres {
		b, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewPostgres(b)
		if err != nil {
			t.Fatal(err)
		}
		bootstrapCallbackSchema(t, schema)
		onboarding, err := onboardingowner.NewPostgres(b, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := identityowner.NewPostgres(b, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		f.selected, f.run, f.settle = callbackSelectedOwners{onboarding, identity}, b.RunTransaction, identity.SettleInboundClaimTx
	} else {
		b, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewSQLiteWithBackend(b, dsn)
		if err != nil {
			t.Fatal(err)
		}
		bootstrapCallbackSchema(t, schema)
		onboarding, err := onboardingowner.NewSQLite(b, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := identityowner.NewSQLite(b, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		f.selected, f.settle = callbackSelectedOwners{onboarding, identity}, identity.SettleInboundClaimTx
		f.run = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
			return b.RunTransaction(ctx, "callback publication proof", fn)
		}
	}
	p, err := f.selected.EnsureOperatorPrincipal(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	f.principal = p.ID
	return f
}
func bootstrapCallbackSchema(t *testing.T, schema schemastore.SchemaBootstrapper) {
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
		PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{SwarmVersion: "callback-lock-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
}
func (f callbackLockFixture) prepare(t *testing.T, verb domain.Verb, now time.Time) domain.Operation {
	t.Helper()
	generation, err := plangeneration.FromCanonicalValue(map[string]string{"test": "callback-lock"})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	op, err := f.selected.ReserveChannelOnboarding(context.Background(), domain.StartRequest{
		OperationID: id, PrincipalID: f.principal, RequestKeyHash: id, RequestHash: id, Verb: verb, Provider: "telegram",
		Interface:      operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel", ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan"},
		Coordinate:     domain.ChannelRuntimeContextCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "callback-test", PackInventoryGeneration: "sha256:inventory", RuntimeInstanceID: "d0df13de-f14d-44cb-b180-17d7b812da2a", ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1},
		TargetSelector: "ingress:channel/flow:telegram", Posture: domain.ActivationWebhookRegistration, Ceremony: domain.CeremonyAuthenticatedTextChallenge, RequestedAt: now,
		CredentialReservations: []domain.CredentialReservation{{Role: "bot_token", StoreKey: "channel.token"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	identityID := uuid.NewString()
	for _, phase := range []domain.Phase{domain.PhaseCredentialsAdmitted, domain.PhaseActivatingProvider, domain.PhaseAwaitingExternalIdentity, domain.PhaseAwaitingOperatorConfirmation, domain.PhasePublishingActivation} {
		req := domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now}
		if phase == domain.PhaseCredentialsAdmitted {
			req.CredentialAdmissions = []domain.CredentialAdmission{{Role: "bot_token", StoreKey: "channel.token", Kind: domain.CredentialAdmissionWritten, Receipt: id, ValueSeal: callbackCredential().Seal}}
			req.ReplaceCredentialAdmissions = true
		}
		if phase == domain.PhaseAwaitingExternalIdentity {
			req.IdentityOperationID = identityID
		}
		if phase == domain.PhaseAwaitingOperatorConfirmation || phase == domain.PhasePublishingActivation {
			req.BindingRevision = 1
		}
		op, err = f.selected.AdvanceChannelOnboarding(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}
	return op
}
func callbackCredential() credentials.ValueEvidence {
	return credentials.ValueEvidence{Key: "channel.token", Seal: credentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("a", 64))}
}
func publishCallbackRequest(op domain.Operation, now time.Time) domain.PublishActivationRequest {
	return domain.PublishActivationRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(), BindingRevision: 1, ConversationRef: "callback-chat", Now: now}
}
func (f callbackLockFixture) bind(t *testing.T, op domain.Operation, now time.Time) {
	t.Helper()
	binding, err := f.selected.BeginChannelBinding(context.Background(), operatorchannel.BeginRequest{OperationID: op.IdentityOperationID, Kind: operatorchannel.OperationConnect, PrincipalID: f.principal, Interface: op.Interface, RequestKeyHash: op.IdentityOperationID, RequestHash: op.IdentityOperationID, ProviderAuthority: operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthorityCredential, Credential: callbackCredential()}, RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL)})
	if err != nil {
		t.Fatal(err)
	}
	claim := operatorchannel.InboundClaim{TextFact: operatorchannel.TextFact{Interface: op.Interface, ExternalAccountRef: "account", ConversationRef: "callback-chat", ConversationScope: operatorchannel.ConversationScopeDirect, Text: binding.Challenge, MessageReference: `{"id":1}`}, Provider: "telegram", ProviderEventID: uuid.NewString(), PublicationID: uuid.NewString(), ProviderAuthorization: "verified-pack", Challenge: binding.Challenge}
	var claimed operatorchannel.ClaimSettlement
	err = f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		claimed, err = f.settle(ctx, tx, claim, now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.selected.ConfirmChannelBinding(context.Background(), operatorchannel.ConfirmRequest{OperationID: binding.OperationID, PrincipalID: f.principal, ExpectedRevision: claimed.Operation.Revision, Approve: true, ProviderAuthorityCurrent: true, ConfirmedAt: now})
	if err != nil {
		t.Fatal(err)
	}
}
func (f callbackLockFixture) finish(t *testing.T, id string, now time.Time) {
	t.Helper()
	op, err := f.selected.GetChannelOnboarding(context.Background(), id)
	if err != nil {
		t.Fatalf("operation: %v", err)
	}
	for _, phase := range []domain.Phase{domain.PhasePromotingRegistration, domain.PhaseRetiringPredecessor, domain.PhaseDeliveringConfirmation, domain.PhaseSucceeded} {
		op, err = f.selected.AdvanceChannelOnboarding(context.Background(), domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func (f callbackLockFixture) chooser(t *testing.T, op domain.Operation, activation domain.ConnectedChannelActivation, now time.Time) (operatorchannel.InboundAction, string) {
	t.Helper()
	audience := render.Audience{PrincipalID: f.principal, InterfaceKey: op.Interface.Key(), DeliveryEpoch: 1, ExternalAccountRef: "account", ConversationRef: "callback-chat", ConversationScope: operatorchannel.ConversationScopeDirect}
	frozen, err := render.FreezeDraftChooser(uuid.NewString(), uuid.NewString(), []render.DraftChoice{{DraftID: uuid.NewString(), CardID: uuid.NewString(), Label: "First"}, {DraftID: uuid.NewString(), CardID: uuid.NewString(), Label: "Second"}}, audience)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err = render.WithPresentation(frozen, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}, 0)
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, renderID, effectID, attemptID, surfaceID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	var actions []render.Action
	err = f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		// These rows qualify native relational locking, not effect settlement. The
		// supported card/effect journey is independently retained in the public tests.
		queries := []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO channel_delivery_plans(delivery_id,source_kind,source_id,request_activation_id,principal_id,interface_key,binding_revision,delivery_epoch,external_account_reference,conversation_reference,conversation_scope,state,current_render_id,current_receipt_operation_id,created_at) VALUES (?,'response',?,?,?,?,1,1,'account','callback-chat','direct','sent',?,?,?)`, []any{deliveryID, frozen.SourceID, activation.ActivationID, f.principal, op.Interface.Key(), renderID, effectID, now}},
			{`INSERT INTO channel_delivery_renders(render_id,delivery_id,source_revision,projection_version,render_input,render_hash,created_at) VALUES (?,?,1,?,?,?,?)`, []any{renderID, deliveryID, render.ProjectionVersion, string(frozen.Input), frozen.Hash, now}},
			{`INSERT INTO runtime_external_effect_operations(operation_id,effect_kind,effect_class,execution_mode,bundle_hash,authority_kind,authority_id,generation,request_fingerprint,state,created_at,updated_at) VALUES (?,'channel_delivery','write_or_unknown','live',?,'channel_delivery',?,1,'native-lock-fixture','settled',?,?)`, []any{effectID, op.Coordinate.BundleHash, effectID, now, now}},
			{`INSERT INTO managed_agent_capability_surfaces(surface_id,integrity_hash,authority_kind,authority_id,execution_kind,execution_authority_id,actor_id,provider,transport,surface) VALUES (?,?,'startup_probe',?,'normal_agent','native-lock-fixture','native-lock-fixture','telegram','api','{}')`, []any{surfaceID, surfaceID, uuid.NewString()}},
			{`INSERT INTO runtime_external_effect_attempts(attempt_id,operation_id,attempt_ordinal,adapter,transport,execution_mode,generation,execution_owner,lease_expires_at,fence_generation,capability_surface_id,state,authorized_at) VALUES (?,?,1,'telegram','api','live',1,'native-lock-fixture',?,1,?,'settled',?)`, []any{attemptID, effectID, now.Add(time.Minute), surfaceID, now}},
			{`INSERT INTO channel_delivery_receipts(effect_operation_id,attempt_id,delivery_id,render_id,state,provider_reference,settled_at) VALUES (?,?,?,?,'sent','{"delivery_reference":{"id":91}}',?)`, []any{effectID, attemptID, deliveryID, renderID, now}},
		}
		for _, q := range queries {
			query := q.sql
			if f.postgres {
				for i := range q.args {
					query = strings.Replace(query, "?", fmt.Sprintf("$%d", i+1), 1)
				}
			}
			if _, err := tx.ExecContext(ctx, query, q.args...); err != nil {
				return err
			}
		}
		var err error
		actions, err = channeldelivery.EnsureRenderActionsTx(ctx, tx, renderID, frozen, f.postgres)
		if err != nil {
			return err
		}
		text := operatorchannel.InboundText{TextFact: operatorchannel.TextFact{
			Interface: op.Interface, ExternalAccountRef: "account", ConversationRef: "callback-chat",
			ConversationScope: operatorchannel.ConversationScopeDirect, Text: "retained answer", MessageReference: `{"id":92}`,
		}, Provider: "telegram", ProviderEventID: uuid.NewString(), PublicationID: frozen.DraftChooser.TextPublicationID,
			ProviderAuthorization: "verified-pack"}
		if err := channeldelivery.InsertTextIntentTx(ctx, tx, text, now, f.postgres); err != nil {
			return err
		}
		return channeldelivery.SettleTextIntentTx(ctx, tx, text, "chooser", f.postgres)
	})
	if err != nil {
		t.Fatal(err)
	}
	action := operatorchannel.InboundAction{ActionFact: operatorchannel.ActionFact{Kind: operatorchannel.ActionSourceCallback, Interface: op.Interface, ExternalAccountRef: "account", ConversationRef: "callback-chat", ConversationScope: operatorchannel.ConversationScopeDirect, MessageReference: `{"id":91}`, InteractionRef: "callback-lock", Token: actions[0].Token}, Provider: "telegram", ProviderEventID: uuid.NewString(), PublicationID: uuid.NewString(), ProviderAuthorization: "verified-pack"}
	if err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		return channeldelivery.InsertActionIntentTx(ctx, tx, action, now, f.postgres)
	}); err != nil {
		t.Fatal(err)
	}
	return action, actions[0].Label
}
