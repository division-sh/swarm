package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestChannelSourceLifecyclePublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, operation := range []string{"disk", "new_process", "retained_reset", "source_reset", "fork"} {
			t.Run(string(backend)+"/"+operation, func(t *testing.T) {
				previous := buildSelectedAPICapabilities
				buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
					caps, err := previous(owner, req)
					if err == nil {
						caps.RunFork = sourceForkErrorProbe{RunForkExecutor: caps.RunFork, t: t}
					}
					return caps, err
				}
				t.Cleanup(func() { buildSelectedAPICapabilities = previous })
				var deliveryReader servedRunDeliveryReader
				var receiverReader receiverProofStateReader
				var sourceReader sourceArtifactReader
				var cards decisioncard.Store
				var nativeRead func(context.Context, channelnative.TransportParent) (channelnative.Qualification, error)
				captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
					deliveryReader = p.deps.DeliveryStore.SummarizeRun
					receiverReader = requireReceiverProofStateReader(t, p.deps.EventStore)
					sourceReader = p.sourceReader
					cards = p.deps.DecisionCards
					if reader, ok := p.channelOnboarding.(interface {
						ReadNativeInboxQualificationParent(context.Context, channelnative.TransportParent) (channelnative.Qualification, error)
					}); ok {
						nativeRead = reader.ReadNativeInboxQualificationParent
					}
				})
				h, db, hash := startChannelAnchorJourney(t, backend, "source-lifecycle-token", false)
				observation := openChannelFixtureObservation(t, string(backend), h.storeDSN)
				if h.registration.DeploymentRegistration {
					if nativeRead == nil {
						t.Fatal("registered lifecycle lacks the native qualification owner")
					}
					runRegisteredSourceLifecycle(t, h, observation, backend, &sourceReader, hash, operation, receiverReader, deliveryReader, cards,
						func(ctx context.Context, parent channelnative.TransportParent) (channelnative.Qualification, error) {
							return nativeRead(ctx, parent)
						})
					return
				}
				var standingRun string
				if err := db.QueryRow(`SELECT run_id FROM runs WHERE flow_path='chat' AND landing_run=TRUE`).Scan(&standingRun); err != nil {
					t.Fatal(err)
				}
				originalStanding, err := readServedLandingRuns(t.Context(), servedTestProcessRuntime(t, h.process))
				if err != nil || len(originalStanding) != 1 || originalStanding[0].RunID != standingRun || originalStanding[0].Source.BundleHash() != hash || originalStanding[0].Scope.FlowPath != "chat" {
					t.Fatalf("source lifecycle requires its exact standing owner: statuses=%+v err=%v", originalStanding, err)
				}
				var originalHeader struct {
					Run operatorread.RunHeader `json:"run"`
				}
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.get", map[string]any{"run_id": standingRun}, &originalHeader)
				if originalHeader.Run.RunID != standingRun || originalHeader.Run.StartedAt.IsZero() {
					t.Fatalf("source lifecycle lost its original run incarnation: %+v", originalHeader.Run)
				}
				standingCard := waitChannelAnchorCard(t, db, standingRun, decisioncard.AnchorKindStageGate, "chat")
				waitChannelAnchorReceipt(t, db, standingCard)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true},
					"idempotency_key": "source-lifecycle-seed",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
				messageID := waitChannelAnchorReceipt(t, db, card)
				token, ok := telegramCallbackToken(h.provider.Delivery(messageID-1), "Approve")
				if !ok {
					t.Fatal("source-lifecycle card lacks frozen control")
				}
				waitChannelDeliverySendsSettled(t, db, backend)
				frozen := readChannelSourceHistory(t, observation, card)
				var originalSource []byte
				if err := db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&originalSource); err != nil {
					t.Fatal(err)
				}
				callback, signing, _ := h.provider.Registration()
				switch operation {
				case "disk", "new_process":
					path := filepath.Join(h.opts.SourceRoot, "chat", "reviews", "schema.yaml")
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					body = append(body, []byte("\n# Changed artifact bytes; semantic declaration remains identical.\n")...)
					if err := os.WriteFile(path, body, 0o600); err != nil {
						t.Fatal(err)
					}
					var identity apiv1.RuntimeIdentityResult
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
					if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash != hash {
						t.Fatalf("disk mutation changed admitted source: %+v", identity)
					}
					if operation == "disk" {
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
						var detail map[string]any
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}, &detail)
						postChannelSourceControl(t, callback, signing, card, token, messageID)
						waitChannelAnchorDecision(t, db, card)
					} else {
						h.stop(t)
						h.start(t)
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
						if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash == hash {
							t.Fatalf("new process did not admit changed artifact: %+v", identity)
						}
						var retained []byte
						if err := db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&retained); err != nil || !bytes.Equal(retained, originalSource) {
							t.Fatalf("changed process replaced retained source: %v", err)
						}
						callback, signing, _ = h.provider.Registration()
						if postChannelSourceControl(t, callback, signing, card, token, messageID) == http.StatusAccepted {
							waitChannelRejectedCallback(t, db, token)
						}
						var decisions int
						if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&decisions); err != nil || decisions != 0 {
							t.Fatalf("replacement admitted a predecessor control: count=%d err=%v", decisions, err)
						}
					}
				case "retained_reset", "source_reset":
					clear := operation == "source_reset"
					command := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
					postChannelTelegramUpdate(t, callback, signing, map[string]any{
						"update_id": 978001, "message": map[string]any{
							"message_id": 978001, "from": map[string]any{"id": 7000},
							"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + command,
						},
					})
					waitNativeIntentCount(t, db, "entry", 1)
					waitNativeInboxDeliveries(t, h, "1001", 1)
					rejectedToken := uuid.NewString()
					postChannelTelegramUpdate(t, callback, signing, map[string]any{
						"update_id": 978002, "callback_query": map[string]any{
							"id": "reset-history-refusal", "from": map[string]any{"id": 7000},
							"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}},
							"data":    rejectedToken,
						},
					})
					waitChannelRejectedCallback(t, db, rejectedToken)
					waitChannelDeliverySendsSettled(t, db, backend)
					for _, table := range []string{"operator_channel_action_intents", "operator_channel_text_intents"} {
						var count int
						if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE state='settled'`).Scan(&count); err != nil || count == 0 {
							t.Fatalf("reset history proof has no settled %s: count=%d err=%v", table, count, err)
						}
					}
					beforeNative := readChannelNativeHistory(t, observation)
					beforeHistory := readChannelPreservedHistory(t, observation)
					writes := len(h.provider.CommandWrites())
					dry := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", map[string]any{
						"dry_run": true, "include_source_artifacts": clear, "idempotency_key": uuid.NewString(),
					})
					if dry.Error != nil || readChannelNativeHistory(t, observation) != beforeNative || readChannelPreservedHistory(t, observation) != beforeHistory {
						t.Fatalf("dry-run changed native identity or populated history: %+v", dry.Error)
					}
					params := map[string]any{
						"include_source_artifacts": clear, "idempotency_key": uuid.NewString(),
					}
					reset := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", params)
					if reset.Error != nil {
						var record string
						readErr := db.QueryRow(`SELECT CAST(record AS TEXT) FROM runtime_reset_operations LIMIT 1`).Scan(&record)
						t.Fatalf("public reset failed: %+v record=%s read=%v\n%s", reset.Error, record, readErr, h.process.outputString())
					}
					var oldRun, activations, artifacts int
					for query, target := range map[string]*int{
						`SELECT COUNT(*) FROM runs WHERE run_id=$1`:                               &oldRun,
						`SELECT COUNT(*) FROM connected_channel_activations WHERE bundle_hash=$1`: &activations,
						`SELECT COUNT(*) FROM source_artifacts WHERE bundle_hash=$1`:              &artifacts,
					} {
						arg := hash
						if target == &oldRun {
							arg = seed.RunID
						}
						if err := db.QueryRow(query, arg).Scan(target); err != nil {
							t.Fatal(err)
						}
					}
					if oldRun != 0 || activations != 0 || (artifacts == 0) != clear {
						t.Fatalf("reset authority/source mismatch: run=%d activation=%d artifact=%d", oldRun, activations, artifacts)
					}
					successorPlan := ""
					if !clear {
						stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
						restore := installChannelOnboardingCLIInput(t, "source-reset-signing-secret\n")
						t.Cleanup(restore)
						code := executeCLI(context.Background(), []string{
							"--config", h.opts.ConfigPath, "secrets", "set", "telegram.webhook_signing_secret", "--stdin",
						}, stdout, stderr, nil)
						restore()
						if code != 0 {
							t.Fatalf("provision retained-source ingress credential: code=%d %s", code, stderr.String())
						}
						h.stop(t)
						h.start(t)
						var successorRun string
						if err := db.QueryRow(`SELECT run_id FROM runs WHERE flow_path='chat' AND landing_run=TRUE`).Scan(&successorRun); err != nil {
							t.Fatal(err)
						}
						freshCard := waitChannelAnchorCard(t, db, successorRun, decisioncard.AnchorKindStageGate, "chat")
						if freshCard == standingCard {
							t.Fatal("retained reset reused predecessor standing card")
						}
						deadline := time.Now().Add(15 * time.Second)
						for {
							var state, render, receipt string
							err := db.QueryRow(`SELECT delivery_id,state,COALESCE(CAST(current_render_id AS TEXT),''),COALESCE(CAST(current_receipt_operation_id AS TEXT),'') FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1 AND resend_generation=0`, freshCard).
								Scan(&successorPlan, &state, &render, &receipt)
							if err == nil {
								if state != "planned" || render != "" || receipt != "" {
									t.Fatal("successor card dispatched without a current activation")
								}
								break
							}
							if err != sql.ErrNoRows || time.Now().After(deadline) {
								t.Fatalf("successor standing card plan: %v", err)
							}
							time.Sleep(20 * time.Millisecond)
						}
					}
					afterNative := readChannelNativeHistory(t, observation)
					wantNative := beforeNative
					wantNative[3], wantNative[7] = "retired", "retired"
					if afterNative != wantNative {
						t.Fatalf("reset altered native identity: before=%v after=%v", beforeNative, afterNative)
					}
					requireChannelPreservedHistory(t, observation, beforeHistory, successorPlan)
					if got := len(h.provider.CommandWrites()); got != writes {
						t.Fatalf("reset wrote native settings: count %d -> %d", writes, got)
					}
					if replay := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", params); replay.Error != nil || readChannelNativeHistory(t, observation) != afterNative {
						t.Fatalf("reset replay changed native history: %+v", replay.Error)
					}
					postChannelSourceControl(t, callback, signing, card, token, messageID)
					for index, text := range []string{"obsolete answer", "/" + command} {
						status := postChannelSourceUpdate(t, callback, signing, map[string]any{
							"update_id": 978003 + index, "message": map[string]any{
								"message_id": 978003 + index, "from": map[string]any{"id": 7000},
								"chat": map[string]any{"id": 1001, "type": "private"}, "text": text,
								"reply_to_message": map[string]any{"message_id": messageID},
							},
						})
						if status != http.StatusNotFound {
							t.Fatalf("reset retained obsolete text/native ingress: status=%d text=%q", status, text)
						}
					}
					if response := requestServedJSONRPC(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}); response.Error == nil {
						t.Fatal("reset retained deleted card execution authority")
					}
					if clear {
						response := requestServedJSONRPC(t, h.rpcEndpoint(), "event.publish", map[string]any{
							"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
						})
						if response.Error == nil || response.Error.Data["code"] != apiv1.BundleUnavailableCode {
							t.Fatalf("source-inclusive reset revived execution: %+v", response.Error)
						}
					} else {
						var retained []byte
						if err := db.QueryRow(`SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&retained); err != nil || !bytes.Equal(retained, originalSource) {
							t.Fatalf("retained reset changed source bytes: %v", err)
						}
						later := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
							"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
						})
						if later.RunID == seed.RunID {
							t.Fatal("retained reset reused predecessor run identity")
						}
					}
					h.stop(t)
					h.start(t)
					if readChannelNativeHistory(t, observation) != afterNative || len(h.provider.CommandWrites()) != writes {
						t.Fatal("restart revived retired native consumers or replayed installation")
					}
					if !clear {
						_, confirmationCount := h.provider.OnboardingCounts()
						journey := runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "reconnect", "source-lifecycle-token", 1001, "private", confirmationCount)
						qualified := waitNativeQualification(t, h, journey.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
						if qualified.SettingID != beforeNative[0] || len(h.provider.CommandWrites()) != writes {
							t.Fatal("reconnect did not reuse acknowledged native installation")
						}
						var state string
						if err := db.QueryRow(`SELECT state FROM channel_native_setting_consumers WHERE activation_id=$1`, beforeNative[6]).Scan(&state); err != nil || state != "retired" {
							t.Fatalf("reconnect revived predecessor consumer: %s %v", state, err)
						}
					}
				case "fork":
					waitServedRunDeliveryQuiescence(t, deliveryReader, seed.RunID)
					requireServedOKJSONRPC(t, h.rpcEndpoint(), "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": uuid.NewString()})
					frontier := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
						"payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
					})
					params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "idempotency_key": uuid.NewString()}
					var fork apiv1.RunForkExecutionResult
					response := requestServedJSONRPC(t, h.rpcEndpoint(), "run.fork", params)
					if response.Error != nil {
						query := `SELECT json_object('state',o.state,'fork_run_id',o.fork_run_id,'failure',o.failure_json,
							'deliveries',(SELECT json_group_array(json_object('id',d.delivery_id,'event',e.event_name,
								'subscriber_type',d.subscriber_type,'subscriber_id',d.subscriber_id,
								'status',d.status,'reason',d.reason_code,'failure',d.failure,
								'handoff',d.continuation_handoff_at,'authority',d.execution_authority_kind,
								'execution',d.selected_execution_id,'generation',d.selected_execution_generation,
								'bundle_hash',d.authority_bundle_hash,'open_attempt',d.current_attempt_open))
								FROM event_deliveries d LEFT JOIN events e ON e.event_id=d.event_id
								WHERE d.run_id=o.fork_run_id AND d.execution_authority_kind='selected_contract_fork'))
							FROM run_fork_operations o WHERE o.idempotency_key=$1`
						if backend == servedparity.BackendExplicitPostgres {
							query = strings.ReplaceAll(strings.ReplaceAll(query, "json_object(", "json_build_object("), "json_group_array(", "json_agg(")
						}
						var failure sql.NullString
						readErr := db.QueryRow(query, params["idempotency_key"]).Scan(&failure)
						t.Fatalf("public fork failed: %+v failure=%s read=%v\n%s", response.Error, failure.String, readErr, h.process.outputString())
					}
					if err := json.Unmarshal(response.Result, &fork); err != nil {
						t.Fatal(err)
					}
					waitServedSelectedForkFrontier(t, receiverReader, deliveryReader, fork.ForkRunID, frontier.EventID)
					var supersessionCount, exactLineageCount int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_superseded'`, fork.ForkRunID).Scan(&supersessionCount); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM events e
						JOIN run_fork_selected_contract_executions x ON x.fork_event_id=e.source_event_id AND x.fork_run_id=e.run_id
						WHERE e.run_id=$1 AND e.event_name='mailbox.card_superseded' AND x.source_event_id=$2`, fork.ForkRunID, frontier.EventID).Scan(&exactLineageCount); err != nil {
						t.Fatal(err)
					}
					if supersessionCount != 1 || exactLineageCount != supersessionCount {
						t.Fatalf("fork stage exit lost its selected frontier: supersessions=%d exact=%d", supersessionCount, exactLineageCount)
					}
					child := waitChannelAnchorCardInState(t, db, fork.ForkRunID, decisioncard.AnchorKindStageGate, "chat/reviews", decisioncard.StatusPending)
					var retiredEvent []byte
					if err := db.QueryRow(`SELECT payload FROM events WHERE run_id=$1 AND event_name='mailbox.card_superseded'`, fork.ForkRunID).Scan(&retiredEvent); err != nil {
						t.Fatal(err)
					}
					var retired struct {
						CardID string `json:"card_id"`
					}
					if err := json.Unmarshal(retiredEvent, &retired); err != nil || retired.CardID == "" || retired.CardID == child {
						t.Fatalf("fork supersession has no exact predecessor: %s err=%v", retiredEvent, err)
					}
					var retiredCards int
					if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE run_id=$1 AND card_id=$2 AND status='superseded'`, fork.ForkRunID, retired.CardID).Scan(&retiredCards); err != nil || retiredCards != 1 {
						t.Fatalf("fork stage exit lost exact predecessor card retirement: count=%d err=%v", retiredCards, err)
					}
					if child == card || fork.ForkRunID == seed.RunID {
						t.Fatal("fork reused parent card/run identity")
					}
					childMessage := waitChannelAnchorReceipt(t, db, child)
					childToken, found := telegramCallbackToken(h.provider.Delivery(childMessage-1), "Approve")
					if !found || childToken == token {
						t.Fatal("fork delivery reused parent control identity")
					}
					readParent := func() [3]string {
						var state [3]string
						if err := db.QueryRow(`SELECT status,COALESCE(verdict,''),CAST(updated_at AS TEXT) FROM decision_cards WHERE card_id=$1`, card).Scan(&state[0], &state[1], &state[2]); err != nil {
							t.Fatal(err)
						}
						return state
					}
					parentBefore := readParent()
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					deadline := time.Now().Add(5 * time.Second)
					for {
						var intentState, disposition string
						err := db.QueryRow(`SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE CAST(fact AS TEXT) LIKE $1`, "%"+childToken+"%").Scan(&intentState, &disposition)
						if err == nil && intentState == "settled" && disposition == "applied" {
							break
						}
						if time.Now().After(deadline) {
							var status, verdict string
							cardErr := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, child).Scan(&status, &verdict)
							var decisions int
							eventErr := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, fork.ForkRunID).Scan(&decisions)
							t.Fatalf("selected-fork callback lacks exact applied disposition: state=%s disposition=%s err=%v card_status=%s verdict=%s card_err=%v decisions=%d event_err=%v", intentState, disposition, err, status, verdict, cardErr, decisions, eventErr)
						}
						time.Sleep(20 * time.Millisecond)
					}
					var status string
					if err := db.QueryRow(`SELECT status FROM decision_cards WHERE card_id=$1`, child).Scan(&status); err != nil || status != "decided" {
						t.Fatalf("fork callback did not settle its exact selected card: status=%s err=%v", status, err)
					}
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					requireServedOKJSONRPC(t, h.rpcEndpoint(), "run.pause", map[string]any{
						"run_id": fork.ForkRunID, "idempotency_key": "selected-source-pause-before-ordinary",
					})
					ordinary := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": uuid.NewString(),
					})
					ordinaryCard := waitChannelAnchorCard(t, db, ordinary.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
					ordinaryMessage := waitChannelAnchorReceipt(t, db, ordinaryCard)
					ordinaryToken, ok := telegramCallbackToken(h.provider.Delivery(ordinaryMessage-1), "Approve")
					if !ok {
						t.Fatal("normal run lost decision control")
					}
					postChannelSourceControl(t, callback, signing, ordinaryCard, ordinaryToken, ordinaryMessage)
					waitChannelAnchorDecision(t, db, ordinaryCard)
					waitChannelDeliverySendsSettled(t, db, backend)
					h.stop(t)
					h.start(t)
					callback, signing, _ = h.provider.Registration()
					postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
					var verdict string
					var events, drafts int
					if err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, child).Scan(&status, &verdict); err != nil || status != "decided" || verdict != "approve" {
						t.Fatalf("fork replay/restart changed child: %s/%s %v", status, verdict, err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, fork.ForkRunID).Scan(&events); err != nil || events != 1 {
						t.Fatalf("fork restart duplicated its selected decision: %d %v", events, err)
					}
					if err := db.QueryRow(`SELECT COUNT(*) FROM decision_card_input_drafts WHERE card_id=$1`, child).Scan(&drafts); err != nil || drafts != 0 {
						t.Fatalf("selected callback or replay created a draft: %d %v", drafts, err)
					}
					if readParent() != parentBefore {
						t.Fatal("child decision or normal-run control mutated the frozen parent card")
					}
				}
				if after := readChannelSourceHistory(t, observation, card); after != frozen {
					t.Fatal("source lifecycle changed historical receipt/render evidence")
				}
			})
		}
	}
}

func runRegisteredSourceLifecycle(t *testing.T, h *channelOnboardingE2EHarness, observation storetest.ChannelObservation, backend servedparity.Backend, sourceReader *sourceArtifactReader, hash, operation string, receiverReader receiverProofStateReader, deliveryReader servedRunDeliveryReader, cards decisioncard.Store, nativeRead func(context.Context, channelnative.TransportParent) (channelnative.Qualification, error)) {
	t.Helper()
	command := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
	initialNative := waitRegisteredSourceQualification(t, h, observation, nativeRead)
	nativeHistory := readChannelNativeHistory(t, observation)
	writes := len(h.provider.CommandWrites())
	seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "cli-source-seed",
	})
	card := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
	messageID := waitObservedChannelReceipt(t, observation, card)
	token, found := telegramCallbackToken(h.provider.Delivery(messageID-1), "Approve")
	if !found {
		t.Fatal("registered lifecycle lacks its frozen channel control")
	}
	waitObservedChannelSendsSettled(t, observation)
	frozenHistory := readChannelSourceHistory(t, observation, card)
	before := readRegisteredChannelCard(t, h, card)
	source, err := (*sourceReader).GetSourceArtifact(t.Context(), hash)
	if err != nil {
		t.Fatal(err)
	}
	switch operation {
	case "disk", "new_process":
		path := filepath.Join(h.opts.SourceRoot, "chat", "reviews", "schema.yaml")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(body, []byte("\n# Changed artifact bytes; retained decision remains frozen.\n")...), 0o600); err != nil {
			t.Fatal(err)
		}
		var identity apiv1.RuntimeIdentityResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
		if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash != hash {
			t.Fatal("disk mutation changed captured authority")
		}
		if operation == "disk" {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			callback, signing, _ := h.provider.Registration()
			postChannelSourceControl(t, callback, signing, card, token, messageID)
			waitObservedChannelDecision(t, h, card)
			standingRun := observedChannelLanding(t, h, "chat").RunID
			standingCard := waitChannelPublicCard(t, h, standingRun, decisioncard.AnchorKindStageGate, "chat")
			standingBefore := readRegisteredChannelCard(t, h, standingCard)
			registeredChannelMailboxCLI(t, h, true, "view", standingCard)
			args := []string{"decide", standingCard, "retire", "--idempotency-key", "cli-" + standingCard}
			registeredChannelMailboxCLI(t, h, true, args...)
			decided := readRegisteredChannelCard(t, h, standingCard)
			if decided.Status != decisioncard.StatusDecided || decided.Verdict != "retire" || decided.DecisionEventID == "" || !bytes.Equal(decided.Snapshot, standingBefore.Snapshot) {
				t.Fatalf("CLI changed the frozen standing gate or failed its authored retirement: %+v", decided)
			}
			registeredChannelMailboxCLI(t, h, true, args...)
			if replay := readRegisteredChannelCard(t, h, standingCard); replay.DecisionEventID != decided.DecisionEventID {
				t.Fatal("replayed standing retirement emitted a different completion")
			}
		} else {
			h.stop(t)
			h.start(t)
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
			if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash == hash {
				t.Fatal("new process did not capture the new source")
			}
			var header struct {
				Run operatorread.RunHeader `json:"run"`
			}
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.get", map[string]any{"run_id": seed.RunID}, &header)
			reissued, err := observation.CountSourceCards(t.Context(), seed.RunID, identity.SourceArtifacts[0].BundleHash)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("changed-source facts backend=%s own_run_status=%s own_run_on_new_source=%t card_on_original_source=%t cards_on_new_source=%d", backend, header.Run.Status, header.Run.BundleHash == identity.SourceArtifacts[0].BundleHash, before.BundleHash == hash, reissued)
			cliFailure := registeredChannelMailboxCLI(t, h, false, "decide", card, "approve", "--idempotency-key", "unloaded-cli-"+card)
			t.Logf("changed-source CLI refusal backend=%s: %s", backend, strings.TrimSpace(cliFailure))
			callback, signing, _ := h.provider.Registration()
			postChannelSourceControl(t, callback, signing, card, token, messageID)
			waitObservedChannelRejectedCallback(t, observation, token)
			refused := readRegisteredChannelCard(t, h, card)
			if refused.Status != decisioncard.StatusPending || refused.Verdict != "" || refused.DecisionEventID != "" || refused.BundleHash != hash || !bytes.Equal(refused.Snapshot, before.Snapshot) {
				t.Fatalf("refusing an unavailable source consumed or changed its card: %+v", refused)
			}
			decisions, err := observation.EventCardinality(t.Context(), seed.RunID, "mailbox.card_decided")
			if err != nil || decisions != 0 {
				t.Fatalf("replacement source admitted a predecessor control: count=%d err=%v", decisions, err)
			}
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			h.stop(t)
			h.start(t)
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
			if len(identity.SourceArtifacts) != 1 || identity.SourceArtifacts[0].BundleHash != hash {
				t.Fatal("explicit restoration did not load the exact original source")
			}
			callback, signing, _ = h.provider.Registration()
			postChannelSourceUpdate(t, callback, signing, map[string]any{"update_id": 979101, "callback_query": map[string]any{
				"id": "restored-source-" + card, "from": map[string]any{"id": 7000},
				"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}}, "data": token,
			}})
			waitObservedChannelDecision(t, h, card)
			if decisions, err = observation.EventCardinality(t.Context(), seed.RunID, "mailbox.card_decided"); err != nil || decisions != 1 {
				t.Fatalf("restored source did not admit exactly one later answer: count=%d err=%v", decisions, err)
			}
			refusals, err := observation.ControlCounts(t.Context(), token)
			if err != nil || refusals.OriginalRefused != 1 {
				t.Fatalf("later answer changed the original refused action: count=%d err=%v", refusals.OriginalRefused, err)
			}
			t.Logf("explicit original-source restoration backend=%s later_decisions=%d original_refused_intents=%d", backend, decisions, refusals.OriginalRefused)
		}
		if retained, err := (*sourceReader).GetSourceArtifact(t.Context(), hash); err != nil || !bytes.Equal(retained.SourceBlob, source.SourceBlob) {
			t.Fatal("source replacement altered retained bytes", err)
		}
		if got := readRegisteredChannelCard(t, h, card); string(got.Snapshot) != string(before.Snapshot) {
			t.Fatal("source replacement rewrote the frozen decision")
		}
	case "retained_reset", "source_reset":
		clear := operation == "source_reset"
		var originalLandings []runlifecycle.LandingRun
		originalCards := map[string]bool{card: true}
		landingReader, ok := servedTestProcessRuntime(t, h.process).Bus.Store().(runlifecycle.LandingRunReader)
		if !ok {
			t.Fatal("registered reset lacks its native landing reader")
		}
		if !clear {
			scopes, err := servedTestProcessRuntime(t, h.process).PlanLiveFlowScopes()
			if err != nil || len(scopes) == 0 {
				t.Fatalf("reset lacks its planned flow scopes: %v", err)
			}
			for _, scope := range scopes {
				landing, found, err := landingReader.LoadLandingRun(t.Context(), scope)
				if err != nil || !found || landing.Scope != scope || landing.Source.BundleHash() != hash || landing.State != runlifecycle.StateLive {
					t.Fatalf("reset predecessor differs from its planned scope/source: %+v found=%t err=%v", landing, found, err)
				}
				originalLandings = append(originalLandings, landing)
				originalCards[waitChannelPublicCard(t, h, landing.RunID, decisioncard.AnchorKindStageGate, "chat")] = true
			}
		}
		callback, signing, _ := h.provider.Registration()
		postChannelTelegramOperatorUpdate(t, callback, signing, map[string]any{
			"update_id": 978001, "message": map[string]any{"message_id": 978001, "from": map[string]any{"id": 7000},
				"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + command},
		})
		waitObservedChannelEntryCount(t, observation, 1)
		waitNativeInboxDeliveries(t, h, "1001", 1)
		rejectedToken := uuid.NewString()
		postChannelTelegramOperatorUpdate(t, callback, signing, map[string]any{
			"update_id": 978002, "callback_query": map[string]any{"id": "registered-reset-history-refusal", "from": map[string]any{"id": 7000},
				"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}}, "data": rejectedToken},
		})
		waitObservedChannelRejectedCallback(t, observation, rejectedToken)
		waitObservedChannelSendsSettled(t, observation)
		preserved := readChannelPreservedHistory(t, observation)
		originalPlans := map[string]string{}
		if !clear {
			for card := range originalCards {
				plan, err := observation.GenerationZeroSentPlan(t.Context(), card)
				if err != nil {
					t.Fatal(err)
				}
				originalPlans[plan] = card
			}
		}
		originalCardFacts := map[string]decisioncard.Card{}
		if clear {
			items, next, err := cards.ListDecisionCards(t.Context(), decisioncard.ListOptions{Status: decisioncard.StatusPending, AnchorKind: string(decisioncard.AnchorKindStageGate), Limit: 200})
			if err != nil || next != "" {
				t.Fatalf("original card inventory: cursor=%s err=%v", next, err)
			}
			for _, item := range items {
				original, err := cards.GetDecisionCard(t.Context(), item.CardID)
				if err != nil {
					t.Fatal(err)
				}
				originalCardFacts[item.CardID] = original
			}
		}
		params := map[string]any{"include_source_artifacts": clear, "idempotency_key": "cli-source-reset"}
		dry := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", map[string]any{"dry_run": true, "include_source_artifacts": clear, "idempotency_key": "cli-source-dry"})
		if dry.Error != nil || readRegisteredChannelCard(t, h, card).Status != before.Status || readChannelNativeHistory(t, observation) != nativeHistory || readChannelPreservedHistory(t, observation) != preserved {
			t.Fatal("dry reset changed card authority", dry.Error)
		}
		if reset := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", params); reset.Error != nil {
			t.Fatalf("public reset failed: error=%+v\n%s", reset.Error, h.process.outputString())
		}
		if readChannelNativeHistory(t, observation) != nativeHistory || len(h.provider.CommandWrites()) != writes {
			t.Fatal("runtime reset changed deployment-owned native identity or replayed installation")
		}
		var successorPlans []string
		if !clear {
			callback, _, _ := h.provider.Registration()
			// Reset changes startup authority; its existing public probe renewal
			// must become current before a delivery or business callback is admitted.
			deadline := time.Now().Add(runtimepublicingress.RenewalInterval + 15*time.Second)
			for {
				response, err := http.Get(callback)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode == http.StatusMethodNotAllowed {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("policy-backed holder callback did not become current after reset: HTTP %d", response.StatusCode)
				}
				time.Sleep(10 * time.Millisecond)
			}
			for _, original := range originalLandings {
				successor, found, err := landingReader.LoadLandingRun(t.Context(), original.Scope)
				if err != nil || !found || successor.Scope != original.Scope || !successor.Source.Matches(original.Source) || successor.State != runlifecycle.StateLive || successor.RunID == original.RunID || successor.RunID == seed.RunID {
					t.Fatalf("reset successor differs from its exact fresh scope/source: %+v found=%t err=%v", successor, found, err)
				}
				fresh := waitChannelPublicCard(t, h, successor.RunID, decisioncard.AnchorKindStageGate, "chat")
				if originalCards[fresh] {
					t.Fatal("retained reset reused a predecessor card identity")
				}
				waitObservedChannelReceipt(t, observation, fresh)
				plan, err := observation.GenerationZeroSentPlan(t.Context(), fresh)
				if err != nil {
					t.Fatal(err)
				}
				if slices.Contains(successorPlans, plan) {
					t.Fatal("distinct fresh cards reused a successor delivery plan")
				}
				successorPlans = append(successorPlans, plan)
			}
			waitObservedChannelSendsSettled(t, observation, originalPlans)
		}
		checkHistory := func() {
			if !clear {
				requireChannelPreservedHistory(t, observation, preserved, successorPlans...)
				return
			}
			actual := readChannelPreservedHistory(t, observation, successorPlans...)
			normalized, err := channelHistoryWithoutResetRenders(preserved, actual, originalCardFacts, backend == servedparity.BackendExplicitPostgres)
			if err != nil || normalized != preserved {
				t.Fatalf("reset altered preserved history: %v", err)
			}
		}
		checkHistory()
		if replay := requestServedJSONRPC(t, h.rpcEndpoint(), "runtime.nuke", params); replay.Error != nil {
			t.Fatal(replay.Error)
		}
		if readChannelNativeHistory(t, observation) != nativeHistory || len(h.provider.CommandWrites()) != writes {
			t.Fatal("reset replay changed deployment-owned native authority")
		}
		checkHistory()
		status := postChannelSourceControl(t, callback, signing, card, token, messageID)
		t.Logf("registered reset old callback backend=%s operation=%s HTTP=%d", backend, operation, status)
		if status == http.StatusOK || status == http.StatusAccepted {
			waitObservedChannelRejectedCallback(t, observation, token)
		} else if status == http.StatusNotFound {
			intents, err := observation.ControlCounts(t.Context(), token)
			if err != nil || intents.Total != 0 {
				t.Fatalf("withdrawn callback route created an action: count=%d err=%v", intents.Total, err)
			}
		} else {
			t.Fatalf("deleted card callback was neither refused at ingress nor admitted for terminal rejection: HTTP %d", status)
		}
		if result := requestServedJSONRPC(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}); result.Error == nil {
			t.Fatal("reset retained deleted card execution")
		}
		publish := requestServedJSONRPC(t, h.rpcEndpoint(), "event.publish", map[string]any{"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "cli-source-after-reset"})
		if clear {
			if publish.Error == nil || publish.Error.Data["code"] != apiv1.BundleUnavailableCode {
				t.Fatal("source-inclusive reset revived execution", publish.Error)
			}
		} else if publish.Error != nil {
			t.Fatal(publish.Error)
		}
		if !clear {
			callback, signing, _ := h.provider.Registration()
			if names := postChannelTelegramUpdate(t, callback, signing, map[string]any{
				"update_id": 978101, "message": map[string]any{"message_id": 978101, "from": map[string]any{"id": 2001},
					"chat": map[string]any{"id": 2001, "type": "private"}, "text": "customer after retained reset"},
			}); !slices.Contains(names, "inbound.telegram.text_message") {
				t.Fatalf("fresh policy-backed holder lost customer ingress after reset: %v", names)
			}
		}
		h.stop(t)
		h.start(t)
		qualified := waitRegisteredSourceQualification(t, h, observation, nativeRead)
		if qualified.SettingID != initialNative.SettingID || qualified.SettingGeneration != initialNative.SettingGeneration || readChannelNativeHistory(t, observation) != nativeHistory || len(h.provider.CommandWrites()) != writes {
			t.Fatal("restart changed the deployment parent or repeated its acknowledged installation")
		}
		var out, errOut lockedBuffer
		if code := cliapp.Execute(t.Context(), []string{"--config", h.opts.ConfigPath, "channel", "connect", "telegram", "420079", "--yes", "--api-server", h.endpoint, "--json"}, &out, &errOut, nil, nil, OpenChannelRegistration); code != 0 {
			t.Fatalf("public registered-account reverify exit=%d err=%s", code, errOut.String())
		}
		var reverified channelonboarding.Result
		if json.Unmarshal([]byte(out.String()), &reverified) != nil || reverified.Operation.OperationID != h.registration.OperationID || reverified.Operation.IdentityOperationID != h.registration.IdentityOperationID || reverified.Operation.Phase != channelonboarding.PhaseSucceeded || len(h.provider.CommandWrites()) != writes {
			t.Fatalf("reverification replaced the parent/child or reinstalled native settings: %s", out.String())
		}
	case "fork":
		frontier := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
			"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "cli-source-frontier",
		})
		var fork apiv1.RunForkExecutionResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.EventID, "idempotency_key": "cli-source-fork"}, &fork)
		waitServedSelectedForkFrontier(t, receiverReader, deliveryReader, fork.ForkRunID, frontier.EventID)
		child := waitChannelPublicCard(t, h, fork.ForkRunID, decisioncard.AnchorKindStageGate, "chat/reviews")
		if fork.ForkRunID == seed.RunID || child == card {
			t.Fatal("selected fork reused predecessor identity")
		}
		childMessage := waitObservedChannelReceipt(t, observation, child)
		childToken, found := telegramCallbackToken(h.provider.Delivery(childMessage-1), "Approve")
		if !found || childToken == token {
			t.Fatal("selected fork reused the parent's channel control")
		}
		callback, signing, _ := h.provider.Registration()
		postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
		waitObservedChannelDecision(t, h, child)
		if got := readRegisteredChannelCard(t, h, card); got.DecisionEventID != "" {
			t.Fatal("child decision escaped to parent")
		}
		decided := readRegisteredChannelCard(t, h, child)
		h.stop(t)
		h.start(t)
		callback, signing, _ = h.provider.Registration()
		postChannelSourceControl(t, callback, signing, child, childToken, childMessage)
		if got := readRegisteredChannelCard(t, h, child); got.DecisionEventID != decided.DecisionEventID || got.Verdict != decided.Verdict {
			t.Fatal("restart changed selected-fork decision")
		}
	}
	if after := readChannelSourceHistory(t, observation, card); after != frozenHistory {
		t.Fatal("registered source lifecycle changed its historical render/receipt evidence")
	}
}

func waitRegisteredSourceQualification(t *testing.T, h *channelOnboardingE2EHarness, observation storetest.ChannelObservation, read func(context.Context, channelnative.TransportParent) (channelnative.Qualification, error)) channelnative.Qualification {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		parent, err := observation.NativeConsumerParent(t.Context(), h.registration.OperationID, true)
		if err != nil && !channelObservationAbsent(err) {
			t.Fatal(err)
		}
		var qualified channelnative.Qualification
		if err == nil {
			if parent.ID != h.registration.OperationID || parent.Revision != h.registration.Revision {
				t.Fatalf("registered native setting lost its exact parent: %+v, %v", parent, err)
			}
			qualified, err = read(t.Context(), parent)
			if err != nil {
				t.Fatal(err)
			}
			if qualified.State == channelnative.QualificationQualified {
				return qualified
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("registered native qualification did not converge: %+v", qualified)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func requireChannelPreservedHistory(t *testing.T, observer storetest.ChannelObservation, beforeHistory string, successorPlans ...string) {
	t.Helper()
	afterHistory := readChannelPreservedHistory(t, observer, successorPlans...)
	for _, plan := range successorPlans {
		var err error
		afterHistory, err = channelHistoryWithoutSuccessorPlan(afterHistory, plan)
		if err != nil {
			t.Fatal(err)
		}
	}
	if afterHistory == beforeHistory {
		return
	}
	var before, after []json.RawMessage
	if err := json.Unmarshal([]byte(beforeHistory), &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(afterHistory), &after); err != nil {
		t.Fatal(err)
	}
	for index, row := range before {
		if !bytes.Equal(row, after[index]) {
			t.Errorf("reset altered preserved history table index %d: byte length %d -> %d", index, len(row), len(after[index]))
		}
	}
	t.FailNow()
}

// Retained reset boots the identical source and its new standing gate. Only
// that independently observed plan is new work, not pre-reset history.
func channelHistoryWithoutSuccessorPlan(history, successorPlan string) (string, error) {
	if successorPlan == "" {
		return history, nil
	}
	var tables []json.RawMessage
	if err := json.Unmarshal([]byte(history), &tables); err != nil {
		return "", err
	}
	var plans []json.RawMessage
	if err := json.Unmarshal(tables[6], &plans); err != nil {
		return "", err
	}
	removed := false
	for index, plan := range plans {
		var fields []json.RawMessage
		if err := json.Unmarshal(plan, &fields); err != nil {
			return "", err
		}
		var id string
		if err := json.Unmarshal(fields[0], &id); err != nil {
			return "", err
		}
		if id == successorPlan {
			plans = append(plans[:index], plans[index+1:]...)
			removed = true
			break
		}
	}
	if !removed {
		return "", errors.New("exact successor plan is absent from history")
	}
	body, err := json.Marshal(plans)
	if err != nil {
		return "", err
	}
	tables[6] = body
	body, err = json.Marshal(tables)
	return string(body), err
}

func channelHistoryWithoutResetRenders(beforeHistory, afterHistory string, cards map[string]decisioncard.Card, postgres bool) (string, error) {
	var before, after [][][]json.RawMessage
	if json.Unmarshal([]byte(beforeHistory), &before) != nil || json.Unmarshal([]byte(afterHistory), &after) != nil || len(before) != 15 || len(after) != 15 {
		return "", errors.New("reset history is malformed")
	}
	originals, seen := map[string]bool{}, map[string]bool{}
	for _, row := range before[7] {
		if len(row) != 7 {
			return "", errors.New("original render is malformed")
		}
		originals[string(row[0])] = true
	}
	var retained [][]json.RawMessage
	for _, row := range after[7] {
		if len(row) != 7 || seen[string(row[0])] {
			return "", errors.New("invalid or duplicated render")
		}
		seen[string(row[0])] = true
		if originals[string(row[0])] {
			retained = append(retained, row)
			continue
		}
		allowed := false
		for _, prior := range before[7] {
			if !bytes.Equal(prior[1], row[1]) {
				continue
			}
			decodeInput := func(value json.RawMessage) ([]byte, error) {
				var input []byte
				var text string
				var err error
				if postgres {
					err = json.Unmarshal(value, &input)
				} else {
					err = json.Unmarshal(value, &text)
					input = []byte(text)
				}
				if err != nil {
					return nil, err
				}
				// Match native render readback: PostgreSQL JSONB is not stored canonically.
				return canonicaljson.Canonicalize(input)
			}
			input, err := decodeInput(prior[4])
			var hash string
			var revision int64
			if err != nil || json.Unmarshal(prior[5], &hash) != nil || json.Unmarshal(row[2], &revision) != nil {
				continue
			}
			original, err := channeldelivery.Decode(input, hash)
			card, found := cards[original.SourceID]
			if err != nil || !found || original.SourceKind != "card" || card.Status != decisioncard.StatusPending || card.Anchor.Kind() != decisioncard.AnchorKindStageGate || revision <= original.Revision || original.ActionPage == nil {
				continue
			}
			card.Status, card.SupersededReason = decisioncard.StatusSuperseded, "run_cancelled"
			expected, err := channeldelivery.FreezeCard(card, revision, "", original.Audience, channeldelivery.DraftPrompt{})
			if err == nil {
				expected, err = channeldelivery.WithPresentation(expected, original.Bounds, original.ActionPage.Index)
			}
			actualInput, decodeErr := decodeInput(row[4])
			if err != nil || decodeErr != nil || !bytes.Equal(actualInput, expected.Input) {
				continue
			}
			want := append([]json.RawMessage(nil), prior...)
			// Retain physical JSONB bytes only after checking the canonical owner output.
			want[0], want[2], want[4], want[6] = row[0], row[2], row[4], row[6]
			want[5], _ = json.Marshal(expected.Hash)
			encoded, _ := json.Marshal(want)
			actual, _ := json.Marshal(row)
			if bytes.Equal(encoded, actual) {
				allowed = true
				break
			}
		}
		if !allowed || seen[string(row[1])+string(row[5])] {
			return "", errors.New("additional render is not the exact original card supersession")
		}
		seen[string(row[1])+string(row[5])] = true
	}
	after[7] = retained
	body, err := json.Marshal(after)
	return string(body), err
}

func TestChannelSourceLifecycleHistoryPreservation(t *testing.T) {
	t.Run("post_reset_settlement", func(t *testing.T) {
		originals := map[string]string{"original-plan": "original-card"}
		for name, mutate := range map[string]func(*storetest.ChannelResetSettlement){
			"settled_original":             func(*storetest.ChannelResetSettlement) {},
			"unrelated_or_fresh_plan":      func(r *storetest.ChannelResetSettlement) { r.PlanID = "other" },
			"wrong_source":                 func(r *storetest.ChannelResetSettlement) { r.SourceID = "other" },
			"not_card":                     func(r *storetest.ChannelResetSettlement) { r.SourceKind = "notice" },
			"live_source":                  func(r *storetest.ChannelResetSettlement) { r.SourcePresent = true },
			"missing_receipt":              func(r *storetest.ChannelResetSettlement) { r.ReceiptState = "" },
			"mismatched_receipt_operation": func(r *storetest.ChannelResetSettlement) { r.ExactReceipt = false },
			"failed_receipt":               func(r *storetest.ChannelResetSettlement) { r.ReceiptState = "failed" },
			"authorized":                   func(r *storetest.ChannelResetSettlement) { r.OperationState = "authorized" },
			"launched":                     func(r *storetest.ChannelResetSettlement) { r.OperationState = "launched" },
			"response_observed":            func(r *storetest.ChannelResetSettlement) { r.OperationState = "response_observed" },
			"uncertain":                    func(r *storetest.ChannelResetSettlement) { r.OperationState = "indeterminate" },
			"failed_operation":             func(r *storetest.ChannelResetSettlement) { r.OperationState = "failed" },
			"another_unsettled_operation":  func(r *storetest.ChannelResetSettlement) { r.UnsafeOperations = true },
		} {
			r := storetest.ChannelResetSettlement{PlanID: "original-plan", SourceID: "original-card", SourceKind: "card", ReceiptState: "sent", OperationState: "settled", ExactReceipt: true}
			mutate(&r)
			if r.SettledOriginal(originals) != (name == "settled_original") {
				t.Errorf("post-reset observation admitted %s", name)
			}
		}
	})
	t.Run("superseded_renders", func(t *testing.T) {
		card := newCursorMailboxFixture(t, servedparity.BackendDefaultSQLite).base
		cards := map[string]decisioncard.Card{card.CardID: card}
		audience := channeldelivery.Audience{PrincipalID: uuid.NewString(), InterfaceKey: "history", DeliveryEpoch: 1, ExternalAccountRef: "account", ConversationRef: "room", ConversationScope: "direct"}
		bounds := packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}
		old, e1 := channeldelivery.FreezeCard(card, 1, "", audience, channeldelivery.DraftPrompt{})
		old, e2 := channeldelivery.WithPresentation(old, bounds, 0)
		card.Status, card.SupersededReason = decisioncard.StatusSuperseded, "run_cancelled"
		terminal, e3 := channeldelivery.FreezeCard(card, 2, "", audience, channeldelivery.DraftPrompt{})
		terminal, e4 := channeldelivery.WithPresentation(terminal, bounds, 0)
		if err := errors.Join(e1, e2, e3, e4); err != nil {
			t.Fatal(err)
		}
		plan := uuid.NewString()
		row := func(id string, f channeldelivery.Frozen) []any {
			return []any{id, plan, f.Revision, channeldelivery.ProjectionVersion, string(f.Input), f.Hash, "2026-11-13T00:00:00Z"}
		}
		before := make([]any, 15)
		before[7] = [][]any{row("original", old)}
		baseline, _ := json.Marshal(before)
		for _, change := range []string{"none", "original_changed", "original_deleted", "duplicate", "plan", "hash", "projection", "revision", "execution", "source_id", "audience", "choices", "full_text"} {
			t.Run(change, func(t *testing.T) {
				after := append([]any(nil), before...)
				rows := [][]any{row("original", old), row("terminal", terminal)}
				switch change {
				case "original_changed":
					rows[0][5] = "changed"
				case "original_deleted":
					rows = rows[1:]
				case "duplicate":
					rows = append(rows, rows[1])
				case "plan":
					rows[1][1] = uuid.NewString()
				case "hash":
					rows[1][5] = "sha256:wrong"
				case "projection":
					rows[1][3] = "wrong-version"
				case "revision":
					rows[1][2] = 1
				case "execution":
					after[13] = [][]string{{"unexpected-effect"}}
				case "source_id", "audience", "choices", "full_text":
					var projection map[string]any
					_ = json.Unmarshal(terminal.Input, &projection)
					projection[change] = map[string]any{"source_id": uuid.NewString(), "audience": map[string]any{"principal_id": audience.PrincipalID, "interface_key": "foreign", "delivery_epoch": 1, "external_account": "account", "conversation_reference": "room", "conversation_scope": "direct"}, "choices": []any{map[string]any{"verdict": "approve", "label": "Approve", "fields": []any{}}}, "full_text": "forged terminal text"}[change]
					raw, _ := json.Marshal(projection)
					input, err := canonicaljson.Canonicalize(raw)
					if err != nil {
						t.Fatal(err)
					}
					rows[1][4], rows[1][5] = string(input), canonicaljson.HashBytes(input)
				}
				after[7] = rows
				body, _ := json.Marshal(after)
				normalized, err := channelHistoryWithoutResetRenders(string(baseline), string(body), cards, false)
				if (err == nil && normalized == string(baseline)) != (change == "none") {
					t.Fatalf("history admitted %s: %v", change, err)
				}
			})
		}
	})
	t.Run("inspection_absence", func(t *testing.T) {
		for _, tc := range []struct {
			err  error
			want bool
		}{
			{sql.ErrNoRows, true},
			{errors.Join(sql.ErrNoRows), true},
			{errors.Join(errors.Join(sql.ErrNoRows)), true},
			{nil, false},
			{context.Canceled, false},
			{errors.Join(sql.ErrNoRows, context.Canceled), false},
			{errors.Join(sql.ErrNoRows, context.DeadlineExceeded), false},
			{errors.Join(sql.ErrNoRows, errors.New("independent read disposal failure")), false},
		} {
			if got := channelObservationAbsent(tc.err); got != tc.want {
				t.Errorf("inspection absence %v: got %t want %t", tc.err, got, tc.want)
			}
		}
	})
	before := make([]any, 15)
	before[6] = [][]string{{"old-plan", "card", "old-card", "frozen"}}
	want, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"none", "changed", "deleted", "unrelated", "duplicate", "missing_successor", "other_table"} {
		t.Run(mutation, func(t *testing.T) {
			after := append([]any(nil), before...)
			plans := [][]string{{"old-plan", "card", "old-card", "frozen"}, {"successor", "card", "fresh-card", "new"}}
			switch mutation {
			case "changed":
				plans[0][3] = "altered"
			case "deleted":
				plans = plans[1:]
			case "unrelated":
				plans = append(plans, []string{"other", "card", "other-card"})
			case "duplicate":
				plans = append(plans, plans[1])
			case "missing_successor":
				plans = plans[:1]
			case "other_table":
				after[8] = [][]string{{"altered-receipt"}}
			}
			after[6] = plans
			body, err := json.Marshal(after)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := channelHistoryWithoutSuccessorPlan(string(body), "successor")
			matches := err == nil && retained == string(want)
			if matches != (mutation == "none") {
				t.Fatalf("history exception accepted %s=%t: %v", mutation, matches, err)
			}
		})
	}
	for _, mutation := range []string{"none", "changed", "deleted", "unrelated", "duplicate", "missing_successor", "other_table"} {
		t.Run("multiple_"+mutation, func(t *testing.T) {
			after := append([]any(nil), before...)
			plans := [][]string{{"old-plan", "card", "old-card", "frozen"}, {"successor-a", "card", "fresh-a", "new-a"}, {"successor-b", "card", "fresh-b", "new-b"}}
			switch mutation {
			case "changed":
				plans[0][3] = "altered"
			case "deleted":
				plans = plans[1:]
			case "unrelated":
				plans = append(plans, []string{"other", "card", "other-card"})
			case "duplicate":
				plans = append(plans, plans[1])
			case "missing_successor":
				plans = plans[:2]
			case "other_table":
				after[8] = [][]string{{"altered-receipt"}}
			}
			after[6] = plans
			body, err := json.Marshal(after)
			if err != nil {
				t.Fatal(err)
			}
			retained := string(body)
			for _, id := range []string{"successor-a", "successor-b"} {
				retained, err = channelHistoryWithoutSuccessorPlan(retained, id)
				if err != nil {
					break
				}
			}
			if matches := err == nil && retained == string(want); matches != (mutation == "none") {
				t.Fatalf("multiple successor exception accepted %s=%t: %v", mutation, matches, err)
			}
		})
	}
}

func readChannelNativeHistory(t *testing.T, observer storetest.ChannelObservation) [8]string {
	t.Helper()
	row, err := observer.NativeHistory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// Mutable retirement and qualification metadata are deliberately not compared
// as immutable evidence. Each historical identity and frozen artifact is.
func readChannelPreservedHistory(t *testing.T, observer storetest.ChannelObservation, successor ...string) string {
	t.Helper()
	body, err := observer.PreservedHistory(t.Context(), successor)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func readChannelSourceHistory(t *testing.T, observer storetest.ChannelObservation, card string) string {
	history, err := observer.FrozenCardHistory(t.Context(), card)
	if err != nil {
		t.Fatal(err)
	}
	return history
}

func postChannelSourceControl(t *testing.T, callback, signing, card, token string, messageID int) int {
	t.Helper()
	return postChannelSourceUpdate(t, callback, signing, map[string]any{"update_id": 976000 + messageID, "callback_query": map[string]any{
		"id": "source-" + card, "from": map[string]any{"id": 7000},
		"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}}, "data": token,
	}})
}

func postChannelSourceUpdate(t *testing.T, callback, signing string, update map[string]any) int {
	t.Helper()
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, callback, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", signing)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNotFound {
		t.Fatalf("source-lifecycle callback returned %d", response.StatusCode)
	}
	return response.StatusCode
}
