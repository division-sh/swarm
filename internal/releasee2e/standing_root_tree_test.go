package releasee2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var standingRootTreeAccounts = []struct{ alias, account, token string }{
	{"alpha", "420079", "tree-alpha-token"},
	{"beta", "420080", "tree-beta-token"},
}

type standingRootTreeProvider struct {
	endpoint *httptest.Server
	listener atomic.Value
	mu       sync.Mutex
	webhooks map[string]struct{ URL, Secret string }
}

func TestStandingRootTreePublicRestartAndResetBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	retainedBinary := buildOwnedMockLifecycleBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			contracts := filepath.Join(root, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/standing_root_tree"), contracts)
			store := goldenSQLiteStore(root)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			native, config, env := prepareStandingRootTreeAccounts(t, binary, root, contracts, store)
			token := filepath.Join(root, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", contracts, "--config", config, "--portable", "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			start := func(source, storedHash string) *releaseServeProcess {
				t.Helper()
				native.listener.Store(fmt.Sprintf("127.0.0.1:%d", freeReleaseTCPPort(t)))
				spec := releaseProcessSpec{BinaryPath: binary, WorkingDir: root, Source: source,
					ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env,
					PublicWebhookBaseURL: native.endpoint.URL, PublicWebhookListen: native.listener.Load().(string),
					RedactValues: []string{fullLifecycleSigningSecret, fullLifecycleBotToken, "tree-alpha-token", "tree-beta-token"}}
				if storedHash != "" {
					spec.InternalMockLifecycleBinary, spec.RetainedBundleHash = retainedBinary, storedHash
					spec.RetainedLiveExecution = true
				}
				p := startReleaseServe(t, spec)
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := p.waitReady(ctx); err != nil {
					t.Fatalf("public standing readiness: %v\n%s", err, p.output.String())
				}
				return p
			}
			p := start(contracts, "")
			hash := goldenServedBundleHash(t, p.rpc, "live")
			services := requireStandingRootTreePublicServices(t, p, hash)
			original := requireStandingRootTreePublicEvidence(t, store, services)
			postStandingRootTreePublicUpdates(t, p, native, store, services, 100)
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(contracts); err != nil {
				t.Fatal(err)
			}
			p = start("", hash)
			if got := goldenServedBundleHash(t, p.rpc, "live"); got != hash {
				t.Fatal("hash-only restart changed the admitted source")
			}
			restarted := requireStandingRootTreePublicServices(t, p, hash)
			for alias, before := range services {
				after := restarted[alias]
				if after.RunID != before.RunID || after.Origin != before.Origin || after.Status != before.Status || !after.StartedAt.Equal(before.StartedAt) {
					t.Fatalf("restart changed standing generation %s: before=%+v after=%+v", alias, before, after)
				}
			}
			services = restarted
			if !reflect.DeepEqual(original, requireStandingRootTreePublicEvidence(t, store, services)) {
				t.Fatal("restart repeated construction or creating work")
			}
			postStandingRootTreePublicUpdates(t, p, native, store, services, 200)
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatal(err)
			}
			t.Log("proof_surface=compiled public verify/initial serve and signed text ingress; H compiled internal retained-artifact live-node lifecycle after source deletion with public terminal readback/reset. Native callback intent admission with zero business events; not public persisted-hash boot, real-provider qualification or provider callback execution")
		})
	}
}

func prepareStandingRootTreeAccounts(t *testing.T, binary, root, contracts string, store goldenStoreSelection) (*standingRootTreeProvider, string, []string) {
	t.Helper()
	native := &standingRootTreeProvider{webhooks: make(map[string]struct{ URL, Secret string })}
	proxy := &httputil.ReverseProxy{Director: func(request *http.Request) {
		request.URL.Scheme, request.URL.Host = "http", native.listener.Load().(string)
	}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/bot") {
			w.Header().Set("Content-Type", "application/json")
			for _, account := range standingRootTreeAccounts {
				method, matched := strings.CutPrefix(request.URL.Path, "/bot"+account.token+"/")
				if !matched {
					continue
				}
				native.mu.Lock()
				defer native.mu.Unlock()
				switch method {
				case "getMe":
					fmt.Fprintf(w, `{"ok":true,"result":{"id":%s,"username":"SwarmTestBot"}}`, account.account)
				case "getWebhookInfo":
					json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]string{"url": native.webhooks[account.token].URL}})
				case "setWebhook":
					var payload struct {
						URL    string `json:"url"`
						Secret string `json:"secret_token"`
					}
					if json.NewDecoder(request.Body).Decode(&payload) != nil {
						http.Error(w, `{"ok":false}`, http.StatusBadRequest)
						return
					}
					native.webhooks[account.token] = struct{ URL, Secret string }{payload.URL, payload.Secret}
					io.WriteString(w, `{"ok":true,"result":true}`)
				default:
					http.Error(w, `{"ok":false}`, http.StatusNotFound)
				}
				return
			}
			http.Error(w, `{"ok":false}`, http.StatusUnauthorized)
			return
		}
		if native.listener.Load() == nil {
			http.Error(w, "no child public listener", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, request)
	}))
	native.endpoint = server
	t.Cleanup(server.Close)
	certificate := filepath.Join(root, "provider-ca.pem")
	writeReleaseFile(t, certificate, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})))
	config := filepath.Join(root, "swarm.yaml")
	packConfig := "platform:\n  packs:\n    platform_dirs:\n"
	credentialEnv := goldenProcessEnv(t, root, "", 0)
	inventoryResult := runReleaseCommand(t, goldenStartupTimeout, root, credentialEnv, "", binary, "packs", "list", contracts, "--json")
	var inventory struct {
		BaseMode string                                   `json:"base_mode"`
		Packs    []struct{ ID, Source, Directory string } `json:"packs"`
	}
	if inventoryResult.err != nil || json.Unmarshal([]byte(inventoryResult.output), &inventory) != nil || inventory.BaseMode != "embedded" || len(inventory.Packs) == 0 {
		t.Fatalf("public embedded pack inventory: err=%v output=%s", inventoryResult.err, inventoryResult.output)
	}
	for _, entry := range inventory.Packs {
		if entry.Source != "embedded" || !filepath.IsLocal(entry.Directory) || entry.ID == "" || filepath.Base(entry.ID) != entry.ID {
			t.Fatalf("invalid fixture pack coordinates: %+v", entry)
		}
		directory := filepath.Join(root, "platform", entry.ID)
		copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "packs", entry.Directory), directory)
		if entry.ID == "telegram" {
			members, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range members {
				path := filepath.Join(directory, member.Name())
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				writeReleaseFile(t, path, strings.ReplaceAll(string(body), "https://api.telegram.org", server.URL))
			}
		}
		packConfig += "      - " + strconv.Quote(directory) + "\n"
	}
	for index, account := range standingRootTreeAccounts {
		source := contracts
		if index != 0 {
			source = filepath.Join(source, "beta")
		}
		for _, name := range []string{"channel.yaml", "connector.yaml"} {
			path := filepath.Join(source, "telegram", name)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			writeReleaseFile(t, path, strings.ReplaceAll(string(body), "https://api.telegram.org", server.URL))
		}
		writeReleaseFile(t, filepath.Join(source, "policy.yaml"), "operator_account: "+strconv.Quote(account.account)+"\n")
	}
	writeReleaseFile(t, config, "paths:\n  swarm_dir: "+strconv.Quote(filepath.Join(root, ".swarm"))+"\n"+goldenRuntimeConfig(store)+packConfig)
	env := goldenProcessEnv(t, root, store.passwordEnv, 0)
	env = append(env, "SSL_CERT_FILE="+certificate)
	for key, value := range map[string]string{
		"telegram.webhook_signing_secret": fullLifecycleSigningSecret,
		"telegram.telegram_bot_token":     fullLifecycleBotToken,
	} {
		result := runReleaseCommand(t, 30*time.Second, root, credentialEnv, value+"\n", binary, "secrets", "set", key, "--stdin")
		if result.err != nil {
			t.Fatalf("provision original source tool credential %s: %v\n%s", key, result.err, result.output)
		}
	}
	for _, account := range standingRootTreeAccounts {
		result := runReleaseCommand(t, 30*time.Second, root, env, account.token+"\n", binary,
			"--config", config, "channel", "connect", "telegram", "--yes", "--json")
		var registered struct {
			Operation struct {
				Provider         string `json:"provider"`
				Account          string `json:"operator_account"`
				AccountConfirmed bool   `json:"account_confirmed"`
				Phase            string `json:"phase"`
			} `json:"operation"`
		}
		if result.err != nil || json.Unmarshal([]byte(result.output), &registered) != nil ||
			registered.Operation.Provider != "telegram" || registered.Operation.Account != account.account ||
			!registered.Operation.AccountConfirmed || registered.Operation.Phase != "succeeded" {
			t.Fatalf("register exact root-tree account %s: err=%v output=%s", account.alias, result.err, result.output)
		}
	}
	return native, config, env
}

func requireStandingRootTreePublicServices(t *testing.T, p *releaseServeProcess, hash string) map[string]fullLifecycleRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	var services map[string]fullLifecycleRun
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		runs, err := listFullLifecycleRuns(ctx, p.rpc, hash)
		if err != nil {
			return false, err
		}
		services = map[string]fullLifecycleRun{}
		for _, run := range runs {
			for alias, flow := range map[string]string{"alpha": ".", "beta": "beta"} {
				if run.Origin.Kind == "deployment" && run.FlowPath == flow && run.Status == "live" {
					services[alias] = run
				}
			}
		}
		return len(services) == 2 && services["alpha"].RunID != services["beta"].RunID, nil
	})
	if err != nil {
		t.Fatalf("public standing generations: %v services=%+v\n%s", err, services, p.output.String())
	}
	return services
}

func requireStandingRootTreePublicEvidence(t *testing.T, store goldenStoreSelection, services map[string]fullLifecycleRun) map[string][]string {
	t.Helper()
	db, err := openLifecycleFailureInspection(store)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result := map[string][]string{}
	for alias, run := range services {
		rows, err := db.Query(`SELECT instance_path, entity_id, flow_template FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`, run.RunID)
		if err != nil {
			t.Fatal(err)
		}
		paths := map[string]bool{}
		for rows.Next() {
			var path, entity, template string
			if err := rows.Scan(&path, &entity, &template); err != nil {
				t.Fatal(err)
			}
			paths[path] = true
			wantTemplate := path
			if path == run.RunID {
				wantTemplate = "."
			}
			if template != wantTemplate || entity == "" || (path == run.RunID && entity != run.RunID) {
				t.Fatalf("noncanonical tree header: path=%s entity=%s template=%s", path, entity, template)
			}
			result[alias+"/headers"] = append(result[alias+"/headers"], path+":"+entity+":"+template)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if len(paths) != 5 || !paths[run.RunID] || !paths["beta"] || !paths["alpha-receiver"] || !paths["alpha-receiver/detail"] || !paths["beta-receiver"] {
			t.Fatalf("generation lacks its full root tree: %+v", paths)
		}
		rows, err = db.Query(`SELECT entity_id, projection FROM workflow_instance_initial_materializations WHERE run_id=$1 ORDER BY entity_id`, run.RunID)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var entity, construction string
			if err := rows.Scan(&entity, &construction); err != nil {
				t.Fatal(err)
			}
			result[alias+"/receipts"] = append(result[alias+"/receipts"], entity+":"+construction)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if len(result[alias+"/receipts"]) != 5 {
			t.Fatalf("generation construction receipts=%+v", result)
		}
	}
	return result
}

func postStandingRootTreePublicUpdates(t *testing.T, p *releaseServeProcess, native *standingRootTreeProvider, store goldenStoreSelection, services map[string]fullLifecycleRun, base int) {
	t.Helper()
	type publicationReceipt struct {
		RunID             string   `json:"run_id"`
		FlowPath          string   `json:"flow_path"`
		Provider          string   `json:"provider"`
		ProviderEventID   string   `json:"provider_event_id"`
		PublicationID     string   `json:"publication_id"`
		EventIDs          []string `json:"event_ids"`
		ActionDisposition string   `json:"operator_channel_action_disposition"`
	}
	for i, account := range standingRootTreeAccounts {
		alias := account.alias
		native.mu.Lock()
		registration := native.webhooks[account.token]
		native.mu.Unlock()
		callback, secret := registration.URL, registration.Secret
		location, err := url.Parse(callback)
		if err != nil || location.Scheme != "https" || !strings.HasPrefix(callback, native.endpoint.URL+"/webhooks/") || secret == "" {
			t.Fatalf("root-tree %s has no exact registered callback", alias)
		}
		for j, shape := range []string{"text", "callback"} {
			id := base + i*10 + j
			body := fmt.Sprintf(`{"update_id":%d,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"hello"}}`, id)
			if shape == "callback" {
				body = fmt.Sprintf(`{"update_id":%d,"callback_query":{"id":"query-%d","from":{"id":41},"message":{"message_id":7,"chat":{"id":42,"type":"private"}},"data":"opaque-action-token"}}`, id, id)
			}
			req, err := http.NewRequest(http.MethodPost, callback, bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
			client := *native.endpoint.Client()
			client.Timeout = p.rpc.client.Timeout
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(response.Body)
			response.Body.Close()
			var receipt publicationReceipt
			if err != nil || response.StatusCode != http.StatusAccepted || json.Unmarshal(raw, &receipt) != nil || receipt.PublicationID == "" {
				t.Fatalf("%s %s ingress: status=%d response=%s err=%v\n%s", alias, shape, response.StatusCode, raw, err, p.output.String())
			}
			flow := "beta"
			if alias == "alpha" {
				flow = "."
			}
			if receipt.RunID != services[alias].RunID || receipt.FlowPath != flow || receipt.Provider != "telegram" || receipt.ProviderEventID != standingRootTreeProviderOccurrence(t, alias, id) {
				t.Fatalf("%s %s ingress lost exact declaration binding: %+v", alias, shape, receipt)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			for _, retired := range []string{"service_id", "generation", "entity_id", "instance_id", "target_flow_instance"} {
				if _, exists := fields[retired]; exists {
					t.Fatalf("declaration receipt includes retired %s: %s", retired, raw)
				}
			}
			duplicateRequest, err := http.NewRequest(http.MethodPost, req.URL.String(), bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			duplicateRequest.Header = req.Header.Clone()
			duplicateResponse, err := client.Do(duplicateRequest)
			if err != nil {
				t.Fatal(err)
			}
			duplicateRaw, err := io.ReadAll(duplicateResponse.Body)
			duplicateResponse.Body.Close()
			var duplicate publicationReceipt
			decodeErr := json.Unmarshal(duplicateRaw, &duplicate)
			// Duplicate readback carries the immutable publication, not pending intent state.
			duplicate.ActionDisposition = receipt.ActionDisposition
			if err != nil || duplicateResponse.StatusCode != http.StatusOK || decodeErr != nil || !reflect.DeepEqual(duplicate, receipt) {
				t.Fatalf("%s %s duplicate: status=%d response=%s err=%v", alias, shape, duplicateResponse.StatusCode, duplicateRaw, err)
			}
			if shape == "callback" {
				if len(receipt.EventIDs) != 0 || receipt.ActionDisposition != "pending" {
					t.Fatalf("callback published business events or lost native intent: %s", raw)
				}
				requireStandingRootTreePublicationMarker(t, p.rpc, services[alias], receipt.FlowPath, id, receipt.PublicationID)
				requireStandingRootTreeActionIntent(t, store, services[alias], alias, id, receipt.PublicationID, receipt.FlowPath)
				continue
			}
			if len(receipt.EventIDs) != 2 || receipt.ActionDisposition != "" {
				t.Fatalf("text lost its raw/normalized business events: %s", raw)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			err = pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				events, err := listFullLifecycleEvents(ctx, p.rpc, services[alias].RunID)
				if err != nil {
					return false, err
				}
				settled := 0
				for _, event := range events {
					if event.EventID != receipt.EventIDs[1] {
						continue
					}
					if len(event.Deliveries) != 2 {
						return false, fmt.Errorf("normalized provider deliveries=%+v", event.Deliveries)
					}
					sourceFlow := "beta"
					if alias == "alpha" {
						sourceFlow = "."
					}
					remaining := map[string]bool{sourceFlow: true, alias + "-receiver": true}
					for _, delivery := range event.Deliveries {
						if delivery.Target.EntityID == "" || !remaining[delivery.Target.FlowID] {
							return false, fmt.Errorf("wrong exact provider target: %+v", delivery)
						}
						delete(remaining, delivery.Target.FlowID)
						if !delivery.Terminal || delivery.Status != "delivered" {
							return false, nil
						}
					}
					settled++
				}
				return settled == 1, nil
			})
			cancel()
			if err != nil {
				t.Fatalf("public provider settlement: %v\n%s", err, p.output.String())
			}
		}
	}
}

func requireStandingRootTreeActionIntent(t *testing.T, store goldenStoreSelection, run fullLifecycleRun, alias string, updateID int, publicationID, declaringFlow string) {
	t.Helper()
	db, err := openLifecycleFailureInspection(store)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	flow, path := "beta", "beta"
	if alias == "alpha" {
		flow, path = ".", run.RunID
	}
	if declaringFlow != flow {
		t.Fatalf("callback declaration=%s, want %s", declaringFlow, flow)
	}
	var headerEntity string
	if err := db.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND flow_template=$3`, run.RunID, path, flow).Scan(&headerEntity); err != nil || headerEntity == "" {
		t.Fatalf("generation lacks its separately constructed header: header=%s err=%v", headerEntity, err)
	}
	provider, providerEvent := "telegram", standingRootTreeProviderOccurrence(t, alias, updateID)
	var memberCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id=$1`, publicationID).Scan(&memberCount); err != nil || memberCount != 0 {
		t.Fatalf("callback has executable business membership: count=%d err=%v", memberCount, err)
	}
	var interfaceKey, intentProvider, intentOccurrence, factJSON, authorization, intentState string
	var disposition sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT interface_key, provider, provider_event_id, fact, provider_authorization, state, disposition FROM operator_channel_action_intents WHERE publication_id=$1`, publicationID).Scan(&interfaceKey, &intentProvider, &intentOccurrence, &factJSON, &authorization, &intentState, &disposition); err != nil {
		t.Fatal(err)
	}
	var fact struct {
		Interface struct {
			Ref        string `json:"interface_ref"`
			Pack       string `json:"channel_pack_id"`
			Version    string `json:"channel_pack_version"`
			Manifest   string `json:"channel_manifest_hash"`
			Generation string `json:"semantic_generation"`
		} `json:"interface"`
		Account      string `json:"external_account"`
		Conversation string `json:"conversation_reference"`
		Message      string `json:"provider_message_reference"`
		Interaction  string `json:"interaction_reference"`
		Token        string `json:"token"`
	}
	if err := json.Unmarshal([]byte(factJSON), &fact); err != nil {
		t.Fatal(err)
	}
	if interfaceKey == "" || authorization == "" || intentProvider != provider || intentOccurrence != providerEvent || fact.Interface.Ref != "swarm.hitl-channel/v2" || fact.Interface.Pack == "" || fact.Interface.Version == "" || fact.Interface.Manifest == "" || fact.Interface.Generation == "" || fact.Account != "41" || fact.Conversation != "42" || fact.Message != `{"id":7}` || fact.Interaction != fmt.Sprintf("query-%d", updateID) || fact.Token != "opaque-action-token" {
		t.Fatalf("callback lost its exact verified provider fact: interface=%s provider=%s occurrence=%s fact=%s", interfaceKey, intentProvider, intentOccurrence, factJSON)
	}
	// The opaque token grants no action authority. Admission may remain pending;
	// if reconciled, it must settle rejected rather than execute business work.
	if (intentState == "pending" && disposition.Valid) || (intentState == "settled" && (!disposition.Valid || disposition.String != "rejected")) || (intentState != "pending" && intentState != "settled") {
		t.Fatalf("callback intent state=%s disposition=%v", intentState, disposition)
	}
}

func requireStandingRootTreePublicationMarker(t *testing.T, rpc *releaseRPCClient, run fullLifecycleRun, flow string, updateID int, publicationID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	evidence, err := listFullLifecycleEvents(ctx, rpc, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, marker := range evidence {
		if marker.EventName != "platform.inbound_recorded" || marker.Payload["publication_id"] != publicationID {
			continue
		}
		matched++
		payload := marker.Payload
		alias := "beta"
		if flow == "." {
			alias = "alpha"
		}
		if marker.RunID != run.RunID || marker.EntityID != "" || payload["provider"] != "telegram" || payload["provider_event_id"] != standingRootTreeProviderOccurrence(t, alias, updateID) ||
			payload["run_id"] != run.RunID || payload["flow_path"] != flow || payload["output_count"] != float64(0) {
			t.Fatalf("committed declaration evidence lost exact binding or borrowed a receiver: %+v", marker)
		}
		for _, field := range []string{"event_ids", "event_names"} {
			children, ok := payload[field].([]any)
			if !ok || len(children) != 0 {
				t.Fatalf("callback declaration evidence has business children: %+v", marker)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("callback publication has %d committed markers, want exactly one", matched)
	}
}

func standingRootTreeProviderOccurrence(t *testing.T, alias string, updateID int) string {
	t.Helper()
	for _, account := range standingRootTreeAccounts {
		if account.alias != alias {
			continue
		}
		encoded, err := json.Marshal([]string{"telegram", account.account, fmt.Sprint(updateID)})
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(encoded)
		return hex.EncodeToString(sum[:])
	}
	t.Fatalf("unknown root-tree account alias %s", alias)
	return ""
}
