package releasee2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

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
			config := filepath.Join(root, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(root, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, root, store.passwordEnv, 0)
			credentialEnv := goldenProcessEnv(t, root, "", 0)
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", contracts, "--config", config, "--portable", "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			for _, alias := range []string{"alpha", "beta"} {
				secret := runReleaseCommand(t, 30*time.Second, root, credentialEnv, alias+"-secret\n", binary, "secrets", "set", "webhook_signing."+alias, "--stdin")
				if secret.err != nil {
					t.Fatalf("signing secret %s: %v\n%s", alias, secret.err, secret.output)
				}
			}
			start := func(source, storedHash string) *releaseServeProcess {
				t.Helper()
				spec := releaseProcessSpec{BinaryPath: binary, WorkingDir: root, Source: source,
					ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env,
					RedactValues: []string{"alpha-secret", "beta-secret"}}
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
			postStandingRootTreePublicUpdates(t, p, store, services, 100)
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
				if after.ControlReason != "standing_reconcile" || after.RunID != before.RunID || after.Origin != before.Origin || after.Status != before.Status || !after.StartedAt.Equal(before.StartedAt) {
					t.Fatalf("restart changed standing generation %s: before=%+v after=%+v", alias, before, after)
				}
			}
			services = restarted
			if !reflect.DeepEqual(original, requireStandingRootTreePublicEvidence(t, store, services)) {
				t.Fatal("restart repeated construction or creating work")
			}
			postStandingRootTreePublicUpdates(t, p, store, services, 200)
			ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
			defer cancel()
			var reset standingRuntimePublicResult
			if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": services["beta"].Origin.ServiceID, "idempotency_key": "root-tree-reset"}, &reset); err != nil {
				t.Fatalf("public reset: %v\n%s", err, p.output.String())
			}
			if reset.RunID == services["beta"].RunID || reset.Generation != services["beta"].Origin.Generation+1 {
				t.Fatalf("reset did not select one new tree: %+v", reset)
			}
			services["beta"] = waitForFullLifecycleStandingRun(t, p.rpc, hash, reset.ServiceID, reset.Generation, reset.RunID)
			requireStandingRootTreePublicEvidence(t, store, services)
			postStandingRootTreePublicUpdates(t, p, store, services, 300)
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatal(err)
			}
			t.Log("proof_surface=compiled public verify/initial serve and signed text ingress; H compiled internal retained-artifact live-node lifecycle after source deletion with public terminal readback/reset. Native callback intent admission with zero business events; not public persisted-hash boot, real-provider qualification or provider callback execution")
		})
	}
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
			// Fixed public service coordinates, independent of the runtime codec.
			for alias, serviceID := range map[string]string{"alpha": "9f3aa24a-6122-56d2-8bcf-0c8d14d3d7bc", "beta": "7eccc4e9-f0f3-5fcd-b5ed-1e1820d4bf7f"} {
				if run.Origin.Kind == "standing_generation" && run.Origin.ServiceID == serviceID && run.Status == "running" {
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

func postStandingRootTreePublicUpdates(t *testing.T, p *releaseServeProcess, store goldenStoreSelection, services map[string]fullLifecycleRun, base int) {
	t.Helper()
	type publicationReceipt struct {
		ServiceID         string   `json:"service_id"`
		RunID             string   `json:"run_id"`
		Generation        int64    `json:"generation"`
		FlowPath          string   `json:"flow_path"`
		Provider          string   `json:"provider"`
		ProviderEventID   string   `json:"provider_event_id"`
		PublicationID     string   `json:"publication_id"`
		EventIDs          []string `json:"event_ids"`
		ActionDisposition string   `json:"operator_channel_action_disposition"`
	}
	for i, alias := range []string{"alpha", "beta"} {
		for j, shape := range []string{"text", "callback"} {
			id := base + i*10 + j
			body := fmt.Sprintf(`{"update_id":%d,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"hello"}}`, id)
			if shape == "callback" {
				body = fmt.Sprintf(`{"update_id":%d,"callback_query":{"id":"query-%d","from":{"id":41},"message":{"message_id":7,"chat":{"id":42,"type":"private"}},"data":"opaque-action-token"}}`, id, id)
			}
			req, err := http.NewRequest(http.MethodPost, p.apiBase+"/webhooks/"+alias+"/telegram", bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", alias+"-secret")
			response, err := p.rpc.client.Do(req)
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
			if receipt.ServiceID != services[alias].Origin.ServiceID || receipt.RunID != services[alias].RunID || receipt.Generation != services[alias].Origin.Generation || receipt.FlowPath != flow || receipt.Provider != "telegram" || receipt.ProviderEventID != fmt.Sprint(id) {
				t.Fatalf("%s %s ingress lost exact declaration binding: %+v", alias, shape, receipt)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			for _, retired := range []string{"entity_id", "instance_id", "target_flow_instance"} {
				if _, exists := fields[retired]; exists {
					t.Fatalf("declaration receipt includes retired %s: %s", retired, raw)
				}
			}
			duplicateRequest, err := http.NewRequest(http.MethodPost, req.URL.String(), bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			duplicateRequest.Header = req.Header.Clone()
			duplicateResponse, err := p.rpc.client.Do(duplicateRequest)
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
	var provider, providerEvent, storedFlow, targetAlias, resolvedRun, service, state string
	var generation int64
	var outputCount, memberCount int
	if err := db.QueryRowContext(ctx, `SELECT provider, provider_event_id, flow_path, target_alias, resolved_run_id, stable_service_id, expected_generation, state, output_count FROM inbound_publications WHERE publication_id=$1`, publicationID).Scan(&provider, &providerEvent, &storedFlow, &targetAlias, &resolvedRun, &service, &generation, &state, &outputCount); err != nil {
		t.Fatal(err)
	}
	if provider != "telegram" || providerEvent != fmt.Sprint(updateID) || storedFlow != flow || targetAlias != alias || resolvedRun != run.RunID || service != run.Origin.ServiceID || generation != run.Origin.Generation || state != "committed" || outputCount != 0 {
		t.Fatalf("callback publication lost exact generation/alias: provider=%s occurrence=%s flow=%s alias=%s run=%s service=%s generation=%d state=%s outputs=%d", provider, providerEvent, storedFlow, targetAlias, resolvedRun, service, generation, state, outputCount)
	}
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
		Account      string `json:"external_account_reference"`
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
