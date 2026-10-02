package releasee2e

import (
	"bytes"
	"context"
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

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestStandingRootTreePublicRestartAndResetBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			contracts := canonicalrouting.CopyStandingRootTreePublic(t)
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
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", contracts, "--config", config, "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			for _, alias := range []string{"alpha", "beta"} {
				secret := runReleaseCommand(t, 30*time.Second, root, credentialEnv, alias+"-secret\n", binary, "secrets", "set", "webhook_signing."+alias, "--stdin")
				if secret.err != nil {
					t.Fatalf("signing secret %s: %v\n%s", alias, secret.err, secret.output)
				}
			}
			start := func(source, hash string) *releaseServeProcess {
				t.Helper()
				p := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: root, Source: source, BundleHash: hash,
					ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env,
					RedactValues: []string{"alpha-secret", "beta-secret"}})
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
			postStandingRootTreePublicUpdates(t, p, services, 100)
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
			postStandingRootTreePublicUpdates(t, p, services, 200)
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
			postStandingRootTreePublicUpdates(t, p, services, 300)
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatal(err)
			}
			t.Log("proof_surface=compiled public verify/serve, real signed provider ingress, source deletion/hash restart, authenticated reset; no private construction or mock lifecycle launcher")
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
			for alias, flow := range map[string]string{"alpha": ".", "beta": "beta"} {
				// The identifier is the public declaration coordinate, not a root-run coordinate.
				if run.Origin.Kind == "standing_generation" && run.Origin.ServiceID == flowidentity.StandingServiceID(flow) && run.Status == "running" {
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

func postStandingRootTreePublicUpdates(t *testing.T, p *releaseServeProcess, services map[string]fullLifecycleRun, base int) {
	t.Helper()
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
			var receipt struct {
				EventIDs []string `json:"event_ids"`
			}
			if err != nil || response.StatusCode != http.StatusAccepted || json.Unmarshal(raw, &receipt) != nil || len(receipt.EventIDs) != 2 {
				t.Fatalf("%s %s ingress: status=%d response=%s err=%v\n%s", alias, shape, response.StatusCode, raw, err, p.output.String())
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
					for _, delivery := range event.Deliveries {
						if delivery.Target.EntityID == "" || (delivery.Target.FlowID != "." && delivery.Target.FlowID != "beta" && delivery.Target.FlowID != alias+"-receiver") {
							return false, fmt.Errorf("wrong exact provider target: %+v", delivery)
						}
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
