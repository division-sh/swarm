package releasee2e

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func runStandingOrphanRestorationPublic(t *testing.T, binary, lifecycle, root string, selected goldenStoreSelection, product string) {
	t.Helper()
	spec := prepareFullLifecycleProject(t, binary, root, selected, false)
	spec.InternalMockLifecycleBinary = lifecycle
	writeReleaseFile(t, filepath.Join(spec.WorkingDir, ".swarm/swarm.yaml"), fullLifecycleRuntimeConfig(selected, false))
	if err := os.Remove(filepath.Join(spec.WorkingDir, spec.ConfigPath)); err != nil {
		t.Fatal(err)
	}
	spec.ConfigPath = ".swarm/swarm.yaml"
	cliEnv := make([]string, 0, len(spec.Env))
	for _, entry := range spec.Env {
		if !strings.HasPrefix(entry, goldenPostgresPass+"=") {
			cliEnv = append(cliEnv, entry)
		}
	}
	start := func() *releaseServeProcess {
		p := startReleaseServe(t, spec)
		ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleStartupLimit)
		defer cancel()
		if err := p.waitReady(ctx); err != nil {
			t.Fatalf("standing restoration startup: %v\n%s", err, p.output.String())
		}
		writeReleaseFile(t, filepath.Join(spec.WorkingDir, "operator.yaml"), "connection:\n  api_server: "+strconv.Quote(p.apiBase)+"\n  api_token_file: api-token\n")
		return p
	}
	p := start()
	standing := waitForFullLifecycleStandingRun(t, p.rpc, requireFullLifecycleHealth(t, p.rpc), "", 0, "")
	card := waitForFullLifecycleCard(t, p.rpc, standing.RunID, "lifecycle_ready")
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	if product == "terminal-orphan" {
		var result any
		if err := p.rpc.call(ctx, "mailbox.decide", map[string]any{"card_id": card.CardID, "verdict": "reject", "fields": map[string]any{}, "observed_content_hash": card.CardContentHash, "idempotency_key": "terminal-orphan-reject"}, &result); err != nil {
			t.Fatal(err)
		}
		waitForFullLifecycleRunStatus(t, p.rpc, standing.RunID, "completed")
	} else {
		decideFullLifecycleCard(t, p.rpc, card, "orphan-precondition-approve")
	}
	if err := p.stopAndWait(15 * time.Second); err != nil {
		t.Fatalf("standing precondition stop: %v\n%s", err, p.output.String())
	}
	if product == "invalid-orphan" || product == "broken-relation" {
		db := openStandingOfflineInspection(t, root, selected)
		query := `DELETE FROM standing_service_generations WHERE service_id=$1`
		args := []any{standing.Origin.ServiceID}
		if product == "invalid-orphan" {
			query = `UPDATE standing_services SET operator_override='suspended',effective_state='suspended',publication_state='pending',override_actor='test-precondition',override_at=$2 WHERE service_id=$1`
			args = append(args, time.Now().UTC())
		}
		_, err := db.ExecContext(ctx, query, args...)
		closeErr := db.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("offline exact standing product: write=%v close=%v", err, closeErr)
		}
	}
	if product == "broken-relation" {
		refused := startReleaseServe(t, spec)
		select {
		case <-refused.exited:
			if refused.waitError() == nil || !strings.Contains(refused.output.String(), "generation") {
				t.Fatalf("broken relation did not fail startup with evidence: %v\n%s", refused.waitError(), refused.output.String())
			}
		case <-time.After(fullLifecycleStartupLimit):
			t.Fatalf("broken relation startup did not exit\n%s", refused.output.String())
		}
		return
	}
	schemaPath := filepath.Join(spec.WorkingDir, spec.Source, "schema.yaml")
	original, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	// Cold declaration removal must remove its dependent producer/consumer
	// graph as well. Keep the authored bytes outside the admitted source root
	// for exact restoration; never fake a raw-provider consumer or input source.
	for _, flow := range []string{"telegram-ingress", "telegram-chat"} {
		if err := os.Rename(filepath.Join(spec.WorkingDir, spec.Source, flow), filepath.Join(root, "removed-"+flow)); err != nil {
			t.Fatal(err)
		}
	}
	writeReleaseFile(t, schemaPath, "name: telegram-lifecycle\n")
	verified := runReleaseCommand(t, fullLifecycleStartupLimit, spec.WorkingDir, spec.Env, "", binary, "verify", spec.Source, "--config", spec.ConfigPath, "--portable", "--json")
	if verified.err != nil {
		t.Fatalf("removed-declaration source is not admitted: %v\n%s", verified.err, verified.output)
	}
	p = start()
	before := captureFullLifecycleEvidence(t, p.rpc, standing.RunID)
	for _, serviceID := range []string{standing.Origin.ServiceID, uuid.NewString()} {
		for _, command := range []string{"suspend", "resume", "reset"} {
			key := command + "-" + serviceID
			var result any
			if err := p.rpc.call(ctx, "standing."+command, map[string]any{"service_id": serviceID, "idempotency_key": "rpc-" + key}, &result); err == nil {
				t.Fatalf("%s admitted unloaded/unknown service %s", command, serviceID)
			}
			resultCLI := runReleaseCommand(t, fullLifecycleStartupLimit, spec.WorkingDir, cliEnv, "", binary, "--config", "operator.yaml", "standing", command, serviceID, "--idempotency-key", "cli-"+key)
			if resultCLI.err == nil {
				t.Fatalf("compiled %s admitted unloaded/unknown service %s", command, serviceID)
			}
		}
	}
	if !reflect.DeepEqual(before, captureFullLifecycleEvidence(t, p.rpc, standing.RunID)) {
		t.Fatal("refused orphan commands mutated predecessor events/deliveries")
	}
	if err := p.stopAndWait(15 * time.Second); err != nil {
		t.Fatalf("orphan stop: %v\n%s", err, p.output.String())
	}
	writeReleaseFile(t, schemaPath, string(original))
	for _, flow := range []string{"telegram-ingress", "telegram-chat"} {
		if err := os.Rename(filepath.Join(root, "removed-"+flow), filepath.Join(spec.WorkingDir, spec.Source, flow)); err != nil {
			t.Fatal(err)
		}
	}
	p = start()
	bundle := requireFullLifecycleHealth(t, p.rpc)
	waitForFullLifecycleStandingRun(t, p.rpc, bundle, standing.Origin.ServiceID, standing.Origin.Generation, standing.RunID)
	if product == "terminal-orphan" {
		waitForFullLifecycleRunStatus(t, p.rpc, standing.RunID, "completed")
	}
	var reset standingRuntimePublicResult
	if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": standing.Origin.ServiceID, "idempotency_key": "restored-reset"}, &reset); err != nil {
		t.Fatalf("restored declaration reset: %v\n%s", err, p.output.String())
	}
	wantState := "active"
	if product == "invalid-orphan" {
		wantState = "suspended"
	}
	if reset.ServiceID != standing.Origin.ServiceID || reset.Generation != standing.Origin.Generation+1 || reset.RunID == standing.RunID || reset.EffectiveState != wantState {
		t.Fatalf("restored reset identity/override=%+v", reset)
	}
	waitForFullLifecycleStandingRun(t, p.rpc, bundle, reset.ServiceID, reset.Generation, reset.RunID)
	if wantState == "active" {
		waitForFullLifecycleCard(t, p.rpc, reset.RunID, "lifecycle_ready")
	} else {
		waitForFullLifecycleRunStatus(t, p.rpc, reset.RunID, "paused")
	}
	if err := p.stopAndWait(15 * time.Second); err != nil {
		t.Fatalf("restored successor stop: %v\n%s", err, p.output.String())
	}
}

func openStandingOfflineInspection(t *testing.T, root string, selected goldenStoreSelection) *sql.DB {
	t.Helper()
	if selected.name == "postgres" {
		if selected.inspectionConnector == nil {
			t.Fatal("exact isolated PostgreSQL inspection connector is missing")
		}
		return sql.OpenDB(selected.inspectionConnector)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	return db
}
