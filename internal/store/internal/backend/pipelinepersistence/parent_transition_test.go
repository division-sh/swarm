package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestPipelineParentTransitionOwnsExactRecoveryAcquisition(t *testing.T) {
	var registry pipelineRecoveryTransitions
	// Reserve before a claim exists: this is the original acquisition/scan
	// association gap that terminalization must not race through.
	finishClaim, ok := registry.reserve("target")
	if !ok {
		t.Fatal("initial scan acquisition refused")
	}
	defer finishClaim()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		transition, err := registry.begin(ctx, "target")
		if transition != nil {
			transition.Done()
		}
		result <- err
	}()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		registry.mu.Lock()
		pending := registry.runs["target"].transitions != 0
		registry.mu.Unlock()
		if pending {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("parent transition never entered recovery admission")
		}
	}
	if done, admitted := registry.reserve("target"); admitted {
		done()
		t.Fatal("new scan acquisition bypassed pending parent transition")
	}
	unrelated, err := registry.begin(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	unrelated.Done()
	select {
	case err := <-result:
		t.Fatalf("parent transition passed a pending acquisition: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled parent admission=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not release parent admission")
	}
	finishClaim()
	transition, err := registry.begin(context.Background(), "target")
	if err != nil {
		t.Fatal(err)
	}
	if done, admitted := registry.reserve("target"); admitted {
		done()
		t.Fatal("scan entered while parent transaction owns exclusion")
	}
	transition.Done()
	transition.Done()
	done, admitted := registry.reserve("target")
	if !admitted {
		t.Fatal("parent settlement did not release scan admission")
	}
	done()
	done()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.runs) != 0 {
		t.Fatalf("settled recovery admission leaked run entries: %d", len(registry.runs))
	}
}

func TestPipelineForkTransitionJoinsStartedWorkButNotPublicationPreparation(t *testing.T) {
	var registry pipelineRecoveryTransitions
	finishPublication, admitted := registry.reservePublication("source")
	if !admitted {
		t.Fatal("source publication refused before fork")
	}
	finishStarted, admitted := registry.reserve("source")
	if !admitted {
		t.Fatal("started source work refused before fork")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		transition, err := registry.beginFork(ctx, "source")
		if transition != nil {
			transition.Done()
		}
		finished <- err
	}()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		registry.mu.Lock()
		state := registry.runs["source"]
		fenced := state != nil && state.forks != 0
		registry.mu.Unlock()
		if fenced {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("fork transition did not fence started-work admission")
		}
	}
	fresh, ok := registry.reservePublication("source")
	if !ok {
		t.Fatal("fresh source publication could not commit while fork joined started work")
	}
	if done, ok := registry.reserve("source"); ok {
		done()
		t.Fatal("fresh recovery crossed source fork fence")
	}
	other, ok := registry.reservePublication("other")
	if !ok {
		t.Fatal("unrelated publication was fenced")
	}
	other()
	var stopRegistry pipelineRecoveryTransitions
	stopPublication, ok := stopRegistry.reservePublication("source")
	if !ok {
		t.Fatal("source publication refused before stop")
	}
	stop, err := stopRegistry.begin(ctx, "source")
	if err != nil {
		t.Fatalf("stop waited for foreground publication: %v", err)
	}
	stop.Done()
	stopPublication()
	select {
	case err := <-finished:
		t.Fatalf("fork did not join started source work: %v", err)
	default:
	}
	finishPublication()
	select {
	case err := <-finished:
		t.Fatalf("fork waited for publication after its started work remained: %v", err)
	default:
	}
	finishStarted()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("fork did not join settled started work")
	}
	fresh()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.runs) != 0 {
		t.Fatalf("settled publication retained run membership: %d", len(registry.runs))
	}
}

func TestPipelineForkTransitionUsesRouteRunForRunlessDecisionEvent(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "route.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE events (event_id TEXT PRIMARY KEY, run_id TEXT);
		CREATE TABLE decision_card_route_obligations (event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	eventID, sourceRun := uuid.NewString(), uuid.NewString()
	if _, err := db.Exec(`INSERT INTO events (event_id, run_id) VALUES (?, NULL)`, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO decision_card_route_obligations (event_id, run_id, status) VALUES (?, ?, 'pending')`, eventID, sourceRun); err != nil {
		t.Fatal(err)
	}
	actual, err := pipelineClaimEventRun(context.Background(), db, eventID, runtimepipelineobligation.PurposeDecisionRoute, false)
	if err != nil || actual != sourceRun {
		t.Fatalf("decision route run = %q, %v; want %q", actual, err, sourceRun)
	}
	var registry pipelineRecoveryTransitions
	claimDone, admitted := registry.reserve(actual)
	if !admitted {
		t.Fatal("route claim refused before source transition")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		transition, err := registry.beginFork(ctx, sourceRun)
		if transition != nil {
			transition.Done()
		}
		result <- err
	}()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		registry.mu.Lock()
		state := registry.runs[sourceRun]
		fenced := state != nil && state.forks != 0
		registry.mu.Unlock()
		if fenced {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("source fork did not install its route fence")
		}
	}
	select {
	case err := <-result:
		t.Fatalf("source fork passed its runless route claim: %v", err)
	default:
	}
	claimDone()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source fork did not join its runless route claim")
	}
}
