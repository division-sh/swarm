package runtimepersistence

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetiredStandingRestartInterpretersStayAbsent(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	retired := []string{
		"StandingRun" + "UsesIntrinsicRecovery",
		"repair" + "StandingServiceTx",
		"standingRun" + "HasLiveWorkTx",
		"copyStanding" + "EntityStateTx",
		"StandingServiceTransition" + "Repaired",
	}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, symbol := range retired {
			if strings.Contains(string(body), symbol) {
				t.Errorf("%s retains retired standing restart interpreter %s", path, symbol)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production standing restart consumers: %v", err)
	}
}

func TestStandingRestartClassificationHasOneProductionOwner(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	allowed := map[string]struct{}{
		filepath.Join(root, "internal", "runtime", "pipeline", "standing_service_store.go"):                 {},
		filepath.Join(root, "internal", "store", "internal", "backend", "standingdisposition", "reader.go"): {},
	}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(body), "ClassifyStandingRestart(") {
			return nil
		}
		if _, ok := allowed[path]; !ok {
			t.Errorf("%s reconstructs standing restart disposition outside the canonical owner", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production standing restart classifiers: %v", err)
	}
}

func TestInboundStandingAdmissionConsumesCanonicalRestartDisposition(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	for _, relative := range []string{
		"internal/store/internal/backend/eventpersistence/inbound_publication.go",
		"internal/store/internal/backend/eventpersistence/sqlite_inbound_publication.go",
	} {
		body, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatalf("read %s: %v", relative, err)
		}
		text := string(body)
		if !strings.Contains(text, "storestandingdisposition.ReadByRun") {
			t.Errorf("%s bypasses the canonical standing restart disposition", relative)
		}
		for _, retired := range []string{"effectiveState != \"active\"", "effectiveState == \"active\""} {
			if strings.Contains(text, retired) {
				t.Errorf("%s retains partial standing admission interpreter %q", relative, retired)
			}
		}
	}
}

func TestStandingRestartSchemaRejectsAutomaticRepair(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	body, err := os.ReadFile(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	spec := string(body)
	if strings.Contains(spec, "'"+"repaired"+"'") {
		t.Fatal("platform schema still admits the retired automatic standing repair transition")
	}
	if !strings.Contains(spec, "'restored_stopped'") {
		t.Fatal("platform schema does not admit typed terminal declaration restoration")
	}
}

func TestDevModeDoesNotImplicitlyAbandonRuns(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	body, err := os.ReadFile(filepath.Join(root, "internal", "cliapp", "cli.go"))
	if err != nil {
		t.Fatalf("read serve CLI composition: %v", err)
	}
	if strings.Contains(string(body), "AbandonActiveRuns = true") {
		t.Fatal("serve CLI restored implicit active-run abandonment")
	}
}
