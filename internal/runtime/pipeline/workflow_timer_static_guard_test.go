package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowTimerLegacyInterpreterStaticAbsence(t *testing.T) {
	assertProductionSymbolsAbsent(t, []string{
		"TimerIntent",
		"TimerOperation",
		"TimerApplier",
		"CanonicalWorkflowTimer",
		"IsWorkflowTimerSchedule",
		"workflowTimerScheduleMatchesActivation",
		"rejectObsoleteWorkflowTimerRows",
	})
}

func TestWorkflowLifecycleLegacyCreatorStaticAbsence(t *testing.T) {
	assertProductionSymbolsAbsent(t, []string{
		"ArmFlowInstanceInitialStageLifecycle",
		"armWorkflowCurrentStageLifecycle",
		"updateEntityState",
		"applyWorkflowGateMutation",
	})
}

func TestWorkflowTimerStoreCannotInterpretTypedIdentity(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	assertSymbolsAbsentUnder(t, filepath.Join(root, "internal", "store"), root, []string{
		"ParseWorkflowTimerActivationTaskID",
		"IsWorkflowTimerActivationTaskID",
		"ParseWorkflowTimerOccurrenceTaskID",
		"WorkflowTimerActivationTaskPrefix",
	})
}

func TestSelectedContractTimerReconstructionWriterStaticAbsence(t *testing.T) {
	assertProductionSymbolsAbsent(t, []string{
		"RunForkHistoricalReplayTimerReconstructionOwner",
		"RemintWorkflowTimerActivationForFork",
		"RemintPersistedWorkflowTimerForFork",
		"planRunForkSelectedContractTimerReconstruction",
		"insertRunForkSelectedContractTimerReconstructions",
		"runForkReplayResumeAdmissionWithTimerReconstruction",
	})
}

func assertProductionSymbolsAbsent(t *testing.T, forbidden []string) {
	t.Helper()
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	assertSymbolsAbsentUnder(t, filepath.Join(root, "internal"), root, forbidden)
}

func assertSymbolsAbsentUnder(t *testing.T, searchRoot, reportRoot string, forbidden []string) {
	t.Helper()
	err := filepath.WalkDir(searchRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(source)
		for _, symbol := range forbidden {
			if strings.Contains(text, symbol) {
				relative, relErr := filepath.Rel(reportRoot, path)
				if relErr != nil {
					relative = path
				}
				t.Errorf("retired workflow lifecycle symbol %q remains in %s", symbol, filepath.ToSlash(relative))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
