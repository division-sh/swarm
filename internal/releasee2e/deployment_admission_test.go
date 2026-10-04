package releasee2e

import (
	"encoding/json"
	"testing"
)

func assertReleaseDeploymentAdmission(t *testing.T, result releaseCommandResult) {
	t.Helper()
	var evidence struct {
		OK                bool   `json:"ok"`
		ValidationScope   string `json:"validation_scope"`
		AdmissionComplete bool   `json:"admission_complete"`
		LiveReadiness     string `json:"live_readiness"`
		Observations      []struct {
			CheckID string `json:"check_id"`
			Status  string `json:"status"`
		} `json:"observations"`
		ExecutionObligations []struct {
			ID string `json:"id"`
		} `json:"execution_obligations"`
	}
	if err := json.Unmarshal([]byte(result.output), &evidence); result.err != nil || err != nil || !evidence.OK ||
		evidence.ValidationScope != "deployment" || !evidence.AdmissionComplete || evidence.LiveReadiness != "not_evaluated" {
		t.Fatalf("release deployment admission: command=%v decode=%v\n%s", result.err, err, result.output)
	}
	observed := map[string]bool{}
	for _, observation := range evidence.Observations {
		if observation.CheckID == "selected_store_access" || observation.CheckID == "startup_process_possession" {
			if observation.Status != "passed" {
				t.Fatalf("deployment did not observe %s: %s", observation.CheckID, result.output)
			}
			observed[observation.CheckID] = true
		}
	}
	if !observed["selected_store_access"] || !observed["startup_process_possession"] || len(evidence.ExecutionObligations) == 0 {
		t.Fatalf("deployment admission omitted store evidence or startup obligations: %s", result.output)
	}
}
