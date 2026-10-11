package testtiming

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestCIArtifactTransportCensusIsCurrentAttemptAndLossless(t *testing.T) {
	w := loadAdmissionWorkflow(t)
	plans, proofs, nativeUploads, nativeDownloads := 0, 0, 0, 0
	for jobID, job := range w.Jobs {
		for _, step := range job.Steps {
			if !strings.Contains(step.Uses, "actions/upload-artifact@") && !strings.Contains(step.Uses, "actions/download-artifact@") {
				continue
			}
			name, _ := step.With["name"].(string)
			pattern, _ := step.With["pattern"].(string)
			switch {
			case strings.HasPrefix(name, "ci-plan"):
				if name != "ci-plan-${{ github.run_id }}-${{ github.run_attempt }}" {
					t.Fatalf("A03/A04 %s borrows non-current plan %q", jobID, name)
				}
				if strings.Contains(step.Uses, "download-artifact") {
					plans++
				}
			case strings.HasPrefix(name, "proof-"):
				if jobID != "proof-unit" || name != "proof-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.unit }}-timing" || step.If != "always()" || step.With["if-no-files-found"] != "error" {
					t.Fatal("A05/A10 physical-batch publication or failure retention changed")
				}
			case strings.HasPrefix(pattern, "proof-"):
				if pattern != "proof-${{ github.run_id }}-${{ github.run_attempt }}-*-timing" || step.With["merge-multiple"] != false || step.With["path"] != "test-results/evidence" {
					t.Fatalf("A07/A14 %s erases attempt or duplicate identity", jobID)
				}
				proofs++
			case strings.HasPrefix(name, "unused-"):
				if name != "unused-linux-${{ github.run_id }}-${{ github.run_attempt }}" && name != "unused-darwin-${{ github.run_id }}-${{ github.run_attempt }}" {
					t.Fatalf("U01/U02 non-current native product %q", name)
				}
				if strings.Contains(step.Uses, "upload-artifact") {
					nativeUploads++
				} else {
					nativeDownloads++
				}
			default:
				continue // Compilation inputs and observational outputs are not execution credit.
			}
			if step.With["run-id"] != nil || step.With["repository"] != nil || step.With["overwrite"] != nil {
				t.Fatalf("%s retains cross-run acquisition or mutable publication", jobID)
			}
		}
	}
	if plans != 5 || proofs != 2 || nativeUploads != 2 || nativeDownloads != 2 {
		t.Fatalf("consumer census changed: plans=%d proofs=%d native=%d/%d", plans, proofs, nativeUploads, nativeDownloads)
	}
	step := findWorkflowStep(w.Jobs["publish-timing-model"].Steps, "Generate and publish model-only PR")
	if step == nil || step.Env["SOURCE_RUN_ID"] != "github-actions-${{ github.run_id }}-${{ github.run_attempt }}" {
		t.Fatal("A17 publisher provenance omits attempt")
	}
}

func TestCIArtifactNamespaceIgnoresStaleNeedsAndRetainedSuccess(t *testing.T) {
	w := loadAdmissionWorkflow(t)
	for _, job := range []string{"proof-unit", "timing-budget", "required-tests", "ci-tier", "publish-timing-model"} {
		t.Run(job, func(t *testing.T) {
			var download *ciWorkflowStep
			for i := range w.Jobs[job].Steps {
				step := w.Jobs[job].Steps[i]
				if strings.Contains(step.Uses, "download-artifact") && strings.HasPrefix(fmt.Sprint(step.With["name"]), "ci-plan") {
					download = &step
				}
			}
			if download == nil {
				t.Fatal("missing plan consumer")
			}
			name := strings.NewReplacer("${{ github.run_id }}", "123", "${{ github.run_attempt }}", "2").Replace(fmt.Sprint(download.With["name"]))
			retained := map[string]bool{"ci-plan": true, "ci-plan-123-1": true, "ci-plan-124-2": true}
			if retained[name] || name != "ci-plan-123-2" {
				t.Fatalf("A04/A11/A24 stale needs can borrow %s", name)
			}
			retained["ci-plan-123-2"] = true
			if !retained[name] {
				t.Fatal("A23 complete current plan cannot be acquired")
			}
		})
	}
	for _, job := range []string{"timing-budget", "publish-timing-model"} {
		t.Run(job+"_proofs", func(t *testing.T) {
			for _, step := range w.Jobs[job].Steps {
				pattern, _ := step.With["pattern"].(string)
				if !strings.HasPrefix(pattern, "proof-") {
					continue
				}
				pattern = strings.NewReplacer("${{ github.run_id }}", "123", "${{ github.run_attempt }}", "2").Replace(pattern)
				for name, expected := range map[string]bool{
					"proof-123-1-store-timing": false, "proof-124-2-store-timing": false,
					"proof-store-timing": false, "proof-123-2-store-timing": true,
					"proof-123-2-timer-timing": true,
				} {
					matches, err := filepath.Match(pattern, name)
					if err != nil || matches != expected {
						t.Fatalf("A01/A02/A06/A14 retained acquisition %s: %t %v", name, matches, err)
					}
				}
			}
		})
	}
}
