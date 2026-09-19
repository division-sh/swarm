package testtiming

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
)

func soakEvidenceProblems(unit testplanning.ProofUnit, evidence CommandEvidence) []string {
	var problems []string
	backend, soak := testplanning.SoakBackend(unit.Run)
	top, cell := false, false
	for _, test := range evidence.Report.Tests {
		if test.Package != testplanning.SoakPackage {
			continue
		}
		if !soak {
			if unit.Skip == testplanning.SoakRun && (test.Test == testplanning.SoakTest || strings.HasPrefix(test.Test, testplanning.SoakTest+"/")) {
				problems = append(problems, "ordinary partition executed excluded soak")
			}
			continue
		}
		switch test.Test {
		case testplanning.SoakTest:
			top = test.Result == "pass"
		case testplanning.SoakTest + "/" + backend:
			cell = test.Result == "pass" && test.Elapsed >= 900
		default:
			problems = append(problems, fmt.Sprintf("soak cell executed unplanned test %s", test.Test))
		}
	}
	// Preserve failed-command diagnostics; only successful receipts can assert
	// completion of the full window and original final drain/readback.
	if soak && evidence.ExitCode == 0 && (!top || !cell) {
		problems = append(problems, "soak requires passing parent and exact backend with at least 900s elapsed")
	}
	return problems
}
