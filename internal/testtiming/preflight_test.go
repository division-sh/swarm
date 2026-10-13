package testtiming

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestPreflightReportRefusesIncompleteAndForeignProof(t *testing.T) {
	pkg := testplanning.PreflightPackage{Package: "proof", Required: []testplanning.RequiredTest{{TestRoot: testplanning.TestRoot{Package: "proof", Name: "TestGuard"}, Children: []string{"positive"}}}}
	valid := `{"Action":"pass","Package":"proof","Test":"TestGuard/positive"}
{"Action":"pass","Package":"proof","Test":"TestGuard"}
{"Action":"pass","Package":"proof"}`
	for _, name := range []string{"valid", "missing", "skipped", "failed", "foreign", "duplicate", "malformed"} {
		t.Run(name, func(t *testing.T) {
			raw := valid
			switch name {
			case "missing":
				raw = strings.ReplaceAll(raw, `{"Action":"pass","Package":"proof","Test":"TestGuard/positive"}`, "")
			case "skipped":
				raw = strings.Replace(raw, `"pass"`, `"skip"`, 1)
			case "failed":
				raw = strings.Replace(raw, `"pass"`, `"fail"`, 1)
			case "foreign":
				raw = strings.ReplaceAll(raw, "proof", "other")
			case "duplicate":
				raw += "\n" + valid
			case "malformed":
				raw += "\nnot-json"
			}
			report, err := ParseReport(strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			err = ValidatePreflightReport(pkg, report)
			if (err == nil) != (name == "valid") {
				t.Fatalf("%s admission: %v", name, err)
			}
		})
	}
}
