package testplanning

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

func TestEffectiveBuildContextRecordsCompilerVersion(t *testing.T) {
	build, err := EffectiveBuildContext(context.Background(), ".")
	if err != nil {
		t.Fatal(err)
	}
	want, err := exec.Command("go", "env", "GOVERSION").Output()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(build)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]string
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if version := strings.TrimSpace(string(want)); version == "" || fields["goversion"] != version {
		t.Fatalf("effective build context %s does not bind compiler %q", raw, version)
	}
}

func TestExecutableBuildContextRequiresCompilerVersion(t *testing.T) {
	build := BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1", GOVERSION: "go1.25.0"}
	if err := build.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", " ", "\t", " go1.25.0", "go1.25.0 "} {
		t.Run("version="+version, func(t *testing.T) {
			bad := build
			bad.GOVERSION = version
			if bad.Validate() == nil {
				t.Fatal("missing or noncanonical effective compiler accepted")
			}
		})
	}
	plan, err := BuildPlan(testPolicy(), WeightModel{Version: WeightModelVersion, SourceRunID: "fixture"}, []string{"module/a", "module/catalog"}, ProfileFull, "policy selection", "head")
	if err != nil || plan.Validate() != nil {
		t.Fatal("pure policy planning changed", err)
	}
	if plan.ValidateExecutionSHA("head") == nil {
		t.Fatal("unbound policy plan acquired execution authority")
	}
	plan.BuildContext = build
	plan.Digest, err = planDigest(plan)
	if err != nil || plan.ValidateExecutionSHA("head") != nil {
		t.Fatal("version-bound plan refused", err)
	}
	for _, version := range []string{"", "go1.26.8"} {
		candidate := plan
		candidate.BuildContext.GOVERSION = version
		if candidate.Validate() == nil {
			t.Fatal("missing version or changed version with old digest accepted")
		}
	}
	changed := plan
	changed.BuildContext.GOVERSION = "go1.26.8"
	changed.Digest, err = planDigest(changed)
	if err != nil || changed.Validate() != nil || changed.Digest == plan.Digest {
		t.Fatal("compiler version is not digest-bound", err)
	}
	inventory := RootInventory{BuildContext: build, Packages: map[string]PackageRoots{"module/a": {Package: "module/a"}}}
	inventory.BuildContext.GOVERSION = ""
	if BindExecution(&plan, inventory, nil, testPolicy()) == nil {
		t.Fatal("unversioned root inventory bound executable work")
	}
}
