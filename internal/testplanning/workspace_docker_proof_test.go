package testplanning

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkspaceDockerProofsHaveProvisionedHostedOwner(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("../..", ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWorkspaceDockerHostedProof(raw); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string]struct{ old, replacement string }{
		"missing image":       {"docker build -t swarm-workspace:latest -f Dockerfile.workspace .", "echo image omitted"},
		"missing opt-in":      {`SWARM_TEST_WORKSPACE_MCP_DOCKER: "1"`, `SWARM_TEST_WORKSPACE_MCP_DOCKER: "0"`},
		"lost worker leaves":  {"'^TestWorkerRealDocker'", "'^TestWorkerRealDockerHTTPDeadlineJoinsGatewayRequest$'"},
		"lost release leaves": {"'^TestWorkspaceMCPCompiledDocker(Conformance|DoctorAndLiveBootRefusal)$'", "'^TestWorkspaceMCPCompiledDockerConformance$'"},
	} {
		t.Run(name, func(t *testing.T) {
			changed := strings.Replace(string(raw), mutation.old, mutation.replacement, 1)
			if changed == string(raw) {
				t.Fatal("negative mutation no longer discriminates")
			}
			if err := validateWorkspaceDockerHostedProof([]byte(changed)); err == nil {
				t.Fatal("lost mandatory hosted Docker proof was accepted")
			}
		})
	}
	public := "go test ./internal/releasee2e -run '^TestWorkspaceMCPCompiledDocker(Conformance|DoctorAndLiveBootRefusal)$' -count=1 -timeout=4m -v"
	worker := "go test ./internal/runtime/workspace -run '^TestWorkerRealDocker' -count=1 -timeout=2m -v"
	changed := strings.Replace(string(raw), public, worker, 1)
	changed = strings.Replace(changed, worker+"\n          "+worker, worker+"\n          "+public, 1)
	if changed == string(raw) || validateWorkspaceDockerHostedProof([]byte(changed)) == nil {
		t.Fatal("worker fixtures before default-topology public proof were accepted")
	}
}

func validateWorkspaceDockerHostedProof(raw []byte) error {
	var workflow struct {
		Jobs map[string]struct {
			RunsOn string `yaml:"runs-on"`
			Steps  []struct {
				Run string
				If  string
				Env map[string]string
			}
		}
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		return err
	}
	job, exists := workflow.Jobs["sqlite-local-dev"]
	if !exists || job.RunsOn != "ubuntu-latest" {
		return fmt.Errorf("missing native Ubuntu workspace proof job")
	}
	command := regexp.MustCompile(`go test ./([a-z0-9/]+) -run '([^']+)' -count=1`)
	imageBuilt, publicChecked, rootsChecked := false, false, 0
	for _, step := range job.Steps {
		if step.If != "" {
			continue
		}
		if strings.Contains(step.Run, "docker build -t swarm-workspace:latest -f Dockerfile.workspace .") {
			imageBuilt = true
		}
		if !imageBuilt || step.Env["SWARM_TEST_WORKSPACE_MCP_DOCKER"] != "1" {
			continue
		}
		for _, match := range command.FindAllStringSubmatch(step.Run, -1) {
			if match[1] == "internal/runtime/workspace" && !publicChecked {
				return fmt.Errorf("worker fixture runs before public default-topology Docker proof")
			}
			selection, err := regexp.Compile(match[2])
			if err != nil {
				return err
			}
			for root, reason := range rootDeferralReasons[match[1]] {
				if strings.HasPrefix(reason, "real Docker proof required separately by the workspace-image CI job;") && selection.MatchString(root) {
					rootsChecked++
				}
			}
			if match[1] == "internal/releasee2e" && selection.MatchString("TestWorkspaceMCPCompiledDockerConformance") && selection.MatchString("TestWorkspaceMCPCompiledDockerDoctorAndLiveBootRefusal") {
				publicChecked = true
			}
		}
	}
	if rootsChecked != 9 {
		return fmt.Errorf("hosted workspace job selects %d separately provisioned proofs, want all 9", rootsChecked)
	}
	return nil
}
