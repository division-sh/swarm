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
	imageBuilt, rootsChecked := false, 0
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
			selection, err := regexp.Compile(match[2])
			if err != nil {
				return err
			}
			for root, reason := range rootDeferralReasons[match[1]] {
				if strings.HasPrefix(reason, "real Docker proof required separately by the workspace-image CI job;") && selection.MatchString(root) {
					rootsChecked++
				}
			}
		}
	}
	if rootsChecked != 9 {
		return fmt.Errorf("hosted workspace job selects %d separately provisioned proofs, want all 9", rootsChecked)
	}
	return nil
}
