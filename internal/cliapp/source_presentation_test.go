package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func Test2376HumanSourceIdentityConsumerTable(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	root := outputModeVerifyFixture(t)
	name := "Shop\u202eReception\u2066\u200dDesk"
	if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte("name: \"Shop\\u202eReception\\u2066\\u200dDesk\"\nversion: 1.2.3\nplatform_version: '>=0.7.0 <0.8.0'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	metadata, present := source.RootManifest()
	if !present || metadata.Name != name || source.HumanLabel() != "Shop Reception  Desk@1.2.3" {
		t.Fatalf("unsafe label or rewritten metadata: %q %#v", source.HumanLabel(), metadata)
	}
	for _, label := range []string{"Reception@1.2.3", "reception@aaaaaaa", "", source.HumanLabel()} {
		want := label
		if want == "" {
			want = "aaaaaaa"
		}
		for _, row := range []struct {
			name   string
			render func(*bytes.Buffer)
		}{
			{"health", func(out *bytes.Buffer) {
				writeDiagnosticHealth(out, diagnosticHealthCheckResult{Bundle: diagnosticBundleIdentity{BundleHash: hash, SourceLabel: label}})
			}},
			{"fork", func(out *bytes.Buffer) {
				frozen := false
				writeRunForkHuman(out, runForkResult{BundleHash: hash, SourceLabel: label, SourceFrozen: &frozen})
			}},
			{"frame", func(out *bytes.Buffer) {
				writeAgentFrameResult(out, agentframe.Inspection{SourceLabel: label, Session: agentframe.InspectionSession{BundleHash: hash}})
			}},
			{"describe", func(out *bytes.Buffer) {
				writeDescribeText(out, authoringview.View{SourceHash: hash, SourceLabel: label})
			}},
			{"channel", func(out *bytes.Buffer) {
				writeChannelList(out, channelListResult{Channels: []channelReadbackResult{{SourceLabel: label, Identity: operatorchannel.Readback{}, Activation: &channelonboarding.ConnectedChannelActivation{Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: hash}}}}})
			}},
			{"version_server", func(out *bytes.Buffer) {
				writeVersionServerIdentity(out, diagnosticHealthCheckResult{Bundle: diagnosticBundleIdentity{BundleHash: hash, SourceLabel: label}})
			}},
			{"version_quiet", func(out *bytes.Buffer) {
				setCLIAPITestToken(t, "test-token")
				server, _ := newDiagnosticSuccessServer(t, func(_ jsonRPCRequest, _ int) map[string]any {
					return map[string]any{"alive": true, "ready": true, "db_ok": true, "runtime_ok": true, "bundle": map[string]any{"bundle_hash": hash, "source_label": label, "workflow_name": ".", "workflow_version": hash}}
				})
				defer server.Close()
				if code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"version", "--server", "--quiet"}, out, io.Discard, testRootCommandOptions(server)); code != 0 {
					t.Errorf("version quiet exit=%d", code)
				}
			}},
		} {
			t.Run(row.name+"/"+want, func(t *testing.T) {
				var out bytes.Buffer
				row.render(&out)
				if strings.Contains(out.String(), hash) || !strings.Contains(out.String(), want) {
					t.Fatalf("human source label lost: %s", &out)
				}
			})
		}
	}
	for _, manifestName := range []string{"", "Reception", name} {
		t.Run(fmt.Sprintf("verify/name=%q", manifestName), func(t *testing.T) {
			root := outputModeVerifyFixture(t)
			manifest := filepath.Join(root, "manifest.yaml")
			if manifestName != "" {
				if err := os.WriteFile(manifest, []byte(fmt.Sprintf("name: %q\nversion: 1.2.3\nplatform_version: '>=0.7.0 <0.8.0'\n", manifestName)), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(manifest); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			var out, errOut bytes.Buffer
			if code := executeRootCommand(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--config", writeTestVerifyRuntimeConfig(t)}, &out, &errOut); code != 0 || !strings.Contains(out.String(), artifact.HumanLabel()) || strings.Contains(out.String(), artifact.BundleHash()) {
				t.Fatalf("verify human identity: code=%d %s / %s", code, &out, &errOut)
			}
			out.Reset()
			errOut.Reset()
			if code := executeRootCommand(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--json", "--config", writeTestVerifyRuntimeConfig(t)}, &out, &errOut); code != 0 {
				t.Fatalf("verify machine identity: code=%d %s / %s", code, &out, &errOut)
			}
			var result verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.BundleHash != artifact.BundleHash() || result.SourceLabel != artifact.HumanLabel() || (result.Manifest != nil) != (manifestName != "") || result.Manifest != nil && result.Manifest.Name != manifestName {
				t.Fatalf("machine identity or original metadata changed: %s", &out)
			}
		})
	}
}

func Test2376MachineIdentityRemainsExact(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	var channel channelListResult
	raw, err := json.Marshal(operatorChannelCLIListResult("current", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &channel); err != nil {
		t.Fatal(err)
	}
	channel.Channels[0].SourceLabel = "Reception@1.2.3"
	channel.Channels[0].Readiness.Coordinate.BundleHash = hash
	for _, row := range []struct {
		name   string
		result any
	}{
		{"health", diagnosticHealthCheckResult{Bundle: diagnosticBundleIdentity{BundleHash: hash, SourceLabel: "Reception@1.2.3"}}},
		{"fork", runForkResult{BundleHash: hash, SourceLabel: "Reception@1.2.3"}},
		{"frame", agentframe.Inspection{SourceLabel: "Reception@1.2.3", Session: agentframe.InspectionSession{BundleHash: hash}}},
		{"describe", authoringview.View{SourceHash: hash, SourceLabel: "Reception@1.2.3"}},
		{"channel", channel},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(row.name+"/"+format, func(t *testing.T) {
				var out bytes.Buffer
				if err := renderCLIOutput(&out, io.Discard, cliOutputOptions{asJSON: format == "json", asYAML: format == "yaml"}, row.result, nil, nil); err != nil || !strings.Contains(out.String(), hash) {
					t.Fatalf("machine identity changed: %s %v", &out, err)
				}
			})
		}
	}
}

func Test2376TestDiagnosticsKeepExactLookupButHumanTeaching(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	server, _ := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
		if req.Method != "runtime.identity" {
			t.Fatal(req.Method)
		}
		return map[string]any{"source_artifacts": []any{map[string]any{"bundle_hash": "bundle-v2:sha256:" + strings.Repeat("b", 64)}}}
	})
	defer server.Close()
	opts := testRootCommandOptions(server)
	opts.invocationRoot = mustInvocationRootForTest(t.TempDir())
	client, err := newCLIAPIClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scenarioTestSourceArtifactFact(context.Background(), client, hash); err == nil || strings.Contains(err.Error(), "bundle-v2:sha256:") || !strings.Contains(err.Error(), "aaaaaaa") || !strings.Contains(err.Error(), "matching source directory") {
		t.Fatalf("test teaching lost exact-source distinction: %v", err)
	}
}
