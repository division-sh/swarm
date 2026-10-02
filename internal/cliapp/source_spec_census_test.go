package cliapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func Test2376PublicSurfaceSpecCensus(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Source struct {
			Manifest struct {
				Fields []string `yaml:"fields"`
			} `yaml:"manifest_metadata"`
			Presentation struct {
				Consumers []string `yaml:"consumers"`
			} `yaml:"human_source_presentation"`
			Bundle struct {
				Vector struct {
					Preimage string `yaml:"preimage_hex"`
					Hash     string `yaml:"bundle_hash"`
				} `yaml:"known_vector"`
			} `yaml:"bundle_v2"`
			Retired struct {
				NoCompatibility bool     `yaml:"no_compatibility"`
				Names           []string `yaml:"names"`
			} `yaml:"retired_surface"`
			Cache struct {
				Rule string `yaml:"rule"`
			} `yaml:"projection_cache_lifetime"`
		} `yaml:"filesystem_source_model"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec.Source.Manifest.Fields, []string{"name", "version", "platform_version"}) {
		t.Fatalf("finite manifest surface drifted: %v", spec.Source.Manifest.Fields)
	}
	if !reflect.DeepEqual(spec.Source.Presentation.Consumers, []string{"verify", "describe", "serve", "health", "version_server_quiet", "run_fork", "agent_frame_effective", "channel_list", "channel_status", "channel_candidates", "test_diagnostics"}) {
		t.Fatalf("human consumer census drifted: %v", spec.Source.Presentation.Consumers)
	}
	preimage, err := hex.DecodeString(spec.Source.Bundle.Vector.Preimage)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("bundle-v2:sha256:%x", sha256.Sum256(preimage)); spec.Source.Bundle.Vector.Hash != want {
		t.Fatalf("literal spec vector hash=%s want=%s", spec.Source.Bundle.Vector.Hash, want)
	}
	if !bytes.HasPrefix(preimage, []byte("swarm-bundle-v2\x00")) || !spec.Source.Retired.NoCompatibility || !strings.Contains(spec.Source.Cache.Rule, "No compiled projection survives a binary") {
		t.Fatal("artifact framing, retirement or process-local cache contract drifted")
	}
	retired := strings.Join(spec.Source.Retired.Names, "\n")
	for _, required := range []string{"--bundle-hash on serve", "--bundle on channel", "static agent frame CLI", "archive transport, registry publication"} {
		if !strings.Contains(retired, required) {
			t.Fatalf("retirement missing %q", required)
		}
	}
	root, err := NewInvocationRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	commands := newRootCommandAtInvocation(context.Background(), root, io.Discard, io.Discard, defaultRootCommandOptions())
	var selectors []string
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.LocalNonPersistentFlags().Lookup("source") != nil {
			selectors = append(selectors, strings.TrimPrefix(cmd.CommandPath(), "swarm "))
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(commands)
	// Logs and secrets list use the same flag spelling for producer/credential filters, not artifacts.
	if !reflect.DeepEqual(selectors, []string{"channel connect", "channel rebind", "channel reconnect", "channel status", "logs", "run fork", "secrets list"}) {
		t.Fatalf("optional source selector census drifted: %v", selectors)
	}
	for _, retired := range []string{"bundle", "mint-element-ids"} {
		for _, command := range commands.Commands() {
			if command.Name() == retired {
				t.Fatalf("retired command remains: %s", retired)
			}
		}
	}
}
