package serveapp

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/tests/support/testpostgres"
	"gopkg.in/yaml.v3"
)

var installedServePostgresFixtures sync.Map

// The injected constructor and the independent boot reader must inspect the
// same fixture. This only supplies explicit coordinates to ordinary test config
// writers; deliberately invalid/password-free config writers do not use it.
func withInstalledServePostgresConfig(t *testing.T, body, backend string) string {
	t.Helper()
	if backend != "" && backend != "postgres" {
		return body
	}
	var raw any
	for name := t.Name(); name != ""; {
		if value, found := installedServePostgresFixtures.Load(name); found {
			raw = value
			break
		}
		end := strings.LastIndexByte(name, '/')
		if end < 0 {
			break
		}
		name = name[:end]
	}
	if raw == nil {
		return body
	}
	connection, err := testpostgres.ParseConnection(raw.(string))
	if err != nil {
		t.Fatal(err)
	}
	parameters := connection.Parameters()
	t.Setenv("SWARM_SERVE_FIXTURE_PASSWORD", parameters.Password)
	var document map[string]any
	if err := yaml.Unmarshal([]byte(body), &document); err != nil {
		t.Fatal(err)
	}
	document["database"] = map[string]any{"host": parameters.Host, "port": parameters.Port, "name": parameters.Database,
		"user": parameters.User, "sslmode": parameters.SSLMode, "password_env": "SWARM_SERVE_FIXTURE_PASSWORD", "pool_size": 5}
	if backend == "" {
		document["store"] = map[string]any{"backend": "postgres"}
	}
	encoded, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(fmt.Errorf("encode exact native fixture coordinates: %w", err))
	}
	return string(encoded)
}

func withServeFixtureStateDirectory(t *testing.T, body string) string {
	t.Helper()
	var document map[string]any
	if err := yaml.Unmarshal([]byte(body), &document); err != nil {
		t.Fatal(err)
	}
	paths, _ := document["paths"].(map[string]any)
	if paths == nil {
		paths = map[string]any{}
	}
	if _, explicit := paths["swarm_dir"]; !explicit {
		paths["swarm_dir"] = t.TempDir()
	}
	document["paths"] = paths
	encoded, err := yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
