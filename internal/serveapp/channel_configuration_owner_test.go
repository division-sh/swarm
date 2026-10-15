package serveapp

import (
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
)

func TestChannelPostgresConfigurationKeepsExactMetadataWithoutConnectionAuthority(t *testing.T) {
	for _, dsn := range []string{
		`host=127.0.0.1 port=55432 user='channel user' password='slash\\ quote\' space' dbname='channel db' sslmode=disable`,
		`postgres://channel%20user:slash%5C%20quote%27%20space@127.0.0.1:55432/channel%20db?sslmode=disable`,
	} {
		t.Run(dsn, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			path := writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
			cfg, err := config.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			want := config.DatabaseConfig{
				Host: "127.0.0.1", Port: 55432, Name: "channel db", User: "channel user",
				PasswordEnv: "PGPASSWORD", SSLMode: "disable", PoolSize: 5,
			}
			if cfg.Database != want || cfg.Store.Backend != "postgres" {
				t.Fatalf("channel configuration changed selected-store coordinates: got=%+v store=%+v want=%+v", cfg.Database, cfg.Store, want)
			}
			if password := os.Getenv("PGPASSWORD"); password != `slash\ quote' space` {
				t.Fatal("channel configuration changed the credential environment value")
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), `slash\ quote' space`) || strings.Contains(string(raw), dsn) {
				t.Fatal("channel configuration persisted the credential or source DSN")
			}
			if cfg.Runtime.RecoveryOnStartup || cfg.Workspace.Backend != "host" || !cfg.Workspace.AllowExecOnHost {
				t.Fatal("connection metadata projection changed the fixture's restart or workspace posture")
			}
		})
	}
}
