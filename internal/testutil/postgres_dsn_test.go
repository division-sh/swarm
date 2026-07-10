package testutil

import (
	"crypto/tls"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestTestPostgresDSNRoundTripsSupportedRepresentations(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "keyword explicit database",
			raw:  "host=127.0.0.1 port=5432 user=tester password=secret dbname=postgres sslmode=disable application_name='swarm tests' search_path='public,extensions'",
		},
		{
			name: "URL explicit database",
			raw:  "postgres://tester:s%20ecret@127.0.0.1:5432/postgres?sslmode=disable&application_name=swarm%20tests&search_path=public%2Cextensions",
		},
		{
			name: "keyword default database",
			raw:  "host=127.0.0.1 port=5432 user=tester password=secret sslmode=disable",
		},
		{
			name: "URL default database",
			raw:  "postgres://tester:secret@127.0.0.1:5432?sslmode=disable",
		},
		{
			name: "keyword escaping",
			raw: "host=127.0.0.1 port=5432 user=tester password=" +
				quotePostgresKeywordValue(`slash\ quote' space`) +
				" dbname=postgres sslmode=disable application_name=" +
				quotePostgresKeywordValue(`worker\ one's`),
		},
		{
			name: "multihost and runtime",
			raw:  "host=one.example,two.example hostaddr=127.0.0.1,127.0.0.2 port=5432,6543 user=tester password=secret dbname=postgres sslmode=require connect_timeout=9 target_session_attrs=read-write load_balance_hosts=random work_mem='16MB'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, err := parseTestPostgresDSN(tt.raw)
			if err != nil {
				t.Fatalf("parseTestPostgresDSN: %v", err)
			}
			projected, err := owner.withDatabase("projected_db")
			if err != nil {
				t.Fatalf("withDatabase: %v", err)
			}

			want := owner.config.Clone()
			want.Database = "projected_db"
			assertPostgresConfigEqual(t, projected.config, want)

			canonical, err := projected.string()
			if err != nil {
				t.Fatalf("string: %v", err)
			}
			if strings.HasPrefix(canonical, "postgres://") || strings.HasPrefix(canonical, "postgresql://") {
				t.Fatalf("canonical DSN remained URL-shaped: %q", canonical)
			}

			reparsed, err := parseTestPostgresDSN(canonical)
			if err != nil {
				t.Fatalf("reparse canonical DSN: %v\nDSN: %s", err, canonical)
			}
			assertPostgresConfigEqual(t, reparsed.config, projected.config)
			if reparsed.config.Database != "projected_db" {
				t.Fatalf("database = %q, want projected_db", reparsed.config.Database)
			}
		})
	}
}

func TestTestPostgresDSNPreservesExplicitEmptyApplicationName(t *testing.T) {
	owner, err := parseTestPostgresDSN("host=127.0.0.1 port=5432 user=tester password=secret dbname=postgres sslmode=disable application_name='' fallback_application_name='fallback'")
	if err != nil {
		t.Fatalf("parse explicit-empty application name: %v", err)
	}
	if owner.config.ApplicationName != "" || owner.config.FallbackApplicationName != "fallback" {
		t.Fatalf("source application names = (%q, %q), want empty and fallback", owner.config.ApplicationName, owner.config.FallbackApplicationName)
	}
	canonical, err := owner.string()
	if err != nil {
		t.Fatalf("serialize explicit-empty application name: %v", err)
	}
	if !strings.Contains(canonical, "application_name=''") {
		t.Fatalf("canonical DSN omitted explicit empty application_name: %q", canonical)
	}
	reparsed, err := parseTestPostgresDSN(canonical)
	if err != nil {
		t.Fatalf("reparse explicit-empty application name: %v", err)
	}
	assertPostgresConfigEqual(t, reparsed.config, owner.config)
}

func TestOwnedDockerPostgresDSNIgnoresAmbientPGEnv(t *testing.T) {
	for key, value := range map[string]string{
		"PGHOST":               "hostile.example",
		"PGHOSTADDR":           "192.0.2.1",
		"PGPORT":               "6543",
		"PGDATABASE":           "hostile",
		"PGUSER":               "hostile",
		"PGPASSWORD":           "hostile",
		"PGPASSFILE":           "/tmp/hostile-passfile",
		"PGOPTIONS":            "-c search_path=hostile",
		"PGAPPNAME":            "hostile",
		"PGSSLMODE":            "verify-full",
		"PGSSLNEGOTIATION":     "direct",
		"PGSSLCERT":            "/tmp/hostile-cert",
		"PGSSLKEY":             "/tmp/hostile-key",
		"PGSSLROOTCERT":        "/tmp/hostile-root",
		"PGSSLSNI":             "0",
		"PGCONNECT_TIMEOUT":    "19",
		"PGCLIENTENCODING":     "LATIN1",
		"PGDATESTYLE":          "SQL, DMY",
		"PGTZ":                 "Pacific/Honolulu",
		"PGGEQO":               "on",
		"PGTARGETSESSIONATTRS": "read-only",
		"PGLOADBALANCEHOSTS":   "random",
	} {
		t.Setenv(key, value)
	}

	owner, err := newOwnedDockerPostgresDSN(55432)
	if err != nil {
		t.Fatalf("newOwnedDockerPostgresDSN: %v", err)
	}
	want, err := newTestPostgresDSN(pq.Config{
		Host:           "127.0.0.1",
		Hostaddr:       netip.MustParseAddr("127.0.0.1"),
		Port:           55432,
		Database:       "postgres",
		User:           "postgres",
		Password:       "postgres",
		SSLMode:        pq.SSLModeDisable,
		SSLSNI:         true,
		ClientEncoding: "UTF8",
		Datestyle:      "ISO, MDY",
	})
	if err != nil {
		t.Fatalf("build expected Docker config: %v", err)
	}
	assertPostgresConfigEqual(t, owner.config, want.config)

	canonical, err := owner.string()
	if err != nil {
		t.Fatalf("serialize owned Docker config: %v", err)
	}
	reparsed, err := parseTestPostgresDSN(canonical)
	if err != nil {
		t.Fatalf("reparse owned Docker config under hostile PG env: %v", err)
	}
	assertPostgresConfigEqual(t, reparsed.config, owner.config)
}

func TestTestPostgresDSNSerializerCoversPQConfigSchema(t *testing.T) {
	typ := reflect.TypeOf(pq.Config{})
	value := reflect.ValueOf(pq.Config{})
	covered := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		key, ok := field.Tag.Lookup("postgres")
		if !ok || key == "" || key == "-" {
			continue
		}
		if covered[key] {
			t.Fatalf("postgres config key %q appears more than once", key)
		}
		if _, err := encodePostgresConfigField(value.Field(i), key); err != nil {
			t.Fatalf("pq.Config.%s (%s) has no serializer: %v", field.Name, key, err)
		}
		covered[key] = true
	}

	for key := range postgresConfigTaggedKeys() {
		if !covered[key] {
			t.Fatalf("postgres config key %q missing serializer coverage", key)
		}
	}
}

func TestTestPostgresDSNRejectsNonTransportableSemantics(t *testing.T) {
	if err := pq.RegisterTLSConfig("swarm-test-process-local", &tls.Config{MinVersion: tls.VersionTLS12}); err != nil {
		t.Fatalf("RegisterTLSConfig: %v", err)
	}
	pq.RegisterGSSProvider(func() (pq.GSS, error) { return nil, nil })
	t.Cleanup(func() { pq.RegisterGSSProvider(nil) })
	if _, err := parseTestPostgresDSN("host=127.0.0.1 user=tester password=secret sslmode=disable"); err != nil {
		t.Fatalf("registered but unused GSS provider changed password-auth DSN acceptance: %v", err)
	}

	tests := []struct {
		name     string
		raw      string
		contains string
	}{
		{name: "malformed", raw: "not-a-dsn", contains: "parse postgres test DSN"},
		{name: "empty password", raw: "host=127.0.0.1 user=tester password='' sslmode=disable", contains: "non-empty effective password"},
		{name: "passfile", raw: "host=127.0.0.1 user=tester password=secret passfile=/tmp/pgpass sslmode=disable", contains: "passfile is unsupported"},
		{name: "service", raw: "host=127.0.0.1 user=tester password=secret service=cluster sslmode=disable", contains: "option \"service\" is unsupported"},
		{name: "servicefile", raw: "host=127.0.0.1 user=tester password=secret servicefile=/tmp/service sslmode=disable", contains: "option \"servicefile\" is unsupported"},
		{name: "custom TLS", raw: "host=127.0.0.1 user=tester password=secret sslmode=pqgo-swarm-test-process-local", contains: "process-local and unsupported"},
		{name: "GSS service", raw: "host=127.0.0.1 user=tester password=secret sslmode=disable krbsrvname=postgres", contains: "GSS options are process-local"},
		{name: "GSS SPN", raw: "host=127.0.0.1 user=tester password=secret sslmode=disable krbspn=postgres/host", contains: "GSS options are process-local"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseTestPostgresDSN(tt.raw)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("error = %v, want substring %q", err, tt.contains)
			}
		})
	}
}

func TestTestPostgresDSNRejectsRuntimeCollisionsAndUnsafeKeys(t *testing.T) {
	base := pq.Config{
		Host:     "127.0.0.1",
		Port:     5432,
		Database: "postgres",
		User:     "tester",
		Password: "secret",
		SSLMode:  pq.SSLModeDisable,
		SSLSNI:   true,
		Runtime:  map[string]string{"host": "other"},
	}
	if _, err := newTestPostgresDSN(base); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("collision error = %v", err)
	}
	base.Runtime = map[string]string{"bad key": "value"}
	if _, err := newTestPostgresDSN(base); err == nil || !strings.Contains(err.Error(), "cannot be transported safely") {
		t.Fatalf("unsafe key error = %v", err)
	}
}

func TestTestPostgresDSNRejectsEnvironmentPassfile(t *testing.T) {
	t.Setenv("PGPASSFILE", "/tmp/postgres test passfile")
	_, err := parseTestPostgresDSN("host=127.0.0.1 user=tester password=secret sslmode=disable")
	if err == nil || !strings.Contains(err.Error(), "passfile is unsupported") {
		t.Fatalf("PGPASSFILE error = %v, want passfile rejection", err)
	}
}

func TestTestPostgresDSNPreservesMaterializedConfigValues(t *testing.T) {
	addr := netip.MustParseAddr("127.0.0.1")
	cfg := pq.Config{
		Host:                        "primary",
		Hostaddr:                    addr,
		Port:                        5432,
		Database:                    "postgres",
		User:                        "tester",
		Password:                    `s\ e'cret`,
		Options:                     "-c statement_timeout=5000",
		ApplicationName:             "swarm tests",
		FallbackApplicationName:     "fallback",
		SSLMode:                     pq.SSLModeVerifyCA,
		SSLNegotiation:              pq.SSLNegotiationPostgres,
		SSLRootCert:                 "/tmp/root cert.pem",
		SSLSNI:                      false,
		ConnectTimeout:              7 * time.Second,
		BinaryParameters:            true,
		DisablePreparedBinaryResult: true,
		ClientEncoding:              "UTF8",
		Datestyle:                   "ISO, MDY",
		TZ:                          "UTC",
		Geqo:                        "off",
		TargetSessionAttrs:          pq.TargetSessionAttrsReadWrite,
		LoadBalanceHosts:            pq.LoadBalanceHostsDisable,
		Runtime:                     map[string]string{"work_mem": "32MB"},
		Multi: []pq.ConfigMultihost{{
			Host:     "secondary",
			Hostaddr: netip.MustParseAddr("127.0.0.2"),
			Port:     6543,
		}},
	}
	owner, err := newTestPostgresDSN(cfg)
	if err != nil {
		t.Fatalf("newTestPostgresDSN: %v", err)
	}
	canonical, err := owner.string()
	if err != nil {
		t.Fatalf("string: %v", err)
	}
	reparsed, err := parseTestPostgresDSN(canonical)
	if err != nil {
		t.Fatalf("reparse: %v\nDSN: %s", err, canonical)
	}
	assertPostgresConfigEqual(t, reparsed.config, cfg)
}

func TestTestPostgresDSNMaterializesEnvironmentDefaults(t *testing.T) {
	t.Setenv("PGPASSWORD", "environment secret")
	t.Setenv("PGAPPNAME", "environment app")
	t.Setenv("PGCONNECT_TIMEOUT", "11")
	t.Setenv("PGOPTIONS", "-c statement_timeout=1234")

	owner, err := parseTestPostgresDSN("host=127.0.0.1 port=5432 user=tester dbname=postgres sslmode=disable")
	if err != nil {
		t.Fatalf("parse environment-backed DSN: %v", err)
	}
	if owner.config.Password != "environment secret" || owner.config.ApplicationName != "environment app" || owner.config.ConnectTimeout != 11*time.Second || owner.config.Options != "-c statement_timeout=1234" {
		t.Fatalf("environment defaults were not materialized: %#v", exportedPostgresConfig(owner.config))
	}
	canonical, err := owner.string()
	if err != nil {
		t.Fatalf("serialize environment-backed DSN: %v", err)
	}

	t.Setenv("PGPASSWORD", "changed secret")
	t.Setenv("PGAPPNAME", "changed app")
	t.Setenv("PGCONNECT_TIMEOUT", "2")
	t.Setenv("PGOPTIONS", "-c statement_timeout=99")
	reparsed, err := parseTestPostgresDSN(canonical)
	if err != nil {
		t.Fatalf("reparse canonical DSN under changed environment: %v", err)
	}
	assertPostgresConfigEqual(t, reparsed.config, owner.config)
}

func TestWithoutPostgresConnectionEnv(t *testing.T) {
	got := withoutPostgresConnectionEnv([]string{
		"PATH=/bin",
		"PGPASSWORD=secret",
		"PGHOST=other",
		"SWARM_TEST_POSTGRES_TEMPLATE_NAME=template",
		"SWARM_CONFIG=/tmp/swarm.yaml",
	})
	want := []string{"PATH=/bin", "SWARM_CONFIG=/tmp/swarm.yaml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("withoutPostgresConnectionEnv() = %v, want %v", got, want)
	}
}

func assertPostgresConfigEqual(t *testing.T, got, want pq.Config) {
	t.Helper()
	if !reflect.DeepEqual(exportedPostgresConfig(got), exportedPostgresConfig(want)) {
		t.Fatalf("postgres configs differ\ngot:  %#v\nwant: %#v", exportedPostgresConfig(got), exportedPostgresConfig(want))
	}
}

func exportedPostgresConfig(cfg pq.Config) map[string]any {
	result := make(map[string]any)
	typ := reflect.TypeOf(cfg)
	value := reflect.ValueOf(cfg)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		result[field.Name] = value.Field(i).Interface()
	}
	return result
}
