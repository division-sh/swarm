package testutil

import (
	"database/sql"
	"fmt"
	"net/netip"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/lib/pq"
)

type testPostgresDSN struct {
	config pq.Config
}

func parseTestPostgresDSN(raw string) (testPostgresDSN, error) {
	cfg, err := pq.NewConfig(strings.TrimSpace(raw))
	if err != nil {
		return testPostgresDSN{}, fmt.Errorf("parse postgres test DSN: %w", err)
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = pq.SSLModeRequire
	}
	return newTestPostgresDSN(cfg)
}

func newTestPostgresDSN(cfg pq.Config) (testPostgresDSN, error) {
	if cfg.SSLNegotiation == "" {
		cfg.SSLNegotiation = pq.SSLNegotiationPostgres
	}
	if cfg.TargetSessionAttrs == "" {
		cfg.TargetSessionAttrs = pq.TargetSessionAttrsAny
	}
	if cfg.LoadBalanceHosts == "" {
		cfg.LoadBalanceHosts = pq.LoadBalanceHostsDisable
	}
	if cfg.ClientEncoding == "" {
		cfg.ClientEncoding = "UTF8"
	}
	if cfg.Datestyle == "" {
		cfg.Datestyle = "ISO, MDY"
	}
	if err := validateTestPostgresConfig(cfg); err != nil {
		return testPostgresDSN{}, err
	}
	return testPostgresDSN{config: cfg.Clone()}, nil
}

func newOwnedDockerPostgresDSN(port uint16) (testPostgresDSN, error) {
	if port == 0 {
		return testPostgresDSN{}, fmt.Errorf("owned Docker postgres port is required")
	}
	owned, err := newTestPostgresDSN(pq.Config{
		Host:           "127.0.0.1",
		Hostaddr:       netip.MustParseAddr("127.0.0.1"),
		Port:           port,
		Database:       "postgres",
		User:           "postgres",
		Password:       "postgres",
		SSLMode:        pq.SSLModeDisable,
		SSLSNI:         true,
		ClientEncoding: "UTF8",
		Datestyle:      "ISO, MDY",
	})
	if err != nil {
		return testPostgresDSN{}, err
	}
	canonical, err := owned.string()
	if err != nil {
		return testPostgresDSN{}, err
	}
	// pq keeps password presence in private parser state and otherwise replaces
	// a directly assigned password with pgpass lookup during connection. Reparse
	// the fully explicit canonical form so pq records that state without giving
	// any supported PG environment field authority over the owned source.
	return parseTestPostgresDSN(canonical)
}

func validateTestPostgresConfig(cfg pq.Config) error {
	if cfg.Password == "" {
		return fmt.Errorf("postgres test DSN requires a non-empty effective password; passfile, default .pgpass, peer, trust, and registry-backed authentication are unsupported")
	}
	if cfg.Passfile != "" {
		return fmt.Errorf("postgres test DSN passfile is unsupported because cleanup runs in a separate process; provide password in SWARM_TEST_POSTGRES_DSN")
	}
	if strings.HasPrefix(string(cfg.SSLMode), "pqgo-") {
		return fmt.Errorf("postgres test DSN sslmode %q is process-local and unsupported; use disable, require, verify-ca, or verify-full", cfg.SSLMode)
	}
	switch cfg.SSLMode {
	case pq.SSLModeDisable, pq.SSLModeRequire, pq.SSLModeVerifyCA, pq.SSLModeVerifyFull:
	default:
		return fmt.Errorf("postgres test DSN sslmode %q is unsupported; use disable, require, verify-ca, or verify-full", cfg.SSLMode)
	}
	if cfg.KrbSrvname != "" || cfg.KrbSpn != "" {
		return fmt.Errorf("postgres test DSN GSS options are process-local and unsupported; use password authentication")
	}

	typedKeys := postgresConfigTaggedKeys()
	for key := range cfg.Runtime {
		if key == "service" || key == "servicefile" {
			return fmt.Errorf("postgres test DSN option %q is unsupported because cleanup runs in a separate process", key)
		}
		if _, collides := typedKeys[key]; collides {
			return fmt.Errorf("postgres test DSN runtime option %q collides with a typed connection option", key)
		}
		if !validPostgresOptionKey(key) {
			return fmt.Errorf("postgres test DSN runtime option key %q cannot be transported safely", key)
		}
	}
	return nil
}

func (d testPostgresDSN) withDatabase(database string) (testPostgresDSN, error) {
	if strings.TrimSpace(database) == "" {
		return testPostgresDSN{}, fmt.Errorf("postgres test database name is required")
	}
	cfg := d.config.Clone()
	cfg.Database = database
	return newTestPostgresDSN(cfg)
}

func (d testPostgresDSN) open() (*sql.DB, error) {
	connector, err := pq.NewConnectorConfig(d.config.Clone())
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func (d testPostgresDSN) string() (string, error) {
	return serializeTestPostgresConfig(d.config)
}

func serializeTestPostgresConfig(cfg pq.Config) (string, error) {
	if err := validateTestPostgresConfig(cfg); err != nil {
		return "", err
	}

	values := make(map[string]string)
	value := reflect.ValueOf(cfg)
	typ := reflect.TypeOf(cfg)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		key, ok := field.Tag.Lookup("postgres")
		if !ok || key == "" || key == "-" {
			continue
		}
		if !shouldSerializePostgresConfigField(value.Field(i), key) {
			continue
		}
		encoded, err := encodePostgresConfigField(value.Field(i), key)
		if err != nil {
			return "", fmt.Errorf("serialize pq.Config.%s: %w", field.Name, err)
		}
		values[key] = encoded
	}

	if len(cfg.Multi) > 0 {
		hosts := []string{cfg.Host}
		hostaddrs := []string{postgresAddrString(cfg.Hostaddr)}
		ports := []string{strconv.Itoa(int(cfg.Port))}
		hasHostaddr := cfg.Hostaddr.IsValid()
		for _, alternate := range cfg.Multi {
			hosts = append(hosts, alternate.Host)
			hostaddrs = append(hostaddrs, postgresAddrString(alternate.Hostaddr))
			ports = append(ports, strconv.Itoa(int(alternate.Port)))
			hasHostaddr = hasHostaddr || alternate.Hostaddr.IsValid()
		}
		values["host"] = strings.Join(hosts, ",")
		if hasHostaddr {
			values["hostaddr"] = strings.Join(hostaddrs, ",")
		}
		values["port"] = strings.Join(ports, ",")
	}

	typedKeys := postgresConfigTaggedKeys()
	for key, runtimeValue := range cfg.Runtime {
		if _, collides := typedKeys[key]; collides {
			return "", fmt.Errorf("runtime option %q collides with a typed connection option", key)
		}
		values[key] = runtimeValue
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var out strings.Builder
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(key)
		out.WriteByte('=')
		out.WriteString(quotePostgresKeywordValue(values[key]))
	}
	return out.String(), nil
}

func shouldSerializePostgresConfigField(value reflect.Value, key string) bool {
	switch value.Kind() {
	case reflect.String:
		if value.String() != "" {
			return true
		}
		// lib/pq rejects empty enum and encoding values. Their effective
		// defaults are materialized before serialization; every other empty
		// string is emitted to preserve explicit-empty and env-shadowing
		// semantics across the string transport boundary.
		switch key {
		case "sslmode", "sslnegotiation", "target_session_attrs", "load_balance_hosts", "client_encoding", "datestyle":
			return false
		default:
			return true
		}
	case reflect.Struct:
		if value.Type() == reflect.TypeOf(netip.Addr{}) {
			return value.Interface().(netip.Addr).IsValid()
		}
	}
	return true
}

func encodePostgresConfigField(value reflect.Value, key string) (string, error) {
	switch value.Kind() {
	case reflect.String:
		return value.String(), nil
	case reflect.Uint16:
		return strconv.FormatUint(value.Uint(), 10), nil
	case reflect.Int64:
		if value.Type() != reflect.TypeOf(time.Duration(0)) || key != "connect_timeout" {
			return "", fmt.Errorf("unsupported int64 field type %s", value.Type())
		}
		return strconv.FormatInt(int64(time.Duration(value.Int())/time.Second), 10), nil
	case reflect.Bool:
		if value.Bool() {
			return "yes", nil
		}
		return "no", nil
	case reflect.Struct:
		if value.Type() != reflect.TypeOf(netip.Addr{}) {
			return "", fmt.Errorf("unsupported struct field type %s", value.Type())
		}
		return postgresAddrString(value.Interface().(netip.Addr)), nil
	default:
		return "", fmt.Errorf("unsupported field type %s", value.Type())
	}
}

func postgresConfigTaggedKeys() map[string]struct{} {
	keys := make(map[string]struct{})
	typ := reflect.TypeOf(pq.Config{})
	for i := 0; i < typ.NumField(); i++ {
		key, ok := typ.Field(i).Tag.Lookup("postgres")
		if !ok || key == "" || key == "-" {
			continue
		}
		keys[key] = struct{}{}
	}
	return keys
}

func postgresAddrString(addr netip.Addr) string {
	if !addr.IsValid() {
		return ""
	}
	return addr.String()
}

func quotePostgresKeywordValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return `'` + value + `'`
}

func validPostgresOptionKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		valid := r == '_' || r == '.' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r))
		if !valid {
			return false
		}
	}
	return true
}
