package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

type DatabasePasswordResolver struct {
	Credentials runtimecredentials.Store
	LookupEnv   func(string) (string, bool)
	ReadFile    func(string) ([]byte, error)
}

func ResolveDatabasePassword(ctx context.Context, cfg config.DatabaseConfig, credentials runtimecredentials.Store) (string, error) {
	return DatabasePasswordResolver{
		Credentials: credentials,
		LookupEnv:   os.LookupEnv,
		ReadFile:    os.ReadFile,
	}.Resolve(ctx, cfg)
}

func (r DatabasePasswordResolver) Resolve(ctx context.Context, cfg config.DatabaseConfig) (string, error) {
	if err := config.ValidatePostgresDatabasePasswordSource(cfg); err != nil {
		return "", err
	}
	switch {
	case strings.TrimSpace(cfg.PasswordSecretKey) != "":
		return r.resolveSecretKey(ctx, cfg.PasswordSecretKey)
	case strings.TrimSpace(cfg.PasswordFile) != "":
		return r.resolvePasswordFile(cfg.PasswordFile)
	case strings.TrimSpace(cfg.PasswordEnv) != "":
		return r.resolvePasswordEnv(cfg.PasswordEnv)
	default:
		return "", errors.New("postgres store requires exactly one database password source")
	}
}

func (r DatabasePasswordResolver) resolveSecretKey(ctx context.Context, key string) (string, error) {
	key = strings.TrimSpace(key)
	if r.Credentials == nil {
		return "", fmt.Errorf("database.password_secret_key %q requires the Swarm credential file store", key)
	}
	value, ok, err := r.Credentials.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("read database.password_secret_key %q: %w", key, err)
	}
	if !ok || value == "" {
		return "", fmt.Errorf("database.password_secret_key %q was not found in swarm secrets; set it with `swarm secrets set %s` or choose database.password_file/database.password_env", key, key)
	}
	return value, nil
}

func (r DatabasePasswordResolver) resolvePasswordFile(path string) (string, error) {
	path = strings.TrimSpace(path)
	readFile := r.ReadFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	raw, err := readFile(path)
	if err != nil {
		return "", fmt.Errorf("read database.password_file %s: %w", path, err)
	}
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" {
		return "", fmt.Errorf("database.password_file %s is empty", path)
	}
	return value, nil
}

func (r DatabasePasswordResolver) resolvePasswordEnv(name string) (string, error) {
	name = strings.TrimSpace(name)
	lookupEnv := r.LookupEnv
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}
	value, ok := lookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("database.password_env %s is unset or empty", name)
	}
	return value, nil
}
