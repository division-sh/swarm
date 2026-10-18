package llm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type ProviderCredentialResolver struct {
	Store     runtimecredentials.Store
	EnvLookup llmselection.EnvLookup
}

type ProviderCredential struct {
	Key         string
	Value       string
	Source      string
	EnvPresent  bool
	EnvShadowed bool
	sourceFact  runtimecredentials.SourceFact
}

func (c ProviderCredential) SourceFact() runtimecredentials.SourceFact { return c.sourceFact }

// The cache belongs to one API runtime. Resolution failures leave it empty;
// request snapshots release synchronization before any provider I/O.
type providerCredentialCache struct {
	mu    sync.Mutex
	value string
}

func (c *providerCredentialCache) resolve(ctx context.Context, resolver ProviderCredentialResolver, profile llmselection.Profile) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.TrimSpace(c.value) != "" {
		return nil
	}
	credential, err := resolver.Resolve(ctx, profile)
	if err != nil {
		return err
	}
	c.value = credential.Value
	return nil
}

func (c *providerCredentialCache) snapshot() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

type MissingProviderCredentialError struct {
	Key        string
	Purpose    string
	EnvPresent bool
}

func NewProviderCredentialResolver(store runtimecredentials.Store) ProviderCredentialResolver {
	return ProviderCredentialResolver{
		Store:     store,
		EnvLookup: os.LookupEnv,
	}
}

func NewProviderCredentialResolverWithEnvLookup(store runtimecredentials.Store, lookup llmselection.EnvLookup) ProviderCredentialResolver {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	return ProviderCredentialResolver{
		Store:     store,
		EnvLookup: lookup,
	}
}

func ProviderCredentialKey(profile llmselection.Profile) string {
	return strings.TrimSpace(profile.Credential.EnvVar)
}

func (e MissingProviderCredentialError) Error() string {
	key := strings.TrimSpace(e.Key)
	if key == "" {
		key = "provider credential"
	}
	purpose := strings.TrimSpace(e.Purpose)
	if purpose == "" {
		purpose = "selected llm provider"
	}
	msg := fmt.Sprintf("%s is required for %s; store it with `swarm secrets set %s`", key, purpose, key)
	if e.EnvPresent {
		msg += fmt.Sprintf("; process env %s is deprecated for Swarm provider credentials and is ignored", key)
	}
	return msg
}

func IsMissingProviderCredential(err error) bool {
	var missing MissingProviderCredentialError
	return errors.As(err, &missing)
}

func (r ProviderCredentialResolver) Resolve(ctx context.Context, profile llmselection.Profile) (ProviderCredential, error) {
	key := ProviderCredentialKey(profile)
	if !profile.Credential.Required {
		return ProviderCredential{Key: key}, nil
	}
	if key == "" {
		return ProviderCredential{}, fmt.Errorf("provider credential key is required for backend %q", strings.TrimSpace(profile.ID))
	}
	envPresent := r.envPresent(key)
	credential, ok, err := r.storeValue(ctx, key)
	if err != nil {
		return ProviderCredential{}, providerCredentialReadFailure(key, "resolve_credential", err)
	}
	if ok {
		credential.EnvPresent, credential.EnvShadowed = envPresent, envPresent
		return credential, nil
	}
	missing := MissingProviderCredentialError{
		Key:        key,
		Purpose:    profile.Credential.Purpose,
		EnvPresent: envPresent,
	}
	return ProviderCredential{}, runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "provider_credential_missing", "llm-provider", "resolve_credential", map[string]any{
		"auth_kind":      "provider_credential",
		"credential_key": key,
		"provider":       strings.TrimSpace(profile.Provider),
	}, missing)
}

func (r ProviderCredentialResolver) Inspect(ctx context.Context, profile llmselection.Profile) (ProviderCredential, error) {
	key := ProviderCredentialKey(profile)
	if !profile.Credential.Required {
		return ProviderCredential{Key: key}, nil
	}
	if key == "" {
		return ProviderCredential{}, fmt.Errorf("provider credential key is required for backend %q", strings.TrimSpace(profile.ID))
	}
	envPresent := r.envPresent(key)
	credential, ok, err := r.storeValue(ctx, key)
	if err != nil {
		return ProviderCredential{}, providerCredentialReadFailure(key, "inspect_credential", err)
	}
	credential.EnvPresent, credential.EnvShadowed = envPresent, envPresent && ok
	return credential, nil
}

func (r ProviderCredentialResolver) envPresent(key string) bool {
	if r.EnvLookup == nil {
		return false
	}
	value, ok := r.EnvLookup(key)
	return ok && strings.TrimSpace(value) != ""
}

func (r ProviderCredentialResolver) storeValue(ctx context.Context, key string) (ProviderCredential, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProviderCredential{}, false, err
	}
	if r.Store == nil {
		return ProviderCredential{}, false, errors.New("provider credential read port is required")
	}
	credential := ProviderCredential{Key: key}
	var present bool
	var metadata runtimecredentials.Metadata
	var err error
	if snapshots, ok := r.Store.(runtimecredentials.Snapshotter); ok {
		var snapshot runtimecredentials.AtomicSnapshot
		snapshot, err = snapshots.Snapshot(ctx, key)
		credential.Value, present = snapshot.CredentialValue(), snapshot.Present
		metadata = snapshot.Metadata()
		credential.sourceFact = snapshot.SourceFact()
	} else {
		credential.Value, present, err = r.Store.Get(ctx, key)
	}
	if err != nil {
		return ProviderCredential{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return ProviderCredential{}, false, err
	}
	credential.Value = strings.TrimSpace(credential.Value)
	if !present || credential.Value == "" {
		return ProviderCredential{Key: key}, false, nil
	}
	if _, atomic := r.Store.(runtimecredentials.Snapshotter); !atomic {
		metadata, err = inspectProviderCredentialStore(ctx, r.Store, key)
		if err != nil {
			return ProviderCredential{}, false, err
		}
	}
	credential.Source = strings.TrimSpace(metadata.Source)
	if credential.Source == "" {
		credential.Source = runtimecredentials.SourceFile
	}
	return credential, true, nil
}

type providerCredentialReadError struct{ cause error }

func (e providerCredentialReadError) Error() string {
	return "provider credential store could not be read"
}
func (e providerCredentialReadError) Unwrap() error { return e.cause }

func providerCredentialReadFailure(key, operation string, cause error) error {
	return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "provider_credential_store_unavailable", "llm-provider", operation,
		map[string]any{"credential_key": key}, providerCredentialReadError{cause: cause})
}

func inspectProviderCredentialStore(ctx context.Context, store runtimecredentials.Store, key string) (runtimecredentials.Metadata, error) {
	if inspector, ok := store.(runtimecredentials.Inspector); ok && inspector != nil {
		meta, err := inspector.Inspect(ctx, key)
		if err != nil {
			return runtimecredentials.Metadata{}, err
		}
		return meta, nil
	}
	if store == nil {
		return runtimecredentials.Metadata{Key: key}, nil
	}
	_, ok, err := store.Get(ctx, key)
	if err != nil {
		return runtimecredentials.Metadata{}, err
	}
	return runtimecredentials.Metadata{Key: key, Present: ok}, nil
}
