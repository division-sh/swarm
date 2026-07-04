package llm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
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
	if key == "" {
		return ProviderCredential{}, fmt.Errorf("provider credential key is required for backend %q", strings.TrimSpace(profile.ID))
	}
	if !profile.Credential.Required {
		return ProviderCredential{Key: key}, nil
	}
	envPresent := r.envPresent(key)
	value, ok, err := r.storeValue(ctx, key)
	if err != nil {
		return ProviderCredential{}, err
	}
	if ok {
		source := runtimecredentials.SourceFile
		if meta, metaErr := inspectProviderCredentialStore(ctx, r.Store, key); metaErr != nil {
			return ProviderCredential{}, metaErr
		} else if strings.TrimSpace(meta.Source) != "" {
			source = strings.TrimSpace(meta.Source)
		}
		return ProviderCredential{
			Key:         key,
			Value:       value,
			Source:      source,
			EnvPresent:  envPresent,
			EnvShadowed: envPresent,
		}, nil
	}
	return ProviderCredential{}, MissingProviderCredentialError{
		Key:        key,
		Purpose:    profile.Credential.Purpose,
		EnvPresent: envPresent,
	}
}

func (r ProviderCredentialResolver) Inspect(ctx context.Context, profile llmselection.Profile) (ProviderCredential, error) {
	key := ProviderCredentialKey(profile)
	if key == "" {
		return ProviderCredential{}, fmt.Errorf("provider credential key is required for backend %q", strings.TrimSpace(profile.ID))
	}
	envPresent := r.envPresent(key)
	value, ok, err := r.storeValue(ctx, key)
	if err != nil {
		return ProviderCredential{}, err
	}
	credential := ProviderCredential{
		Key:         key,
		Value:       value,
		EnvPresent:  envPresent,
		EnvShadowed: envPresent && ok,
	}
	if ok {
		credential.Source = runtimecredentials.SourceFile
		if meta, metaErr := inspectProviderCredentialStore(ctx, r.Store, key); metaErr != nil {
			return ProviderCredential{}, metaErr
		} else if strings.TrimSpace(meta.Source) != "" {
			credential.Source = strings.TrimSpace(meta.Source)
		}
	}
	return credential, nil
}

func (r ProviderCredentialResolver) envPresent(key string) bool {
	if r.EnvLookup == nil {
		return false
	}
	value, ok := r.EnvLookup(key)
	return ok && strings.TrimSpace(value) != ""
}

func (r ProviderCredentialResolver) storeValue(ctx context.Context, key string) (string, bool, error) {
	if r.Store == nil {
		return "", false, nil
	}
	value, ok, err := r.Store.Get(ctx, key)
	if err != nil {
		return "", false, err
	}
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return "", false, nil
	}
	return value, true, nil
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
