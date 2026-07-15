package cliapp

import (
	"os"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
)

func CredentialFileStore() (*runtimecredentials.FileStore, error) {
	path := strings.TrimSpace(os.Getenv("SWARM_CREDENTIALS_FILE"))
	if path == "" {
		var err error
		path, err = runtimecredentials.DefaultFilePath()
		if err != nil {
			return nil, err
		}
	}
	return runtimecredentials.NewFileStore(path)
}

func BuildCredentialStore() (runtimecredentials.Store, error) {
	fileStore, err := CredentialFileStore()
	if err != nil {
		return nil, err
	}
	return runtimecredentials.NewOverlayStore(runtimecredentials.NewEnvStore(), fileStore), nil
}

func BuildManagedCredentialStore() (runtimemanagedcredentials.Store, error) {
	return runtimemanagedcredentials.NewDefaultFileStore()
}

func BuildProviderCredentialStore() (runtimecredentials.Store, error) {
	return CredentialFileStore()
}
