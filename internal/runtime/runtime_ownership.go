package runtime

import (
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
)

func newRuntimeOwnerID() string {
	host, _ := os.Hostname()
	host = strings.TrimSpace(host)
	if host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString())
}

func coalesceRuntimeIdentity(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "unversioned-runtime-bundle"
}
