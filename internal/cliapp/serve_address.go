package cliapp

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/platform"
)

func ValidateServeListenAddr(flagName, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("%s must be a host:port listen address", flagName)
	}
	if strings.Contains(addr, "://") {
		return fmt.Errorf("%s must be a host:port listen address, not a URL", flagName)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be a host:port listen address: %w", flagName, err)
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	port = strings.TrimSpace(port)
	if host == "" {
		return fmt.Errorf("%s must include an explicit host", flagName)
	}
	if port == "" {
		return fmt.Errorf("%s must include an explicit port", flagName)
	}
	numericPort, err := strconv.Atoi(port)
	if err != nil || numericPort < 0 || numericPort > 65535 {
		return fmt.Errorf("%s port must be between 0 and 65535", flagName)
	}
	return nil
}

func EmbeddedPlatformSpecPath() (string, error) {
	return platform.MaterializePlatformSpecFile()
}
