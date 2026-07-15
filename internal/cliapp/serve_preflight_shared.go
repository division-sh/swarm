package cliapp

import (
	"fmt"
	"net"
	"strings"
)

func ListenServeHTTPListener(name, addr string) (net.Listener, error) {
	addr = strings.TrimSpace(addr)
	if err := ValidateServeListenAddr("--"+name+"-listen-addr", addr); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s listener bind failed: %w", name, err)
	}
	return listener, nil
}

var RetiredToolGatewayURLEnvNames = []string{"SWARM_TOOL_GATEWAY_URL", "SWARM_TOOL_GATEWAY_CONTAINER_URL"}

func ValidateRetiredToolGatewayURLEnv(name, raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return fmt.Errorf("%s is retired and not accepted as gateway endpoint configuration; unset %s because swarm derives the tool gateway endpoint from ToolGatewayBinding", name, name)
}
