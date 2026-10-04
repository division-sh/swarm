package cliapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
)

func ListenServeHTTPListener(name, addr string) (net.Listener, error) {
	return ListenServeHTTPListenerInContext(context.Background(), name, addr)
}

func ListenServeHTTPListenerInContext(ctx context.Context, name, addr string) (net.Listener, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	addr = strings.TrimSpace(addr)
	if err := ValidateServeListenAddr("--"+name+"-listen-addr", addr); err != nil {
		return nil, err
	}
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s listener bind failed: %w", name, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, listener.Close())
	}
	return listener, nil
}
