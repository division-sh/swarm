package cliapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/google/uuid"
)

func applyDoctorGatewayProbe(ctx context.Context, report *LocalPreflightReport, cfg *config.Config, backend WorkspaceBackendSelection, listenAddr string) {
	findings := report.Findings[:0]
	for _, finding := range report.Findings {
		if finding.Code != "workspace_gateway_not_probed" {
			findings = append(findings, finding)
		}
	}
	report.Findings = findings
	err := probeDoctorGateway(ctx, cfg, backend, listenAddr)
	if err != nil {
		failure := failures.Normalize(err, "doctor", "gateway_probe")
		report.add(localPreflightGatewayPrerequisite, failure.Detail.Code, LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, fmt.Sprintf("target-local gateway probe failed: %s (endpoint=%v, status=%v, http_status=%v); provider credential validity not probed", failure.Detail.Code, failure.Detail.Attributes["endpoint"], failure.Detail.Attributes["status"], failure.Detail.Attributes["http_status"]), "check the workspace network, MCP bind address, image/worker and host firewall; doctor does not change them")
		return
	}
	report.add(localPreflightGatewayPrerequisite, "workspace_gateway_probed", LocalPreflightSeverityInfo, LocalPreflightStatusOK, "gateway network and boot authentication checked from inside the selected workspace; no model call, tool admission or provider credential validity was probed", "")
}

func probeDoctorGateway(ctx context.Context, cfg *config.Config, backend WorkspaceBackendSelection, listenAddr string) (retErr error) {
	listener, err := ListenServeHTTPListener("mcp", listenAddr)
	if err != nil {
		return failures.Wrap(failures.ClassDependencyUnavailable, "workspace_gateway_unreachable", "doctor", "bind_gateway", nil, err)
	}
	hostURL, containerURL, err := toolgateway.ListenerEndpoints(listener.Addr())
	if err != nil {
		return errors.Join(err, listener.Close())
	}
	token, err := toolgateway.GenerateAuthToken()
	if err != nil {
		return errors.Join(err, listener.Close())
	}
	// The ordinary gateway admits initialize without a turn. No executor,
	// registry, credential, database or fictional actor is installed here.
	server := &http.Server{Handler: mcp.NewGateway(nil, token, mcp.GatewayHooks{}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	joined := make(chan error, 1)
	go func() { joined <- server.Serve(listener) }()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(cleanupCtx)
		closeErr := server.Close()
		serveErr := <-joined
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		retErr = errors.Join(retErr, shutdownErr, closeErr, serveErr)
	}()
	target := &workspace.Target{Backend: workspace.BackendHost}
	endpoint := hostURL
	dockerBin := workspace.DefaultDockerBin()
	if backend.Backend == workspace.BackendDocker {
		manager := workspace.NewDockerManager()
		dockerCfg := workspace.DefaultDockerConfig()
		if cfg != nil {
			if cfg.Workspace.DockerBin != "" {
				dockerCfg.DockerBin = cfg.Workspace.DockerBin
			}
			if cfg.Workspace.Image != "" {
				dockerCfg.WorkspaceImage = cfg.Workspace.Image
			}
			if cfg.Workspace.Network != "" {
				dockerCfg.WorkspaceNetwork = cfg.Workspace.Network
			}
		}
		manager.SetConfig(dockerCfg)
		dockerBin = manager.DockerBin()
		if err := manager.EnsurePrereqs(ctx); err != nil {
			return err
		}
		name := "swarm-doctor-gateway-" + uuid.NewString()
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			retErr = errors.Join(retErr, removeDoctorGatewayContainer(cleanupCtx, manager, name))
		}()
		if err := manager.EnsureContainerRunning(ctx, name, []string{"--entrypoint", "sleep", dockerCfg.WorkspaceImage, "infinity"}); err != nil {
			return err
		}
		target = &workspace.Target{Backend: workspace.BackendDocker, Container: name, Workdir: "/"}
		endpoint = containerURL
	} else {
		root, err := os.MkdirTemp("", "swarm-doctor-gateway-")
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, os.RemoveAll(root)) }()
		target.Workdir = root
	}
	_, err = workspace.RunWorker(ctx, target, dockerBin, worker.Request{Mode: "gateway", Gateway: toolgateway.HTTPObservation{URL: endpoint, Headers: map[string]string{"Authorization": "Bearer " + token}}})
	return err
}

func removeDoctorGatewayContainer(ctx context.Context, manager *workspace.DockerManager, name string) error {
	id, err := manager.RunDocker(ctx, "inspect", "--format", "{{.Id}}", name)
	if err != nil && strings.Contains(err.Error(), "No such object") {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = manager.RunDocker(ctx, "rm", "--force", strings.TrimSpace(id))
	return err
}
