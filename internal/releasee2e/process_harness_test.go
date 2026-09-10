package releasee2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func buildReleaseBinary(t *testing.T, outputRoot string) string {
	return buildReleaseBinaryWithArgs(t, outputRoot)
}

func buildRaceReleaseBinary(t *testing.T, outputRoot string) string {
	return buildReleaseBinaryWithArgs(t, outputRoot, "-race")
}

func buildOwnedMockLifecycleBinary(t *testing.T, outputRoot string, buildArgs ...string) string {
	t.Helper()
	path := filepath.Join(outputRoot, "internal-mock-lifecycle.test")
	args := append([]string{"test", "-c"}, buildArgs...)
	args = append(args, "-o", path, "./internal/serveapp")
	cmd := exec.Command("go", args...)
	cmd.Dir = releaseE2ERepoRoot(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build internal retained lifecycle binary: %v\n%s", err, output)
	}
	return path
}

func buildReleaseBinaryWithArgs(t *testing.T, outputRoot string, buildArgs ...string) string {
	t.Helper()
	binaryPath := filepath.Join(outputRoot, "swarm")
	args := append([]string{"build"}, buildArgs...)
	args = append(args, "-o", binaryPath, "./cmd/swarm")
	cmd := exec.Command("go", args...)
	cmd.Dir = releaseE2ERepoRoot(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build release binary: %v\n%s", err, output)
	}
	return binaryPath
}

type releaseProcessOutput struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	secrets []string
}

func (o *releaseProcessOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.Write(p)
}

func (o *releaseProcessOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	output := o.buf.String()
	for _, secret := range o.secrets {
		if secret != "" {
			output = strings.ReplaceAll(output, secret, "[REDACTED]")
		}
	}
	return output
}

type releaseProcessSpec struct {
	BinaryPath                  string
	InternalMockLifecycleBinary string
	WorkingDir                  string
	ConfigPath                  string
	Source                      string
	Store                       string
	Dev                         bool
	APIPort                     int
	MCPListenHost               string
	PublicWebhookBaseURL        string
	PublicWebhookListen         string
	TokenFile                   string
	Token                       string
	Env                         []string
	WorkspaceBackend            string
	DefaultExecutionSelection   bool
	RedactValues                []string
}

type releaseServeProcess struct {
	cmd               *exec.Cmd
	output            *releaseProcessOutput
	exited            chan struct{}
	waitMu            sync.Mutex
	waitErr           error
	apiBase           string
	rpc               *releaseRPCClient
	stopOnce          sync.Once
	internalLifecycle bool
}

func startReleaseServe(t *testing.T, options releaseProcessSpec) *releaseServeProcess {
	t.Helper()
	output := &releaseProcessOutput{secrets: append([]string{options.Token}, options.RedactValues...)}
	workspaceBackend := options.WorkspaceBackend
	if workspaceBackend == "" {
		workspaceBackend = "host"
	}
	args := []string{"serve"}
	if !options.DefaultExecutionSelection {
		args = append(args, "--backend", "claude_cli", "--workspace-backend", workspaceBackend)
	}
	args = append(args,
		"--api-listen-addr", fmt.Sprintf("127.0.0.1:%d", options.APIPort),
		"--mcp-listen-addr", "127.0.0.1:0",
		"--shutdown-grace", "2s",
		"--no-color",
	)
	if options.MCPListenHost != "" {
		for i := range args {
			if args[i] == "--mcp-listen-addr" {
				args[i+1] = net.JoinHostPort(options.MCPListenHost, "0")
			}
		}
	}
	if options.ConfigPath != "" {
		args = append(args, "--config", options.ConfigPath)
	}
	if options.Source != "" {
		args = append(args, options.Source)
	}
	if options.Store != "" {
		args = append(args, "--store", options.Store)
	}
	if options.TokenFile != "" {
		args = append(args, "--api-token-file", options.TokenFile)
	}
	if options.Dev {
		args = append(args, "--dev")
	}
	if options.PublicWebhookBaseURL != "" {
		args = append(args, "--public-webhook-base-url", options.PublicWebhookBaseURL)
	}
	if options.PublicWebhookListen != "" {
		args = append(args, "--public-webhook-listen", options.PublicWebhookListen)
	}
	binary := options.BinaryPath
	env := append([]string(nil), options.Env...)
	if options.InternalMockLifecycleBinary != "" {
		if options.PublicWebhookBaseURL != "" || options.PublicWebhookListen != "" {
			t.Fatal("internal mock lifecycle does not own public exposure/registration proof")
		}
		request, err := json.Marshal(struct {
			ConfigPath string
			Source     string
			Store      string
			Dev        bool
			APIPort    int
			Token      string
		}{options.ConfigPath, options.Source, options.Store, options.Dev, options.APIPort, options.Token})
		if err != nil {
			t.Fatal(err)
		}
		binary = options.InternalMockLifecycleBinary
		args = []string{"-test.run=^TestOwnedMockLifecycleProcessEntry$", "-test.v", "-test.timeout=10m"}
		env = append(env, "SWARM_INTERNAL_MOCK_LIFECYCLE_REQUEST="+string(request))
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir = options.WorkingDir
	cmd.Env = env
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start release serve: %v", err)
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", options.APIPort)
	process := &releaseServeProcess{
		cmd:               cmd,
		output:            output,
		exited:            make(chan struct{}),
		apiBase:           baseURL,
		internalLifecycle: options.InternalMockLifecycleBinary != "",
		rpc: &releaseRPCClient{
			endpoint:     baseURL + "/v1/rpc",
			token:        options.Token,
			client:       &http.Client{Timeout: 5 * time.Second},
			processID:    cmd.Process.Pid,
			redactValues: append([]string{options.Token}, options.RedactValues...),
		},
	}
	go func() {
		err := cmd.Wait()
		process.waitMu.Lock()
		process.waitErr = err
		process.waitMu.Unlock()
		close(process.exited)
	}()
	t.Cleanup(process.stopForCleanup)
	return process
}

func (p *releaseServeProcess) waitReady(ctx context.Context) error {
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.apiBase+"/readyz", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-p.exited:
			return fmt.Errorf("serve exited before readiness: %v\n%s", p.waitError(), p.output.String())
		case <-ctx.Done():
			p.collectStartupEvidence()
			return fmt.Errorf("wait for release readiness: %w\n%s", ctx.Err(), p.output.String())
		case <-ticker.C:
		}
	}
}

func (p *releaseServeProcess) collectStartupEvidence() {
	if !p.internalLifecycle {
		return
	}
	if !strings.Contains(p.output.String(), fmt.Sprintf("lifecycle evidence available pid=%d", p.cmd.Process.Pid)) {
		fmt.Fprintf(p.output, "lifecycle evidence unavailable before handler installation pid=%d\n", p.cmd.Process.Pid)
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		fmt.Fprintf(p.output, "lifecycle evidence signal pid=%d: %v\n", p.cmd.Process.Pid, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	marker := fmt.Sprintf("lifecycle evidence end pid=%d", p.cmd.Process.Pid)
	for !strings.Contains(p.output.String(), marker) {
		select {
		case <-p.exited:
			return
		case <-ctx.Done():
			fmt.Fprintf(p.output, "lifecycle evidence capture incomplete pid=%d\n", p.cmd.Process.Pid)
			return
		case <-ticker.C:
		}
	}
}

func (p *releaseServeProcess) killAndWait(timeout time.Duration) error {
	if err := p.cmd.Process.Kill(); err != nil {
		select {
		case <-p.exited:
			return nil
		default:
			return err
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.exited:
		return nil
	case <-timer.C:
		return fmt.Errorf("serve did not exit within %s", timeout)
	}
}

func (p *releaseServeProcess) stopAndWait(timeout time.Duration) error {
	select {
	case <-p.exited:
		return p.waitError()
	default:
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		select {
		case <-p.exited:
			return p.waitError()
		default:
			return err
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.exited:
		return p.waitError()
	case <-timer.C:
		return fmt.Errorf("serve did not stop within %s", timeout)
	}
}

func (p *releaseServeProcess) stopForCleanup() {
	p.stopOnce.Do(func() {
		select {
		case <-p.exited:
			return
		default:
		}
		_ = p.cmd.Process.Signal(os.Interrupt)
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-p.exited:
		case <-timer.C:
			_ = p.cmd.Process.Kill()
			<-p.exited
		}
	})
}

func (p *releaseServeProcess) waitError() error {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

type releaseRPCClient struct {
	endpoint     string
	token        string
	client       *http.Client
	processID    int
	redactValues []string
}

func (c *releaseRPCClient) call(ctx context.Context, method string, params map[string]any, result any) error {
	requestBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      method,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", method, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return fmt.Errorf("build %s request: %w", method, err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("call %s: %w", method, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read %s response: %w; %s", method, err, c.failureEvidence(method, response.StatusCode, body))
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP %d; %s", method, response.StatusCode, c.failureEvidence(method, response.StatusCode, body))
	}
	var envelope struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    any             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode %s response: %w; %s", method, err, c.failureEvidence(method, response.StatusCode, body))
	}
	var responseID string
	if json.Unmarshal(envelope.ID, &responseID) != nil || responseID != method {
		return fmt.Errorf("%s response identity mismatch; %s", method, c.failureEvidence(method, response.StatusCode, body))
	}
	if envelope.Error != nil {
		return fmt.Errorf("%s failed; %s", method, c.failureEvidence(method, response.StatusCode, body))
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return fmt.Errorf("%s returned no result; %s", method, c.failureEvidence(method, response.StatusCode, body))
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode %s result: %w; %s", method, err, c.failureEvidence(method, response.StatusCode, body))
	}
	return nil
}

// Failure-only protocol evidence: retain envelope identity, never arbitrary result
// or error-data payloads. Non-JSON bodies retain size/hash, not free-form secrets.
func (c *releaseRPCClient) failureEvidence(method string, status int, body []byte) string {
	sanitize := func(value string) string {
		for _, secret := range append([]string{c.token}, c.redactValues...) {
			if secret != "" {
				value = strings.ReplaceAll(value, secret, "[REDACTED]")
			}
		}
		if len(value) > 256 {
			value = value[:256] + "[TRUNCATED]"
		}
		return value
	}
	endpoint := "invalid endpoint"
	if parsed, err := url.Parse(c.endpoint); err == nil {
		endpoint = sanitize(parsed.Scheme + "://" + parsed.Host + parsed.EscapedPath())
	}
	evidence := map[string]any{
		"http_status": status, "endpoint": endpoint, "process_id": c.processID,
		"request_id": method, "body_bytes": len(body), "body_sha256": fmt.Sprintf("%x", sha256.Sum256(body)),
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err == nil && envelope != nil {
		projection := map[string]any{}
		for _, key := range []string{"jsonrpc", "id", "result", "error"} {
			value, present := envelope[key]
			if !present {
				continue
			}
			if string(value) == "null" {
				projection[key] = nil
				continue
			}
			switch key {
			case "jsonrpc", "id":
				var decoded any
				_ = json.Unmarshal(value, &decoded)
				switch v := decoded.(type) {
				case string:
					projection[key] = sanitize(v)
				case float64:
					projection[key] = v
				default:
					projection[key] = "[INVALID TYPE]"
				}
			case "error":
				var fields map[string]json.RawMessage
				if json.Unmarshal(value, &fields) != nil || fields == nil {
					projection[key] = "[INVALID TYPE]"
					continue
				}
				safeError := map[string]any{}
				for _, field := range []string{"code", "message"} {
					if raw, ok := fields[field]; ok {
						var decoded any
						_ = json.Unmarshal(raw, &decoded)
						switch v := decoded.(type) {
						case string:
							safeError[field] = sanitize(v)
						case float64:
							safeError[field] = v
						default:
							safeError[field] = "[INVALID TYPE]"
						}
					}
				}
				if _, ok := fields["data"]; ok {
					safeError["data"] = "[OMITTED]"
				}
				projection[key] = safeError
			default:
				projection[key] = "[OMITTED]"
			}
		}
		evidence["response_envelope"] = projection
		evidence["envelope_field_count"] = len(envelope)
	} else {
		evidence["response_envelope"] = "[NON-OBJECT OR INVALID JSON]"
	}
	encoded, _ := json.Marshal(evidence)
	return string(encoded)
}

func pollReleaseCondition(ctx context.Context, interval time.Duration, check func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
