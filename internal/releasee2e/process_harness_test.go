package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func buildReleaseBinary(t *testing.T, outputRoot string) string {
	t.Helper()
	binaryPath := filepath.Join(outputRoot, "swarm")
	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/swarm")
	cmd.Dir = releaseE2ERepoRoot(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build release binary: %v\n%s", err, output)
	}
	return binaryPath
}

type releaseProcessOutput struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (o *releaseProcessOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.Write(p)
}

func (o *releaseProcessOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

type releaseProcessSpec struct {
	BinaryPath string
	WorkingDir string
	ConfigPath string
	Contracts  string
	Data       string
	Store      string
	APIPort    int
	MCPPort    int
	TokenFile  string
	Token      string
	Env        []string
}

type releaseServeProcess struct {
	cmd      *exec.Cmd
	output   *releaseProcessOutput
	exited   chan struct{}
	waitMu   sync.Mutex
	waitErr  error
	apiBase  string
	rpc      *releaseRPCClient
	stopOnce sync.Once
}

func startReleaseServe(t *testing.T, options releaseProcessSpec) *releaseServeProcess {
	t.Helper()
	output := &releaseProcessOutput{}
	cmd := exec.Command(
		options.BinaryPath,
		"serve",
		"--config", options.ConfigPath,
		"--contracts", options.Contracts,
		"--data", options.Data,
		"--store", options.Store,
		"--backend", "claude_cli",
		"--workspace-backend", "host",
		"--api-listen-addr", fmt.Sprintf("127.0.0.1:%d", options.APIPort),
		"--mcp-listen-addr", fmt.Sprintf("127.0.0.1:%d", options.MCPPort),
		"--api-token-file", options.TokenFile,
		"--shutdown-grace", "2s",
		"--no-color",
	)
	cmd.Dir = options.WorkingDir
	cmd.Env = options.Env
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start release serve: %v", err)
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", options.APIPort)
	process := &releaseServeProcess{
		cmd:     cmd,
		output:  output,
		exited:  make(chan struct{}),
		apiBase: baseURL,
		rpc: &releaseRPCClient{
			endpoint: baseURL + "/v1/rpc",
			token:    options.Token,
			client:   &http.Client{Timeout: 5 * time.Second},
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
			return fmt.Errorf("wait for release readiness: %w\n%s", ctx.Err(), p.output.String())
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
	endpoint string
	token    string
	client   *http.Client
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
		return fmt.Errorf("read %s response: %w", method, err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP %d: %s", method, response.StatusCode, body)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    any             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode %s response: %w: %s", method, err, body)
	}
	if envelope.Error != nil {
		return fmt.Errorf("%s failed (%v): %s data=%s", method, envelope.Error.Code, envelope.Error.Message, envelope.Error.Data)
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return fmt.Errorf("%s returned no result", method)
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode %s result: %w: %s", method, err, envelope.Result)
	}
	return nil
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
