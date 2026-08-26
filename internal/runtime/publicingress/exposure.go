package publicingress

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	ModeManagedQuickTunnel = "managed_quick_tunnel"
	ModeExternalOrigin     = "external_origin"
)

var cloudflaredVersionPattern = regexp.MustCompile(`(?i)cloudflared version\s+([0-9]+)\.([0-9]+)\.([0-9]+)`) // exact CLI contract

type Generation struct {
	ID            string
	Mode          string
	PublicOrigin  string
	ListenAddress string
	CreatedAt     time.Time
}

type Process interface {
	Done() <-chan error
	Stop() error
}

type ManagedLauncher interface {
	Launch(context.Context, string, string, string) (Process, error)
}

type Options struct {
	Mode              string
	PublicOrigin      string
	ListenAddress     string
	CloudflaredBinary string
	Handler           http.Handler
	HTTPClient        *http.Client
	Launcher          ManagedLauncher
	Readiness         *ReadinessOwner
	StartupAuthority  func() string
	OnGeneration      func(context.Context, Generation) error
	OnFatal           func(error)
	Now               func() time.Time
}

type Controller struct {
	opts        Options
	mu          sync.RWMutex
	generation  Generation
	listener    net.Listener
	server      *http.Server
	process     Process
	cancel      context.CancelFunc
	done        chan struct{}
	supervising bool
	nonce       string
	waitRetry   func(context.Context, time.Duration) error
}

func PreflightCloudflared(ctx context.Context, binary string) error {
	binary = strings.TrimSpace(binary)
	if binary == "" {
		binary = "cloudflared"
	}
	resolved, err := exec.LookPath(binary)
	if err != nil {
		return fmt.Errorf("cloudflared is required for --expose; install it (brew install cloudflared) or use --public-webhook-base-url with --public-webhook-listen: %w", err)
	}
	command := exec.CommandContext(ctx, resolved, "--version")
	raw, err := command.Output()
	if err != nil {
		return fmt.Errorf("inspect cloudflared version: %w", err)
	}
	return admitCloudflaredVersion(raw)
}

func admitCloudflaredVersion(raw []byte) error {
	match := cloudflaredVersionPattern.FindStringSubmatch(strings.TrimSpace(string(raw)))
	if len(match) != 4 {
		return fmt.Errorf("cloudflared --version output is unsupported; require semver >= 2021.7.0")
	}
	major, _ := strconv.Atoi(match[1])
	minor, _ := strconv.Atoi(match[2])
	patchVersion, _ := strconv.Atoi(match[3])
	if major < 2021 || major == 2021 && minor < 7 {
		return fmt.Errorf("cloudflared %d.%d.%d is unsupported; require semver >= 2021.7.0", major, minor, patchVersion)
	}
	return nil
}

func NewController(opts Options) (*Controller, error) {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Handler == nil {
		return nil, fmt.Errorf("public ingress handler is required")
	}
	if err := ValidateConfiguration(opts.Mode, opts.PublicOrigin, opts.ListenAddress); err != nil {
		return nil, err
	}
	return &Controller{opts: opts, done: make(chan struct{}), waitRetry: waitPublicRouteRetry}, nil
}

func ValidateConfiguration(mode, publicOrigin, listenAddress string) error {
	switch strings.TrimSpace(mode) {
	case ModeManagedQuickTunnel:
		if strings.TrimSpace(publicOrigin) != "" || strings.TrimSpace(listenAddress) != "" {
			return fmt.Errorf("managed --expose does not accept an external public origin or listen address")
		}
	case ModeExternalOrigin:
		if _, err := admitPublicOrigin(publicOrigin); err != nil {
			return err
		}
		if err := admitLoopbackListen(listenAddress, false); err != nil {
			return err
		}
	default:
		return fmt.Errorf("public exposure mode %q is unsupported", mode)
	}
	return nil
}

func (c *Controller) Start(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("public exposure controller is required")
	}
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	listenAddress := strings.TrimSpace(c.opts.ListenAddress)
	if c.opts.Mode == ModeManagedQuickTunnel {
		listenAddress = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		cancel()
		return fmt.Errorf("bind dedicated public webhook listener: %w", err)
	}
	if err := admitLoopbackListen(listener.Addr().String(), true); err != nil {
		listener.Close()
		cancel()
		return err
	}
	c.listener = listener
	c.nonce, err = randomToken(24)
	if err != nil {
		listener.Close()
		cancel()
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/webhooks/_swarm_probe/", c.serveProbe)
	mux.Handle("/webhooks/", c.opts.Handler)
	c.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = c.server.Serve(listener) }()
	if err := c.establish(ctx); err != nil {
		_ = c.Stop(context.Background())
		return err
	}
	c.mu.Lock()
	c.supervising = true
	c.mu.Unlock()
	go c.supervise(ctx)
	return nil
}

func (c *Controller) establish(ctx context.Context) error {
	publicOrigin := strings.TrimRight(strings.TrimSpace(c.opts.PublicOrigin), "/")
	var process Process
	succeeded := false
	defer func() {
		if succeeded || process == nil {
			return
		}
		_ = process.Stop()
		select {
		case <-process.Done():
		case <-time.After(5 * time.Second):
		}
		if c.opts.Readiness != nil {
			c.opts.Readiness.RevokeExposure("public exposure generation did not converge")
		}
	}()
	if c.opts.Mode == ModeManagedQuickTunnel {
		launcher := c.opts.Launcher
		if launcher == nil {
			launcher = execManagedLauncher{binary: c.opts.CloudflaredBinary}
		}
		metricsAddress, err := reserveLoopbackAddress()
		if err != nil {
			return err
		}
		process, err = launcher.Launch(ctx, c.listener.Addr().String(), metricsAddress, c.opts.CloudflaredBinary)
		if err != nil {
			return fmt.Errorf("start cloudflared quick tunnel: %w", err)
		}
		hostname, err := waitForQuickTunnel(ctx, c.httpClient(), metricsAddress, process.Done())
		if err != nil {
			return err
		}
		publicOrigin = "https://" + hostname
	}
	probe := c.probePublicRoute
	if c.opts.Mode == ModeManagedQuickTunnel {
		probe = c.probeManagedPublicRoute
	}
	if err := probe(ctx, publicOrigin); err != nil {
		return err
	}
	generation := Generation{ID: uuid.NewString(), Mode: c.opts.Mode, PublicOrigin: publicOrigin, ListenAddress: c.listener.Addr().String(), CreatedAt: c.opts.Now().UTC()}
	c.mu.Lock()
	c.generation = generation
	c.mu.Unlock()
	if c.opts.Readiness != nil {
		c.opts.Readiness.SetExposure(ExposureEvidence{
			GenerationID: generation.ID, Mode: generation.Mode, PublicOrigin: generation.PublicOrigin, ListenAddress: generation.ListenAddress,
			StartupAuthorityID: c.startupAuthority(), ObservedAt: generation.CreatedAt, ExpiresAt: generation.CreatedAt.Add(EvidenceTTL),
		})
	}
	if c.opts.OnGeneration != nil {
		if err := c.opts.OnGeneration(ctx, generation); err != nil {
			return err
		}
	}
	if process != nil {
		c.mu.Lock()
		c.process = process
		c.mu.Unlock()
	}
	succeeded = true
	return nil
}

func (c *Controller) supervise(ctx context.Context) {
	defer close(c.done)
	if c.opts.Mode != ModeManagedQuickTunnel {
		<-ctx.Done()
		return
	}
	for {
		c.mu.RLock()
		process := c.process
		c.mu.RUnlock()
		if process == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case cause := <-process.Done():
			if ctx.Err() != nil {
				return
			}
			log.Printf("public ingress degraded: managed tunnel stopped: %s; attempting replacement", errorText(cause))
			if c.opts.Readiness != nil {
				c.opts.Readiness.RevokeExposure("managed tunnel stopped: " + errorText(cause))
			}
			var last error
			for attempt, backoff := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				last = c.establish(ctx)
				if last == nil {
					log.Printf("public ingress restored with generation %s", c.Generation().ID)
					break
				}
				if attempt == 2 && c.opts.OnFatal != nil {
					c.opts.OnFatal(fmt.Errorf("public tunnel re-establishment failed after 3 attempts: %w", last))
					return
				}
			}
		}
	}
}

func (c *Controller) Renew(ctx context.Context) error {
	generation := c.Generation()
	if generation.ID == "" {
		return fmt.Errorf("public exposure generation is unavailable")
	}
	if err := c.probePublicRoute(ctx, generation.PublicOrigin); err != nil {
		if c.opts.Readiness != nil {
			c.opts.Readiness.RevokeExposure(err.Error())
		}
		return err
	}
	now := c.opts.Now().UTC()
	if c.opts.Readiness != nil {
		c.opts.Readiness.SetExposure(ExposureEvidence{
			GenerationID: generation.ID, Mode: generation.Mode, PublicOrigin: generation.PublicOrigin, ListenAddress: generation.ListenAddress,
			StartupAuthorityID: c.startupAuthority(), ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
		})
	}
	return nil
}

func (c *Controller) Generation() Generation {
	if c == nil {
		return Generation{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.generation
}

func (c *Controller) Stop(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.RLock()
	process := c.process
	supervising := c.supervising
	c.mu.RUnlock()
	if process != nil {
		_ = process.Stop()
	}
	if c.server != nil {
		_ = c.server.Shutdown(ctx)
	}
	if c.opts.Readiness != nil {
		c.opts.Readiness.RevokeExposure("serve shutdown")
	}
	if supervising {
		select {
		case <-c.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else if process != nil {
		select {
		case <-process.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *Controller) serveProbe(w http.ResponseWriter, request *http.Request) {
	want := "/webhooks/_swarm_probe/" + c.nonce
	if request.Method != http.MethodGet || request.URL.Path != want {
		http.NotFound(w, request)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusNoContent)
}

func (c *Controller) probePublicRoute(ctx context.Context, origin string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(origin, "/")+"/webhooks/_swarm_probe/"+c.nonce, nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient().Do(request)
	if err != nil {
		return fmt.Errorf("prove public webhook route: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("prove public webhook route: got HTTP %d, want 204", response.StatusCode)
	}
	return nil
}

func (c *Controller) probeManagedPublicRoute(ctx context.Context, origin string) error {
	var last error
	for attempt, backoff := range append([]time.Duration{0}, time.Second, 2*time.Second, 4*time.Second) {
		if backoff > 0 {
			wait := c.waitRetry
			if wait == nil {
				wait = waitPublicRouteRetry
			}
			if err := wait(ctx, backoff); err != nil {
				return err
			}
		}
		last = c.probePublicRoute(ctx, origin)
		if last == nil {
			return nil
		}
		if attempt == 3 {
			break
		}
	}
	return fmt.Errorf("prove managed public webhook route after bounded propagation retries: %w", last)
}

func waitPublicRouteRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Controller) httpClient() *http.Client {
	if c.opts.HTTPClient != nil {
		return c.opts.HTTPClient
	}
	return &http.Client{Timeout: 5 * time.Second}
}

func (c *Controller) startupAuthority() string {
	if c.opts.StartupAuthority == nil {
		return ""
	}
	return strings.TrimSpace(c.opts.StartupAuthority())
}

func admitPublicOrigin(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" {
		return nil, fmt.Errorf("--public-webhook-base-url must be an HTTPS origin with no path, query, fragment, or userinfo")
	}
	return parsed, nil
}

func admitLoopbackListen(raw string, allowEphemeral bool) error {
	host, portRaw, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("public webhook listener must be an explicit loopback host:port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("public webhook listener must bind an explicit loopback IP")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port < 0 || port > 65535 || port == 0 && !allowEphemeral {
		return fmt.Errorf("public webhook listener requires a valid non-zero port")
	}
	return nil
}

func waitForQuickTunnel(ctx context.Context, client *http.Client, metricsAddress string, done <-chan error) (string, error) {
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case err := <-done:
			return "", fmt.Errorf("cloudflared exited before readiness: %w", err)
		case <-deadline.C:
			return "", fmt.Errorf("cloudflared quick tunnel did not become ready within 15s")
		case <-ticker.C:
			ready, hostname := readQuickTunnelEndpoints(ctx, client, metricsAddress)
			if ready && hostname != "" {
				return hostname, nil
			}
		}
	}
}

func readQuickTunnelEndpoints(ctx context.Context, client *http.Client, metricsAddress string) (bool, string) {
	base := "http://" + metricsAddress
	readyRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ready", nil)
	if err != nil {
		return false, ""
	}
	readyResponse, err := client.Do(readyRequest)
	if err != nil {
		return false, ""
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(readyResponse.Body, 4096))
	readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK {
		return false, ""
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/quicktunnel", nil)
	if err != nil {
		return false, ""
	}
	response, err := client.Do(request)
	if err != nil {
		return false, ""
	}
	defer response.Body.Close()
	var payload struct {
		Hostname string `json:"hostname"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4096))
	decoder.DisallowUnknownFields()
	if response.StatusCode != http.StatusOK || decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return false, ""
	}
	hostname := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(payload.Hostname), "."))
	if hostname == "" || !strings.HasSuffix(hostname, ".trycloudflare.com") || strings.Count(hostname, ".") < 2 {
		return false, ""
	}
	return true, hostname
}

func reserveLoopbackAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return address, nil
}

func randomToken(bytes int) (string, error) {
	buffer := make([]byte, bytes)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func errorText(err error) string {
	if err == nil {
		return "unknown exit"
	}
	return err.Error()
}

type execManagedLauncher struct{ binary string }

func (l execManagedLauncher) Launch(ctx context.Context, originAddress, metricsAddress, binaryOverride string) (Process, error) {
	binary := strings.TrimSpace(binaryOverride)
	if binary == "" {
		binary = strings.TrimSpace(l.binary)
	}
	if binary == "" {
		binary = "cloudflared"
	}
	resolved, err := exec.LookPath(binary)
	if err != nil {
		return nil, err
	}
	config, err := os.CreateTemp("", "swarm-cloudflared-*.yaml")
	if err != nil {
		return nil, err
	}
	configPath := config.Name()
	if err := config.Chmod(0o600); err != nil {
		config.Close()
		os.Remove(configPath)
		return nil, err
	}
	if err := config.Close(); err != nil {
		os.Remove(configPath)
		return nil, err
	}
	command := exec.CommandContext(ctx, resolved, "tunnel", "--config", configPath, "--no-autoupdate", "--output", "json", "--metrics", metricsAddress, "--url", "http://"+originAddress)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		os.Remove(configPath)
		return nil, err
	}
	process := &execProcess{command: command, configPath: configPath, done: make(chan error, 1)}
	go func() {
		process.done <- command.Wait()
		close(process.done)
		_ = os.Remove(configPath)
	}()
	return process, nil
}

type execProcess struct {
	command    *exec.Cmd
	configPath string
	done       chan error
	stopOnce   sync.Once
}

func (p *execProcess) Done() <-chan error { return p.done }
func (p *execProcess) Stop() error {
	if p == nil || p.command == nil || p.command.Process == nil {
		return nil
	}
	var err error
	p.stopOnce.Do(func() { err = p.command.Process.Kill() })
	return err
}

func CallbackURL(generation Generation, alias, provider, token string) (string, error) {
	alias = strings.TrimSpace(alias)
	provider = strings.TrimSpace(provider)
	token = strings.TrimSpace(token)
	if !validCallbackRouteSegment(alias) || !validCallbackRouteSegment(provider) || token == "" || strings.HasPrefix(alias, "_") {
		return "", fmt.Errorf("callback route identity is invalid")
	}
	parsed, err := admitPublicOrigin(generation.PublicOrigin)
	if err != nil {
		return "", err
	}
	parsed.Path = path.Join("/webhooks", alias, provider)
	query := parsed.Query()
	query.Set("swarm_callback_generation", token)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func validCallbackRouteSegment(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/%?# \t\r\n")
}
