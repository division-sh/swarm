package cliapp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

func TestChannelTerminalCredentialPrecedesAccountApproval(t *testing.T) {
	for _, cancelAccount := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelAccount), func(t *testing.T) {
			master, terminal := channelCredentialTerminal(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			const secret = "synthetic-telegram-credential"
			var admitted, approved atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
				switch req.Method {
				case "channel.onboarding_start":
					response["error"] = map[string]any{"code": -32000, "message": "credential required", "data": map[string]any{
						"code": "CHANNEL_CREDENTIAL_REQUIRED", "details": map[string]any{"operation_id": operatorChannelCLIOperation, "role": "bot_token", "store_key": "channel.telegram.bot_token"}}}
				case "channel.onboarding_get":
					response["result"] = deploymentAccountPreviewCLIResult(t, "telegram", false)
				case "channel.onboarding_retry":
					if req.Params["provider_credential"] == secret {
						admitted.Store(true)
					} else if req.Params["approve_account"] == true && admitted.Load() {
						approved.Store(true)
					} else {
						t.Error("credential/account boundary lost its explicit input")
					}
					response["result"] = deploymentAccountPreviewCLIResult(t, "telegram", approved.Load())
				default:
					t.Errorf("unexpected method %s", req.Method)
				}
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			setCLIAPITestToken(t, operatorChannelCLIToken)
			opts := testRootCommandOptions(server)
			opts.input = terminal
			opts.stdinIsTerminal = func() bool { return term.IsTerminal(int(terminal.Fd())) }
			project := t.TempDir()
			done := make(chan struct{})
			var code int
			go func() {
				defer close(done)
				code = executeRootCommandWithOptions(ctx, project, []string{"channel", "connect", "telegram"}, terminal, terminal, opts)
			}()
			defer func() { cancel(); _ = master.Close(); <-done }()
			input, closeInput, err := channelConfirmationReader(ctx, master)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); _ = closeInput() }()
			reader := bufio.NewReader(input)
			var transcript strings.Builder
			readUntil := func(want string) {
				t.Helper()
				for !strings.Contains(transcript.String(), want) {
					b, err := reader.ReadByte()
					if err != nil {
						t.Fatalf("terminal did not reach %q: %v", want, err)
					}
					transcript.WriteByte(b)
				}
				if strings.Contains(transcript.String(), secret) {
					t.Fatal("terminal echoed the provider credential")
				}
			}
			for _, prompt := range []string{"Secret value: ", "Confirm secret value: "} {
				readUntil(prompt)
				for {
					state, err := unix.IoctlGetTermios(int(terminal.Fd()), unix.TCGETS)
					if err != nil || ctx.Err() != nil {
						t.Fatal("terminal did not enter hidden input", err, ctx.Err())
					}
					if state.Lflag&unix.ECHO == 0 {
						break
					}
					runtime.Gosched()
				}
				if _, err := io.WriteString(master, secret+"\n"); err != nil {
					t.Fatal(err)
				}
			}
			readUntil("Is this the right account? [y/N] ")
			if state, err := unix.IoctlGetTermios(int(terminal.Fd()), unix.TCGETS); err != nil || state.Lflag&unix.ECHO == 0 {
				t.Fatal("secret acquisition did not restore terminal echo", err)
			}
			if !admitted.Load() || approved.Load() {
				t.Fatal("secret admission consumed account consent")
			}
			if cancelAccount {
				cancel()
			} else if _, err := io.WriteString(master, "y\n"); err != nil {
				t.Fatal(err)
			} else {
				readUntil("Registered telegram account 420079.")
			}
			<-done
			if (code == 0) == cancelAccount || approved.Load() == cancelAccount {
				t.Fatal("terminal consent/cancellation did not join exactly", code, approved.Load())
			}
		})
	}
}

func channelCredentialTerminal(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	master := os.NewFile(uintptr(fd), "channel-credential-terminal")
	t.Cleanup(func() { _ = master.Close() })
	if err := unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	index, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", index), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = terminal.Close() })
	return master, terminal
}
