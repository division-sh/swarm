//go:build linux || darwin

package serveapp

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/mailbox"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Invariant: an actual registered SDK socket drop creates one-line operator notices through normal plans/renders/sent receipts, both stores.
func TestChannelNoticeLinkRegisteredBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := prepareChannelAnchorPublicJourney(t, backend, "link-notice-token", false, 0, func(opts *cliapp.ServeOptions) {
				opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply")
			})
			mux := http.NewServeMux()
			mux.Handle("/", h.provider)
			mux.Handle("/webhooks/_swarm_probe/", httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: h.opts.PublicWebhookListen}))
			peer := newServeNativeProtocolPeer(t, mux)
			_ = issue3011RegisterForSend(t, h.opts, peer, 1)
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			list := func() []mailbox.V1Item {
				t.Helper()
				var page struct {
					Items []struct {
						Notice mailbox.V1Item `json:"notice"`
					} `json:"items"`
				}
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.list", map[string]any{"type": render.LinkNoticeType, "limit": 200}, &page)
				var result []mailbox.V1Item
				for _, entry := range page.Items {
					if entry.Notice.Type == render.LinkNoticeType {
						result = append(result, entry.Notice)
					}
				}
				return result
			}
			if len(list()) != 0 {
				t.Fatal("healthy startup announced a condition")
			}
			peer.disconnect()
			waitSessionRecovery(t, func() bool { return len(list()) == 2 }, h.process.outputString)
			notices := list()
			reader := openChannelAnchorObservation(t, h)
			kinds := map[string]int{}
			for _, notice := range notices {
				messageID := waitChannelAnchorObservedReceipt(t, reader, notice.MailboxID)
				message := h.provider.Delivery(messageID - 1)
				text := fmt.Sprint(message["text"])
				if fmt.Sprint(message["chat_id"]) != "1001" || strings.Contains(text, "\n") || message["reply_markup"] != nil {
					t.Fatal("notice wire changed recipient/body/controls", message)
				}
				for _, kind := range []string{"down:", "back after"} {
					if strings.HasPrefix(text, "Swarm Notice: WhatsApp link "+kind) {
						kinds[kind]++
					}
				}
			}
			if kinds["down:"] != 1 || kinds["back after"] != 1 {
				t.Fatal("wire did not contain one down and one back", kinds)
			}
			h.stop(t)
			h.start(t)
			if len(list()) != 2 {
				t.Fatal("healthy restart reconstructed or resent an outage")
			}
		})
	}
}
