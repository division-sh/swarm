package serveapp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestServeNoticePresentationIsProcessScopedAndOneShot(t *testing.T) {
	var output bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &output, ErrorOutput: &output, NoColor: true})
	sink := newServeNoticePresentationSink(presenter)

	sink.PresentCommittedInformationalNotice(runtimetools.CommittedInformationalNotice{MailboxID: "notice-1", UnreadCount: 1})
	sink.PresentCommittedInformationalNotice(runtimetools.CommittedInformationalNotice{MailboxID: "notice-2", UnreadCount: 2})

	text := output.String()
	if strings.Count(text, "notices are accumulating with no delivery channel configured") != 1 {
		t.Fatalf("teaching note output = %q", text)
	}
	if !strings.Contains(text, "1 unread") || !strings.Contains(text, "mailbox_id=notice-1") || strings.Contains(text, "notice-2") {
		t.Fatalf("one-shot committed fact output = %q", text)
	}
}

func TestServeReadyPresentsGlobalUnreadInformationalNoticeCount(t *testing.T) {
	var output bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &output, ErrorOutput: &output, NoColor: true})
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project", UnreadInformationalNotices: 3})
	if !strings.Contains(output.String(), "operator notices") || !strings.Contains(output.String(), "⚠ 3 unread") {
		t.Fatalf("ready output = %q", output.String())
	}
}
