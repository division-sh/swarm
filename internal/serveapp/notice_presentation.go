package serveapp

import (
	"fmt"
	"sync"

	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type serveNoticePresentationSink struct {
	once      sync.Once
	presenter *serveLifecyclePresenter
}

func newServeNoticePresentationSink(presenter *serveLifecyclePresenter) *serveNoticePresentationSink {
	return &serveNoticePresentationSink{presenter: presenter}
}

func (s *serveNoticePresentationSink) PresentCommittedInformationalNotice(notice runtimetools.CommittedInformationalNotice) {
	if s == nil || s.presenter == nil {
		return
	}
	s.once.Do(func() {
		s.presenter.presentCommittedInformationalNotice(notice)
	})
}

func (p *serveLifecyclePresenter) presentCommittedInformationalNotice(notice runtimetools.CommittedInformationalNotice) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, _ = fmt.Fprintf(p.out, "\n⚠ notices are accumulating with no delivery channel configured - see swarm channels / #2079 (%d unread; latest mailbox_id=%s)\n", notice.UnreadCount, notice.MailboxID)
}
