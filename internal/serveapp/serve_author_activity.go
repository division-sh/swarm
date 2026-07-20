package serveapp

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/lipgloss"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"golang.org/x/term"
)

const serveAuthorActivityPageSize = 100

var serveAuthorActivityPollInterval = 100 * time.Millisecond

type serveAuthorActivityReader interface {
	HeadAuthorActivity(context.Context) (int64, error)
	ListAuthorActivity(context.Context, runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error)
}

type serveAuthorActivityScope interface {
	BundleHashes() []string
}

type serveAuthorActivityFollower struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func newServeAuthorActivityFollower(
	parent context.Context,
	owner *worklifetime.Process,
	reader serveAuthorActivityReader,
	presenter *serveLifecyclePresenter,
	runtimeInstanceID string,
	scope serveAuthorActivityScope,
	cursor int64,
	renderer runtimeauthoractivity.HumanRenderer,
) (*serveAuthorActivityFollower, error) {
	if owner == nil {
		return nil, fmt.Errorf("author activity follower requires a process work owner")
	}
	lease, err := owner.Begin(parent)
	if err != nil {
		return nil, fmt.Errorf("admit author activity follower: %w", err)
	}
	ctx, cancel := context.WithCancel(lease.Context())
	follower := &serveAuthorActivityFollower{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(follower.done)
		defer func() { _ = lease.Done() }()
		runServeAuthorActivityFollower(ctx, reader, presenter, runtimeInstanceID, scope, cursor, renderer)
	}()
	return follower, nil
}

func (f *serveAuthorActivityFollower) StopAndWait() {
	if f == nil {
		return
	}
	f.once.Do(f.cancel)
	<-f.done
}

func runServeAuthorActivityFollower(
	ctx context.Context,
	reader serveAuthorActivityReader,
	presenter *serveLifecyclePresenter,
	runtimeInstanceID string,
	scope serveAuthorActivityScope,
	cursor int64,
	renderer runtimeauthoractivity.HumanRenderer,
) {
	lastWarning := ""
	warn := func(err error) {
		if err == nil || ctx.Err() != nil {
			return
		}
		message := strings.TrimSpace(err.Error())
		if message == lastWarning {
			return
		}
		lastWarning = message
		presenter.storyWarning(err)
	}
	defer func() {
		flushed, next, err := renderer.PrepareFlush()
		if err != nil {
			warn(err)
			return
		}
		if len(flushed) > 0 {
			if err := presenter.writeStory(flushed); err != nil {
				warn(err)
				return
			}
		}
		renderer = next
	}()
	if reader == nil {
		warn(fmt.Errorf("author activity reader is unavailable"))
		return
	}
	if scope == nil {
		warn(fmt.Errorf("author activity runtime context scope is unavailable"))
		return
	}
	ticker := time.NewTicker(serveAuthorActivityPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		window, next, err := renderer.PrepareWindowClose(time.Now())
		if err != nil {
			warn(err)
			continue
		}
		if len(window) > 0 {
			if err := presenter.writeStory(window); err != nil {
				warn(err)
				continue
			}
		}
		renderer = next
		page, err := reader.ListAuthorActivity(ctx, runtimeauthoractivity.ListOptions{
			AfterSequence: cursor, Limit: serveAuthorActivityPageSize,
			RuntimeInstanceID: strings.TrimSpace(runtimeInstanceID), BundleHashes: scope.BundleHashes(), IncludeRuntimeScope: true,
		})
		if err != nil {
			warn(err)
			continue
		}
		if len(page.Occurrences) == 0 {
			lastWarning = ""
			continue
		}
		rendered, next, err := renderer.PreparePage(page.Occurrences)
		if err != nil {
			warn(err)
			continue
		}
		if len(rendered) > 0 {
			if err := presenter.writeStory(rendered); err != nil {
				warn(err)
				continue
			}
		}
		renderer = next
		cursor = page.NextCursor
		lastWarning = ""
	}
}

func serveAuthorActivityReaderFromStores(stores storeBundle) serveAuthorActivityReader {
	if reader, ok := stores.EventStore.(serveAuthorActivityReader); ok && reader != nil {
		return reader
	}
	if reader, ok := stores.InboundStore.(serveAuthorActivityReader); ok && reader != nil {
		return reader
	}
	return nil
}

func serveAuthorActivityRenderOptions(out io.Writer, noColor bool) runtimeauthoractivity.RenderOptions {
	mode := runtimeauthoractivity.RenderPlain
	noColorEnvironment := strings.TrimSpace(os.Getenv("NO_COLOR")) != ""
	file, isFile := out.(*os.File)
	if !noColor && !noColorEnvironment && isFile && file != nil && term.IsTerminal(int(file.Fd())) {
		mode = runtimeauthoractivity.RenderTTY
	}
	return runtimeauthoractivity.RenderOptions{Mode: mode, Width: 120, Palette: serveAuthorActivityPalette(mode)}
}

func serveAuthorActivityPalette(mode runtimeauthoractivity.RenderMode) runtimeauthoractivity.Palette {
	if mode != runtimeauthoractivity.RenderTTY {
		return runtimeauthoractivity.Palette{}
	}
	dim := lipgloss.NewStyle().Faint(true)
	subject := lipgloss.NewStyle().Bold(true)
	identity := lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	subjectIdentity := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	green := lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	yellow := lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	red := lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	return runtimeauthoractivity.Palette{
		Time:            func(value string) string { return dim.Render(value) },
		Subject:         func(value string) string { return subject.Render(value) },
		Identity:        func(value string) string { return identity.Render(value) },
		SubjectIdentity: func(value string) string { return subjectIdentity.Render(value) },
		Success:         func(value string) string { return strings.ReplaceAll(value, "✓", green.Render("✓")) },
		Warning:         func(value string) string { return yellow.Render(value) },
		Failure:         func(value string) string { return red.Render(value) },
	}
}
