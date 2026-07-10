package diaglog

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type Level string

const (
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

func (l Level) String() string { return string(l) }

func NormalizeLevel(raw string) Level {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

type RunEntry struct {
	Level       Level
	Message     string
	Component   string
	Action      string
	EventID     string
	EventType   string
	AgentID     string
	EntityID    string
	SessionID   string
	Correlation map[string]string
	Detail      any
	Failure     *runtimefailures.Envelope
	StackTrace  string
	DurationUS  int
}

func (e RunEntry) EffectiveEntityID() string {
	return strings.TrimSpace(e.EntityID)
}

func (e *RunEntry) NormalizeEntityID() {
	if e == nil {
		return
	}
	e.EntityID = e.EffectiveEntityID()
}

type RunLogger interface {
	LogRuntime(ctx context.Context, entry RunEntry) error
}

func RunLog(ctx context.Context, logger RunLogger, entry RunEntry) error {
	if logger == nil {
		return nil
	}
	return logger.LogRuntime(ctx, entry)
}

func ProcessLog(level Level, component, message string, fields ...any) {
	level = NormalizeLevel(level.String())
	component = strings.TrimSpace(component)
	if component == "" {
		component = "runtime"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	if len(fields) == 0 {
		log.Printf("%s component=%s message=%s", levelTag(level), component, message)
		return
	}
	payload := map[string]any{
		"message": message,
	}
	for i := 0; i+1 < len(fields); i += 2 {
		key := strings.TrimSpace(fmt.Sprint(fields[i]))
		if key == "" {
			continue
		}
		payload[key] = fields[i+1]
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("%s component=%s message=%s", levelTag(level), component, message)
		return
	}
	log.Printf("%s component=%s detail=%s", levelTag(level), component, string(raw))
}

func levelTag(level Level) string {
	switch NormalizeLevel(level.String()) {
	case LevelWarn:
		return "runtime.warn"
	case LevelError:
		return "runtime.error"
	default:
		return "runtime.info"
	}
}
