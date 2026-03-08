package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultMonitorDir = "/tmp/empire-monitor"

type MonitorTurnMeta struct {
	AgentID    string
	Runtime    string
	SessionID  string
	ScopeKey   string
	InputRole  string
	InputText  string
	TargetName string
}

type MonitorTurnWriter interface {
	WriteStdout(line []byte)
	WriteStderr(line []byte)
	WriteNotice(format string, args ...any)
	Close() error
}

type MonitorSink interface {
	OpenTurn(ctx context.Context, meta MonitorTurnMeta) (MonitorTurnWriter, error)
}

type fileMonitorSink struct {
	rootDir string
}

type fileMonitorTurnWriter struct {
	file *os.File
}

func DefaultMonitorDir() string {
	if raw := strings.TrimSpace(os.Getenv("EMPIREAI_MONITOR_DIR")); raw != "" {
		return raw
	}
	return defaultMonitorDir
}

func MonitorLogPath(rootDir, agentID string) string {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		rootDir = DefaultMonitorDir()
	}
	return filepath.Join(rootDir, sanitizeMonitorName(agentID)+".log")
}

func NewFileMonitorSink(rootDir string) MonitorSink {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		rootDir = DefaultMonitorDir()
	}
	return &fileMonitorSink{rootDir: rootDir}
}

func (s *fileMonitorSink) OpenTurn(_ context.Context, meta MonitorTurnMeta) (MonitorTurnWriter, error) {
	if s == nil {
		return nil, nil
	}
	path := MonitorLogPath(s.rootDir, meta.AgentID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create monitor dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open monitor log: %w", err)
	}
	w := &fileMonitorTurnWriter{file: f}
	w.WriteNotice(
		"turn.start runtime=%s session=%s scope=%s role=%s target=%s input=%s",
		strings.TrimSpace(meta.Runtime),
		strings.TrimSpace(meta.SessionID),
		strings.TrimSpace(meta.ScopeKey),
		strings.TrimSpace(meta.InputRole),
		strings.TrimSpace(meta.TargetName),
		snippetForLog(meta.InputText, 240),
	)
	return w, nil
}

func (w *fileMonitorTurnWriter) WriteStdout(line []byte) {
	w.writeLine(summarizeMonitorEventLine(line))
}

func (w *fileMonitorTurnWriter) WriteStderr(line []byte) {
	msg := strings.TrimSpace(string(line))
	if msg == "" {
		return
	}
	w.writeLine("stderr: " + msg)
}

func (w *fileMonitorTurnWriter) WriteNotice(format string, args ...any) {
	if w == nil {
		return
	}
	w.writeLine(fmt.Sprintf(format, args...))
}

func (w *fileMonitorTurnWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *fileMonitorTurnWriter) writeLine(msg string) {
	if w == nil || w.file == nil {
		return
	}
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return
	}
	_, _ = fmt.Fprintf(w.file, "%s %s\n", time.Now().UTC().Format(time.RFC3339), msg)
}

func sanitizeMonitorName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func summarizeMonitorEventLine(line []byte) string {
	raw := strings.TrimSpace(string(line))
	if raw == "" {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return raw
	}
	typ := strings.ToLower(strings.TrimSpace(asString(obj["type"])))
	switch typ {
	case "assistant":
		if msg := summarizeMonitorAssistantEvent(obj); msg != "" {
			return msg
		}
	case "user":
		if msg := summarizeMonitorUserEvent(obj); msg != "" {
			return msg
		}
	case "system":
		subtype := strings.TrimSpace(asString(obj["subtype"]))
		if subtype == "" {
			subtype = strings.TrimSpace(asString(obj["event"]))
		}
		if subtype == "" {
			subtype = "system"
		}
		if sid := strings.TrimSpace(coalesce(asString(obj["session_id"]), asString(obj["sessionId"]))); sid != "" {
			return fmt.Sprintf("system[%s] session=%s", subtype, sid)
		}
		return fmt.Sprintf("system[%s]", subtype)
	case "result":
		result := snippetForLog(asString(obj["result"]), 240)
		if result == "" {
			result = strings.TrimSpace(asString(obj["message"]))
		}
		if result == "" {
			result = "completed"
		}
		if sid := strings.TrimSpace(coalesce(asString(obj["session_id"]), asString(obj["sessionId"]))); sid != "" {
			return fmt.Sprintf("result session=%s %s", sid, result)
		}
		return "result " + result
	}
	return raw
}

func summarizeMonitorAssistantEvent(obj map[string]any) string {
	payload := obj["message"]
	if payload == nil {
		payload = obj
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	resp := parseCLIResponse(b)
	if resp == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if text := snippetForLog(resp.Message.Content, 240); text != "" {
		parts = append(parts, "assistant: "+text)
	}
	if len(resp.ToolCalls) > 0 {
		names := make([]string, 0, len(resp.ToolCalls))
		for _, call := range resp.ToolCalls {
			if name := strings.TrimSpace(call.Name); name != "" {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			parts = append(parts, "tools="+strings.Join(names, ","))
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func summarizeMonitorUserEvent(obj map[string]any) string {
	payload := obj["message"]
	if payload == nil {
		payload = obj
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	resp := parseCLIResponse(b)
	if resp == nil {
		return ""
	}
	if text := snippetForLog(resp.Message.Content, 240); text != "" {
		return "user: " + text
	}
	return ""
}
