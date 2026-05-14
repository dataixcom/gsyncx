package gsyncx

import (
	"bytes"
	"log/slog"
	"testing"
)

func TestNewSyncLogger(t *testing.T) {
	logger := NewSyncLogger()
	if logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewProductionSyncLogger(t *testing.T) {
	logger := NewProductionSyncLogger()
	if logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewSyncLoggerWithLevel(t *testing.T) {
	logger := NewSyncLoggerWithLevel(LogLevelWarn)
	if logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewNopLogger(t *testing.T) {
	logger := NewNopLogger()
	if logger == nil {
		t.Error("expected non-nil logger")
	}
	logger.Info("should not panic")
	logger.Warn("should not panic")
	logger.Error("should not panic")
	logger.Debug("should not panic")
}

func TestNewFuncLogger(t *testing.T) {
	called := false
	logger := NewFuncLogger(
		func(msg string, fields ...LogField) { called = true },
		nil, nil, nil,
	)
	logger.Info("test")
	if !called {
		t.Error("expected info function to be called")
	}
}

func TestFuncLogger_NilFunctions(t *testing.T) {
	logger := NewFuncLogger(nil, nil, nil, nil)
	logger.Info("should not panic")
	logger.Warn("should not panic")
	logger.Error("should not panic")
	logger.Debug("should not panic")
}

func TestSlogLogger_Methods(t *testing.T) {
	logger := NewSyncLogger()
	logger.Info("test info", F("key", "value"))
	logger.Warn("test warn", F("key", "value"))
	logger.Error("test error", F("key", "value"))
	logger.Debug("test debug", F("key", "value"))
}

func TestSlogLogger_GetLogger(t *testing.T) {
	logger := NewSyncLogger()
	inner := logger.GetLogger()
	if inner == nil {
		t.Error("expected non-nil inner logger")
	}
}

func TestToSlogArgs(t *testing.T) {
	fields := []LogField{
		F("name", "test"),
		F("age", 25),
	}
	args := toSlogArgs(fields)
	if len(args) != 4 {
		t.Errorf("expected 4 args, got %d", len(args))
	}
	if args[0] != "name" {
		t.Errorf("expected first arg 'name', got %v", args[0])
	}
	if args[1] != "test" {
		t.Errorf("expected second arg 'test', got %v", args[1])
	}
}

func TestF(t *testing.T) {
	field := F("key", "value")
	if field.Key != "key" {
		t.Errorf("expected key 'key', got %s", field.Key)
	}
	if field.Value != "value" {
		t.Errorf("expected value 'value', got %v", field.Value)
	}
}

func TestSetDefaultLogger(t *testing.T) {
	original := GetDefaultLogger()
	defer SetDefaultLogger(original)

	newLogger := NewProductionSyncLogger()
	SetDefaultLogger(newLogger)

	current := GetDefaultLogger()
	if current != newLogger {
		t.Error("expected default logger to be updated")
	}
}

func TestSetDefaultLogger_Nil(t *testing.T) {
	original := GetDefaultLogger()
	defer SetDefaultLogger(original)

	SetDefaultLogger(nil)
	if GetDefaultLogger() != original {
		t.Error("expected default logger to remain unchanged when nil is passed")
	}
}

func TestResolveLogger(t *testing.T) {
	logger := NewSyncLogger()

	resolved := ResolveLogger(nil)
	if resolved == nil {
		t.Error("expected non-nil resolved logger from nil input")
	}

	resolved = ResolveLogger(logger)
	if resolved != logger {
		t.Error("expected same logger instance")
	}

	resolved = ResolveLogger(nil, nil, logger)
	if resolved != logger {
		t.Error("expected first non-nil logger")
	}
}

func TestNewSyncLoggerWithSlog(t *testing.T) {
	inner := slog.Default()
	logger := NewSyncLoggerWithSlog(inner)
	if logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewSyncLoggerWithSlog_Nil(t *testing.T) {
	logger := NewSyncLoggerWithSlog(nil)
	if logger == nil {
		t.Error("expected non-nil logger with nil input")
	}
}

func TestSlogLogger_WithTaskID(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	taskLogger := logger.WithTaskID("task-123")
	taskLogger.Info("test message")

	output := buf.String()
	if !contains(output, "task_id") {
		t.Error("expected task_id in output")
	}
	if !contains(output, "task-123") {
		t.Error("expected task-123 in output")
	}
}

func TestSlogLogger_WithModule(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	moduleLogger := logger.WithModule("engine")
	moduleLogger.Info("test message")

	output := buf.String()
	if !contains(output, "module") {
		t.Error("expected module in output")
	}
	if !contains(output, "engine") {
		t.Error("expected engine in output")
	}
}

func TestSlogLogger_WithFields(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	fieldLogger := logger.WithFields(F("env", "production"), F("version", "1.0"))
	fieldLogger.Info("test message")

	output := buf.String()
	if !contains(output, "env") {
		t.Error("expected env in output")
	}
	if !contains(output, "production") {
		t.Error("expected production in output")
	}
}

func TestSlogLogger_WithTaskIDAndModule(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	contextLogger := logger.WithTaskID("task-456").WithModule("reader")
	contextLogger.Info("reading data", F("table", "users"))

	output := buf.String()
	if !contains(output, "task_id") {
		t.Error("expected task_id in output")
	}
	if !contains(output, "task-456") {
		t.Error("expected task-456 in output")
	}
	if !contains(output, "module") {
		t.Error("expected module in output")
	}
	if !contains(output, "reader") {
		t.Error("expected reader in output")
	}
	if !contains(output, "table") {
		t.Error("expected table in output")
	}
}

func TestSlogLogger_SetLevel(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	dh := newDynamicHandler(handler, slog.LevelDebug)
	logger := &SlogLogger{logger: slog.New(dh), handler: dh}

	logger.Debug("debug before level change")
	if !contains(buf.String(), "debug before level change") {
		t.Error("expected debug message before level change")
	}

	buf.Reset()
	logger.SetLevel(LogLevelWarn)

	logger.Debug("debug after level change")
	if contains(buf.String(), "debug after level change") {
		t.Error("expected debug message to be suppressed after level change to warn")
	}

	logger.Warn("warn after level change")
	if !contains(buf.String(), "warn after level change") {
		t.Error("expected warn message after level change")
	}
}

func TestSlogLogger_DynamicLevel_Info(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	dh := newDynamicHandler(handler, slog.LevelDebug)
	logger := &SlogLogger{logger: slog.New(dh), handler: dh}

	logger.SetLevel(LogLevelInfo)

	logger.Debug("should be suppressed")
	if contains(buf.String(), "should be suppressed") {
		t.Error("expected debug to be suppressed at Info level")
	}

	logger.Info("should be visible")
	if !contains(buf.String(), "should be visible") {
		t.Error("expected info to be visible at Info level")
	}
}

func TestNopLogger_WithTaskID(t *testing.T) {
	logger := NewNopLogger()
	result := logger.WithTaskID("test")
	if result == nil {
		t.Error("expected non-nil logger from WithTaskID")
	}
}

func TestNopLogger_WithModule(t *testing.T) {
	logger := NewNopLogger()
	result := logger.WithModule("test")
	if result == nil {
		t.Error("expected non-nil logger from WithModule")
	}
}

func TestNopLogger_WithFields(t *testing.T) {
	logger := NewNopLogger()
	result := logger.WithFields(F("key", "value"))
	if result == nil {
		t.Error("expected non-nil logger from WithFields")
	}
}

func TestFuncLogger_WithTaskID(t *testing.T) {
	logger := NewFuncLogger(nil, nil, nil, nil)
	result := logger.WithTaskID("test")
	if result == nil {
		t.Error("expected non-nil logger from WithTaskID")
	}
}

func TestLogLevel_ToSlogLevel(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected slog.Level
	}{
		{LogLevelDebug, slog.LevelDebug},
		{LogLevelInfo, slog.LevelInfo},
		{LogLevelWarn, slog.LevelWarn},
		{LogLevelError, slog.LevelError},
		{LogLevel(99), slog.LevelInfo},
	}
	for _, tt := range tests {
		got := tt.level.toSlogLevel()
		if got != tt.expected {
			t.Errorf("LogLevel(%d).toSlogLevel() = %v, want %v", tt.level, got, tt.expected)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
