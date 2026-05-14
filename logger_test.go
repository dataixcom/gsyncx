package gsyncx

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
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

func TestSlogLogger_ErrorIncludesStackTrace(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	logger.Error("something went wrong", F("error", "test error"))

	output := buf.String()
	if !contains(output, "stack_trace") {
		t.Error("expected stack_trace in error log output")
	}
	if !contains(output, "TestSlogLogger_ErrorIncludesStackTrace") {
		t.Error("expected current function name in stack trace")
	}
}

func TestSlogLogger_ErrorNoDuplicateStackTrace(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	logger.Error("test", F("stack_trace", "custom stack"))

	output := buf.String()
	if !contains(output, "custom stack") {
		t.Error("expected custom stack_trace to be preserved")
	}
	idx := strings.Index(output, "stack_trace")
	idx2 := strings.Index(output[idx+len("stack_trace"):], "stack_trace")
	if idx2 != -1 {
		t.Error("expected only one stack_trace field in output")
	}
}

func TestSlogLogger_InfoDoesNotIncludeStackTrace(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	logger.Info("normal info message")

	output := buf.String()
	if contains(output, "stack_trace") {
		t.Error("expected no stack_trace in info log output")
	}
}

func TestCaptureStack(t *testing.T) {
	stack := captureStack(0)
	if stack == "" {
		t.Error("expected non-empty stack trace")
	}
	if !contains(stack, "TestCaptureStack") {
		t.Errorf("expected TestCaptureStack in stack trace, got: %s", stack)
	}
}

func TestCaptureStack_Skip(t *testing.T) {
	helper := func() string {
		return captureStack(1)
	}
	stack := helper()
	if stack == "" {
		t.Error("expected non-empty stack trace")
	}
	if contains(stack, "TestCaptureStack_Skip.helper") {
		t.Error("expected helper function to be skipped in stack trace")
	}
}

func TestMaskSensitive(t *testing.T) {
	tests := []struct {
		key      string
		value    interface{}
		expected string
	}{
		{"password", "secret123", "s***3"},
		{"passwd", "abc", "a***c"},
		{"token", "tok_12345", "t***5"},
		{"api_key", "key_abc", "k***c"},
		{"apikey", "mykey", "m***y"},
		{"username", "john", "john"},
		{"host", "localhost", "localhost"},
		{"password", "ab", "a***b"},
		{"password", "a", "***"},
	}
	for _, tt := range tests {
		result := MaskSensitive(tt.key, tt.value)
		resultStr, ok := result.(string)
		if !ok {
			t.Errorf("MaskSensitive(%q, %v) returned non-string: %T", tt.key, tt.value, result)
			continue
		}
		if resultStr != tt.expected {
			t.Errorf("MaskSensitive(%q, %v) = %q, want %q", tt.key, tt.value, resultStr, tt.expected)
		}
	}
}

func TestMaskSensitiveMap(t *testing.T) {
	data := map[string]interface{}{
		"username": "admin",
		"password": "secret123",
		"host":     "localhost",
		"token":    "tok_abc",
	}
	result := MaskSensitiveMap(data)

	if result["username"] != "admin" {
		t.Errorf("expected username to be unmasked, got %v", result["username"])
	}
	if result["host"] != "localhost" {
		t.Errorf("expected host to be unmasked, got %v", result["host"])
	}
	if result["password"] == "secret123" {
		t.Error("expected password to be masked")
	}
	if result["token"] == "tok_abc" {
		t.Error("expected token to be masked")
	}
}

func TestMaskSensitiveMap_Nil(t *testing.T) {
	result := MaskSensitiveMap(nil)
	if result != nil {
		t.Errorf("expected nil result for nil input, got %v", result)
	}
}

func TestNewSyncLogger_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	logger.Info("json format test", F("key", "value"))

	output := buf.String()
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Errorf("output is not valid JSON: %s, error: %v", output, err)
	}
}

func TestNewSyncLogger_SourceLocation(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	logger.Info("source location test")

	output := buf.String()
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %s", output)
	}
	source, ok := parsed["source"]
	if !ok {
		t.Error("expected source field in JSON log output")
	}
	sourceMap, ok := source.(map[string]interface{})
	if !ok {
		t.Error("expected source to be a map")
	}
	if _, ok := sourceMap["function"]; !ok {
		t.Error("expected function field in source")
	}
	if _, ok := sourceMap["file"]; !ok {
		t.Error("expected file field in source")
	}
	if _, ok := sourceMap["line"]; !ok {
		t.Error("expected line field in source")
	}
}

func TestNewSyncLogger_StandardFields(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	slogger := slog.New(handler)
	logger := NewSyncLoggerWithSlog(slogger)

	logger.Info("standard fields test")

	output := buf.String()
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %s", output)
	}
	if _, ok := parsed["time"]; !ok {
		t.Error("expected time field in JSON log output")
	}
	if _, ok := parsed["level"]; !ok {
		t.Error("expected level field in JSON log output")
	}
	if _, ok := parsed["msg"]; !ok {
		t.Error("expected msg field in JSON log output")
	}
}

func TestNewProductionSyncLogger_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo, AddSource: true})
	dh := newDynamicHandler(handler, slog.LevelInfo)
	logger := &SlogLogger{logger: slog.New(dh), handler: dh}

	logger.Info("production json test")

	output := buf.String()
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Errorf("production logger output is not valid JSON: %s, error: %v", output, err)
	}
}

func TestNewSyncLoggerWithLevel_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
	dh := newDynamicHandler(handler, slog.LevelDebug)
	logger := &SlogLogger{logger: slog.New(dh), handler: dh}

	logger.Debug("level json test", F("key", "value"))

	output := buf.String()
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Errorf("level logger output is not valid JSON: %s, error: %v", output, err)
	}
}
