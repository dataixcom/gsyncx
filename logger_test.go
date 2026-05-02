package gsyncx

import (
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

func TestGlogxLogger_Methods(t *testing.T) {
	logger := NewSyncLogger()

	logger.Info("test info", F("key", "value"))
	logger.Warn("test warn", F("key", "value"))
	logger.Error("test error", F("key", "value"))
	logger.Debug("test debug", F("key", "value"))
}

func TestGlogxLogger_GetLogger(t *testing.T) {
	logger := NewSyncLogger()
	inner := logger.GetLogger()
	if inner == nil {
		t.Error("expected non-nil inner logger")
	}
}

func TestToGlogxArgs(t *testing.T) {
	fields := []LogField{
		F("name", "test"),
		F("age", 25),
	}

	args := toGlogxArgs(fields)
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

func TestNewSyncLoggerWithGlogx(t *testing.T) {
	inner := NewSyncLogger().GetLogger()
	logger := NewSyncLoggerWithGlogx(inner)
	if logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestNewSyncLoggerWithGlogx_Nil(t *testing.T) {
	logger := NewSyncLoggerWithGlogx(nil)
	if logger == nil {
		t.Error("expected non-nil logger with nil input")
	}
}
