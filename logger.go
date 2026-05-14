package gsyncx

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

func (l LogLevel) toSlogLevel() slog.Level {
	switch l {
	case LogLevelDebug:
		return slog.LevelDebug
	case LogLevelInfo:
		return slog.LevelInfo
	case LogLevelWarn:
		return slog.LevelWarn
	case LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type SyncLogger interface {
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
	Debug(msg string, fields ...LogField)
	WithTaskID(taskID string) SyncLogger
	WithModule(module string) SyncLogger
	WithFields(fields ...LogField) SyncLogger
}

type LogField struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func F(key string, value interface{}) LogField {
	return LogField{Key: key, Value: value}
}

type SlogLogger struct {
	logger   *slog.Logger
	handler  *dynamicHandler
	preFields []LogField
}

type dynamicHandler struct {
	mu     sync.RWMutex
	level  slog.Level
	inner  slog.Handler
}

func newDynamicHandler(inner slog.Handler, level slog.Level) *dynamicHandler {
	return &dynamicHandler{inner: inner, level: level}
}

func (h *dynamicHandler) Enabled(ctx context.Context, level slog.Level) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return level >= h.level
}

func (h *dynamicHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

func (h *dynamicHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return newDynamicHandler(h.inner.WithAttrs(attrs), h.level)
}

func (h *dynamicHandler) WithGroup(name string) slog.Handler {
	return newDynamicHandler(h.inner.WithGroup(name), h.level)
}

func (h *dynamicHandler) setLevel(level slog.Level) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.level = level
}

func NewSyncLogger() *SlogLogger {
	dh := newDynamicHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}), slog.LevelDebug)
	return &SlogLogger{logger: slog.New(dh), handler: dh}
}

func NewSyncLoggerWithSlog(l *slog.Logger) *SlogLogger {
	if l == nil {
		return NewSyncLogger()
	}
	return &SlogLogger{logger: l}
}

func NewProductionSyncLogger() *SlogLogger {
	dh := newDynamicHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}), slog.LevelInfo)
	return &SlogLogger{logger: slog.New(dh), handler: dh}
}

func NewSyncLoggerWithLevel(level LogLevel) *SlogLogger {
	dh := newDynamicHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level.toSlogLevel()}), level.toSlogLevel())
	return &SlogLogger{logger: slog.New(dh), handler: dh}
}

func (l *SlogLogger) Info(msg string, fields ...LogField) {
	l.logger.Info(msg, toSlogArgs(append(l.preFields, fields...))...)
}

func (l *SlogLogger) Warn(msg string, fields ...LogField) {
	l.logger.Warn(msg, toSlogArgs(append(l.preFields, fields...))...)
}

func (l *SlogLogger) Error(msg string, fields ...LogField) {
	l.logger.Error(msg, toSlogArgs(append(l.preFields, fields...))...)
}

func (l *SlogLogger) Debug(msg string, fields ...LogField) {
	l.logger.Debug(msg, toSlogArgs(append(l.preFields, fields...))...)
}

func (l *SlogLogger) WithTaskID(taskID string) SyncLogger {
	return &SlogLogger{
		logger:    l.logger.With("task_id", taskID),
		handler:   l.handler,
		preFields: l.preFields,
	}
}

func (l *SlogLogger) WithModule(module string) SyncLogger {
	return &SlogLogger{
		logger:    l.logger.With("module", module),
		handler:   l.handler,
		preFields: l.preFields,
	}
}

func (l *SlogLogger) WithFields(fields ...LogField) SyncLogger {
	newPre := make([]LogField, len(l.preFields), len(l.preFields)+len(fields))
	copy(newPre, l.preFields)
	newPre = append(newPre, fields...)
	return &SlogLogger{
		logger:    l.logger.With(toSlogArgs(fields)...),
		handler:   l.handler,
		preFields: newPre,
	}
}

func (l *SlogLogger) SetLevel(level LogLevel) {
	if l.handler != nil {
		l.handler.setLevel(level.toSlogLevel())
	}
}

func (l *SlogLogger) GetLogger() *slog.Logger {
	return l.logger
}

type NopLogger struct{}

func NewNopLogger() *NopLogger {
	return &NopLogger{}
}

func (n *NopLogger) Info(string, ...LogField)                        {}
func (n *NopLogger) Warn(string, ...LogField)                        {}
func (n *NopLogger) Error(string, ...LogField)                       {}
func (n *NopLogger) Debug(string, ...LogField)                       {}
func (n *NopLogger) WithTaskID(string) SyncLogger                    { return n }
func (n *NopLogger) WithModule(string) SyncLogger                    { return n }
func (n *NopLogger) WithFields(...LogField) SyncLogger               { return n }

type LoggerFunc func(msg string, fields ...LogField)

type FuncLogger struct {
	infoFn  LoggerFunc
	warnFn  LoggerFunc
	errorFn LoggerFunc
	debugFn LoggerFunc
}

func NewFuncLogger(infoFn, warnFn, errorFn, debugFn LoggerFunc) *FuncLogger {
	return &FuncLogger{
		infoFn:  infoFn,
		warnFn:  warnFn,
		errorFn: errorFn,
		debugFn: debugFn,
	}
}

func (l *FuncLogger) Info(msg string, fields ...LogField) {
	if l.infoFn != nil {
		l.infoFn(msg, fields...)
	}
}

func (l *FuncLogger) Warn(msg string, fields ...LogField) {
	if l.warnFn != nil {
		l.warnFn(msg, fields...)
	}
}

func (l *FuncLogger) Error(msg string, fields ...LogField) {
	if l.errorFn != nil {
		l.errorFn(msg, fields...)
	}
}

func (l *FuncLogger) Debug(msg string, fields ...LogField) {
	if l.debugFn != nil {
		l.debugFn(msg, fields...)
	}
}

func (l *FuncLogger) WithTaskID(string) SyncLogger      { return l }
func (l *FuncLogger) WithModule(string) SyncLogger      { return l }
func (l *FuncLogger) WithFields(...LogField) SyncLogger { return l }

func toSlogArgs(fields []LogField) []any {
	args := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		args = append(args, f.Key, f.Value)
	}
	return args
}
