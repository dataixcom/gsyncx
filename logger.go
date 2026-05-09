package gsyncx

import (
	"log/slog"
	"os"
)

type SyncLogger interface {
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
	Debug(msg string, fields ...LogField)
}

type LogField struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func F(key string, value interface{}) LogField {
	return LogField{Key: key, Value: value}
}

type SlogLogger struct {
	logger *slog.Logger
}

func NewSyncLogger() *SlogLogger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return &SlogLogger{logger: slog.New(handler)}
}

func NewSyncLoggerWithSlog(l *slog.Logger) *SlogLogger {
	if l == nil {
		return NewSyncLogger()
	}
	return &SlogLogger{logger: l}
}

func NewProductionSyncLogger() *SlogLogger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return &SlogLogger{logger: slog.New(handler)}
}

func (l *SlogLogger) Info(msg string, fields ...LogField) {
	l.logger.Info(msg, toSlogArgs(fields)...)
}

func (l *SlogLogger) Warn(msg string, fields ...LogField) {
	l.logger.Warn(msg, toSlogArgs(fields)...)
}

func (l *SlogLogger) Error(msg string, fields ...LogField) {
	l.logger.Error(msg, toSlogArgs(fields)...)
}

func (l *SlogLogger) Debug(msg string, fields ...LogField) {
	l.logger.Debug(msg, toSlogArgs(fields)...)
}

func (l *SlogLogger) GetLogger() *slog.Logger {
	return l.logger
}

type NopLogger struct{}

func NewNopLogger() *NopLogger {
	return &NopLogger{}
}

func (n *NopLogger) Info(string, ...LogField)  {}
func (n *NopLogger) Warn(string, ...LogField)  {}
func (n *NopLogger) Error(string, ...LogField) {}
func (n *NopLogger) Debug(string, ...LogField) {}

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

func toSlogArgs(fields []LogField) []any {
	args := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		args = append(args, f.Key, f.Value)
	}
	return args
}
