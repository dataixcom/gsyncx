package gsyncx

import (
	"github.com/dataixcom/glogx"
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

type GlogxLogger struct {
	logger glogx.Logger
}

func NewSyncLogger() *GlogxLogger {
	return &GlogxLogger{logger: glogx.NewDevelopmentLogger()}
}

func NewSyncLoggerWithGlogx(l glogx.Logger) *GlogxLogger {
	if l == nil {
		l = glogx.NewDevelopmentLogger()
	}
	return &GlogxLogger{logger: l}
}

func NewProductionSyncLogger() *GlogxLogger {
	return &GlogxLogger{logger: glogx.NewProductionLogger()}
}

func (l *GlogxLogger) Info(msg string, fields ...LogField) {
	l.logger.Info(msg, toGlogxArgs(fields)...)
}

func (l *GlogxLogger) Warn(msg string, fields ...LogField) {
	l.logger.Warn(msg, toGlogxArgs(fields)...)
}

func (l *GlogxLogger) Error(msg string, fields ...LogField) {
	l.logger.Error(msg, toGlogxArgs(fields)...)
}

func (l *GlogxLogger) Debug(msg string, fields ...LogField) {
	l.logger.Debug(msg, toGlogxArgs(fields)...)
}

func (l *GlogxLogger) GetLogger() glogx.Logger {
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

func toGlogxArgs(fields []LogField) []any {
	args := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		args = append(args, f.Key, f.Value)
	}
	return args
}
