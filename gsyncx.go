package gsyncx

import (
	"time"
)

func NewSyncConfig(opts ...Option) *SyncConfig {
	cfg := &SyncConfig{
		SyncMode:         SyncModeFull,
		BatchSize:        1000,
		Parallelism:      4,
		RetryMaxAttempts: 3,
		RetryDelay:       time.Second,
		ErrorThreshold:   0,
		ContinueOnError:  true,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.logger == nil {
		cfg.logger = defaultLogger
	}

	return cfg
}

var defaultLogger SyncLogger

func init() {
	defaultLogger = NewSyncLogger()
}

func SetDefaultLogger(l SyncLogger) {
	if l != nil {
		defaultLogger = l
	}
}

func GetDefaultLogger() SyncLogger {
	return defaultLogger
}

func ResolveLogger(loggers ...SyncLogger) SyncLogger {
	for _, l := range loggers {
		if l != nil {
			return l
		}
	}
	return defaultLogger
}
