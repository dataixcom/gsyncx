package gsyncx

import (
	"time"
)

type Option func(*SyncConfig)

func WithSyncMode(mode SyncMode) Option {
	return func(c *SyncConfig) {
		c.SyncMode = mode
	}
}

func WithBatchSize(size int) Option {
	return func(c *SyncConfig) {
		c.BatchSize = size
	}
}

func WithParallelism(n int) Option {
	return func(c *SyncConfig) {
		c.Parallelism = n
	}
}

func WithCheckpoint(enabled bool, path string) Option {
	return func(c *SyncConfig) {
		c.CheckpointEnabled = enabled
		c.CheckpointPath = path
	}
}

func WithIncrementalField(field *Field, strategy IncrementalStrategy) Option {
	return func(c *SyncConfig) {
		c.IncrementalField = field
		c.IncrementalStrategy = strategy
	}
}

func WithIncrementalCondition(condition string) Option {
	return func(c *SyncConfig) {
		c.IncrementalCondition = condition
	}
}

func WithIntegrityCheck(mode IntegrityCheckMode) Option {
	return func(c *SyncConfig) {
		c.IntegrityCheck = mode
	}
}

func WithAutoMapping(enabled bool) Option {
	return func(c *SyncConfig) {
		c.AutoMapping = enabled
		c.MappingConfig.AutoMapping = enabled
	}
}

func WithLastSyncTime(t time.Time) Option {
	return func(c *SyncConfig) {
		c.LastSyncTime = t
	}
}

func WithLastSyncValue(v interface{}) Option {
	return func(c *SyncConfig) {
		c.LastSyncValue = v
	}
}

func WithReaderConfig(cfg ReaderConfig) Option {
	return func(c *SyncConfig) {
		c.ReaderConfig = cfg
	}
}

func WithWriterConfig(cfg WriterConfig) Option {
	return func(c *SyncConfig) {
		c.WriterConfig = cfg
	}
}

func WithTransformConfig(cfg TransformConfig) Option {
	return func(c *SyncConfig) {
		c.TransformConfig = cfg
	}
}

func WithMappingConfig(cfg MappingConfig) Option {
	return func(c *SyncConfig) {
		c.MappingConfig = cfg
	}
}

func WithRetry(maxAttempts int, delay time.Duration) Option {
	return func(c *SyncConfig) {
		c.RetryMaxAttempts = maxAttempts
		c.RetryDelay = delay
	}
}

func WithContinueOnError(continueOnError bool) Option {
	return func(c *SyncConfig) {
		c.ContinueOnError = continueOnError
	}
}

func WithPreviewMode(limit int) Option {
	return func(c *SyncConfig) {
		c.PreviewMode = true
		c.PreviewLimit = limit
	}
}

func WithErrorThreshold(threshold int) Option {
	return func(c *SyncConfig) {
		c.ErrorThreshold = threshold
	}
}

func WithLogger(logger SyncLogger) Option {
	return func(c *SyncConfig) {
		c.logger = logger
	}
}
