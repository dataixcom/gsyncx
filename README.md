# gsyncx

高性能、可扩展的数据同步基础库，基于 `reader → transform → mapping → writer` 四阶段流水线架构，支持 MySQL、PostgreSQL、Oracle 等多种数据库之间的数据传输与转换。

## 核心特性

- **四阶段流水线**：Reader → Transform → Mapping → Writer，每个阶段可独立配置和替换
- **多数据库支持**：通过 gdbx 支持 MySQL、PostgreSQL、Oracle
- **全量同步**：完整同步所有数据，支持数据完整性校验（计数/校验和）
- **增量同步**：支持时间戳、自增ID、版本号、自定义条件四种增量策略
- **实时同步**：基于 Redis Stream 的实时数据同步，支持消费者组、消息确认、Pending 消息处理
- **同步模式切换**：运行时无缝切换全量/增量/实时模式
- **智能字段映射**：自动匹配同名字段，支持显式映射 + 自动映射混合模式
- **映射可视化**：MappingDebugger 提供映射调试和映射报告生成
- **Lua 脚本转换**：内置沙箱化 Lua 引擎，支持自定义数据转换逻辑
- **断点续传**：内存和文件两种检查点存储，支持同步中断后恢复
- **Hook 机制**：12 个 Hook 点，覆盖流水线全生命周期
- **依赖注入日志**：支持日志实例注入、NopLogger、FuncLogger 等多种日志适配
- **错误隔离**：单条记录转换/映射失败不影响整批数据
- **预览模式**：支持预览模式，只读取不写入，用于数据验证
- **可扩展架构**：Reader/Writer 工厂注册中心，支持自定义数据源类型

## 安装

```bash
go get github.com/dataixcom/gsyncx
```

## 快速开始

### 基本全量同步

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/dataixcom/gsyncx"
    "github.com/dataixcom/gsyncx/engine"
)

func main() {
    cfg := gsyncx.NewSyncConfig(
        gsyncx.WithSyncMode(gsyncx.SyncModeFull),
        gsyncx.WithBatchSize(1000),
        gsyncx.WithParallelism(4),
        gsyncx.WithContinueOnError(true),
        gsyncx.WithRetry(3, time.Second),
        gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
            DSNConfig: &gsyncx.DSNConfig{
                DBType:   gsyncx.DBMySQL,
                Host:     "localhost",
                Port:     3306,
                User:     "root",
                Password: "password",
                Schema:   "source_db",
            },
            TableName: "users",
        }),
        gsyncx.WithWriterConfig(gsyncx.WriterConfig{
            DSNConfig: &gsyncx.DSNConfig{
                DBType:   gsyncx.DBMySQL,
                Host:     "localhost",
                Port:     3306,
                User:     "root",
                Password: "password",
                Schema:   "target_db",
            },
            TableName: "users",
            WriteMode: gsyncx.WriteModeUpsert,
        }),
    )

    result, err := engine.RunSync(context.Background(), cfg, nil)
    if err != nil {
        log.Fatalf("sync failed: %v", err)
    }

    fmt.Printf("Sync completed: status=%s read=%d written=%d failed=%d\n",
        result.Status, result.TotalRead, result.TotalWritten, result.TotalFailed)
}
```

### 增量同步

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
    gsyncx.WithIncrementalField(
        &gsyncx.Field{FieldName: "updated_at"},
        gsyncx.StrategyTimestamp,
    ),
    gsyncx.WithLastSyncTime(time.Now().Add(-24*time.Hour)),
    gsyncx.WithCheckpoint(true, "./checkpoints"),
    // ... ReaderConfig 和 WriterConfig
)
```

### 自定义增量条件

使用 `StrategyCustom` 和 `WithIncrementalCondition` 实现灵活的增量条件：

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
    gsyncx.WithIncrementalField(
        &gsyncx.Field{FieldName: "updated_at"},
        gsyncx.StrategyCustom,
    ),
    gsyncx.WithIncrementalCondition("{field} >= '{last_sync_time}' AND status = 'active'"),
    gsyncx.WithLastSyncTime(time.Now().Add(-7*24*time.Hour)),
)
```

占位符说明：
- `{field}` — 增量字段名（自动转义）
- `{last_sync_time}` — 上次同步时间（格式化为 datetime）
- `{last_sync_value}` — 上次同步值（用于自增/版本策略）

### 同步模式切换

运行时在全量、增量和实时模式间无缝切换：

```go
eng, _ := engine.NewSyncEngine(cfg, ...)

// 先执行全量同步
result, _ := eng.Run(ctx)

// 切换到增量模式
eng.SwitchToIncrementalSync(
    &gsyncx.Field{FieldName: "updated_at"},
    gsyncx.StrategyTimestamp,
)
eng.GetConfig().LastSyncTime = time.Now().Add(-24 * time.Hour)

// 执行增量同步
result, _ = eng.Run(ctx)

// 切换到实时模式（基于 Redis Stream）
eng.SwitchToRealtimeSync()

// 切换回全量模式
eng.SwitchToFullSync()
```

也可以直接在 SyncConfig 上切换：

```go
cfg := gsyncx.NewSyncConfig(gsyncx.WithSyncMode(gsyncx.SyncModeFull))

cfg.SwitchToIncrementalSync(&gsyncx.Field{FieldName: "id"}, gsyncx.StrategyAutoInc)
cfg.SwitchToRealtimeSync()
cfg.SwitchToFullSync()

// 查询当前模式
cfg.IsFullSync()        // true
cfg.IsIncrementalSync() // false
cfg.IsRealtimeSync()    // false
```

### Redis Stream 实时同步

基于 Redis Stream 实现消息实时同步，支持消费者组、消息确认、Pending 消息处理和同步状态监控。

#### 架构概览

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────┐
│  Redis       │    │  RedisStream     │    │  RedisMessage   │    │  Database    │
│  Stream      │───▶│  Reader          │───▶│  Transformer    │───▶│  Writer      │
│  (数据源)    │    │  (读取消息)       │    │  (字段映射/转换) │    │  (写入目标)  │
└─────────────┘    └──────────────────┘    └─────────────────┘    └──────────────┘
                          │                        │
                          │                        │
                   ┌──────▼──────┐          ┌──────▼──────┐
                   │  RedisStream  │          │  Field      │
                   │  Monitor    │          │  Mapping    │
                   │  (状态监控) │          │  (字段映射) │
                   └─────────────┘          └─────────────┘
```

#### 数据同步流程

```
Redis Stream ──XREADGROUP──▶ 消息批次 ──字段映射──▶ 映射后数据 ──转换──▶ 目标格式 ──XACK──▶ 确认完成
     │                           │                          │
     │                     _stream_id                  字段重命名
     │                     原始字段值                  类型转换
     │                     payload 解析               内置转换函数
     │
     └──▶ Pending 消息 ──XCLAIM──▶ 重新处理
```

#### 基本使用

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/dataixcom/gsyncx"
    "github.com/dataixcom/gsyncx/engine"
    "github.com/dataixcom/gsyncx/reader"
    "github.com/dataixcom/gsyncx/transform"
    "github.com/dataixcom/gsyncx/writer"
    "github.com/dataixcom/gsyncx/datasource"
)

func main() {
    logger := gsyncx.NewSyncLogger()

    redisConfig := &reader.RedisStreamConfig{
        Addr:          "localhost:6379",
        Password:      "",
        DB:            0,
        Stream:        "data-sync-stream",
        ConsumerGroup: "gsyncx-sync-group",
        ConsumerName:  "gsyncx-consumer-1",
        Count:         100,
        Block:         5 * time.Second,
        BatchSize:     100,
        AutoCreate:    true,
        StartID:       "0",
    }

    redisReader, err := reader.NewRedisStreamReader(redisConfig, logger)
    if err != nil {
        panic(err)
    }
    defer redisReader.Close()

    // 2. 配置字段映射
    fieldMapping := map[string]string{
        "user_id":  "id",
        "username": "name",
    }
    transformer := transform.NewRedisMessageTransformer(fieldMapping, logger)

    // 3. 配置目标数据库 Writer
    targetDS, _ := datasource.NewGdbxDataSource(gsyncx.DSNConfig{
        DBType:   gsyncx.DBMySQL,
        Host:     "localhost",
        Port:     3306,
        User:     "root",
        Password: "password",
        Schema:   "target_db",
    })
    dbWriter := writer.NewDatabaseWriter(targetDS, logger)

    // 4. 创建同步引擎并运行
    cfg := gsyncx.NewSyncConfig(
        gsyncx.WithSyncMode(gsyncx.SyncModeRealtime),
        gsyncx.WithBatchSize(100),
    )

    eng, _ := engine.NewSyncEngine(cfg,
        engine.WithReader(redisReader),
        engine.WithWriter(dbWriter),
        engine.WithTransformer(transformer),
        engine.WithLogger(logger),
    )

    result, err := eng.Run(context.Background())
    if err != nil {
        panic(err)
    }

    fmt.Printf("Realtime sync: status=%s read=%d written=%d\n",
        result.Status, result.TotalRead, result.TotalWritten)
}
```

#### RedisStreamConfig 配置

| 参数 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `Addr` | string | Redis 服务器地址 | `"localhost:6379"` |
| `Password` | string | Redis 密码 | `""` |
| `DB` | int | Redis 数据库编号 | `0` |
| `Stream` | string | Stream 名称（必填） | - |
| `ConsumerGroup` | string | 消费者组名称 | `"gsyncx-consumer-group"` |
| `ConsumerName` | string | 消费者名称 | `"gsyncx-consumer-{随机}"` |
| `Count` | int64 | 每次读取消息数 | `100` |
| `Block` | time.Duration | 阻塞等待时间 | `5s` |
| `BatchSize` | int | 批次大小 | `100` |
| `AutoCreate` | bool | 自动创建 Stream 和 Group | `false` |
| `StartID` | string | 起始消息 ID | `"0"` |

#### 消息格式

Redis Stream 消息支持两种格式：

**1. 扁平键值格式**

```
XADD data-sync-stream * user_id 42 username alice email alice@example.com
```

读取后自动转换为 Record：
```go
Record{
    Data: map[string]interface{}{
        "_stream_id": "1234567890-0",
        "user_id":    "42",
        "username":   "alice",
        "email":      "alice@example.com",
    },
}
```

**2. Payload JSON 格式**

```
XADD data-sync-stream * payload '{"order_id":"ORD-001","amount":99.99}'
```

`payload` 字段会自动解析为 JSON，展开到 Record 中：
```go
Record{
    Data: map[string]interface{}{
        "_stream_id": "1234567890-0",
        "order_id":   "ORD-001",
        "amount":     99.99,
    },
}
```

#### 消息确认与 Pending 处理

```go
// 读取消息后确认
err := redisReader.Ack(ctx, "1234567890-0", "1234567891-0")

// 查看 Pending 消息
pending, _ := redisReader.Pending(ctx)

// 认领 Pending 消息（处理超时未确认的消息）
messages, _ := redisReader.Claim(ctx, 5*time.Minute, "1234567890-0")
```

#### 同步状态监控

通过 `RedisStreamReader` 的方法直接监控同步状态：

```go
// 获取 Stream 长度
count, _ := redisReader.Count(ctx, &gsyncx.SyncConfig{})
fmt.Printf("Stream length: %d\n", count)

// 获取 Pending 消息信息
pending, _ := redisReader.Pending(ctx)
fmt.Printf("Pending: %d\n", pending.Count)

// 确认已处理的消息
redisReader.Ack(ctx, "1234567890-0")

// 认领 Pending 消息（处理超时消息）
messages, _ := redisReader.Claim(ctx, 5*time.Minute, "1234567890-0")
```

#### 使用已有 Redis 客户端

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

reader, _ := reader.NewRedisStreamReaderWithClient(client, &reader.RedisStreamConfig{
    Stream:        "my-stream",
    ConsumerGroup: "my-group",
    AutoCreate:    true,
}, logger)
```

### 数据完整性校验

全量同步后可自动校验数据完整性：

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithSyncMode(gsyncx.SyncModeFull),
    gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckCount),
    // ... 其他配置
)

result, _ := engine.RunSync(ctx, cfg, nil)
if result.IntegrityResult != nil {
    fmt.Printf("Integrity: passed=%v source=%d target=%d\n",
        result.IntegrityResult.Passed,
        result.IntegrityResult.SourceCount,
        result.IntegrityResult.TargetCount,
    )
}
```

校验模式：
| 模式 | 说明 |
|------|------|
| `IntegrityCheckNone` | 不校验（默认） |
| `IntegrityCheckCount` | 比较源和目标记录数 |
| `IntegrityCheckChecksum` | 比较校验和 |
| `IntegrityCheckFull` | 完整校验（计数 + 校验和） |

### 智能字段映射

当源字段和目标字段名称一致时，自动建立映射关系：

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithAutoMapping(true),
    gsyncx.WithMappingConfig(gsyncx.MappingConfig{
        Mappings: []gsyncx.FieldMapping{
            {SourceField: "src_name", TargetField: "name", Transform: "trim"},
            {SourceField: "src_email", TargetField: "email", Transform: "to_lower"},
        },
        AutoMapping: true,
    }),
)
```

显式映射优先，未显式映射的同名字段自动映射。上例中 `src_name → name` 和 `src_email → email` 是显式映射，而 `id → id`、`age → age` 等同名字段自动映射。

#### 构建智能映射

```go
sourceFields := []string{"id", "name", "email", "age"}
targetFields := []string{"id", "name", "phone", "email"}

// 自动映射：仅匹配同名字段
autoMappings := mapping.BuildAutoMappings(sourceFields, targetFields)
// 结果: id→id, name→name, email→email

// 智能映射：已有显式映射 + 自动补充同名字段
existing := []gsyncx.FieldMapping{
    {SourceField: "name", TargetField: "full_name"},
}
smartMappings := mapping.BuildSmartMappings(sourceFields, targetFields, existing)
// 结果: name→full_name (显式), id→id (自动), email→email (自动)
```

### 映射调试与报告

```go
engine := mapping.NewFieldMappingEngineWithMappings(mappings, nil)
debugger := mapping.NewMappingDebugger(engine, nil)

// 调试单条记录的映射过程
debugResult, _ := debugger.DebugRecord(record)
for _, step := range debugResult.Steps {
    fmt.Printf("%s → %s: status=%s\n", step.SourceField, step.TargetField, step.Status)
}

// 生成映射报告
report := debugger.GenerateMappingReport(sourceFields, targetFields)
fmt.Printf("Matched: %d, Unmapped source: %v, Unmapped target: %v\n",
    len(report.MatchedFields), report.UnmappedSource, report.UnmappedTarget)
```

### 自定义 SQL 读取

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
        DSNConfig: &gsyncx.DSNConfig{ /* ... */ },
        SQL:       "SELECT id, name, email FROM users WHERE status = 'active'",
    }),
    // ... WriterConfig
)
```

## 数据库连接配置

### DSNConfig

DSNConfig 用于配置数据库连接信息，支持 MySQL、PostgreSQL、Oracle：

```go
dsn := gsyncx.DSNConfig{
    DBType:   gsyncx.DBMySQL,     // 或 gsyncx.DBPostgres / gsyncx.DBOracle
    Host:     "localhost",
    Port:     3306,
    User:     "root",
    Password: "password",
    Schema:   "mydb",
    MaxIdle:  5,                   // 最大空闲连接数
    MaxOpen:  20,                  // 最大打开连接数
}

// 生成 DSN 连接字符串
connectionString := dsn.BuildDSN()
```

### 独立数据源配置

Reader 和 Writer 可以使用独立的数据源配置：

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
        DSNConfig: &gsyncx.DSNConfig{
            DBType:   gsyncx.DBPostgres,
            Host:     "pg-source.example.com",
            Port:     5432,
            User:     "reader",
            Password: "reader_pass",
            Schema:   "source_db",
        },
        TableName: "source_table",
    }),
    gsyncx.WithWriterConfig(gsyncx.WriterConfig{
        DSNConfig: &gsyncx.DSNConfig{
            DBType:   gsyncx.DBMySQL,
            Host:     "mysql-target.example.com",
            Port:     3306,
            User:     "writer",
            Password: "writer_pass",
            Schema:   "target_db",
        },
        TableName: "target_table",
        WriteMode: gsyncx.WriteModeUpsert,
    }),
)
```

## 日志系统

### 日志接口

gsyncx 定义了统一的 `SyncLogger` 接口：

```go
type SyncLogger interface {
    Info(msg string, fields ...LogField)
    Warn(msg string, fields ...LogField)
    Error(msg string, fields ...LogField)
    Debug(msg string, fields ...LogField)
}
```

### 内置日志实现

| 实现 | 说明 |
|------|------|
| `NewSyncLogger()` | 开发环境日志（默认） |
| `NewProductionSyncLogger()` | 生产环境日志 |
| `NewNopLogger()` | 空日志，不输出任何内容 |
| `NewFuncLogger(...)` | 函数式日志，每个级别可自定义处理函数 |
| `NewSyncLoggerWithX2Slog(l)` | 使用已有的 x2slog.Logger 实例 |

### 日志注入方式

**方式一：通过 WithLogger 选项注入**

```go
logger := gsyncx.NewProductionSyncLogger()
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithLogger(logger),
)
```

**方式二：通过 SetDefaultLogger 全局注入**

```go
gsyncx.SetDefaultLogger(gsyncx.NewProductionSyncLogger())
// 之后所有未指定 logger 的组件都使用此默认实例
```

**方式三：通过 ResolveLogger 按优先级选择**

```go
// 优先使用 customLogger，如果为 nil 则使用默认日志
logger := gsyncx.ResolveLogger(customLogger)
```

**方式四：使用 FuncLogger 自定义处理**

```go
logger := gsyncx.NewFuncLogger(
    func(msg string, fields ...gsyncx.LogField) {
        log.Printf("[INFO] %s %v", msg, fields)
    },
    func(msg string, fields ...gsyncx.LogField) {
        log.Printf("[WARN] %s %v", msg, fields)
    },
    func(msg string, fields ...gsyncx.LogField) {
        log.Printf("[ERROR] %s %v", msg, fields)
    },
    func(msg string, fields ...gsyncx.LogField) {
        log.Printf("[DEBUG] %s %v", msg, fields)
    },
)
```

### LogField 快捷创建

使用 `F()` 函数快速创建日志字段：

```go
logger.Info("sync started",
    gsyncx.F("table", "users"),
    gsyncx.F("batch_size", 1000),
)
```

## API 接口文档

### 核心接口

#### Reader

```go
type Reader interface {
    Read(ctx context.Context, cfg *SyncConfig) (<-chan []Record, <-chan error)
    Count(ctx context.Context, cfg *SyncConfig) (int64, error)
    GetSplitKeys(ctx context.Context, cfg *SyncConfig) ([]SplitKey, error)
}
```

| 方法 | 说明 |
|------|------|
| `Read` | 流式读取数据，返回记录通道和错误通道 |
| `Count` | 获取总记录数 |
| `GetSplitKeys` | 获取分片键，用于并行读取 |

#### Writer

```go
type Writer interface {
    Write(ctx context.Context, records []Record) (WriteResult, error)
    WriteWithMode(ctx context.Context, records []Record, mode WriteMode) (WriteResult, error)
    Flush(ctx context.Context) error
    Close() error
}
```

| 方法 | 说明 |
|------|------|
| `Write` | 使用默认模式（Upsert）写入记录 |
| `WriteWithMode` | 指定写入模式写入记录 |
| `Flush` | 刷新缓冲区 |
| `Close` | 关闭写入器 |

#### Transformer

```go
type Transformer interface {
    Transform(ctx context.Context, records []Record) ([]Record, []FailedRecord, error)
}
```

#### Mapper

```go
type Mapper interface {
    Map(records []Record) ([]Record, []FailedRecord, error)
}
```

#### IntegrityChecker

```go
type IntegrityChecker interface {
    Check(ctx context.Context, cfg *SyncConfig) (*IntegrityResult, error)
}
```

#### CheckpointStore

```go
type CheckpointStore interface {
    Save(ctx context.Context, cp *Checkpoint) error
    Load(ctx context.Context, tableName string) (*Checkpoint, error)
    Delete(ctx context.Context, tableName string) error
    SaveProgress(ctx context.Context, progress *SyncProgress) error
}
```

### 配置选项

| 选项函数 | 说明 | 默认值 |
|----------|------|--------|
| `WithSyncMode(mode)` | 同步模式：full/incremental/realtime | `full` |
| `WithBatchSize(size)` | 批次大小 | `1000` |
| `WithParallelism(n)` | 并行度 | `4` |
| `WithCheckpoint(enabled, path)` | 启用断点续传 | `false, ""` |
| `WithIncrementalField(field, strategy)` | 增量字段和策略 | - |
| `WithIncrementalCondition(condition)` | 自定义增量条件 | - |
| `WithLastSyncTime(t)` | 上次同步时间 | - |
| `WithLastSyncValue(v)` | 上次同步值 | - |
| `WithReaderConfig(cfg)` | 读取器配置 | - |
| `WithWriterConfig(cfg)` | 写入器配置 | - |
| `WithTransformConfig(cfg)` | 转换配置 | - |
| `WithMappingConfig(cfg)` | 映射配置 | - |
| `WithRetry(maxAttempts, delay)` | 重试策略 | `3, 1s` |
| `WithContinueOnError(bool)` | 出错继续 | `true` |
| `WithPreviewMode(limit)` | 预览模式 | `false, 0` |
| `WithErrorThreshold(threshold)` | 错误阈值 | `0` |
| `WithIntegrityCheck(mode)` | 完整性校验模式 | `none` |
| `WithAutoMapping(enabled)` | 启用智能映射 | `false` |
| `WithLogger(logger)` | 日志实例 | 默认开发日志 |

### 写入模式

| 模式 | 常量 | 说明 |
|------|------|------|
| Insert | `WriteModeInsert` | 仅插入 |
| Update | `WriteModeUpdate` | 仅更新 |
| Upsert | `WriteModeUpsert` | 插入或更新（默认） |

### 增量策略

| 策略 | 常量 | 说明 |
|------|------|------|
| Timestamp | `StrategyTimestamp` | 基于时间戳字段 |
| AutoInc | `StrategyAutoInc` | 基于自增ID字段 |
| Version | `StrategyVersion` | 基于版本号字段 |
| Custom | `StrategyCustom` | 自定义增量条件 |

### 任务模块 API

#### 配置加载与解析

| 函数 | 说明 |
|------|------|
| `LoadTaskConfig(path)` | 从文件加载任务配置 |
| `ParseTaskConfig(data)` | 从字节解析任务配置 |
| `cfg.Validate()` | 验证配置合法性 |
| `cfg.ToJSON()` | 导出配置为 JSON |

#### TaskExecutor

| 方法 | 说明 |
|------|------|
| `NewTaskExecutor(cfg, opts...)` | 创建任务执行器 |
| `Execute(ctx)` | 执行同步任务，返回 TaskResult |
| `Stop()` | 停止任务 |
| `GetStatus()` | 获取任务状态 |
| `GetResult()` | 获取执行结果 |
| `RegisterReader(type, factory)` | 注册自定义 Reader 工厂 |
| `RegisterWriter(type, factory)` | 注册自定义 Writer 工厂 |
| `RegisterTransformer(type, factory)` | 注册自定义 Transformer 工厂 |
| `RegisterMapper(type, factory)` | 注册自定义 Mapper 工厂 |

#### TaskExecutor 选项

| 选项函数 | 说明 |
|----------|------|
| `WithTaskLogger(logger)` | 设置日志实例 |
| `WithTaskConfigPath(path)` | 设置配置文件路径 |
| `WithReaderFactory(type, factory)` | 注册 Reader 工厂 |
| `WithWriterFactory(type, factory)` | 注册 Writer 工厂 |
| `WithTransformerFactory(type, factory)` | 注册 Transformer 工厂 |
| `WithMapperFactory(type, factory)` | 注册 Mapper 工厂 |
| `WithTaskHook(point, fn)` | 添加 Hook 函数 |

#### 便捷函数

| 函数 | 说明 |
|------|------|
| `ExecuteTask(ctx, path, opts...)` | 从配置文件执行任务 |
| `ExecuteTaskFromBytes(ctx, data, opts...)` | 从 JSON 字节执行任务 |

#### 任务状态

| 状态 | 常量 | 说明 |
|------|------|------|
| Pending | `TaskStatusPending` | 待执行 |
| Running | `TaskStatusRunning` | 执行中 |
| Completed | `TaskStatusCompleted` | 执行完成 |
| Failed | `TaskStatusFailed` | 执行失败 |
| Cancelled | `TaskStatusCancelled` | 已取消 |

### Reader/Writer 工厂注册 API

| 函数 | 说明 |
|------|------|
| `reader.RegisterReader(type, factory)` | 注册自定义 Reader 工厂 |
| `reader.CreateReader(type, config, logger)` | 通过工厂创建 Reader |
| `reader.RegisteredReaderTypes()` | 获取已注册的 Reader 类型列表 |
| `writer.RegisterWriter(type, factory)` | 注册自定义 Writer 工厂 |
| `writer.CreateWriter(type, config, logger)` | 通过工厂创建 Writer |
| `writer.RegisteredWriterTypes()` | 获取已注册的 Writer 类型列表 |

### Reader/Writer 类型常量

| 常量 | 值 | 说明 |
|------|------|------|
| `ReaderTypeDatabase` | `"database"` | 数据库 Reader |
| `ReaderTypeSQL` | `"sql"` | SQL Reader |
| `ReaderTypeRedisStream` | `"redis_stream"` | Redis Stream Reader |

### Reader 工厂注册

所有 Reader 实现均通过 `ReaderRegistry` 工厂模式注册，支持通过配置动态创建：

```go
// 通过工厂创建 Reader
r, err := reader.GlobalReaderRegistry().Create(gsyncx.ReaderTypeRedisStream, config, logger)
```

| Reader 类型 | 注册键 | 实现类 | 配置类型 |
|-------------|--------|--------|---------|
| Database Reader | `database` | `reader.DatabaseReader` | `*datasource.GdbxDataSource` |
| SQL Reader | `sql` | `reader.SQLReader` | `*datasource.GdbxDataSource` |
| Redis Stream Reader | `redis_stream` | `reader.RedisStreamReader` | `*reader.RedisStreamConfig` |
| `WriterTypeDatabase` | `"database"` | 数据库 Writer |

### Hook 点

| Hook 点 | 常量 | 触发时机 |
|---------|------|----------|
| BeforeRead | `HookBeforeRead` | 读取前 |
| AfterRead | `HookAfterRead` | 读取后 |
| BeforeTransform | `HookBeforeTransform` | 转换前 |
| AfterTransform | `HookAfterTransform` | 转换后 |
| BeforeMap | `HookBeforeMap` | 映射前 |
| AfterMap | `HookAfterMap` | 映射后 |
| BeforeWrite | `HookBeforeWrite` | 写入前 |
| AfterWrite | `HookAfterWrite` | 写入后 |
| OnError | `HookOnError` | 发生错误时 |
| OnSkip | `HookOnSkip` | 跳过记录时 |
| OnRetry | `HookOnRetry` | 重试时 |
| OnComplete | `HookOnComplete` | 同步完成时 |

### Hook 使用示例

```go
eng, _ := engine.NewSyncEngine(cfg,
    engine.WithHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
        fmt.Printf("Read %d records\n", len(hctx.Records))
        return nil
    }),
    engine.WithHook(gsyncx.HookOnComplete, func(ctx context.Context, hctx *gsyncx.HookContext) error {
        fmt.Printf("Sync completed: %s\n", hctx.Progress.Status)
        return nil
    }),
)
```

## 项目结构

```
gsyncx/
├── gsyncx.go          # 核心入口：NewSyncConfig、默认日志管理、ResolveLogger
├── option.go          # 所有配置选项函数（With*）
├── types.go           # 核心类型定义和接口
├── config.go          # gdbx 类型别名和工具函数
├── logger.go          # 日志系统：SyncLogger、NopLogger、FuncLogger
├── incremental.go     # 增量同步辅助器（含自定义条件）
├── checkpoint/        # 断点续传存储
│   └── checkpoint.go  # FileCheckpointStore、MemoryCheckpointStore
├── datasource/        # 数据源封装
│   └── gdbx.go        # GdbxDataSource
├── engine/            # 同步引擎
│   ├── sync.go        # SyncEngine、EngineOption、RunSync、模式切换
│   └── validate.go    # ConfigValidator
├── mapping/           # 字段映射
│   └── mapping.go     # FieldMappingEngine、智能映射、MappingDebugger、MappingReport
├── performance/       # 性能优化
│   └── optimizer.go   # PerformanceOptimizer、ShardManager
├── reader/            # 数据读取模块
│   ├── database.go    # DatabaseReader、SQLReader、ReaderRegistry
│   └── redis_stream.go # RedisStreamReader、RedisStreamConfig
├── sqlparser/         # SQL 解析
│   └── parser.go      # SQLParser
├── task/              # 任务模块
│   ├── config.go      # TaskConfig、配置解析、验证、转换
│   └── executor.go    # TaskExecutor、工厂注册、任务执行
├── transform/         # 数据转换
│   ├── transformer.go # DefaultTransformer、ScriptManager、ScriptTransformer
│   └── lua.go         # LuaEngine
├── validator/         # 数据验证
│   └── validator.go   # DataValidator
└── examples/          # 使用示例
    ├── basic_sync/        # 基本全量同步
    ├── incremental_sync/  # 增量同步
    ├── custom_incremental/ # 自定义增量条件
    ├── custom_transform/  # Lua 脚本转换
    ├── field_mapping/     # 字段映射
    ├── smart_mapping/     # 智能映射
    ├── custom_logger/     # 自定义日志
    ├── mode_switching/    # 同步模式切换
    ├── redis_stream_sync/ # Redis Stream 实时同步
    ├── task_configs/      # 任务配置文件示例
    ├── task_module/       # 任务模块完整示例
    └── checkpoint/        # 断点续传
```

## 任务模块

任务模块支持通过 JSON 配置文件定义和执行数据同步任务，配置格式参考 DataX 规范。

### 基本使用

**从配置文件执行任务：**

```go
result, err := task.ExecuteTask(ctx, "config.json")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Status: %s, Read: %d, Written: %d\n",
    result.Status, result.SyncResult.TotalRead, result.SyncResult.TotalWritten)
```

**从 JSON 字节执行任务：**

```go
data, _ := os.ReadFile("config.json")
result, err := task.ExecuteTaskFromBytes(ctx, data)
```

**使用 TaskExecutor 精细控制：**

```go
cfg, _ := task.LoadTaskConfig("config.json")
executor := task.NewTaskExecutor(cfg,
    task.WithTaskLogger(logger),
    task.WithTaskConfigPath("config.json"),
)

result, err := executor.Execute(ctx)

// 查询状态
status := executor.GetStatus()
result := executor.GetResult()

// 停止任务
executor.Stop()
```

### 配置文件格式

配置文件采用 JSON 格式，包含以下核心配置项：

```json
{
  "job_id": "sync-001",
  "job_name": "数据同步任务",
  "version": "1.0.0",
  "reader": {
    "type": "database",
    "dsn_config": {
      "db_type": "mysql",
      "host": "localhost",
      "port": 3306,
      "user": "root",
      "password": "password",
      "schema": "source_db"
    },
    "table_name": "users",
    "primary_key": {"field_name": "id"},
    "where_clause": "status = 'active'"
  },
  "transform": {
    "type": "script",
    "script": "function transform(records) return records end",
    "script_lang": "lua"
  },
  "mapping": {
    "mappings": [
      {"source_field": "user_name", "target_field": "name", "transform": "trim"},
      {"source_field": "email", "target_field": "email", "transform": "to_lower"}
    ],
    "auto_mapping": true,
    "ignore_missing": true
  },
  "writer": {
    "type": "database",
    "dsn_config": {
      "db_type": "mysql",
      "host": "localhost",
      "port": 3306,
      "user": "root",
      "password": "password",
      "schema": "target_db"
    },
    "table_name": "users",
    "write_mode": "upsert",
    "primary_key": {"field_name": "id"}
  },
  "setting": {
    "sync_mode": "full",
    "batch_size": 1000,
    "parallelism": 4,
    "retry_max_attempts": 3,
    "continue_on_error": true,
    "integrity_check": "count"
  }
}
```

### 配置参数详解

#### Reader 配置

| 字段 | 类型 | 说明 | 必填 |
|------|------|------|------|
| `type` | string | 读取器类型：`database`/`sql`/`redis_stream` | 是 |
| `dsn_config` | object | 数据源连接配置（database/sql 类型必填） | 否 |
| `table_name` | string | 表名（database 类型必填） | 否 |
| `sql` | string | SQL 查询（sql 类型必填） | 否 |
| `primary_key` | object | 主键字段配置 | 否 |
| `where_clause` | string | WHERE 条件 | 否 |
| `fields` | array | 字段列表 | 否 |
| `redis` | object | Redis Stream 配置（redis_stream 类型必填） | 否 |

#### Writer 配置

| 字段 | 类型 | 说明 | 必填 |
|------|------|------|------|
| `type` | string | 写入器类型：`database` | 是 |
| `dsn_config` | object | 数据源连接配置（database 类型必填） | 是 |
| `table_name` | string | 目标表名 | 是 |
| `write_mode` | string | 写入模式：`insert`/`update`/`upsert` | 否 |
| `primary_key` | object | 主键字段配置 | 否 |
| `batch_size` | int | 批次大小 | 否 |
| `use_transaction` | bool | 使用事务 | 否 |

#### Transform 配置

| 字段 | 类型 | 说明 | 必填 |
|------|------|------|------|
| `type` | string | 转换类型：`default`/`script`/`redis_message` | 否 |
| `script` | string | 脚本内容 | 否 |
| `script_path` | string | 脚本文件路径 | 否 |
| `script_lang` | string | 脚本语言：`lua`/`javascript` | 否 |
| `field_mapping` | map | 字段映射（redis_message 类型使用） | 否 |

#### Mapping 配置

| 字段 | 类型 | 说明 | 必填 |
|------|------|------|------|
| `mappings` | array | 字段映射规则列表 | 否 |
| `mappings[].source_field` | string | 源字段名 | 是 |
| `mappings[].target_field` | string | 目标字段名 | 是 |
| `mappings[].transform` | string | 内置转换函数 | 否 |
| `mappings[].default` | any | 默认值 | 否 |
| `mappings[].required` | bool | 是否必填 | 否 |
| `auto_mapping` | bool | 启用自动映射 | 否 |
| `ignore_missing` | bool | 忽略缺失字段 | 否 |
| `strict_mode` | bool | 严格模式 | 否 |
| `default_values` | map | 默认值映射 | 否 |

#### Setting 配置

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `sync_mode` | string | 同步模式：full/incremental/realtime | full |
| `batch_size` | int | 批次大小 | 1000 |
| `parallelism` | int | 并行度 | 4 |
| `retry_max_attempts` | int | 重试次数 | 3 |
| `retry_delay` | duration | 重试间隔 | 1s |
| `continue_on_error` | bool | 出错继续 | true |
| `error_threshold` | int | 错误阈值 | 0 |
| `preview_mode` | bool | 预览模式 | false |
| `integrity_check` | string | 完整性校验：none/count/checksum | none |
| `checkpoint_enabled` | bool | 启用断点续传 | false |
| `checkpoint_path` | string | 断点存储路径 | "" |
| `incremental_field` | object | 增量字段配置 | - |
| `last_sync_time` | string | 上次同步时间（RFC3339） | - |

### 可扩展架构

任务模块支持注册自定义的 Reader、Writer、Transformer 和 Mapper 工厂：

```go
executor := task.NewTaskExecutor(cfg)

// 注册自定义 Reader
executor.RegisterReader("kafka", func(cfg *task.TaskConfig) (gsyncx.Reader, error) {
    return NewKafkaReader(cfg.Reader.Args), nil
})

// 注册自定义 Writer
executor.RegisterWriter("elasticsearch", func(cfg *task.TaskConfig) (gsyncx.Writer, error) {
    return NewESWriter(cfg.Writer.Args), nil
})

// 注册自定义 Transformer
executor.RegisterTransformer("protobuf", func(cfg *task.TaskConfig) (gsyncx.Transformer, error) {
    return NewProtobufTransformer(), nil
})

// 也可以在创建时通过选项注册
executor := task.NewTaskExecutor(cfg,
    task.WithReaderFactory("kafka", kafkaReaderFactory),
    task.WithWriterFactory("elasticsearch", esWriterFactory),
)
```

### 任务状态监控

```go
executor := task.NewTaskExecutor(cfg)

// 获取当前状态
status := executor.GetStatus()  // pending / running / completed / failed / cancelled

// 获取执行结果
result := executor.GetResult()
// result.JobID, result.JobName, result.Status
// result.SyncResult.TotalRead, result.SyncResult.TotalWritten
// result.Error (失败时的错误信息)
// result.StartTime, result.EndTime, result.Duration

// 停止任务
executor.Stop()
```

### 任务执行结果

```go
type TaskResult struct {
    JobID      string             // 任务ID
    JobName    string             // 任务名称
    Status     TaskStatus         // 任务状态
    SyncResult *gsyncx.SyncResult // 同步结果详情
    Error      string             // 错误信息
    StartTime  time.Time          // 开始时间
    EndTime    time.Time          // 结束时间
    Duration   time.Duration      // 执行耗时
    ConfigPath string             // 配置文件路径
}
```

### 配置验证

任务模块在执行前自动验证配置的合法性：

- `job_id` 和 `job_name` 必填
- Reader 类型必须指定，且对应类型的必填字段完整
- Writer 类型必须指定，且对应类型的必填字段完整
- Transform 和 Mapping 配置（如果提供）也会验证

```go
cfg, _ := task.LoadTaskConfig("config.json")
if err := cfg.Validate(); err != nil {
    log.Fatalf("配置验证失败: %v", err)
}
```

### 示例配置文件

项目提供了 3 个典型场景的配置文件：

| 场景 | 文件 | 说明 |
|------|------|------|
| 全量同步 | `examples/task_configs/mysql_full_sync.json` | MySQL 全量同步 + 字段映射 |
| 增量同步 | `examples/task_configs/mysql_incremental_sync.json` | MySQL 增量同步 + Lua 转换 + 断点续传 |
| 实时同步 | `examples/task_configs/redis_realtime_sync.json` | Redis Stream 实时同步 + 消息转换 |

## 架构设计

### Reader/Writer 抽象层

Reader 和 Writer 模块采用接口抽象 + 工厂注册的架构设计，遵循依赖倒置原则（DIP），支持灵活扩展。

```
┌──────────────────────────────────────────────────────────────────┐
│                        抽象接口层                                 │
│  ┌──────────────────┐              ┌──────────────────┐          │
│  │  gsyncx.Reader   │              │  gsyncx.Writer   │          │
│  │  Read/Count/     │              │  Write/WriteWith │          │
│  │  GetSplitKeys/   │              │  Mode/Flush/     │          │
│  │  Close           │              │  Close           │          │
│  └────────┬─────────┘              └────────┬─────────┘          │
│           │                                 │                    │
│  ┌────────┴─────────┐              ┌────────┴─────────┐          │
│  │  TypedReader     │              │  TypedWriter     │          │
│  │  +SourceType()   │              │  +TargetType()   │          │
│  └──────────────────┘              └──────────────────┘          │
│                                                                  │
│  ┌──────────────────┐              ┌──────────────────┐          │
│  │IncrementalReader │              │  (可选能力接口)   │          │
│  │ReadWithIncremental│             │                  │          │
│  └──────────────────┘              └──────────────────┘          │
├──────────────────────────────────────────────────────────────────┤
│                        工厂注册中心                               │
│  ┌──────────────────┐              ┌──────────────────┐          │
│  │ ReaderRegistry   │              │ WriterRegistry   │          │
│  │ Register/Create/ │              │ Register/Create/ │          │
│  │ Has/Types        │              │ Has/Types        │          │
│  └──────────────────┘              └──────────────────┘          │
├──────────────────────────────────────────────────────────────────┤
│                        具体实现层                                 │
│  ┌────────────┐ ┌─────────┐ ┌───────────┐  ┌────────────┐      │
│  │ Database   │ │ SQL     │ │ Redis     │  │ Database   │      │
│  │ Reader     │ │ Reader  │ │ Stream    │  │ Writer     │      │
│  │ (mysql/    │ │         │ │ Reader    │  │ (mysql/    │      │
│  │  pg/oracle)│ │         │ │           │  │  pg/oracle)│      │
│  └────────────┘ └─────────┘ └───────────┘  └────────────┘      │
│                                                                  │
│  ┌────────────┐ ┌─────────┐ ┌───────────┐  ┌────────────┐      │
│  │  (未来)    │ │ (未来)  │ │ (未来)    │  │  (未来)    │      │
│  │ Kafka      │ │ HTTP    │ │ MongoDB   │  │ Elastic    │      │
│  │ Reader     │ │ Reader  │ │ Reader    │  │ Writer     │      │
│  └────────────┘ └─────────┘ └───────────┘  └────────────┘      │
└──────────────────────────────────────────────────────────────────┘
```

### 接口定义

**Reader 接口：**

```go
type Reader interface {
    Read(ctx context.Context, cfg *SyncConfig) (<-chan []Record, <-chan error)
    Count(ctx context.Context, cfg *SyncConfig) (int64, error)
    GetSplitKeys(ctx context.Context, cfg *SyncConfig) ([]SplitKey, error)
    Close() error
}

type TypedReader interface {
    Reader
    SourceType() ReaderType
}

type IncrementalReader interface {
    ReadWithIncremental(ctx context.Context, cfg *SyncConfig, field string, lastValue interface{}) (<-chan []Record, <-chan error)
}
```

**Writer 接口：**

```go
type Writer interface {
    Write(ctx context.Context, records []Record) (WriteResult, error)
    WriteWithMode(ctx context.Context, records []Record, mode WriteMode) (WriteResult, error)
    Flush(ctx context.Context) error
    Close() error
}

type TypedWriter interface {
    Writer
    TargetType() WriterType
}
```

### 扩展开发指南

#### 1. 实现自定义 Reader

```go
package kafka

import (
    "context"
    "github.com/dataixcom/gsyncx"
)

type KafkaReader struct {
    brokers []string
    topic   string
}

func (r *KafkaReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
    // 实现从 Kafka 读取消息的逻辑
}

func (r *KafkaReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
    return 0, nil
}

func (r *KafkaReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
    return nil, nil
}

func (r *KafkaReader) Close() error {
    return nil
}

func (r *KafkaReader) SourceType() gsyncx.ReaderType {
    return gsyncx.ReaderType("kafka")
}
```

#### 2. 注册自定义 Reader

```go
// 方式一：使用包级注册函数
reader.RegisterReader(gsyncx.ReaderType("kafka"), func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
    cfg := config.(*KafkaConfig)
    return NewKafkaReader(cfg.Brokers, cfg.Topic), nil
})

// 方式二：使用 TaskExecutor 的工厂注册
executor := task.NewTaskExecutor(taskCfg)
executor.RegisterReader("kafka", func(cfg *task.TaskConfig) (gsyncx.Reader, error) {
    return NewKafkaReader(cfg.Reader.Args), nil
})
```

#### 3. 使用注册中心创建 Reader

```go
rd, err := reader.CreateReader(gsyncx.ReaderType("kafka"), kafkaConfig, logger)
if err != nil {
    log.Fatal(err)
}
defer rd.Close()
```

#### 4. 实现自定义 Writer

```go
type ElasticsearchWriter struct {
    client *es.Client
    index  string
}

func (w *ElasticsearchWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
    // 实现写入 Elasticsearch 的逻辑
}

func (w *ElasticsearchWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
    return w.Write(ctx, records)
}

func (w *ElasticsearchWriter) Flush(ctx context.Context) error { return nil }
func (w *ElasticsearchWriter) Close() error                   { return nil }
func (w *ElasticsearchWriter) TargetType() gsyncx.WriterType  { return gsyncx.WriterType("elasticsearch") }
```

#### 5. 注册自定义 Writer

```go
writer.RegisterWriter(gsyncx.WriterType("elasticsearch"), func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
    cfg := config.(*ESConfig)
    return NewElasticsearchWriter(cfg), nil
})
```

### 内置 Reader/Writer 类型

| 类型 | ReaderType/WriterType | 实现类 | 说明 |
|------|----------------------|--------|------|
| Database Reader | `database` | `reader.DatabaseReader` | RDBMS 数据读取 |
| SQL Reader | `sql` | `reader.SQLReader` | 自定义 SQL 查询读取 |
| Redis Stream Reader | `redis_stream` | `reader.RedisStreamReader` | Redis Stream 实时读取 |
| Database Writer | `database` | `writer.DatabaseWriter` | RDBMS 数据写入 |

### 兼容性说明

本次重构保持了向后兼容：

- `gsyncx.Reader` 接口新增了 `Close()` 方法，所有现有实现已更新
- `gsyncx.Writer` 接口保持不变（已有 `Close()` 方法）
- 新增 `TypedReader`/`TypedWriter` 接口为可选扩展，不影响现有代码
- 新增 `ReaderRegistry`/`WriterRegistry` 为新增功能，不影响现有代码
- 所有现有示例和测试代码已更新适配

## FAQ

### Q: 如何在不实际写入的情况下预览数据？

使用预览模式，只读取和转换数据，不执行写入操作：

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithPreviewMode(100),  // 预览前 100 条记录
    // ... 其他配置
)

result, _ := engine.RunSync(ctx, cfg, nil)
for _, record := range result.PreviewData {
    fmt.Printf("%v\n", record.Data)
}
```

### Q: 如何通过配置文件执行同步任务？

使用任务模块，从 JSON 配置文件加载并执行：

```go
result, err := task.ExecuteTask(ctx, "config.json")
```

详见 [任务模块](#任务模块) 章节。

### Q: 如何注册自定义的 Reader 或 Writer？

通过 TaskExecutor 的工厂注册机制：

```go
executor := task.NewTaskExecutor(cfg)
executor.RegisterReader("kafka", func(cfg *task.TaskConfig) (gsyncx.Reader, error) {
    return NewKafkaReader(cfg.Reader.Args), nil
})
```

### Q: 如何实现自定义的 Reader 或 Writer（编程方式）？

实现对应的接口即可：

```go
type MyReader struct{}

func (r *MyReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
    // 自定义读取逻辑
}

func (r *MyReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
    // 返回总记录数
}

func (r *MyReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
    // 返回分片键
}

// 注入到引擎
eng, _ := engine.NewSyncEngine(cfg,
    engine.WithReader(&MyReader{}),
)
```

### Q: 如何禁用日志输出？

```go
gsyncx.SetDefaultLogger(gsyncx.NewNopLogger())
```

或在配置中指定：

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithLogger(gsyncx.NewNopLogger()),
)
```

### Q: 如何处理同步过程中的错误？

- `WithContinueOnError(true)`（默认）：单条记录失败不影响整批
- `WithErrorThreshold(n)`：设置错误阈值，超过则中止
- `WithRetry(maxAttempts, delay)`：配置重试策略
- `HookOnError`：通过 Hook 自定义错误处理

### Q: 如何在同步过程中暂停和恢复？

```go
eng, _ := engine.NewSyncEngine(cfg, ...)

go func() {
    eng.Run(ctx)
}()

eng.Pause()   // 暂停
eng.Resume()  // 恢复
eng.Stop()    // 停止

progress := eng.GetProgress()  // 获取进度
```

### Q: 如何在全量、增量和实时模式间切换？

```go
eng, _ := engine.NewSyncEngine(cfg, ...)

// 切换到增量模式
eng.SwitchToIncrementalSync(&gsyncx.Field{FieldName: "id"}, gsyncx.StrategyAutoInc)

// 切换到实时模式（需配合 Redis Stream Reader）
eng.SwitchToRealtimeSync()

// 切换回全量模式
eng.SwitchToFullSync()
```

### Q: 如何使用自定义增量条件？

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
    gsyncx.WithIncrementalField(&gsyncx.Field{FieldName: "updated_at"}, gsyncx.StrategyCustom),
    gsyncx.WithIncrementalCondition("{field} >= '{last_sync_time}' AND status = 'active'"),
    gsyncx.WithLastSyncTime(time.Now().Add(-24*time.Hour)),
)
```

### Q: 如何实现 Redis Stream 实时同步？

1. 配置 `RedisStreamConfig` 并创建 `RedisStreamReader`
2. 使用 `RedisMessageTransformer` 进行字段映射和转换
3. 配置目标数据库 `Writer`
4. 创建 `SyncEngine` 并以 `SyncModeRealtime` 模式运行

详见 [Redis Stream 实时同步](#redis-stream-实时同步) 章节。

### Q: 如何监控 Redis Stream 同步状态？

通过 `RedisStreamReader` 的方法直接监控：

```go
count, _ := redisReader.Count(ctx, &gsyncx.SyncConfig{})
pending, _ := redisReader.Pending(ctx)
messages, _ := redisReader.Claim(ctx, 5*time.Minute, "msg-id")
```

### Q: 如何处理 Redis Stream 中的 Pending 消息？

使用 `Claim` 方法认领超时未确认的消息：

```go
messages, _ := reader.Claim(ctx, 5*time.Minute, "message-id")
```

建议在应用启动时检查 Pending 消息并重新处理，确保数据不丢失。

### Q: 如何实现智能字段映射？

```go
cfg := gsyncx.NewSyncConfig(
    gsyncx.WithAutoMapping(true),
    gsyncx.WithMappingConfig(gsyncx.MappingConfig{
        Mappings: []gsyncx.FieldMapping{
            {SourceField: "src_name", TargetField: "name"},  // 显式映射
        },
        AutoMapping: true,  // 同名字段自动映射
    }),
)
```

### Q: Lua 脚本中可以使用哪些功能？

沙箱模式下，Lua 脚本只能使用基础字符串和表操作。脚本需要定义 `transform` 函数：

```lua
function transform(table)
    local result = {}
    for k, v in pairs(table) do
        result[k] = string.upper(v)
    end
    result["processed"] = "true"
    return result
end
```

如需完整 Lua 标准库，使用非沙箱模式：

```go
engine := transform.NewLuaEngineUnsandboxed(logger)
```

## License

MIT
