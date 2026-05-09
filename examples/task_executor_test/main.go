package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/task"
)

func main() {
	logger := gsyncx.NewSyncLogger()

	fmt.Println("========================================================")
	fmt.Println("  gsyncx Task 执行测试")
	fmt.Println("  配置文件: mysql_full_sync_user.json")
	fmt.Println("========================================================")

	configPath := findConfigFile("mysql_full_sync_user.json")
	if configPath == "" {
		fmt.Println("[FAIL] 配置文件未找到: mysql_full_sync_user.json")
		os.Exit(1)
	}
	fmt.Printf("配置文件: %s\n\n", configPath)

	cfg, err := task.LoadTaskConfig(configPath)
	if err != nil {
		fmt.Printf("[FAIL] 加载配置失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("--- 步骤1: 配置验证 ---")
	if err := cfg.Validate(); err != nil {
		fmt.Printf("[FAIL] 配置验证失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("[PASS] 配置验证通过")
	fmt.Printf("  JobID:   %s\n", cfg.JobID)
	fmt.Printf("  JobName: %s\n", cfg.JobName)
	fmt.Printf("  Reader:  %s -> %s\n", cfg.Reader.Type, cfg.Reader.TableName)
	fmt.Printf("  Writer:  %s -> %s (%s)\n", cfg.Writer.Type, cfg.Writer.TableName, cfg.Writer.WriteMode)
	if cfg.Mapping != nil {
		fmt.Printf("  Mapping: %d 字段映射\n", len(cfg.Mapping.Mappings))
	}
	fmt.Printf("  Setting: %s, batch=%d, parallel=%d\n",
		cfg.Setting.SyncMode, cfg.Setting.BatchSize, cfg.Setting.Parallelism)

	fmt.Println("\n--- 步骤2: 执行器初始化 ---")
	executor := task.NewTaskExecutor(cfg,
		task.WithTaskLogger(logger),
		task.WithTaskConfigPath(configPath),
		task.WithTaskHook(gsyncx.HookBeforeRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			fmt.Println("  [Hook] 开始读取数据...")
			return nil
		}),
		task.WithTaskHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			fmt.Printf("  [Hook] 读取完成: %d 条记录\n", len(hctx.Records))
			return nil
		}),
		task.WithTaskHook(gsyncx.HookAfterWrite, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			fmt.Printf("  [Hook] 写入完成: %d 条记录\n", len(hctx.Records))
			return nil
		}),
		task.WithTaskHook(gsyncx.HookOnComplete, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			fmt.Printf("  [Hook] 同步完成: status=%s\n", hctx.Progress.Status)
			return nil
		}),
	)

	status := executor.GetStatus()
	fmt.Printf("[PASS] 执行器初始化成功, 状态: %s\n", status)

	fmt.Println("\n--- 步骤3: 执行任务 ---")
	fmt.Printf("开始时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	result, err := executor.Execute(context.Background())

	fmt.Printf("结束时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	fmt.Println("\n--- 步骤4: 执行结果 ---")
	if err != nil {
		fmt.Printf("[FAIL] 执行失败: %v\n", err)
		if result != nil {
			printResult(result)
		}
		os.Exit(1)
	}

	fmt.Println("[PASS] 执行成功")
	printResult(result)
}

func printResult(result *task.TaskResult) {
	fmt.Printf("  JobID:     %s\n", result.JobID)
	fmt.Printf("  JobName:   %s\n", result.JobName)
	fmt.Printf("  Status:    %s\n", result.Status)
	fmt.Printf("  Duration:  %v\n", result.Duration)
	if result.SyncResult != nil {
		fmt.Printf("  读取总数:  %d\n", result.SyncResult.TotalRead)
		fmt.Printf("  写入成功:  %d\n", result.SyncResult.TotalWritten)
		fmt.Printf("  写入失败:  %d\n", result.SyncResult.TotalFailed)
		fmt.Printf("  跳过数量:  %d\n", result.SyncResult.TotalSkipped)
	}
}

func findConfigFile(name string) string {
	_, thisFile, _, _ := runtime.Caller(0)
	examplesDir := filepath.Dir(filepath.Dir(thisFile))
	configPath := filepath.Join(examplesDir, "task_configs", name)
	if _, err := os.Stat(configPath); err == nil {
		return configPath
	}
	return ""
}
