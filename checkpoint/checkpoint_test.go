package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dataixcom/gsyncx"
)

func TestNewFileCheckpointStore(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Error("expected non-nil store")
	}
}

func TestNewFileCheckpointStore_CreateDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "checkpoint")
	store, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatalf("unexpected error creating nested dir: %v", err)
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Error("expected directory to be created")
	}
	_ = store
}

func TestFileCheckpointStore_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	cp := &gsyncx.Checkpoint{
		TableName:    "users",
		FieldName:    "updated_at",
		LastValue:    "2024-01-01",
		LastSyncTime: time.Now(),
		BatchNum:     5,
		BatchOffset:  10,
	}

	err := store.Save(ctx, cp)
	if err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	loaded, err := store.Load(ctx, "users")
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil checkpoint")
	}
	if loaded.TableName != "users" {
		t.Errorf("expected table users, got %s", loaded.TableName)
	}
	if loaded.FieldName != "updated_at" {
		t.Errorf("expected field updated_at, got %s", loaded.FieldName)
	}
	if loaded.LastValue != "2024-01-01" {
		t.Errorf("expected last value 2024-01-01, got %v", loaded.LastValue)
	}
	if loaded.BatchNum != 5 {
		t.Errorf("expected batch num 5, got %d", loaded.BatchNum)
	}
	if loaded.BatchOffset != 10 {
		t.Errorf("expected batch offset 10, got %d", loaded.BatchOffset)
	}
}

func TestFileCheckpointStore_LoadNotFound(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	loaded, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil for nonexistent checkpoint")
	}
}

func TestFileCheckpointStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	cp := &gsyncx.Checkpoint{TableName: "users", FieldName: "id"}
	_ = store.Save(ctx, cp)

	err := store.Delete(ctx, "users")
	if err != nil {
		t.Fatalf("failed to delete checkpoint: %v", err)
	}

	loaded, _ := store.Load(ctx, "users")
	if loaded != nil {
		t.Error("expected nil after delete")
	}
}

func TestFileCheckpointStore_DeleteNonExistent(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	err := store.Delete(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("should not error on deleting nonexistent: %v", err)
	}
}

func TestFileCheckpointStore_SaveProgress(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	progress := &gsyncx.SyncProgress{
		Status:        gsyncx.StatusRunning,
		TotalRecords:  1000,
		SyncedRecords: 500,
	}

	err := store.SaveProgress(ctx, progress)
	if err != nil {
		t.Fatalf("failed to save progress: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "progress.json"))
	if err != nil {
		t.Fatalf("failed to read progress file: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty progress file")
	}
}

func TestFileCheckpointStore_Overwrite(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	cp1 := &gsyncx.Checkpoint{TableName: "users", FieldName: "id", LastValue: "100"}
	_ = store.Save(ctx, cp1)

	cp2 := &gsyncx.Checkpoint{TableName: "users", FieldName: "id", LastValue: "200"}
	_ = store.Save(ctx, cp2)

	loaded, _ := store.Load(ctx, "users")
	if loaded.LastValue != "200" {
		t.Errorf("expected last value 200 after overwrite, got %v", loaded.LastValue)
	}
}

func TestFileCheckpointStore_SpecialCharsInTableName(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileCheckpointStore(dir)
	ctx := context.Background()

	cp := &gsyncx.Checkpoint{TableName: "schema/table:name", FieldName: "id"}
	err := store.Save(ctx, cp)
	if err != nil {
		t.Fatalf("failed to save checkpoint with special chars: %v", err)
	}

	loaded, err := store.Load(ctx, "schema/table:name")
	if err != nil {
		t.Fatalf("failed to load checkpoint with special chars: %v", err)
	}
	if loaded == nil {
		t.Error("expected non-nil checkpoint")
	}
}

func TestSanitizeFileName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with/slash", "with_slash"},
		{"with\\backslash", "with_backslash"},
		{"with:colon", "with_colon"},
		{"with*asterisk", "with_asterisk"},
		{"with?question", "with_question"},
		{"with\"quote", "with_quote"},
		{"with<less", "with_less"},
		{"with>greater", "with_greater"},
		{"with|pipe", "with_pipe"},
	}

	for _, tt := range tests {
		result := sanitizeFileName(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeFileName(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestReplaceAll(t *testing.T) {
	result := replaceAll("hello/world/test", "/", "_")
	if result != "hello_world_test" {
		t.Errorf("expected hello_world_test, got %s", result)
	}

	result = replaceAll("no-match", "/", "_")
	if result != "no-match" {
		t.Errorf("expected no-match, got %s", result)
	}

	result = replaceAll("", "/", "_")
	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

func TestMemoryCheckpointStore_SaveAndLoad(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	cp := &gsyncx.Checkpoint{
		TableName:    "users",
		FieldName:    "updated_at",
		LastValue:    "2024-01-01",
		LastSyncTime: time.Now(),
		BatchNum:     5,
	}

	err := store.Save(ctx, cp)
	if err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	loaded, err := store.Load(ctx, "users")
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}
	if loaded.TableName != "users" {
		t.Errorf("expected table users, got %s", loaded.TableName)
	}
}

func TestMemoryCheckpointStore_LoadNotFound(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	loaded, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil for nonexistent checkpoint")
	}
}

func TestMemoryCheckpointStore_Delete(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	cp := &gsyncx.Checkpoint{TableName: "users", FieldName: "id"}
	_ = store.Save(ctx, cp)

	err := store.Delete(ctx, "users")
	if err != nil {
		t.Fatalf("failed to delete checkpoint: %v", err)
	}

	loaded, _ := store.Load(ctx, "users")
	if loaded != nil {
		t.Error("expected nil after delete")
	}
}

func TestMemoryCheckpointStore_DeleteNonExistent(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	err := store.Delete(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("should not error on deleting nonexistent: %v", err)
	}
}

func TestMemoryCheckpointStore_SaveProgress(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	progress := &gsyncx.SyncProgress{
		Status:        gsyncx.StatusRunning,
		TotalRecords:  1000,
		SyncedRecords: 500,
	}

	err := store.SaveProgress(ctx, progress)
	if err != nil {
		t.Fatalf("failed to save progress: %v", err)
	}

	loaded := store.GetProgress()
	if loaded == nil {
		t.Error("expected non-nil progress")
	}
	if loaded.Status != gsyncx.StatusRunning {
		t.Errorf("expected running status, got %s", loaded.Status)
	}
	if loaded.TotalRecords != 1000 {
		t.Errorf("expected 1000 total records, got %d", loaded.TotalRecords)
	}
}

func TestMemoryCheckpointStore_GetProgress_Nil(t *testing.T) {
	store := NewMemoryCheckpointStore()
	progress := store.GetProgress()
	if progress != nil {
		t.Error("expected nil progress for new store")
	}
}

func TestMemoryCheckpointStore_Overwrite(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	cp1 := &gsyncx.Checkpoint{TableName: "users", FieldName: "id", LastValue: "100"}
	_ = store.Save(ctx, cp1)

	cp2 := &gsyncx.Checkpoint{TableName: "users", FieldName: "id", LastValue: "200"}
	_ = store.Save(ctx, cp2)

	loaded, _ := store.Load(ctx, "users")
	if loaded.LastValue != "200" {
		t.Errorf("expected last value 200 after overwrite, got %v", loaded.LastValue)
	}
}

func TestMemoryCheckpointStore_MultipleTables(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	cp1 := &gsyncx.Checkpoint{TableName: "users", FieldName: "id", LastValue: "100"}
	cp2 := &gsyncx.Checkpoint{TableName: "orders", FieldName: "id", LastValue: "500"}

	_ = store.Save(ctx, cp1)
	_ = store.Save(ctx, cp2)

	loaded1, _ := store.Load(ctx, "users")
	loaded2, _ := store.Load(ctx, "orders")

	if loaded1.LastValue != "100" {
		t.Errorf("expected users last value 100, got %v", loaded1.LastValue)
	}
	if loaded2.LastValue != "500" {
		t.Errorf("expected orders last value 500, got %v", loaded2.LastValue)
	}
}
