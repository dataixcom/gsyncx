package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/dataixcom/gsyncx"
)

type FileCheckpointStore struct {
	dirPath string
	mu      sync.Mutex
}

func NewFileCheckpointStore(dirPath string) (*FileCheckpointStore, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}
	return &FileCheckpointStore{dirPath: dirPath}, nil
}

func (s *FileCheckpointStore) Save(_ context.Context, cp *gsyncx.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.filePath(cp.TableName)
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint file: %w", err)
	}

	return nil
}

func (s *FileCheckpointStore) Load(_ context.Context, tableName string) (*gsyncx.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.filePath(tableName)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var cp gsyncx.Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	return &cp, nil
}

func (s *FileCheckpointStore) Delete(_ context.Context, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.filePath(tableName)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete checkpoint file: %w", err)
	}

	return nil
}

func (s *FileCheckpointStore) SaveProgress(_ context.Context, progress *gsyncx.SyncProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := filepath.Join(s.dirPath, "progress.json")
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal progress: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write progress file: %w", err)
	}

	return nil
}

func (s *FileCheckpointStore) filePath(tableName string) string {
	safeName := sanitizeFileName(tableName)
	return filepath.Join(s.dirPath, safeName+".json")
}

func sanitizeFileName(name string) string {
	replacer := []string{"/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_", "<", "_", ">", "_", "|", "_"}
	result := name
	for i := 0; i < len(replacer); i += 2 {
		result = replaceAll(result, replacer[i], replacer[i+1])
	}
	return result
}

func replaceAll(s, old, new string) string {
	for i := 0; i < len(s); i++ {
		if len(s)-i >= len(old) && s[i:i+len(old)] == old {
			s = s[:i] + new + s[i+len(old):]
			i += len(new) - 1
		}
	}
	return s
}

type MemoryCheckpointStore struct {
	checkpoints map[string]*gsyncx.Checkpoint
	progress    *gsyncx.SyncProgress
	mu          sync.RWMutex
}

func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{
		checkpoints: make(map[string]*gsyncx.Checkpoint),
	}
}

func (s *MemoryCheckpointStore) Save(_ context.Context, cp *gsyncx.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[cp.TableName] = cp
	return nil
}

func (s *MemoryCheckpointStore) Load(_ context.Context, tableName string) (*gsyncx.Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp, ok := s.checkpoints[tableName]
	if !ok {
		return nil, nil
	}
	return cp, nil
}

func (s *MemoryCheckpointStore) Delete(_ context.Context, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.checkpoints, tableName)
	return nil
}

func (s *MemoryCheckpointStore) SaveProgress(_ context.Context, progress *gsyncx.SyncProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.progress = progress
	return nil
}

func (s *MemoryCheckpointStore) GetProgress() *gsyncx.SyncProgress {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.progress
}
