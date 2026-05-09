package transform

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dataixcom/gsyncx"
)

type DefaultTransformer struct{}

func NewDefaultTransformer() *DefaultTransformer {
	return &DefaultTransformer{}
}

func (t *DefaultTransformer) Transform(_ context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	return records, nil, nil
}

type TransformFunc func(ctx context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error)

func (f TransformFunc) Transform(ctx context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	return f(ctx, records)
}

type ScriptLanguage string

const (
	ScriptLangLua        ScriptLanguage = "lua"
	ScriptLangJavaScript ScriptLanguage = "javascript"
)

type ScriptEngine interface {
	Execute(ctx context.Context, script string, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error)
	Validate(script string) error
}

type ScriptManager struct {
	engines map[ScriptLanguage]ScriptEngine
	cache   map[string]string
	mu      sync.RWMutex
	logger  gsyncx.SyncLogger
}

func NewScriptManager(logger gsyncx.SyncLogger) *ScriptManager {
	mgr := &ScriptManager{
		engines: make(map[ScriptLanguage]ScriptEngine),
		cache:   make(map[string]string),
		logger:  gsyncx.ResolveLogger(logger),
	}
	mgr.RegisterEngine(ScriptLangLua, NewLuaEngine(mgr.logger))
	return mgr
}

func (m *ScriptManager) RegisterEngine(lang ScriptLanguage, engine ScriptEngine) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.engines[lang] = engine
}

func (m *ScriptManager) GetEngine(lang ScriptLanguage) (ScriptEngine, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	engine, ok := m.engines[lang]
	return engine, ok
}

func (m *ScriptManager) LoadScript(name, script string, lang ScriptLanguage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	engine, ok := m.engines[lang]
	if !ok {
		return fmt.Errorf("unsupported script language: %s", lang)
	}

	if err := engine.Validate(script); err != nil {
		return fmt.Errorf("script validation failed: %w", err)
	}

	m.cache[name] = script
	m.logger.Debug("script loaded",
		gsyncx.F("name", name),
		gsyncx.F("language", lang),
	)
	return nil
}

func (m *ScriptManager) GetScript(name string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	script, ok := m.cache[name]
	return script, ok
}

func (m *ScriptManager) Execute(ctx context.Context, name string, lang ScriptLanguage, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	m.mu.RLock()
	script, ok := m.cache[name]
	m.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("script %q not found", name)
	}

	engine, ok := m.GetEngine(lang)
	if !ok {
		return nil, nil, fmt.Errorf("unsupported script language: %s", lang)
	}

	return engine.Execute(ctx, script, records)
}

func (m *ScriptManager) ListScripts() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.cache))
	for name := range m.cache {
		names = append(names, name)
	}
	return names
}

func (m *ScriptManager) RemoveScript(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cache, name)
}

type ScriptTransformer struct {
	manager   *ScriptManager
	script    string
	lang      ScriptLanguage
	logger    gsyncx.SyncLogger
	timeout   int
	maxMemory int64
}

func NewScriptTransformer(manager *ScriptManager, script string, lang ScriptLanguage, logger gsyncx.SyncLogger) *ScriptTransformer {
	return &ScriptTransformer{
		manager: manager,
		script:  script,
		lang:    lang,
		logger:  gsyncx.ResolveLogger(logger),
	}
}

func (t *ScriptTransformer) Transform(ctx context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	if t.script == "" || len(records) == 0 {
		return records, nil, nil
	}

	engine, ok := t.manager.GetEngine(t.lang)
	if !ok {
		return nil, nil, fmt.Errorf("unsupported script language: %s", t.lang)
	}

	return engine.Execute(ctx, t.script, records)
}

func ApplyBuiltinTransform(value interface{}, transform string) (interface{}, error) {
	switch strings.ToLower(transform) {
	case "tostring", "to_string":
		return fmt.Sprintf("%v", value), nil
	case "toupper", "to_upper":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("toupper requires string value")
		}
		return strings.ToUpper(s), nil
	case "tolower", "to_lower":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("tolower requires string value")
		}
		return strings.ToLower(s), nil
	case "trim":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("trim requires string value")
		}
		return strings.TrimSpace(s), nil
	default:
		return nil, fmt.Errorf("unsupported transform: %s", transform)
	}
}
