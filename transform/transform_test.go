package transform

import (
	"context"
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestDefaultTransformer(t *testing.T) {
	tr := NewDefaultTransformer()
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice"}},
	}

	result, failed, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestTransformFunc(t *testing.T) {
	fn := TransformFunc(func(ctx context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
		return records, nil, nil
	})

	records := []gsyncx.Record{{Data: map[string]interface{}{"key": "value"}}}
	result, failed, err := fn.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestApplyBuiltinTransform(t *testing.T) {
	tests := []struct {
		name      string
		value     interface{}
		transform string
		expected  interface{}
		wantErr   bool
	}{
		{"to_string", 42, "to_string", "42", false},
		{"to_upper", "hello", "to_upper", "HELLO", false},
		{"to_lower", "HELLO", "to_lower", "hello", false},
		{"trim", "  hello  ", "trim", "hello", false},
		{"to_upper_non_string", 42, "to_upper", nil, true},
		{"to_lower_non_string", 42, "to_lower", nil, true},
		{"trim_non_string", 42, "trim", nil, true},
		{"unsupported", "hello", "unknown", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyBuiltinTransform(tt.value, tt.transform)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyBuiltinTransform() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("ApplyBuiltinTransform() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestScriptManager(t *testing.T) {
	mgr := NewScriptManager(nil)

	script := `
function transform(table)
    local result = {}
    for k, v in pairs(table) do
        result[k] = string.upper(v)
    end
    return result
end
`

	err := mgr.LoadScript("uppercase", script, ScriptLangLua)
	if err != nil {
		t.Fatalf("failed to load script: %v", err)
	}

	scripts := mgr.ListScripts()
	if len(scripts) != 1 {
		t.Errorf("expected 1 script, got %d", len(scripts))
	}

	loaded, ok := mgr.GetScript("uppercase")
	if !ok {
		t.Error("expected script to be found")
	}
	if loaded != script {
		t.Error("expected same script content")
	}
}

func TestScriptManager_UnsupportedLanguage(t *testing.T) {
	mgr := NewScriptManager(nil)
	err := mgr.LoadScript("test", "code", ScriptLangJavaScript)
	if err == nil {
		t.Error("expected error for unsupported language")
	}
}

func TestScriptManager_GetEngine(t *testing.T) {
	mgr := NewScriptManager(nil)

	engine, ok := mgr.GetEngine(ScriptLangLua)
	if !ok {
		t.Error("expected Lua engine to be registered")
	}
	if engine == nil {
		t.Error("expected non-nil engine")
	}

	_, ok = mgr.GetEngine(ScriptLangJavaScript)
	if ok {
		t.Error("expected JavaScript engine to not be registered")
	}
}

func TestScriptManager_RemoveScript(t *testing.T) {
	mgr := NewScriptManager(nil)
	_ = mgr.LoadScript("test", "function transform(t) return t end", ScriptLangLua)

	mgr.RemoveScript("test")

	_, ok := mgr.GetScript("test")
	if ok {
		t.Error("expected script to be removed")
	}
}

func TestScriptManager_Execute_NotFound(t *testing.T) {
	mgr := NewScriptManager(nil)
	_, _, err := mgr.Execute(context.Background(), "nonexistent", ScriptLangLua, nil)
	if err == nil {
		t.Error("expected error for nonexistent script")
	}
}

func TestScriptTransformer(t *testing.T) {
	mgr := NewScriptManager(nil)
	script := `function transform(t) return t end`
	_ = mgr.LoadScript("identity", script, ScriptLangLua)

	tr := NewScriptTransformer(mgr, script, ScriptLangLua, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"key": "value"}},
	}

	result, _, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
}

func TestScriptTransformer_EmptyScript(t *testing.T) {
	mgr := NewScriptManager(nil)
	tr := NewScriptTransformer(mgr, "", ScriptLangLua, nil)

	records := []gsyncx.Record{{Data: map[string]interface{}{"key": "value"}}}
	result, _, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result for empty script, got %d", len(result))
	}
}

func TestScriptTransformer_UnsupportedLanguage(t *testing.T) {
	mgr := NewScriptManager(nil)
	tr := NewScriptTransformer(mgr, "code", ScriptLangJavaScript, nil)

	records := []gsyncx.Record{{Data: map[string]interface{}{"key": "value"}}}
	_, _, err := tr.Transform(context.Background(), records)
	if err == nil {
		t.Error("expected error for unsupported language")
	}
}

func TestLuaEngine_Validate(t *testing.T) {
	engine := NewLuaEngine(nil)

	err := engine.Validate("function transform(t) return t end")
	if err != nil {
		t.Errorf("expected valid script, got error: %v", err)
	}

	err = engine.Validate("invalid lua syntax !!!")
	if err == nil {
		t.Error("expected error for invalid syntax")
	}
}

func TestLuaEngine_Validate_Empty(t *testing.T) {
	engine := NewLuaEngine(nil)
	err := engine.Validate("")
	if err != nil {
		t.Errorf("expected nil for empty script, got error: %v", err)
	}
}

func TestLuaEngine_Execute_EmptyScript(t *testing.T) {
	engine := NewLuaEngine(nil)
	records := []gsyncx.Record{{Data: map[string]interface{}{"key": "value"}}}

	result, failed, err := engine.Execute(context.Background(), "", records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result for empty script, got %d", len(result))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestLuaEngine_Execute_CancelledContext(t *testing.T) {
	engine := NewLuaEngine(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records := []gsyncx.Record{{Data: map[string]interface{}{"key": "value"}}}
	_, _, err := engine.Execute(ctx, "function transform(t) return t end", records)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}
