package mapping

import (
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestFieldMappingEngine_Map_Basic(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "full_name"},
		{SourceField: "age", TargetField: "user_age"},
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice", "age": 30}},
		{Data: map[string]interface{}{"name": "Bob", "age": 25}},
	}

	result, failed, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed records, got %d", len(failed))
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 records, got %d", len(result))
	}
	if result[0].Data["full_name"] != "Alice" {
		t.Errorf("expected full_name=Alice, got %v", result[0].Data["full_name"])
	}
	if result[0].Data["user_age"] != 30 {
		t.Errorf("expected user_age=30, got %v", result[0].Data["user_age"])
	}
}

func TestFieldMappingEngine_Map_WithTransform(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "name", Transform: "to_upper"},
		{SourceField: "email", TargetField: "email", Transform: "to_lower"},
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice", "email": "ALICE@EXAMPLE.COM"}},
	}

	result, _, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["name"] != "ALICE" {
		t.Errorf("expected name=ALICE, got %v", result[0].Data["name"])
	}
	if result[0].Data["email"] != "alice@example.com" {
		t.Errorf("expected email=alice@example.com, got %v", result[0].Data["email"])
	}
}

func TestFieldMappingEngine_Map_WithDefault(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "status", TargetField: "status", Default: "active"},
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{}},
		{Data: map[string]interface{}{"status": "inactive"}},
	}

	result, _, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["status"] != "active" {
		t.Errorf("expected default status=active, got %v", result[0].Data["status"])
	}
	if result[1].Data["status"] != "inactive" {
		t.Errorf("expected status=inactive, got %v", result[1].Data["status"])
	}
}

func TestFieldMappingEngine_Map_RequiredField(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "email", TargetField: "email", Required: true},
		},
		StrictMode: true,
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice"}},
	}

	_, _, err := engine.Map(records)
	if err == nil {
		t.Error("expected error for missing required field")
	}
}

func TestFieldMappingEngine_Map_RequiredField_NonStrict(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "email", TargetField: "email", Required: true},
		},
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice"}},
	}

	_, failed, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed record, got %d", len(failed))
	}
}

func TestFieldMappingEngine_Map_AutoMapping(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "src_name", TargetField: "name"},
		},
		AutoMapping: true,
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{
			"src_name": "Alice",
			"age":      30,
			"email":    "alice@example.com",
		}},
	}

	result, _, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result[0].Data["name"])
	}
	if result[0].Data["age"] != 30 {
		t.Errorf("expected age=30 (auto-mapped), got %v", result[0].Data["age"])
	}
	if result[0].Data["email"] != "alice@example.com" {
		t.Errorf("expected email (auto-mapped), got %v", result[0].Data["email"])
	}
}

func TestFieldMappingEngine_Map_IgnoreMissing(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "name", TargetField: "name"},
			{SourceField: "optional_field", TargetField: "optional_field"},
		},
		IgnoreMissing: true,
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice"}},
	}

	result, _, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result[0].Data["name"])
	}
	if _, exists := result[0].Data["optional_field"]; exists {
		t.Error("expected optional_field to not exist when missing and ignoreMissing=true")
	}
}

func TestFieldMappingEngine_Map_DefaultValues(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "name", TargetField: "name"},
			{SourceField: "status", TargetField: "status"},
		},
		DefaultValues: map[string]any{
			"status": "pending",
		},
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"name": "Alice"}},
	}

	result, _, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["status"] != "pending" {
		t.Errorf("expected status=pending from default values, got %v", result[0].Data["status"])
	}
}

func TestFieldMappingEngine_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mappings []gsyncx.FieldMapping
		wantErr bool
	}{
		{
			name: "valid mappings",
			mappings: []gsyncx.FieldMapping{
				{SourceField: "a", TargetField: "b"},
			},
			wantErr: false,
		},
		{
			name:     "empty mappings",
			mappings: []gsyncx.FieldMapping{},
			wantErr:  true,
		},
		{
			name: "empty source field",
			mappings: []gsyncx.FieldMapping{
				{SourceField: "", TargetField: "b"},
			},
			wantErr: true,
		},
		{
			name: "empty target field",
			mappings: []gsyncx.FieldMapping{
				{SourceField: "a", TargetField: ""},
			},
			wantErr: true,
		},
		{
			name: "duplicate target field",
			mappings: []gsyncx.FieldMapping{
				{SourceField: "a", TargetField: "b"},
				{SourceField: "c", TargetField: "b"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewFieldMappingEngineWithMappings(tt.mappings, nil)
			err := engine.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFieldMappingEngine_GetMappings(t *testing.T) {
	mappings := []gsyncx.FieldMapping{
		{SourceField: "a", TargetField: "b"},
		{SourceField: "c", TargetField: "d"},
	}
	engine := NewFieldMappingEngineWithMappings(mappings, nil)

	result := engine.GetMappings()
	if len(result) != 2 {
		t.Errorf("expected 2 mappings, got %d", len(result))
	}
}

func TestBuildAutoMappings(t *testing.T) {
	source := []string{"id", "name", "email", "age"}
	target := []string{"id", "name", "phone", "email"}

	mappings := BuildAutoMappings(source, target)

	if len(mappings) != 3 {
		t.Errorf("expected 3 auto mappings, got %d", len(mappings))
	}

	mapped := make(map[string]string)
	for _, m := range mappings {
		mapped[m.SourceField] = m.TargetField
	}
	if mapped["id"] != "id" {
		t.Error("expected id -> id mapping")
	}
	if mapped["name"] != "name" {
		t.Error("expected name -> name mapping")
	}
	if mapped["email"] != "email" {
		t.Error("expected email -> email mapping")
	}
	if _, ok := mapped["age"]; ok {
		t.Error("age should not be auto-mapped (not in target)")
	}
}

func TestBuildAutoMappings_CaseInsensitive(t *testing.T) {
	source := []string{"ID", "Name", "Email"}
	target := []string{"id", "name", "email"}

	mappings := BuildAutoMappings(source, target)

	if len(mappings) != 3 {
		t.Errorf("expected 3 auto mappings (case insensitive), got %d", len(mappings))
	}
}

func TestBuildSmartMappings(t *testing.T) {
	source := []string{"id", "name", "email", "age"}
	target := []string{"id", "name", "phone", "email"}
	existing := []gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "full_name"},
	}

	mappings := BuildSmartMappings(source, target, existing)

	if len(mappings) != 3 {
		t.Errorf("expected 3 smart mappings, got %d", len(mappings))
	}

	hasExplicit := false
	hasAuto := false
	for _, m := range mappings {
		if m.SourceField == "name" && m.TargetField == "full_name" {
			hasExplicit = true
		}
		if m.SourceField == "id" && m.TargetField == "id" {
			hasAuto = true
		}
	}
	if !hasExplicit {
		t.Error("expected explicit mapping name -> full_name")
	}
	if !hasAuto {
		t.Error("expected auto mapping id -> id")
	}
}

func TestMappingDebugger_DebugRecord(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "full_name"},
		{SourceField: "email", TargetField: "email", Transform: "to_lower"},
	}, nil)

	debugger := NewMappingDebugger(engine, nil)

	record := gsyncx.Record{Data: map[string]interface{}{
		"name":  "Alice",
		"email": "ALICE@EXAMPLE.COM",
	}}

	result, err := debugger.DebugRecord(record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(result.Steps))
	}
	if result.SourceFields["name"] != "Alice" {
		t.Error("expected source field name=Alice")
	}
	if result.TargetFields["full_name"] != "Alice" {
		t.Error("expected target field full_name=Alice")
	}
	if result.TargetFields["email"] != "alice@example.com" {
		t.Error("expected target field email=alice@example.com")
	}
}

func TestMappingDebugger_DebugRecord_AutoMapping(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "src_name", TargetField: "name"},
		},
		AutoMapping: true,
	}, nil)

	debugger := NewMappingDebugger(engine, nil)

	record := gsyncx.Record{Data: map[string]interface{}{
		"src_name": "Alice",
		"age":      30,
	}}

	result, err := debugger.DebugRecord(record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hasAutoStep := false
	for _, step := range result.Steps {
		if step.Status == "auto_mapped" && step.SourceField == "age" {
			hasAutoStep = true
		}
	}
	if !hasAutoStep {
		t.Error("expected auto_mapped step for age field")
	}
}

func TestMappingDebugger_GenerateMappingReport(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "full_name"},
	}, nil)

	debugger := NewMappingDebugger(engine, nil)

	report := debugger.GenerateMappingReport(
		[]string{"name", "email", "age"},
		[]string{"full_name", "email", "phone"},
	)

	if len(report.MatchedFields) < 2 {
		t.Errorf("expected at least 2 matched fields, got %d", len(report.MatchedFields))
	}
	if len(report.UnmappedSource) != 1 || report.UnmappedSource[0] != "age" {
		t.Errorf("expected unmapped source [age], got %v", report.UnmappedSource)
	}
	if len(report.UnmappedTarget) != 1 || report.UnmappedTarget[0] != "phone" {
		t.Errorf("expected unmapped target [phone], got %v", report.UnmappedTarget)
	}

	hasExplicit := false
	hasAuto := false
	for _, m := range report.MatchedFields {
		if m.MatchType == "explicit" && m.SourceField == "name" {
			hasExplicit = true
		}
		if m.MatchType == "auto" && m.SourceField == "email" {
			hasAuto = true
		}
	}
	if !hasExplicit {
		t.Error("expected explicit match for name")
	}
	if !hasAuto {
		t.Error("expected auto match for email")
	}
}

func TestFieldMappingEngine_Map_EmptyRecords(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings(nil, nil)
	result, failed, err := engine.Map(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 results, got %d", len(result))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestFieldMappingEngine_Map_TransformError_NonStrict(t *testing.T) {
	engine := NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "age", TargetField: "age", Transform: "to_upper"},
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"age": 30}},
	}

	result, _, err := engine.Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Data["age"] != 30 {
		t.Errorf("expected original value on transform error, got %v", result[0].Data["age"])
	}
}

func TestFieldMappingEngine_Map_TransformError_Strict(t *testing.T) {
	engine := NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "age", TargetField: "age", Transform: "to_upper"},
		},
		StrictMode: true,
	}, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"age": 30}},
	}

	_, _, err := engine.Map(records)
	if err == nil {
		t.Error("expected error for transform failure in strict mode")
	}
}
