package validator

import (
	"fmt"
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestNewDataValidator(t *testing.T) {
	v := NewDataValidator(nil)
	if v == nil {
		t.Error("expected non-nil validator")
	}
}

func TestDataValidator_ValidateRecord_Required(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "email",
		Required:  true,
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{}})
	if err == nil {
		t.Error("expected error for missing required field")
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"email": "test@example.com"}})
	if err != nil {
		t.Errorf("expected valid record, got error: %v", err)
	}
}

func TestDataValidator_ValidateRecord_Type(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "age",
		Type:      "int",
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"age": 25}})
	if err != nil {
		t.Errorf("expected valid record, got error: %v", err)
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"age": "not_int"}})
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestDataValidator_ValidateRecord_StringLength(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "name",
		Type:      "string",
		MinLength: 2,
		MaxLength: 10,
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"name": "Alice"}})
	if err != nil {
		t.Errorf("expected valid record, got error: %v", err)
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"name": "A"}})
	if err == nil {
		t.Error("expected error for too short")
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"name": "VeryLongName"}})
	if err == nil {
		t.Error("expected error for too long")
	}
}

func TestDataValidator_ValidateRecord_CustomFunc(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "code",
		CustomFunc: func(value interface{}) error {
			s, ok := value.(string)
			if !ok || len(s) != 3 {
				return fmt.Errorf("code must be 3 characters")
			}
			return nil
		},
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"code": "ABC"}})
	if err != nil {
		t.Errorf("expected valid record, got error: %v", err)
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"code": "AB"}})
	if err == nil {
		t.Error("expected error for custom validation")
	}
}

func TestDataValidator_ValidateRecords(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "email",
		Required:  true,
	})

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"email": "a@example.com"}},
		{Data: map[string]interface{}{"email": "b@example.com"}},
	}

	err := v.ValidateRecords(records)
	if err != nil {
		t.Errorf("expected valid records, got error: %v", err)
	}

	invalidRecords := []gsyncx.Record{
		{Data: map[string]interface{}{"email": "a@example.com"}},
		{Data: map[string]interface{}{}},
	}

	err = v.ValidateRecords(invalidRecords)
	if err == nil {
		t.Error("expected error for invalid records")
	}
}

func TestDataValidator_ValidateRecord_OptionalField(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "nickname",
		Type:      "string",
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{}})
	if err != nil {
		t.Errorf("expected valid record with optional field missing, got error: %v", err)
	}
}

func TestDataValidator_ValidateRecord_NilValue(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "name",
		Required:  true,
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"name": nil}})
	if err == nil {
		t.Error("expected error for nil required field")
	}
}

func TestDataValidator_ValidateType_Bool(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "active",
		Type:      "bool",
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"active": true}})
	if err != nil {
		t.Errorf("expected valid bool, got error: %v", err)
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"active": "yes"}})
	if err == nil {
		t.Error("expected error for non-bool value")
	}
}

func TestDataValidator_ValidateType_Float(t *testing.T) {
	v := NewDataValidator(nil)
	v.AddFieldValidator(FieldValidator{
		FieldName: "score",
		Type:      "float64",
	})

	err := v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"score": 3.14}})
	if err != nil {
		t.Errorf("expected valid float, got error: %v", err)
	}

	err = v.ValidateRecord(gsyncx.Record{Data: map[string]interface{}{"score": "not_float"}})
	if err == nil {
		t.Error("expected error for non-float value")
	}
}
