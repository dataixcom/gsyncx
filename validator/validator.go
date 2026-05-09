package validator

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dataixcom/gsyncx"
)

type ValidatorFunc func(value interface{}) error

type FieldValidator struct {
	FieldName  string        `json:"field_name"`
	Required   bool          `json:"required"`
	Type       string        `json:"type,omitempty"`
	MinLength  int           `json:"min_length,omitempty"`
	MaxLength  int           `json:"max_length,omitempty"`
	MinValue   interface{}   `json:"min_value,omitempty"`
	MaxValue   interface{}   `json:"max_value,omitempty"`
	Pattern    string        `json:"pattern,omitempty"`
	CustomFunc ValidatorFunc `json:"-"`
}

type DataValidator struct {
	fieldValidators map[string]FieldValidator
	logger          gsyncx.SyncLogger
}

func NewDataValidator(logger gsyncx.SyncLogger) *DataValidator {
	return &DataValidator{
		fieldValidators: make(map[string]FieldValidator),
		logger:          gsyncx.ResolveLogger(logger),
	}
}

func (v *DataValidator) AddFieldValidator(fv FieldValidator) {
	v.fieldValidators[fv.FieldName] = fv
}

func (v *DataValidator) ValidateRecord(record gsyncx.Record) error {
	for fieldName, validator := range v.fieldValidators {
		value, exists := record.Data[fieldName]

		if !exists || value == nil {
			if validator.Required {
				return fmt.Errorf("field %s is required but missing", fieldName)
			}
			continue
		}

		if err := v.validateValue(fieldName, value, validator); err != nil {
			return err
		}
	}

	return nil
}

func (v *DataValidator) ValidateRecords(records []gsyncx.Record) error {
	for i, record := range records {
		if err := v.ValidateRecord(record); err != nil {
			return fmt.Errorf("record %d validation failed: %w", i, err)
		}
	}
	return nil
}

func (v *DataValidator) validateValue(fieldName string, value interface{}, validator FieldValidator) error {
	if validator.Type != "" {
		if err := v.validateType(fieldName, value, validator.Type); err != nil {
			return err
		}
	}

	if validator.CustomFunc != nil {
		if err := validator.CustomFunc(value); err != nil {
			return fmt.Errorf("field %s custom validation failed: %w", fieldName, err)
		}
	}

	switch val := value.(type) {
	case string:
		if validator.MinLength > 0 && len(val) < validator.MinLength {
			return fmt.Errorf("field %s length %d is less than minimum %d", fieldName, len(val), validator.MinLength)
		}
		if validator.MaxLength > 0 && len(val) > validator.MaxLength {
			return fmt.Errorf("field %s length %d exceeds maximum %d", fieldName, len(val), validator.MaxLength)
		}
	}

	return nil
}

func (v *DataValidator) validateType(fieldName string, value interface{}, expectedType string) error {
	val := reflect.ValueOf(value)
	if !val.IsValid() {
		return fmt.Errorf("field %s has invalid value", fieldName)
	}

	actualKind := val.Kind()
	switch strings.ToLower(expectedType) {
	case "string":
		if actualKind != reflect.String {
			return fmt.Errorf("field %s expected string, got %s", fieldName, actualKind)
		}
	case "int", "int64", "integer":
		if actualKind != reflect.Int && actualKind != reflect.Int64 && actualKind != reflect.Int32 {
			return fmt.Errorf("field %s expected int, got %s", fieldName, actualKind)
		}
	case "float", "float64":
		if actualKind != reflect.Float64 && actualKind != reflect.Float32 {
			return fmt.Errorf("field %s expected float, got %s", fieldName, actualKind)
		}
	case "bool", "boolean":
		if actualKind != reflect.Bool {
			return fmt.Errorf("field %s expected bool, got %s", fieldName, actualKind)
		}
	}

	return nil
}
