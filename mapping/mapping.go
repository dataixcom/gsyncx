package mapping

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/transform"
)

type FieldMappingEngine struct {
	mappings      []gsyncx.FieldMapping
	defaultValues map[string]interface{}
	ignoreMissing bool
	strictMode    bool
	autoMapping   bool
	logger        gsyncx.SyncLogger
}

func NewFieldMappingEngine(cfg gsyncx.MappingConfig, logger gsyncx.SyncLogger) *FieldMappingEngine {
	return &FieldMappingEngine{
		mappings:      cfg.Mappings,
		defaultValues: cfg.DefaultValues,
		ignoreMissing: cfg.IgnoreMissing,
		strictMode:    cfg.StrictMode,
		autoMapping:   cfg.AutoMapping,
		logger:        gsyncx.ResolveLogger(logger),
	}
}

func NewFieldMappingEngineWithMappings(mappings []gsyncx.FieldMapping, logger gsyncx.SyncLogger) *FieldMappingEngine {
	return &FieldMappingEngine{
		mappings:      mappings,
		defaultValues: make(map[string]interface{}),
		logger:        gsyncx.ResolveLogger(logger),
	}
}

func (e *FieldMappingEngine) Map(records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	result := make([]gsyncx.Record, 0, len(records))
	var failed []gsyncx.FailedRecord

	for i, record := range records {
		mapped, err := e.mapRecord(record)
		if err != nil {
			if e.strictMode {
				return nil, nil, fmt.Errorf("record %d mapping failed: %w", i, err)
			}
			failed = append(failed, gsyncx.FailedRecord{
				Record: record,
				Error:  err,
				Stage:  gsyncx.StageMap,
			})
			continue
		}
		result = append(result, mapped)
	}

	return result, failed, nil
}

func (e *FieldMappingEngine) mapRecord(record gsyncx.Record) (gsyncx.Record, error) {
	newData := make(map[string]interface{})
	var mappingErrors []string

	explicitSources := make(map[string]bool)
	for _, m := range e.mappings {
		explicitSources[m.SourceField] = true
	}

	for _, m := range e.mappings {
		sourceValue, exists := record.Data[m.SourceField]

		if !exists || sourceValue == nil {
			if m.Default != nil {
				newData[m.TargetField] = m.Default
				continue
			}
			if defaultVal, hasDefault := e.defaultValues[m.TargetField]; hasDefault {
				newData[m.TargetField] = defaultVal
				continue
			}
			if m.Required {
				mappingErrors = append(mappingErrors, fmt.Sprintf("required field %s is missing", m.SourceField))
				continue
			}
			if e.ignoreMissing {
				continue
			}
			continue
		}

		if m.Transform != "" {
			transformed, err := transform.ApplyBuiltinTransform(sourceValue, m.Transform)
			if err != nil {
				e.logger.Warn("field transform failed",
					gsyncx.F("field", m.SourceField),
					gsyncx.F("transform", m.Transform),
					gsyncx.F("error", err),
				)
				if e.strictMode {
					return record, fmt.Errorf("transform failed for field %s: %w", m.SourceField, err)
				}
				newData[m.TargetField] = sourceValue
				continue
			}
			newData[m.TargetField] = transformed
		} else {
			newData[m.TargetField] = sourceValue
		}
	}

	for k, v := range record.Data {
		if explicitSources[k] {
			continue
		}

		if e.autoMapping {
			newData[k] = v
		} else {
			_, alreadyMapped := newData[k]
			if !alreadyMapped {
				newData[k] = v
			}
		}
	}

	if len(mappingErrors) > 0 {
		if e.strictMode {
			return record, fmt.Errorf("mapping errors: %s", strings.Join(mappingErrors, "; "))
		}
		return record, fmt.Errorf("mapping errors: %s", strings.Join(mappingErrors, "; "))
	}

	return gsyncx.Record{Data: newData, Meta: record.Meta}, nil
}

func (e *FieldMappingEngine) GetMappings() []gsyncx.FieldMapping {
	return e.mappings
}

func (e *FieldMappingEngine) Validate() error {
	if len(e.mappings) == 0 {
		return fmt.Errorf("no field mappings defined")
	}

	seen := make(map[string]bool)
	for _, m := range e.mappings {
		if m.SourceField == "" {
			return fmt.Errorf("source field cannot be empty")
		}
		if m.TargetField == "" {
			return fmt.Errorf("target field cannot be empty for source field %s", m.SourceField)
		}
		if seen[m.TargetField] {
			return fmt.Errorf("duplicate target field: %s", m.TargetField)
		}
		seen[m.TargetField] = true
	}

	return nil
}

func BuildAutoMappings(sourceFields, targetFields []string) []gsyncx.FieldMapping {
	targetSet := make(map[string]bool)
	for _, f := range targetFields {
		targetSet[strings.ToLower(f)] = true
	}

	var mappings []gsyncx.FieldMapping
	for _, src := range sourceFields {
		if targetSet[strings.ToLower(src)] {
			mappings = append(mappings, gsyncx.FieldMapping{
				SourceField: src,
				TargetField: src,
			})
		}
	}

	return mappings
}

func BuildSmartMappings(sourceFields, targetFields []string, existingMappings []gsyncx.FieldMapping) []gsyncx.FieldMapping {
	explicitTargets := make(map[string]bool)
	explicitSources := make(map[string]bool)
	for _, m := range existingMappings {
		explicitTargets[strings.ToLower(m.TargetField)] = true
		explicitSources[strings.ToLower(m.SourceField)] = true
	}

	targetSet := make(map[string]string)
	for _, f := range targetFields {
		targetSet[strings.ToLower(f)] = f
	}

	result := make([]gsyncx.FieldMapping, len(existingMappings))
	copy(result, existingMappings)

	for _, src := range sourceFields {
		if explicitSources[strings.ToLower(src)] {
			continue
		}
		if targetName, ok := targetSet[strings.ToLower(src)]; ok {
			if !explicitTargets[strings.ToLower(src)] {
				result = append(result, gsyncx.FieldMapping{
					SourceField: src,
					TargetField: targetName,
				})
			}
		}
	}

	return result
}

type MappingDebugger struct {
	engine *FieldMappingEngine
	logger gsyncx.SyncLogger
}

func NewMappingDebugger(engine *FieldMappingEngine, logger gsyncx.SyncLogger) *MappingDebugger {
	return &MappingDebugger{engine: engine, logger: gsyncx.ResolveLogger(logger)}
}

func (d *MappingDebugger) DebugRecord(record gsyncx.Record) (*MappingDebugResult, error) {
	result := &MappingDebugResult{
		SourceFields: make(map[string]interface{}),
		TargetFields: make(map[string]interface{}),
		Steps:        make([]MappingStep, 0),
	}

	for k, v := range record.Data {
		result.SourceFields[k] = v
	}

	for _, m := range d.engine.mappings {
		step := MappingStep{
			SourceField: m.SourceField,
			TargetField: m.TargetField,
			Transform:   m.Transform,
		}

		sourceValue, exists := record.Data[m.SourceField]
		if !exists {
			step.Status = "missing"
			if m.Default != nil {
				step.ResultValue = m.Default
				step.Status = "default_applied"
			}
		} else {
			step.SourceValue = sourceValue
			if m.Transform != "" {
				transformed, err := transform.ApplyBuiltinTransform(sourceValue, m.Transform)
				if err != nil {
					step.Status = "transform_error"
					step.Error = err.Error()
					step.ResultValue = sourceValue
				} else {
					step.Status = "transformed"
					step.ResultValue = transformed
				}
			} else {
				step.Status = "direct"
				step.ResultValue = sourceValue
			}
		}

		result.Steps = append(result.Steps, step)
		if step.ResultValue != nil {
			result.TargetFields[m.TargetField] = step.ResultValue
		}
	}

	if d.engine.autoMapping {
		for k, v := range record.Data {
			explicitSource := false
			for _, m := range d.engine.mappings {
				if m.SourceField == k {
					explicitSource = true
					break
				}
			}
			if !explicitSource {
				if _, exists := result.TargetFields[k]; !exists {
					result.TargetFields[k] = v
					result.Steps = append(result.Steps, MappingStep{
						SourceField: k,
						TargetField: k,
						SourceValue: v,
						ResultValue: v,
						Status:      "auto_mapped",
					})
				}
			}
		}
	}

	return result, nil
}

func (d *MappingDebugger) GenerateMappingReport(sourceFields, targetFields []string) *MappingReport {
	report := &MappingReport{
		SourceFields:   sourceFields,
		TargetFields:   targetFields,
		MatchedFields:  make([]MappingMatch, 0),
		UnmappedSource: make([]string, 0),
		UnmappedTarget: make([]string, 0),
	}

	explicitSources := make(map[string]bool)
	explicitTargets := make(map[string]bool)
	for _, m := range d.engine.mappings {
		explicitSources[m.SourceField] = true
		explicitTargets[m.TargetField] = true
		report.MatchedFields = append(report.MatchedFields, MappingMatch{
			SourceField: m.SourceField,
			TargetField: m.TargetField,
			Transform:   m.Transform,
			MatchType:   "explicit",
		})
	}

	targetSet := make(map[string]string)
	for _, f := range targetFields {
		targetSet[strings.ToLower(f)] = f
	}

	for _, src := range sourceFields {
		if explicitSources[src] {
			continue
		}
		if targetName, ok := targetSet[strings.ToLower(src)]; ok {
			report.MatchedFields = append(report.MatchedFields, MappingMatch{
				SourceField: src,
				TargetField: targetName,
				MatchType:   "auto",
			})
		} else {
			report.UnmappedSource = append(report.UnmappedSource, src)
		}
	}

	sourceSet := make(map[string]string)
	for _, f := range sourceFields {
		sourceSet[strings.ToLower(f)] = f
	}

	for _, tgt := range targetFields {
		if !explicitTargets[tgt] {
			if _, ok := sourceSet[strings.ToLower(tgt)]; !ok {
				report.UnmappedTarget = append(report.UnmappedTarget, tgt)
			}
		}
	}

	sort.Strings(report.UnmappedSource)
	sort.Strings(report.UnmappedTarget)

	return report
}

type MappingDebugResult struct {
	SourceFields map[string]interface{} `json:"source_fields"`
	TargetFields map[string]interface{} `json:"target_fields"`
	Steps        []MappingStep          `json:"steps"`
}

type MappingStep struct {
	SourceField string      `json:"source_field"`
	TargetField string      `json:"target_field"`
	SourceValue interface{} `json:"source_value,omitempty"`
	ResultValue interface{} `json:"result_value,omitempty"`
	Transform   string      `json:"transform,omitempty"`
	Status      string      `json:"status"`
	Error       string      `json:"error,omitempty"`
}

type MappingReport struct {
	SourceFields   []string       `json:"source_fields"`
	TargetFields   []string       `json:"target_fields"`
	MatchedFields  []MappingMatch `json:"matched_fields"`
	UnmappedSource []string       `json:"unmapped_source"`
	UnmappedTarget []string       `json:"unmapped_target"`
}

type MappingMatch struct {
	SourceField string `json:"source_field"`
	TargetField string `json:"target_field"`
	Transform   string `json:"transform,omitempty"`
	MatchType   string `json:"match_type"`
}
