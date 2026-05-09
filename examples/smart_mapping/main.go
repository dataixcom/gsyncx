package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/mapping"
)

func main() {
	sourceFields := []string{"id", "name", "email", "age", "created_at"}
	targetFields := []string{"id", "name", "email", "phone", "age"}

	explicitMappings := []gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "name", Transform: "trim"},
		{SourceField: "email", TargetField: "email", Transform: "to_lower"},
	}

	smartMappings := mapping.BuildSmartMappings(sourceFields, targetFields, explicitMappings)
	fmt.Println("Smart Mappings:")
	for _, m := range smartMappings {
		fmt.Printf("  %s -> %s", m.SourceField, m.TargetField)
		if m.Transform != "" {
			fmt.Printf(" (transform: %s)", m.Transform)
		}
		fmt.Println()
	}

	autoMappings := mapping.BuildAutoMappings(sourceFields, targetFields)
	fmt.Println("\nAuto Mappings (name-matched only):")
	for _, m := range autoMappings {
		fmt.Printf("  %s -> %s\n", m.SourceField, m.TargetField)
	}

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithAutoMapping(true),
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
		gsyncx.WithMappingConfig(gsyncx.MappingConfig{
			Mappings:    explicitMappings,
			AutoMapping: true,
		}),
	)

	result, err := engine.RunSync(context.Background(), cfg, nil)
	if err != nil {
		log.Fatalf("sync failed: %v", err)
	}

	fmt.Printf("\nSync completed: status=%s written=%d\n",
		result.Status, result.TotalWritten)
}
