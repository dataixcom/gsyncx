package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/engine"
)

func main() {
	luaScript := `
function transform(table)
    local result = {}
    for k, v in pairs(table) do
        result[k] = string.upper(v)
    end
    result["processed"] = "true"
    return result
end
`

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithTransformConfig(gsyncx.TransformConfig{
			Script:     luaScript,
			ScriptLang: "lua",
		}),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			DSNConfig: &gsyncx.DSNConfig{
				DBType:   gsyncx.DBMySQL,
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Schema:   "source_db",
			},
			TableName: "products",
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
			TableName: "products",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	result, err := engine.RunSync(context.Background(), cfg, nil)
	if err != nil {
		log.Fatalf("transform sync failed: %v", err)
	}

	fmt.Printf("Transform sync completed: status=%s read=%d written=%d failed=%d\n",
		result.Status, result.TotalRead, result.TotalWritten, result.TotalFailed)
}
