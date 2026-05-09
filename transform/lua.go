package transform

import (
	"context"
	"fmt"
	"sync"

	"github.com/dataixcom/gsyncx"
	"github.com/yuin/gopher-lua"
)

type LuaEngine struct {
	logger    gsyncx.SyncLogger
	pool      sync.Pool
	sandboxed bool
}

func NewLuaEngine(logger gsyncx.SyncLogger) *LuaEngine {
	return &LuaEngine{
		logger:    gsyncx.ResolveLogger(logger),
		sandboxed: true,
		pool: sync.Pool{
			New: func() interface{} {
				L := lua.NewState(lua.Options{
					SkipOpenLibs: true,
				})
				return L
			},
		},
	}
}

func (e *LuaEngine) Execute(ctx context.Context, script string, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	if script == "" {
		return records, nil, nil
	}

	L := e.pool.Get().(*lua.LState)
	defer L.Close()

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	var success []gsyncx.Record
	var failed []gsyncx.FailedRecord

	for i, record := range records {
		select {
		case <-ctx.Done():
			failed = append(failed, gsyncx.FailedRecord{
				Record: record,
				Error:  ctx.Err(),
				Stage:  gsyncx.StageTransform,
			})
			continue
		default:
		}

		transformed, err := e.transformRecord(L, record, script)
		if err != nil {
			e.logger.Warn("lua transform failed for record",
				gsyncx.F("index", i),
				gsyncx.F("error", err),
			)
			failed = append(failed, gsyncx.FailedRecord{
				Record: record,
				Error:  err,
				Stage:  gsyncx.StageTransform,
			})
			continue
		}
		success = append(success, transformed)
	}

	return success, failed, nil
}

func (e *LuaEngine) Validate(script string) error {
	if script == "" {
		return nil
	}

	L := lua.NewState()
	defer L.Close()

	if err := L.DoString(script); err != nil {
		return fmt.Errorf("lua script syntax error: %w", err)
	}

	return nil
}

func (e *LuaEngine) transformRecord(L *lua.LState, record gsyncx.Record, script string) (gsyncx.Record, error) {
	if err := L.DoString(script); err != nil {
		return record, fmt.Errorf("lua script error: %w", err)
	}

	transformFn := L.GetGlobal("transform")
	if transformFn.Type() == lua.LTNil {
		return record, nil
	}

	table := L.NewTable()
	for k, v := range record.Data {
		table.RawSetString(k, lua.LString(fmt.Sprintf("%v", v)))
	}

	if err := L.CallByParam(lua.P{
		Fn:      transformFn,
		NRet:    1,
		Protect: true,
	}, table); err != nil {
		return record, fmt.Errorf("lua transform call failed: %w", err)
	}

	ret := L.Get(-1)
	L.Pop(1)

	if retTbl, ok := ret.(*lua.LTable); ok {
		newData := make(map[string]interface{})
		retTbl.ForEach(func(k, v lua.LValue) {
			newData[k.String()] = v.String()
		})
		return gsyncx.Record{Data: newData, Meta: record.Meta}, nil
	}

	return record, nil
}
