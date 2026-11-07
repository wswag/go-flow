# go-flow

`go-flow` is a persistent workflow engine for Go. It stores flow instances,
node checkpoints, and token data through GORM. Flow definitions and token
payloads are application-owned; the engine provides execution, persistence,
resume, retry, child-flow dispatch, and status events.

## Install

```sh
go get github.com/wswag/go-flow
```

## Minimal setup

```go
import (
	"context"

	flow "github.com/wswag/go-flow"
	"gorm.io/gorm"
)

func startExample(ctx context.Context, db *gorm.DB) error {
	if err := flow.AutoMigrate(db); err != nil {
		return err
	}
	engine := flow.NewService(flow.NewRepository(db))
	if err := engine.RegisterFlow(flow.Flow{
		Name: "example",
		Nodes: []flow.FlowNode{{
			Name: "finish",
			Action: func(ctx context.Context, input string, runtime flow.Runtime) (string, error) {
				return input, nil
			},
		}},
	}); err != nil {
		return err
	}
	_, err := engine.StartFlow(ctx, "example", "start-token")
	return err
}
```

Pass a `*gorm.DB` opened with the database driver used by your application.
