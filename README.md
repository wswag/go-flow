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

## Releases

Releases are created from pushes to `main` using Conventional Commits. The
workflow creates a `vX.Y.Z` Git tag and a GitHub Release when it finds a commit
that requires a version bump. The first development release starts at `v0.1.0`:

- `fix: ...` creates a patch release.
- `feat: ...` creates a minor release.
- `feat!: ...` or a `BREAKING CHANGE:` footer marks a breaking change; before
  `1.0.0` it increments the minor version, and from `1.0.0` it increments major.
- Documentation, test, and maintenance commits do not create a release by
  themselves.

For Go module releases at `v2` and above, update the module path in `go.mod` to
include the major suffix (for example `/v2`).
