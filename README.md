# go-flow

`go-flow` provides a persistent workflow engine and an assistant engine for Go. It stores flow instances,
node checkpoints, and token data through GORM. Flow definitions and token
payloads are application-owned; the engine provides execution, persistence,
resume, retry, child-flow dispatch, and status events.

## Install

```sh
go get github.com/wswag/go-flow
```

## Workflow example

```go
import (
	"context"

	"github.com/wswag/go-flow/workflows"
	"github.com/wswag/go-flow/tokens"
	_ "github.com/wswag/go-flow/tokens/gzip" // registers the gz: storage codec
	"gorm.io/gorm"
)

func startExample(ctx context.Context, db *gorm.DB) error {
	if err := workflows.AutoMigrate(db); err != nil {
		return err
	}
	engine := workflows.NewService(workflows.NewRepository(db))
	if err := engine.RegisterFlow(workflows.Flow{
		Name: "example",
		Nodes: []workflows.FlowNode{{
			Name: "finish",
			Action: func(ctx context.Context, input string, runtime workflows.Runtime) (string, error) {
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

Registering `tokens/gzip` enables transparent compression for persisted workflow
tokens. The same package provides the shared payload format:

```go
payload, err := tokens.BuildTokenPayload(
	map[string]any{"request": map[string]string{"text": "hello"}},
	map[string]any{"admin.primary_set": "request"},
)
if err != nil {
	return err
}
parsed, err := tokens.ParseTokenPayload(payload)
```

## Assistant example

```go
import (
	"github.com/wswag/go-flow/assistants"
	_ "github.com/wswag/go-flow/tokens/gzip" // registers the gz: storage codec
	"gorm.io/gorm"
)

func startAssistant(db *gorm.DB) (*assistants.Service, error) {
	if err := assistants.AutoMigrate(db); err != nil {
		return nil, err
	}
	service := assistants.NewService(assistants.NewRepository(db))
	err := service.RegisterChain(assistants.ChainDefinition{
		Name: "onboarding",
		Steps: []assistants.StepDefinition{{Name: "profile", Sequence: 1, Input: map[string]any{"name": ""}}},
	})
	return service, err
}
```

The gzip plugin stores compressible assistant state with the `gz:` marker.
Existing uncompressed `StateJSON` values have no marker and continue to load as
plain text. Pass a `*gorm.DB` opened with your application's database driver.

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
