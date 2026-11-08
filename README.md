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

### Flow dependencies

Register application dependencies on a flow definition with `UserInterfaces`.
A node resolves a dependency by its interface or concrete type. Since Go does
not support generic methods, use the generic package function
`workflows.GetInterface[T](runtime)`:

```go
type Notifier interface {
	Notify(context.Context, string) error
}

func registerNotificationFlow(engine *workflows.Service, notifier Notifier) error {
	return engine.RegisterFlow(workflows.Flow{
		Name:           "notify",
		UserInterfaces: []any{notifier},
		Nodes: []workflows.FlowNode{{
			Name: "send",
			Action: func(ctx context.Context, input string, runtime workflows.Runtime) (string, error) {
				service, err := workflows.GetInterface[Notifier](runtime)
				if err != nil {
					return "", err
				}
				if err := service.Notify(ctx, input); err != nil {
					return "", err
				}
				return input, nil
			},
		}},
	})
}
```

If no dependency matches the requested type, or multiple dependencies match,
`GetInterface` returns an error. Registered dependency instances are shared by
all executions of that flow. Action middleware can resolve dependencies from
its `Runtime`; start middleware can resolve them from its `StartRequest`, for
example `workflows.GetInterface[Notifier](request)`. When middleware redirects
a start to another flow name, the next middleware sees that flow's registered
dependencies. A caller can pass `StartOptions{UserInterfaces: []any{...}}` to
override matching flow-level dependencies for that start; lookup falls back to
the flow definition for types not supplied by the caller.

### Fluent token access

`TokenBuilder` and `TokenReader` provide a fluent API for creating, reading,
and updating payloads. `EditableSet` selects the set through
`admin.editable_set`; `EditableValue` replaces that set's value, while
`MergeEditableValue` shallow-merges fields into an object set (incoming fields
win on name conflicts).

```go
import "github.com/wswag/go-flow/tokens"

func updateToken(raw string) (string, error) {
	if raw == "" {
		var err error
		raw, err = tokens.NewTokenBuilder().
			Set("request", map[string]any{"text": "hello", "status": "new"}).
			EditableSet("request").
			Build()
		if err != nil {
			return "", err
		}
	}

	reader := tokens.NewTokenReader(raw)
	if err := reader.Err(); err != nil {
		return "", err
	}

	return tokens.NewTokenBuilderFrom(raw).
		EditableSet(reader.EditableSet()).
		MergeEditableValue(map[string]any{"status": "ready"}).
		Build()
}
```

Use `EditableValue` instead of `MergeEditableValue` to replace the entire
selected set. For typed reads, call `reader.Decode("request", &request)`; use
`reader.DecodeMetaSet("admin.editable_set", &request)` to resolve the set key
from metadata and decode it in one step. `TokenReader` parses once; check
`Err()` before reading when the input may be invalid.

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

### Application-owned access metadata

Each persisted assistant state can also carry an opaque `AccessToken` and an
indexed `AccessRef`. The assistant package does not interpret either value:
the application decides how to validate the token and what the reference
identifies. Keep secrets in `AccessToken`; use `AccessRef` only as a selector,
such as a public draft ID or a user-context key.

```go
key := assistants.StateKey{ScopeKey: "public", ActorKey: "draft:abc123"}
if err := repository.UpdateAccess(ctx, "onboarding", key, "draft:abc123", tokenHash); err != nil {
	return err
}

matches, err := repository.FindRecordsByAccessRef(ctx, "draft:abc123")
if err != nil {
	return err
}
for _, record := range matches {
	if record.Name == "onboarding" && record.AccessToken == tokenHash {
		// The application authorizes this request and can bind the state
		// to the authenticated user's key.
	}
}
```

For a public draft, the application can validate its cookie or bearer token,
then update the state key and `AccessRef` when the user signs in. It should
replace or clear the public access token as part of that transition. Keep the
authorization decision in the application; these fields only store and select
state.

Create each execution explicitly with `NewRun`. `ScopeKey` and `ActorKey`
group runs for a scope and actor; the returned `DraftID` identifies one
specific run. It is the same identifier whether the application exposes the
run publicly or keeps it private. `AccessRef` and `AccessToken` remain
independent application-owned access metadata.

```go
runKey, err := service.NewRun(ctx, "onboarding", assistants.StateKey{
	ScopeKey: "household:12",
	ActorKey: "anonymous",
}, "draft:requested-by-app", tokenHash, assistants.NewRunOptions{
	DiscardOpen: true,
})
if err != nil {
	return err
}
// Use runKey for all reads and commits in this execution. The application
// validates its credential before allowing public access.
```

Mark a step `OncePerRun` to prevent its commit callback from running again
within that `DraftID`. A repeated commit returns the current step snapshot,
which is already the next step after a successful transition. To start over,
discard the old run and call `NewRun` again. Generic one-off operations can
also use `service.ClaimAction(ctx, chainName, runKey, actionKey)`; it returns
`true` only for the first claim. Discarding the run removes its action claims.
Set `ExpiresIn` in `NewRunOptions` to enable expiration. `ResolveRun` checks the
deadline and `ExtendRun` refreshes it. `CleanupRuns` accepts combined filters:

```go
import "time"

removed, err := service.CleanupRuns(ctx, assistants.CleanupRunsOptions{
	Expired: true,
	MinimumAge: 14 * 24 * time.Hour,
	Statuses: []assistants.RunStatus{assistants.RunStatusDiscarded, assistants.RunStatusCompleted},
	Limit: 500,
})
```

At least one filter is required. When combined, all supplied filters apply.
The application remains responsible for checking the opaque `AccessToken`
before exposing a resolved run.
Set `ExtendTTLOnAction: true` alongside `ExpiresIn` to refresh the deadline
when the application commits a step, changes step input, or claims an action.

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
