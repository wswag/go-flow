package workflows

import "context"

// StartRequest contains the mutable input to one StartFlow call.
type StartRequest struct {
	FlowName   string
	StartToken string
	Options    StartOptions
}

// StartFlowNext continues the start middleware chain with a request. A
// middleware may call it with modified flow name, token, or options.
type StartFlowNext func(ctx context.Context, request StartRequest) (*FlowExecutionContext, error)

// StartMiddleware can modify a workflow start or return an execution directly
// without starting a new flow. Call next to continue the chain. A middleware
// that allocates resources during Start can also implement
// StartMiddlewareRollback to clean them up if starting fails synchronously.
type StartMiddleware interface {
	Start(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error)
}

// StartMiddlewareRollback is an optional interface for middleware that needs
// to undo setup when the start chain or the synchronous workflow start fails.
// RollbackStart is called in reverse middleware order. It is not called for
// errors that occur later while the workflow is executing.
type StartMiddlewareRollback interface {
	RollbackStart(ctx context.Context, request StartRequest, startErr error) error
}

// StartMiddlewareFunc adapts a function to the StartMiddleware interface.
type StartMiddlewareFunc func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error)

// Start calls f with the request and next handler.
func (f StartMiddlewareFunc) Start(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
	return f(ctx, request, next)
}
