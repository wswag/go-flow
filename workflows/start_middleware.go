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
// without starting a new flow. Call next to continue the chain.
type StartMiddleware interface {
	Start(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error)
}

// StartMiddlewareFunc adapts a function to the StartMiddleware interface.
type StartMiddlewareFunc func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error)

// Start calls f with the request and next handler.
func (f StartMiddlewareFunc) Start(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
	return f(ctx, request, next)
}
