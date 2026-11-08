package workflows

import (
	"context"
	"reflect"
)

// StartRequest contains the mutable input to one StartFlow call.
type StartRequest struct {
	FlowName   string
	StartToken string
	Options    StartOptions
	// UserInterfaces contains call-specific dependencies, for use by start
	// middleware.
	UserInterfaces []any
	// FlowUserInterfaces contains dependencies registered on the flow definition
	// currently named by FlowName. UserInterfaces contains call-specific values
	// and takes precedence over this slice.
	FlowUserInterfaces []any
}

func (request StartRequest) resolveUserInterface(targetType reflect.Type) (any, error) {
	return resolveUserInterfaceFrom(request.UserInterfaces, request.FlowUserInterfaces, targetType)
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

// NewSequencedStartMiddleware assigns an ordering sequence to middleware.
// StartFlow sorts engine, flow-definition, and per-call middleware together
// in ascending sequence order. Middleware without an explicit sequence uses 0;
// ties retain their source order (engine, flow definition, then per-call).
func NewSequencedStartMiddleware(sequence int, middleware StartMiddleware) StartMiddleware {
	if middleware == nil {
		return nil
	}
	return sequencedStartMiddleware{sequence: sequence, middleware: middleware}
}

type sequencedStartMiddleware struct {
	sequence   int
	middleware StartMiddleware
}

func (m sequencedStartMiddleware) Start(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
	return m.middleware.Start(ctx, request, next)
}

func (m sequencedStartMiddleware) Sequence() int { return m.sequence }

func (m sequencedStartMiddleware) UnwrapStartMiddleware() StartMiddleware { return m.middleware }
