package workflows

// ActionMiddleware wraps a workflow node action.
type ActionMiddleware interface {
	Wrap(next Action) Action
}

// ActionMiddlewareFunc adapts an action-wrapping function to ActionMiddleware.
type ActionMiddlewareFunc func(next Action) Action

// Wrap applies the middleware function to next.
func (f ActionMiddlewareFunc) Wrap(next Action) Action { return f(next) }
