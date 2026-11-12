package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/wswag/go-flow/tokens"
)

func TestStartMiddlewaresUseSequenceAcrossEngineFlowAndCall(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	var order []string
	middleware := func(name string) StartMiddleware {
		return StartMiddlewareFunc(func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
			order = append(order, name)
			return next(ctx, request)
		})
	}
	svc.RegisterStartMiddleware(NewSequencedStartMiddleware(30, middleware("engine")))
	if err := svc.RegisterFlow(Flow{
		Name: "sequenced-flow",
		StartMiddlewares: []StartMiddleware{
			NewSequencedStartMiddleware(20, middleware("flow")),
		},
		Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, input string, _ Runtime) (string, error) {
			return input, nil
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	exec, err := svc.StartFlow(context.Background(), "sequenced-flow", "input", StartOptions{
		Middlewares: []StartMiddleware{NewSequencedStartMiddleware(10, middleware("call"))},
	})
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if len(order) != 3 || order[0] != "call" || order[1] != "flow" || order[2] != "engine" {
		t.Fatalf("middleware order = %v, want [call flow engine]", order)
	}
}

func TestStartMiddlewareRedirectsFlowAndToken(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	for _, name := range []string{"requested", "proxy"} {
		name := name
		if err := svc.RegisterFlow(Flow{Name: name, Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, input string, _ Runtime) (string, error) {
			return input, nil
		}}}}); err != nil {
			t.Fatalf("register %s flow: %v", name, err)
		}
	}
	svc.RegisterStartMiddleware(StartMiddlewareFunc(func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
		request.FlowName = "proxy"
		request.StartToken += "-redirected"
		return next(ctx, request)
	}))

	exec, err := svc.StartFlow(context.Background(), "requested", "input")
	if err != nil {
		t.Fatalf("start redirected flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.FlowName != "proxy" || exec.EndToken != "input-redirected" {
		t.Fatalf("redirected execution = flow %q output %q", exec.FlowName, exec.EndToken)
	}
}

func TestPerCallStartMiddlewareCanReturnExistingExecution(t *testing.T) {
	svc := NewService(nil)
	existing := &FlowExecutionContext{FlowName: "existing", FlowInstanceID: 42}
	exec, err := svc.StartFlow(context.Background(), "not-registered", "ignored", StartOptions{
		Middlewares: []StartMiddleware{StartMiddlewareFunc(func(_ context.Context, _ StartRequest, _ StartFlowNext) (*FlowExecutionContext, error) {
			return existing, nil
		})},
	})
	if err != nil {
		t.Fatalf("start through short-circuit middleware: %v", err)
	}
	if exec != existing {
		t.Fatalf("execution = %#v, want existing execution %#v", exec, existing)
	}
}

func TestPerCallStartMiddlewareRunsForTriggeredChildFlow(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{Name: "child-proxy", Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, input string, _ Runtime) (string, error) {
		return input + "-child-proxy", nil
	}}}}); err != nil {
		t.Fatalf("register child proxy flow: %v", err)
	}
	if err := svc.RegisterFlow(Flow{Name: "parent", Nodes: []FlowNode{{Name: "run", Action: func(ctx context.Context, input string, runtime Runtime) (string, error) {
		if err := runtime.TriggerFlow(ctx, "child", input); err != nil {
			return "", err
		}
		return input, nil
	}}}}); err != nil {
		t.Fatalf("register parent flow: %v", err)
	}
	engineChildMiddlewareCalled := make(chan struct{}, 1)
	svc.RegisterStartMiddleware(StartMiddlewareFunc(func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
		if request.FlowName == "child" {
			engineChildMiddlewareCalled <- struct{}{}
		}
		return next(ctx, request)
	}))
	childMiddlewareCalled := make(chan struct{}, 1)
	exec, err := svc.StartFlow(context.Background(), "parent", "input", StartOptions{
		Middlewares: []StartMiddleware{StartMiddlewareFunc(func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
			if request.FlowName == "child" {
				childMiddlewareCalled <- struct{}{}
				request.FlowName = "child-proxy"
			}
			return next(ctx, request)
		})},
	})
	if err != nil {
		t.Fatalf("start parent flow: %v", err)
	}
	select {
	case <-childMiddlewareCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("per-call middleware was not invoked for triggered child flow")
	}
	select {
	case <-engineChildMiddlewareCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("engine middleware was not invoked for triggered child flow")
	}
	child := waitForFlowInstanceByNameAndStatus(t, svc.store, "child-proxy", FlowStatusFinished)
	if child.OutputToken == nil || child.OutputToken.RawData != "input-child-proxy" {
		t.Fatalf("triggered child output = %#v", child.OutputToken)
	}
	waitForDone(t, exec.Done())
}

func TestActionMiddlewaresComposeForEngineAndPerCall(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{Name: "action-middleware", Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, input string, _ Runtime) (string, error) {
		return input, nil
	}}}}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	svc.RegisterActionMiddleware(ActionMiddlewareFunc(func(next Action) Action {
		return func(ctx context.Context, input string, runtime Runtime) (string, error) {
			output, err := next(ctx, input, runtime)
			return output + "-engine", err
		}
	}))

	exec, err := svc.StartFlow(context.Background(), "action-middleware", "input", StartOptions{
		ActionMiddlewares: []ActionMiddleware{ActionMiddlewareFunc(func(next Action) Action {
			return func(ctx context.Context, input string, runtime Runtime) (string, error) {
				output, err := next(ctx, input, runtime)
				return output + "-call", err
			}
		})},
	})
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.EndToken != "input-call-engine" {
		t.Fatalf("end token = %q, want input-call-engine", exec.EndToken)
	}
}

func TestRuntimeNewTokenBuilderUsesCurrentFlowAndNode(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{Name: "runtime-flow", Nodes: []FlowNode{{Name: "prepare-state", Action: func(_ context.Context, _ string, runtime Runtime) (string, error) {
		return runtime.NewTokenBuilder(map[string]any{"value": "ready"}).Build()
	}}}}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	exec, err := svc.StartFlow(context.Background(), "runtime-flow", "input")
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	reader := tokens.NewTokenReader(exec.EndToken)
	if err := reader.Err(); err != nil {
		t.Fatalf("read runtime token: %v", err)
	}
	if flowName, ok := reader.Meta(TokenMetaFlowName); !ok || flowName != "runtime-flow" {
		t.Fatalf("flow metadata = %#v, want runtime-flow", flowName)
	}
	if nodeName, ok := reader.Meta(TokenMetaFlowNode); !ok || nodeName != "prepare-state" {
		t.Fatalf("node metadata = %#v, want prepare-state", nodeName)
	}
	if got := reader.Type(); got != FlowTokenTypeNode {
		t.Fatalf("token type = %q, want %q", got, FlowTokenTypeNode)
	}
	var state map[string]string
	if err := reader.State(&state); err != nil {
		t.Fatalf("decode state set: %v", err)
	}
	if state["value"] != "ready" {
		t.Fatalf("state = %#v, want ready value", state)
	}
}
