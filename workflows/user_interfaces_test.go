package workflows

import (
	"context"
	"errors"
	"testing"
)

type testGreeter interface {
	Greet() string
}

type testGreeterValue string

func (g testGreeterValue) Greet() string { return string(g) }

func TestFlowUserInterfaceCanBeResolvedFromNode(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{
		Name:           "dependency-flow",
		UserInterfaces: []any{testGreeterValue("hello from flow DI")},
		Nodes: []FlowNode{{Name: "use-dependency", Action: func(_ context.Context, _ string, runtime Runtime) (string, error) {
			greeter, err := GetInterface[testGreeter](runtime)
			if err != nil {
				return "", err
			}
			return greeter.Greet(), nil
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}

	exec, err := svc.StartFlow(context.Background(), "dependency-flow", "input")
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFinished || exec.EndToken != "hello from flow DI" {
		t.Fatalf("execution status/output = %s/%q, error %v", exec.Status, exec.EndToken, exec.Error)
	}
}

func TestGetInterfaceRejectsAmbiguousRegistrations(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{
		Name:           "ambiguous-dependency-flow",
		UserInterfaces: []any{testGreeterValue("one"), testGreeterValue("two")},
		Nodes: []FlowNode{{Name: "use-dependency", Action: func(_ context.Context, _ string, runtime Runtime) (string, error) {
			greeter, err := GetInterface[testGreeter](runtime)
			if err != nil {
				return "", err
			}
			return greeter.Greet(), nil
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}

	exec, err := svc.StartFlow(context.Background(), "ambiguous-dependency-flow", "input")
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.Status != FlowStatusFailed || !errors.Is(exec.Error, ErrAmbiguousUserInterface) {
		t.Fatalf("execution status/error = %s/%v, want ambiguous dependency error", exec.Status, exec.Error)
	}
}

func TestStartMiddlewareCanResolveFlowUserInterface(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{
		Name:           "start-middleware-dependency-flow",
		UserInterfaces: []any{testGreeterValue("middleware dependency")},
		Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, input string, _ Runtime) (string, error) {
			return input, nil
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	got := make(chan string, 1)
	svc.RegisterStartMiddleware(StartMiddlewareFunc(func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
		greeter, err := GetInterface[testGreeter](request)
		if err != nil {
			return nil, err
		}
		got <- greeter.Greet()
		return next(ctx, request)
	}))

	if _, err := svc.StartFlow(context.Background(), "start-middleware-dependency-flow", "input"); err != nil {
		t.Fatalf("start flow: %v", err)
	}
	select {
	case value := <-got:
		if value != "middleware dependency" {
			t.Fatalf("resolved dependency value = %q", value)
		}
	default:
		t.Fatal("start middleware did not resolve flow dependency")
	}
}

func TestCallUserInterfaceOverridesFlowDefinitionInMiddlewareAndNode(t *testing.T) {
	svc := NewService(NewRepository(openFlowTestDB(t)))
	if err := AutoMigrate(svc.store.db); err != nil {
		t.Fatalf("auto migrate flow tables: %v", err)
	}
	if err := svc.RegisterFlow(Flow{
		Name:           "call-dependency-flow",
		UserInterfaces: []any{testGreeterValue("flow value")},
		Nodes: []FlowNode{{Name: "run", Action: func(_ context.Context, _ string, runtime Runtime) (string, error) {
			greeter, err := GetInterface[testGreeter](runtime)
			if err != nil {
				return "", err
			}
			return greeter.Greet(), nil
		}}},
	}); err != nil {
		t.Fatalf("register flow: %v", err)
	}
	middlewareValue := make(chan string, 1)
	svc.RegisterStartMiddleware(StartMiddlewareFunc(func(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
		greeter, err := GetInterface[testGreeter](request)
		if err != nil {
			return nil, err
		}
		middlewareValue <- greeter.Greet()
		return next(ctx, request)
	}))

	exec, err := svc.StartFlow(context.Background(), "call-dependency-flow", "input", StartOptions{
		UserInterfaces: []any{testGreeterValue("call value")},
	})
	if err != nil {
		t.Fatalf("start flow: %v", err)
	}
	waitForDone(t, exec.Done())
	if exec.EndToken != "call value" {
		t.Fatalf("node resolved %q, want call value", exec.EndToken)
	}
	select {
	case value := <-middlewareValue:
		if value != "call value" {
			t.Fatalf("middleware resolved %q, want call value", value)
		}
	default:
		t.Fatal("start middleware did not resolve call-specific dependency")
	}
}
