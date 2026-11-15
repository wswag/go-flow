package workflows

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wswag/go-flow/tokens"
)

// TestWaitForFlowReturnsTerminalResultSetsImmediately verifies completed
// output sets are returned without waiting for the timeout.
func TestWaitForFlowReturnsTerminalResultSetsImmediately(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	token, err := tokens.BuildTokenPayload(map[string]any{"example.result": map[string]string{"title": "Fertig"}}, map[string]any{"source": "test"})
	if err != nil {
		t.Fatal(err)
	}
	instance := &FlowInstance{Name: "done", Status: FlowStatusFinished, MetadataJSON: `{"source":"test"}`, StartedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := repo.CreateFlowInstance(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	output := &Token{RawData: token}
	if err := repo.CreateToken(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	instance.OutputTokenID = &output.ID
	if err := repo.UpdateFlowInstance(context.Background(), instance); err != nil {
		t.Fatal(err)
	}

	result, err := svc.WaitForFlow(context.Background(), instance.ID, FlowWaitOptions{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != FlowStatusFinished {
		t.Fatalf("status = %s", result.Status)
	}
	if string(result.ResultSets["example.result"]) != `{"title":"Fertig"}` {
		t.Fatalf("sets = %s", result.ResultSets["example.result"])
	}
	if result.Metadata["source"] != "test" {
		t.Fatalf("metadata = %#v", result.Metadata)
	}
}

// TestWaitForFlowReturnsEmptySetsAfterActiveTimeout verifies active output
// sets are suppressed when the wait timeout expires.
func TestWaitForFlowReturnsEmptySetsAfterActiveTimeout(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	instance := &FlowInstance{Name: "waiting", Status: FlowStatusWaiting, StartedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := repo.CreateFlowInstance(context.Background(), instance); err != nil {
		t.Fatal(err)
	}

	result, err := svc.WaitForFlow(context.Background(), instance.ID, FlowWaitOptions{Timeout: 15 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != FlowStatusWaiting {
		t.Fatalf("status = %s", result.Status)
	}
	if len(result.ResultSets) != 0 {
		t.Fatalf("unexpected result sets: %#v", result.ResultSets)
	}
}

// TestWaitingParentFailsWhenChildActionFails verifies a child failure reaches
// its waiting parent.
func TestWaitingParentFailsWhenChildActionFails(t *testing.T) {
	db := openFlowTestDB(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(db)
	svc := NewService(repo)
	childStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	if err := svc.RegisterFlow(Flow{Name: "failing-child", Nodes: []FlowNode{{Name: "run", Action: func(context.Context, string, Runtime) (string, error) {
		close(childStarted)
		<-releaseChild
		return "", errors.New("child failure")
	}}}}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterFlow(Flow{Name: "waiting-parent", Nodes: []FlowNode{{Name: "wait", Action: func(ctx context.Context, raw string, runtime Runtime) (string, error) {
		ref, err := runtime.StartFlow(ctx, "failing-child", raw)
		if err != nil {
			return "", err
		}
		result, err := runtime.AwaitFlow(ctx, ref)
		return string(result), err
	}}}}); err != nil {
		t.Fatal(err)
	}

	exec, err := svc.startFlowExecution(context.Background(), "waiting-parent", testEditablePayload(t, "input"))
	if err != nil {
		t.Fatal(err)
	}
	<-childStarted
	parent := waitForFlowInstanceByNameAndStatus(t, repo, "waiting-parent", FlowStatusRunning)
	close(releaseChild)
	result, err := svc.WaitForFlow(context.Background(), parent.ID, FlowWaitOptions{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != FlowStatusFailed {
		t.Fatalf("parent status = %s", result.Status)
	}
	waitForDone(t, exec.Done())
}
