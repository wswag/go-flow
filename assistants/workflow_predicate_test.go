package assistants

import (
	"context"
	"errors"
	"testing"

	"github.com/wswag/go-flow/workflows"
)

type testWorkflowReader struct {
	statuses map[uint]workflows.FlowStatus
	err      error
}

func (r testWorkflowReader) FindFlowInstanceByID(_ context.Context, id uint) (workflows.FlowInstance, error) {
	if r.err != nil {
		return workflows.FlowInstance{}, r.err
	}
	status, ok := r.statuses[id]
	if !ok {
		return workflows.FlowInstance{}, errors.New("workflow missing")
	}
	return workflows.FlowInstance{Status: status}, nil
}

func TestAllWorkflowsCompleted(t *testing.T) {
	failure := errors.New("read failed")
	for _, test := range []struct {
		name      string
		ids       []uint
		reader    WorkflowInstanceReader
		want      bool
		wantError bool
	}{
		{name: "no references", reader: testWorkflowReader{}},
		{name: "all finished", ids: []uint{1, 2}, reader: testWorkflowReader{statuses: map[uint]workflows.FlowStatus{1: workflows.FlowStatusFinished, 2: workflows.FlowStatusFinished}}, want: true},
		{name: "failure is terminal", ids: []uint{1, 2}, reader: testWorkflowReader{statuses: map[uint]workflows.FlowStatus{1: workflows.FlowStatusFailed, 2: workflows.FlowStatusFinished}}, want: true},
		{name: "still running", ids: []uint{1, 2}, reader: testWorkflowReader{statuses: map[uint]workflows.FlowStatus{1: workflows.FlowStatusFinished, 2: workflows.FlowStatusRunning}}},
		{name: "stopped is not complete", ids: []uint{1}, reader: testWorkflowReader{statuses: map[uint]workflows.FlowStatus{1: workflows.FlowStatusStopped}}},
		{name: "invalid reference", ids: []uint{0}, reader: testWorkflowReader{}, wantError: true},
		{name: "typed nil reader", ids: []uint{1}, reader: (*workflows.Repository)(nil), wantError: true},
		{name: "missing reader", ids: []uint{1}, wantError: true},
		{name: "read error", ids: []uint{1}, reader: testWorkflowReader{err: failure}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := AllWorkflowsCompleted(test.reader)(context.Background(), PrepareContext{WorkflowIDs: test.ids})
			if got != test.want || (err != nil) != test.wantError {
				t.Fatalf("predicate = %v, %v; want %v, error %v", got, err, test.want, test.wantError)
			}
		})
	}
}
