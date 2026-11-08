package assistants

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wswag/go-flow/workflows"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openAssistantTestService(t *testing.T) *Service {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("migrate assistant db: %v", err)
	}
	return NewService(NewRepository(db))
}

func TestAssistantServiceAdvancesWithoutCommitHandler(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10, Input: map[string]any{"value": "a"}},
			{Name: "second", Sequence: 20, Input: map[string]any{"value": "b"}},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	step, chain, err := svc.CommitStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "first")
	if err != nil {
		t.Fatalf("commit step: %v", err)
	}
	if step.Name != "second" || chain.CurrentStep != "second" {
		t.Fatalf("current step = %q / %q, want second", step.Name, chain.CurrentStep)
	}
}

func TestAssistantServiceResetsStaleChainWhenPreparedWorkflowIsNoLongerRegistered(t *testing.T) {
	svc := openAssistantTestService(t)
	if err := svc.RegisterChain(ChainDefinition{
		Name: "stale-workflow-demo",
		Steps: []StepDefinition{
			{Name: "start", Sequence: 10, Input: map[string]any{"title": "Neu beginnen"}},
			{
				Name:     "workflow_wait",
				Sequence: 20,
				Prepare: func(context.Context, PrepareContext) (any, error) {
					return nil, workflows.ErrFlowNotRegistered
				},
			},
		},
	}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1", ActorKey: "actor:1"}
	if err := svc.repo.SaveRecord(context.Background(), &ChainRecord{
		Name:      "stale-workflow-demo",
		ScopeKey:  key.ScopeKey,
		ActorKey:  key.ActorKey,
		StateJSON: `{"currentStep":"workflow_wait","steps":{"start":{"completedAt":"2026-08-31T10:00:00Z"}}}`,
	}); err != nil {
		t.Fatalf("save stale state: %v", err)
	}

	step, chain, err := svc.CurrentStep(context.Background(), "stale-workflow-demo", key)
	if err != nil {
		t.Fatalf("CurrentStep() error = %v", err)
	}
	if step.Name != "start" || chain.CurrentStep != "start" || chain.Completed {
		t.Fatalf("reset chain = %#v / %#v, want first step", step, chain)
	}
	if _, err := svc.repo.FindRecord(context.Background(), "stale-workflow-demo", key); err == nil {
		t.Fatal("stale assistant state was not removed")
	}
}

func TestAssistantServiceAdvancePendingChainsHonorsAutoCommitPredicate(t *testing.T) {
	svc := openAssistantTestService(t)
	allWorkflowPartsFinished := false
	prepareCalls := 0
	workflowID := uint(41)
	if err := svc.RegisterChain(ChainDefinition{
		Name:               "waiting-demo",
		CompletionBehavior: ChainCompletionKeepComplete,
		Steps: []StepDefinition{
			{Name: "input", Sequence: 10},
			{
				Name:     "workflow_wait",
				Sequence: 20,
				Input:    map[string]any{"workflowId": workflowID},
				Prepare: func(context.Context, PrepareContext) (any, error) {
					prepareCalls++
					return map[string]any{}, nil
				},
				WorkflowReference: func(_ context.Context, result ClientResultContext) (uint, error) {
					var ref struct {
						WorkflowID uint `json:"workflowId"`
					}
					if err := json.Unmarshal(result.Step.Input, &ref); err != nil {
						return 0, err
					}
					return ref.WorkflowID, nil
				},
				AutoCommit: AutoCommitDefinition{Predicate: func(context.Context, PrepareContext) (bool, error) {
					return allWorkflowPartsFinished, nil
				}},
			},
		},
	}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1", ActorKey: "actor:1"}
	if err := svc.repo.SaveRecord(context.Background(), &ChainRecord{
		Name:      "waiting-demo",
		ScopeKey:  key.ScopeKey,
		ActorKey:  key.ActorKey,
		StateJSON: `{"currentStep":"workflow_wait","steps":{}}`,
	}); err != nil {
		t.Fatalf("save waiting state: %v", err)
	}
	events, unsubscribe := svc.Subscribe()
	defer unsubscribe()

	chains, err := svc.AdvancePendingChains(context.Background(), key, 99)
	if err != nil {
		t.Fatalf("advance incomplete workflows: %v", err)
	}
	if len(chains) != 0 || prepareCalls != 0 {
		t.Fatalf("unrelated workflow event advanced chain: chains=%#v prepareCalls=%d", chains, prepareCalls)
	}

	allWorkflowPartsFinished = true
	chains, err = svc.AdvancePendingChains(context.Background(), key, workflowID)
	if err != nil {
		t.Fatalf("advance completed workflows: %v", err)
	}
	if len(chains) != 1 || chains[0].CurrentStep != "workflow_wait" || !chains[0].Completed {
		t.Fatalf("completed workflow state = %#v, want completed chain", chains)
	}
	<-events
}

func TestAssistantServiceAdvancePendingChainsSkipsUnrelatedPrepare(t *testing.T) {
	svc := openAssistantTestService(t)
	prepareCalls := 0
	if err := svc.RegisterChain(ChainDefinition{
		Name: "unrelated-wait-demo",
		Steps: []StepDefinition{
			{Name: "input", Sequence: 10},
			{
				Name:     "workflow_wait",
				Sequence: 20,
				Input:    map[string]any{"workflowId": 41},
				Prepare: func(context.Context, PrepareContext) (any, error) {
					prepareCalls++
					return nil, nil
				},
				WorkflowReference: func(_ context.Context, result ClientResultContext) (uint, error) {
					var ref struct {
						WorkflowID uint `json:"workflowId"`
					}
					if err := json.Unmarshal(result.Step.Input, &ref); err != nil {
						return 0, err
					}
					return ref.WorkflowID, nil
				},
				AutoCommit: AutoCommitDefinition{Predicate: func(context.Context, PrepareContext) (bool, error) { return true, nil }},
			},
		},
	}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1", ActorKey: "actor:1"}
	if err := svc.repo.SaveRecord(context.Background(), &ChainRecord{
		Name:      "unrelated-wait-demo",
		ScopeKey:  key.ScopeKey,
		ActorKey:  key.ActorKey,
		StateJSON: `{"currentStep":"workflow_wait","steps":{}}`,
	}); err != nil {
		t.Fatalf("save waiting state: %v", err)
	}
	chains, err := svc.AdvancePendingChains(context.Background(), key, 42)
	if err != nil {
		t.Fatalf("advance unrelated workflow: %v", err)
	}
	if len(chains) != 0 || prepareCalls != 0 {
		t.Fatalf("unrelated event evaluated workflow prepare: chains=%#v prepareCalls=%d", chains, prepareCalls)
	}
}

func TestAssistantServiceAdvancePendingChainsOnlyAdvancesMatchingWorkflow(t *testing.T) {
	svc := openAssistantTestService(t)
	prepareCalls := map[string]int{}
	for _, test := range []struct {
		name       string
		workflowID uint
	}{
		{name: "wait-a", workflowID: 41},
		{name: "wait-b", workflowID: 42},
	} {
		test := test
		if err := svc.RegisterChain(ChainDefinition{
			Name: test.name,
			Steps: []StepDefinition{
				{Name: "input", Sequence: 10},
				{
					Name:     "workflow_wait",
					Sequence: 20,
					Input:    map[string]any{"workflowId": test.workflowID},
					Prepare: func(context.Context, PrepareContext) (any, error) {
						prepareCalls[test.name]++
						return nil, nil
					},
					WorkflowReference: func(_ context.Context, result ClientResultContext) (uint, error) {
						var ref struct {
							WorkflowID uint `json:"workflowId"`
						}
						if err := json.Unmarshal(result.Step.Input, &ref); err != nil {
							return 0, err
						}
						return ref.WorkflowID, nil
					},
					AutoCommit: AutoCommitDefinition{Predicate: func(context.Context, PrepareContext) (bool, error) { return true, nil }},
				},
			},
		}); err != nil {
			t.Fatalf("register %s: %v", test.name, err)
		}
		key := StateKey{ScopeKey: "scope:1", ActorKey: "actor:1"}
		if err := svc.repo.SaveRecord(context.Background(), &ChainRecord{
			Name:      test.name,
			ScopeKey:  key.ScopeKey,
			ActorKey:  key.ActorKey,
			StateJSON: `{"currentStep":"workflow_wait","steps":{}}`,
		}); err != nil {
			t.Fatalf("save %s: %v", test.name, err)
		}
	}

	key := StateKey{ScopeKey: "scope:1", ActorKey: "actor:1"}
	chains, err := svc.AdvancePendingChains(context.Background(), key, 41)
	if err != nil {
		t.Fatalf("advance matching workflow: %v", err)
	}
	if len(chains) != 1 || chains[0].Name != "wait-a" {
		t.Fatalf("advanced chains = %#v, want only wait-a", chains)
	}
	if prepareCalls["wait-a"] != 1 || prepareCalls["wait-b"] != 0 {
		t.Fatalf("prepare calls = %#v, want only wait-a prepared", prepareCalls)
	}
}

func TestAssistantServiceResetsToFirstStepAfterLastCommitByDefault(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10, Input: map[string]any{"value": "a"}},
			{Name: "second", Sequence: 20, Input: map[string]any{"value": "b"}},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	step, chain, err := svc.CommitStep(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("commit second: %v", err)
	}
	if step.Name != "first" || chain.CurrentStep != "first" {
		t.Fatalf("current step after reset = %q / %q, want first", step.Name, chain.CurrentStep)
	}
	if chain.Completed {
		t.Fatalf("chain should not remain completed after default reset")
	}
	for _, chainStep := range chain.Steps {
		if chainStep.Completed {
			t.Fatalf("step %q should be reset after final commit", chainStep.Name)
		}
	}
}

func TestAssistantServicePrepareAndPatchMergeInput(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10, Input: map[string]any{"name": "A"}},
			{
				Name:     "second",
				Sequence: 20,
				Input:    map[string]any{"kind": "base"},
				Prepare: func(ctx context.Context, prepare PrepareContext) (any, error) {
					first, _ := prepare.Step("first")
					return map[string]any{
						"source": json.RawMessage(first.Input),
					}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	if _, _, err := svc.UpdateStepInput(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "second", map[string]any{
		"kind": "patched",
	}); err != nil {
		t.Fatalf("update step input: %v", err)
	}
	step, _, err := svc.Step(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "second")
	if err != nil {
		t.Fatalf("load step: %v", err)
	}
	var input map[string]any
	if err := json.Unmarshal(step.Input, &input); err != nil {
		t.Fatalf("unmarshal input: %v", err)
	}
	if input["kind"] != "patched" {
		t.Fatalf("kind = %#v, want patched", input["kind"])
	}
	if _, ok := input["source"]; !ok {
		t.Fatalf("expected prepared source in input: %#v", input)
	}
}

func TestAssistantServiceCurrentStepDoesNotPrepareFutureSteps(t *testing.T) {
	svc := openAssistantTestService(t)
	futurePrepareCalls := 0
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10, Input: map[string]any{"name": "A"}},
			{
				Name:     "second",
				Sequence: 20,
				Prepare: func(ctx context.Context, prepare PrepareContext) (any, error) {
					futurePrepareCalls++
					return map[string]any{"prepared": true}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	if _, _, err := svc.CurrentStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}); err != nil {
		t.Fatalf("current step: %v", err)
	}
	if futurePrepareCalls != 0 {
		t.Fatalf("future prepare calls after current step = %d, want 0", futurePrepareCalls)
	}

	step, _, err := svc.Step(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "second")
	if err != nil {
		t.Fatalf("future step: %v", err)
	}
	if futurePrepareCalls != 1 {
		t.Fatalf("future prepare calls after targeted step = %d, want 1", futurePrepareCalls)
	}
	var input map[string]any
	if err := json.Unmarshal(step.Input, &input); err != nil {
		t.Fatalf("unmarshal input: %v", err)
	}
	if input["prepared"] != true {
		t.Fatalf("prepared input = %#v, want true", input["prepared"])
	}
}

func TestAssistantServicePersistsLargeStepInputPatch(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10, Input: map[string]any{"title": "Start"}},
			{Name: "second", Sequence: 20},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	largeNote := strings.Repeat("feedback-", 80)
	key := StateKey{ScopeKey: "scope:1"}
	step, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "first", map[string]any{
		"note": largeNote,
	})
	if err != nil {
		t.Fatalf("update large input patch: %v", err)
	}

	var input map[string]any
	if err := json.Unmarshal(step.Input, &input); err != nil {
		t.Fatalf("unmarshal step input: %v", err)
	}
	if input["note"] != largeNote {
		t.Fatalf("note length = %d, want %d", len(input["note"].(string)), len(largeNote))
	}

	reloaded, _, err := svc.Step(context.Background(), "demo", key, "first")
	if err != nil {
		t.Fatalf("reload step: %v", err)
	}
	input = map[string]any{}
	if err := json.Unmarshal(reloaded.Input, &input); err != nil {
		t.Fatalf("unmarshal reloaded input: %v", err)
	}
	if input["note"] != largeNote {
		t.Fatalf("reloaded note length = %d, want %d", len(input["note"].(string)), len(largeNote))
	}
}

func TestAssistantServiceCommitStepWithInputPatchPersistsAndAdvances(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				Input:    map[string]any{"title": "Start"},
				Commit: func(ctx context.Context, commit CommitContext) (*CommitOutcome, error) {
					var input map[string]any
					if err := json.Unmarshal(commit.Step.Input, &input); err != nil {
						return nil, err
					}
					if input["note"] != "fast" {
						t.Fatalf("commit input note = %#v, want fast", input["note"])
					}
					return &CommitOutcome{NextStep: "second"}, nil
				},
			},
			{
				Name:     "second",
				Sequence: 20,
				Prepare: func(ctx context.Context, prepare PrepareContext) (any, error) {
					first, _ := prepare.Step("first")
					return map[string]any{"firstInput": json.RawMessage(first.Input)}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	step, chain, err := svc.CommitStepWithInputPatch(context.Background(), "demo", key, "first", map[string]any{
		"note": "fast",
	})
	if err != nil {
		t.Fatalf("commit with input patch: %v", err)
	}
	if step.Name != "second" || chain.CurrentStep != "second" {
		t.Fatalf("current step = %q / %q, want second", step.Name, chain.CurrentStep)
	}
	var secondInput struct {
		FirstInput map[string]any `json:"firstInput"`
	}
	if err := json.Unmarshal(step.Input, &secondInput); err != nil {
		t.Fatalf("unmarshal second input: %v", err)
	}
	if secondInput.FirstInput["note"] != "fast" {
		t.Fatalf("prepared first input note = %#v, want fast", secondInput.FirstInput["note"])
	}

	first, _, err := svc.Step(context.Background(), "demo", key, "first")
	if err != nil {
		t.Fatalf("reload first step: %v", err)
	}
	var firstInput map[string]any
	if err := json.Unmarshal(first.Input, &firstInput); err != nil {
		t.Fatalf("unmarshal first input: %v", err)
	}
	if firstInput["note"] != "fast" {
		t.Fatalf("persisted first input note = %#v, want fast", firstInput["note"])
	}
}

func TestAssistantServiceAutoCommitAdvancesCurrentStep(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{
				Name:     "wait",
				Sequence: 20,
				AutoCommit: AutoCommitDefinition{
					Predicate: func(ctx context.Context, prepare PrepareContext) (bool, error) {
						return true, nil
					},
					Commit: func(ctx context.Context, commit CommitContext) (*CommitOutcome, error) {
						return &CommitOutcome{
							Result: map[string]any{"done": true},
						}, nil
					},
				},
			},
			{Name: "done", Sequence: 30},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	step, _, err := svc.CurrentStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"})
	if err != nil {
		t.Fatalf("current step: %v", err)
	}
	if step.Name != "done" {
		t.Fatalf("current step = %q, want done", step.Name)
	}
}

func TestAssistantServiceKeepsCompletedFinalStepWhenConfigured(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name:               "demo",
		CompletionBehavior: ChainCompletionKeepComplete,
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "done", Sequence: 20},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	step, chain, err := svc.CommitStep(context.Background(), "demo", key, "done")
	if err != nil {
		t.Fatalf("commit done: %v", err)
	}
	if step.Name != "done" || chain.CurrentStep != "done" {
		t.Fatalf("current step after completion = %q / %q, want done", step.Name, chain.CurrentStep)
	}
	if !chain.Completed || chain.CompletedAt == nil {
		t.Fatalf("chain should remain completed")
	}
	if !step.Completed {
		t.Fatalf("final step should remain completed")
	}
}

func TestAssistantServiceRejectsCommitForCompletedChain(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name:               "demo",
		CompletionBehavior: ChainCompletionKeepComplete,
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err == nil {
		t.Fatalf("expected second commit to fail on completed chain")
	}
}

func TestAssistantServiceRejectsNonCurrentCommit(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "second", Sequence: 20},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	if _, _, err := svc.CommitStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "second"); err == nil {
		t.Fatalf("expected non-current step commit to fail")
	}
}

func TestAssistantServiceCommittedStepUpdateMarksFollowingStepsStale(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				Input:    map[string]any{"value": "a"},
				UpdatePolicy: UpdatePolicyDefinition{
					CanUpdate: func(ctx context.Context, update UpdatePolicyContext) (bool, error) {
						return true, nil
					},
				},
			},
			{
				Name:     "second",
				Sequence: 20,
				Prepare: func(ctx context.Context, prepare PrepareContext) (any, error) {
					first, _ := prepare.Step("first")
					return map[string]any{"source": json.RawMessage(first.Input)}, nil
				},
			},
			{Name: "third", Sequence: 30},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "second"); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	if _, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "first", map[string]any{"value": "b"}); err != nil {
		t.Fatalf("update first step: %v", err)
	}

	first, chain, err := svc.Step(context.Background(), "demo", key, "first")
	if err != nil {
		t.Fatalf("load first step: %v", err)
	}
	if chain.CurrentStep != "first" {
		t.Fatalf("current step = %q, want first", chain.CurrentStep)
	}
	if first.Stale {
		t.Fatalf("updated step itself should not be stale")
	}
	second, _, err := svc.Step(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("load second step: %v", err)
	}
	if !second.Stale {
		t.Fatalf("second step should be stale after earlier step update")
	}
}

func TestAssistantServiceRecommittingUnchangedInputKeepsFollowingSteps(t *testing.T) {
	svc := openAssistantTestService(t)
	if err := svc.RegisterChain(ChainDefinition{
		Name:               "demo",
		CompletionBehavior: ChainCompletionKeepComplete,
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				UpdatePolicy: UpdatePolicyDefinition{
					CanUpdate: func(context.Context, UpdatePolicyContext) (bool, error) { return true, nil },
				},
			},
			{Name: "second", Sequence: 20},
		},
	}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStepWithInputPatch(context.Background(), "demo", key, "first", map[string]any{"value": "same"}); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "second"); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	if _, _, err := svc.JumpToStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("jump first: %v", err)
	}
	if _, _, err := svc.CommitStepWithInputPatch(context.Background(), "demo", key, "first", map[string]any{"value": "same"}); err != nil {
		t.Fatalf("recommit unchanged first: %v", err)
	}
	second, _, err := svc.Step(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("load second: %v", err)
	}
	if second.Stale {
		t.Fatalf("unchanged recommit marked following step stale")
	}
}

func TestAssistantServiceUpdatingUnchangedInputKeepsCurrentProgress(t *testing.T) {
	svc := openAssistantTestService(t)
	if err := svc.RegisterChain(ChainDefinition{
		Name:               "demo",
		CompletionBehavior: ChainCompletionKeepComplete,
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				UpdatePolicy: UpdatePolicyDefinition{
					CanUpdate: func(context.Context, UpdatePolicyContext) (bool, error) { return true, nil },
				},
			},
			{Name: "second", Sequence: 20},
		},
	}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStepWithInputPatch(context.Background(), "demo", key, "first", map[string]any{"value": "same"}); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "second"); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	if _, chain, err := svc.UpdateStepInput(context.Background(), "demo", key, "first", map[string]any{"value": "same"}); err != nil {
		t.Fatalf("update unchanged first: %v", err)
	} else if chain.CurrentStep != "second" {
		t.Fatalf("current step = %q, want second", chain.CurrentStep)
	}
	second, _, err := svc.Step(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("load second: %v", err)
	}
	if second.Stale {
		t.Fatalf("unchanged update marked following step stale")
	}
}

func TestAssistantServiceCommittedStepUpdateUsesDedicatedUpdateCommit(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				Commit: func(ctx context.Context, commit CommitContext) (*CommitOutcome, error) {
					return &CommitOutcome{Result: map[string]any{"mode": "initial"}}, nil
				},
				UpdatePolicy: UpdatePolicyDefinition{
					CanUpdate: func(ctx context.Context, update UpdatePolicyContext) (bool, error) {
						return true, nil
					},
					Commit: func(ctx context.Context, commit CommitContext) (*CommitOutcome, error) {
						return &CommitOutcome{Result: map[string]any{"mode": "update"}}, nil
					},
				},
			},
			{Name: "second", Sequence: 20},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "first", map[string]any{"value": "b"}); err != nil {
		t.Fatalf("update first input: %v", err)
	}
	step, _, err := svc.CommitStep(context.Background(), "demo", key, "first")
	if err != nil {
		t.Fatalf("update commit first: %v", err)
	}
	if step.Name != "second" {
		t.Fatalf("current step after update commit = %q, want second", step.Name)
	}
	first, _, err := svc.Step(context.Background(), "demo", key, "first")
	if err != nil {
		t.Fatalf("load first step: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal(first.Result, &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if result["mode"] != "update" {
		t.Fatalf("result mode = %#v, want update", result["mode"])
	}
}

func TestAssistantServiceRejectsCommittedStepUpdateWithoutPolicy(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "second", Sequence: 20},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "first", map[string]any{"value": "b"}); err == nil {
		t.Fatalf("expected committed step update without policy to fail")
	}
}

func TestAssistantServiceStepSnapshotSeparatesPreparedInputAndPatch(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				Input:    map[string]any{"base": "alpha"},
			},
			{
				Name:     "second",
				Sequence: 20,
				Input:    map[string]any{"kind": "default"},
				Prepare: func(ctx context.Context, prepare PrepareContext) (any, error) {
					first, _ := prepare.Step("first")
					return map[string]any{"prepared": json.RawMessage(first.Input)}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	step, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "second", map[string]any{
		"userField": "custom",
	})
	if err != nil {
		t.Fatalf("update second input: %v", err)
	}
	var prepared map[string]any
	if err := json.Unmarshal(step.PreparedInput, &prepared); err != nil {
		t.Fatalf("unmarshal prepared input: %v", err)
	}
	if _, ok := prepared["prepared"]; !ok {
		t.Fatalf("prepared input missing prepared field: %#v", prepared)
	}
	var patch map[string]any
	if err := json.Unmarshal(step.InputPatch, &patch); err != nil {
		t.Fatalf("unmarshal patch input: %v", err)
	}
	if patch["userField"] != "custom" {
		t.Fatalf("patch userField = %#v, want custom", patch["userField"])
	}
}

func TestAssistantServiceRejectsUnknownExplicitNextStep(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				Commit: func(ctx context.Context, commit CommitContext) (*CommitOutcome, error) {
					return &CommitOutcome{NextStep: "missing"}, nil
				},
			},
			{Name: "second", Sequence: 20},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}

	if _, _, err := svc.CommitStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "first"); err == nil {
		t.Fatalf("expected invalid explicit next step to fail")
	}
}

func TestAssistantServiceResetStepClearsFollowingState(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "second", Sequence: 20},
			{Name: "third", Sequence: 30},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "second"); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	step, _, err := svc.ResetStep(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("reset step: %v", err)
	}
	if step.Name != "second" {
		t.Fatalf("current step = %q, want second", step.Name)
	}
	second, chain, err := svc.Step(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("load second step: %v", err)
	}
	if second.Completed {
		t.Fatalf("second step should no longer be completed")
	}
	for _, chainStep := range chain.Steps {
		if chainStep.Name == "third" && chainStep.Completed {
			t.Fatalf("third step should no longer be completed")
		}
	}
}

func TestAssistantServiceResetStepRestoresPreparedInputWithoutPatches(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "first",
				Sequence: 10,
				Input:    map[string]any{"base": "alpha"},
			},
			{
				Name:     "second",
				Sequence: 20,
				Input:    map[string]any{"kind": "default"},
				Prepare: func(ctx context.Context, prepare PrepareContext) (any, error) {
					first, _ := prepare.Step("first")
					return map[string]any{
						"fromPrepare": json.RawMessage(first.Input),
					}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "second", map[string]any{
		"kind":        "patched",
		"userField":   "custom",
		"fromPrepare": map[string]any{"base": "override"},
	}); err != nil {
		t.Fatalf("update second input: %v", err)
	}
	stepBefore, _, err := svc.Step(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("load step before reset: %v", err)
	}
	var before map[string]any
	if err := json.Unmarshal(stepBefore.Input, &before); err != nil {
		t.Fatalf("unmarshal before input: %v", err)
	}
	if before["kind"] != "patched" {
		t.Fatalf("kind before reset = %#v, want patched", before["kind"])
	}

	stepAfter, _, err := svc.ResetStep(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("reset step: %v", err)
	}
	var after map[string]any
	if err := json.Unmarshal(stepAfter.Input, &after); err != nil {
		t.Fatalf("unmarshal after input: %v", err)
	}
	if after["kind"] != "default" {
		t.Fatalf("kind after reset = %#v, want default", after["kind"])
	}
	if _, ok := after["userField"]; ok {
		t.Fatalf("userField should be cleared after reset: %#v", after)
	}
	fromPrepare, ok := after["fromPrepare"].(map[string]any)
	if !ok {
		t.Fatalf("fromPrepare missing after reset: %#v", after)
	}
	if fromPrepare["base"] != "alpha" {
		t.Fatalf("prepared base after reset = %#v, want alpha", fromPrepare["base"])
	}
}

func TestAssistantServiceJumpToStepKeepsFollowingState(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "second", Sequence: 20},
			{Name: "third", Sequence: 30},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "second"); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	step, _, err := svc.JumpToStep(context.Background(), "demo", key, "first")
	if err != nil {
		t.Fatalf("jump to first: %v", err)
	}
	if step.Name != "first" {
		t.Fatalf("current step = %q, want first", step.Name)
	}
	third, chain, err := svc.Step(context.Background(), "demo", key, "third")
	if err != nil {
		t.Fatalf("load third step: %v", err)
	}
	if third.Completed {
		t.Fatalf("third should not be completed")
	}
	second, _, err := svc.Step(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("load second step: %v", err)
	}
	if !second.Completed {
		t.Fatalf("second should stay completed after jump")
	}
	if chain.CurrentStep != "first" {
		t.Fatalf("chain current step = %q, want first", chain.CurrentStep)
	}
}

func TestAssistantServiceJumpToStepRejectsUnreachedFutureStep(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "second", Sequence: 20},
			{Name: "third", Sequence: 30},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	if _, _, err := svc.JumpToStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "third"); err == nil {
		t.Fatalf("expected jump to unreached step to fail")
	}
}

func TestAssistantServiceJumpToStepAllowsExplicitUnreachedEntryStep(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "legacy-first", Sequence: 10},
			{Name: "new-entry", Sequence: 20, AllowJumpToUnreached: true},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	step, _, err := svc.JumpToStep(context.Background(), "demo", StateKey{ScopeKey: "scope:1"}, "new-entry")
	if err != nil {
		t.Fatalf("jump to explicitly allowed entry step: %v", err)
	}
	if step.Name != "new-entry" {
		t.Fatalf("current step = %q, want new-entry", step.Name)
	}
}

func TestAssistantServiceJumpToStepAllowsPersistedReachabilityProof(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{
				Name:     "source",
				Sequence: 10,
				Commit: func(_ context.Context, _ CommitContext) (*CommitOutcome, error) {
					return &CommitOutcome{Result: map[string]any{"workflowID": 42}}, nil
				},
			},
			{
				Name:     "workflow-wait",
				Sequence: 20,
				CanJumpToUnreached: func(_ context.Context, prepare PrepareContext) (bool, error) {
					source, ok := prepare.Step("source")
					return ok && hasJSONPayload(source.Result), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.JumpToStep(context.Background(), "demo", key, "workflow-wait"); err == nil {
		t.Fatalf("expected missing reachability proof to reject jump")
	}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "source"); err != nil {
		t.Fatalf("commit source: %v", err)
	}
	if _, _, err := svc.JumpToStep(context.Background(), "demo", key, "source"); err != nil {
		t.Fatalf("jump source: %v", err)
	}
	if _, _, err := svc.JumpToStep(context.Background(), "demo", key, "workflow-wait"); err != nil {
		t.Fatalf("jump to workflow wait with persisted proof: %v", err)
	}
}

func TestAssistantServiceJumpToStepAllowsReachedInputPatch(t *testing.T) {
	svc := openAssistantTestService(t)
	err := svc.RegisterChain(ChainDefinition{
		Name: "demo",
		Steps: []StepDefinition{
			{Name: "first", Sequence: 10},
			{Name: "second", Sequence: 20},
			{Name: "third", Sequence: 30},
		},
	})
	if err != nil {
		t.Fatalf("register chain: %v", err)
	}
	key := StateKey{ScopeKey: "scope:1"}
	if _, _, err := svc.CommitStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if _, _, err := svc.UpdateStepInput(context.Background(), "demo", key, "second", map[string]any{"choice": "filled"}); err != nil {
		t.Fatalf("update second: %v", err)
	}
	if _, _, err := svc.JumpToStep(context.Background(), "demo", key, "first"); err != nil {
		t.Fatalf("jump to first: %v", err)
	}
	step, _, err := svc.JumpToStep(context.Background(), "demo", key, "second")
	if err != nil {
		t.Fatalf("jump to second: %v", err)
	}
	if step.Name != "second" {
		t.Fatalf("current step = %q, want second", step.Name)
	}
}

func TestHasStepProgressKeepsPersistedResultReachableWhenStale(t *testing.T) {
	if !hasStepProgress(StepSnapshot{Stale: true, Result: json.RawMessage(`{"workflowId":42}`)}) {
		t.Fatal("a stale step with a persisted result should remain reachable")
	}
	if hasStepProgress(StepSnapshot{Stale: true, InputPatch: json.RawMessage(`{"choice":"changed"}`)}) {
		t.Fatal("a stale input patch alone should not make a step reachable")
	}
}

func TestAssistantServiceWaitClientStepReturnsOnlyExplicitProjection(t *testing.T) {
	svc := openAssistantTestService(t)
	if err := svc.RegisterChain(ChainDefinition{Name: "public", Steps: []StepDefinition{{
		Name:  "wait",
		Input: map[string]any{"secret": "must-not-leak", "visible": "source"},
		ClientResult: func(_ context.Context, result ClientResultContext) (any, error) {
			return map[string]any{"visible": "released", "step": result.Step.Name}, nil
		},
	}}}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	result, err := svc.WaitClientStep(context.Background(), "public", StateKey{ScopeKey: "public:1"}, "wait", nil, 0)
	if err != nil {
		t.Fatalf("wait public step: %v", err)
	}
	if result.StepName != "wait" || result.Pending {
		t.Fatalf("public wait result = %#v", result)
	}
	if strings.Contains(string(result.Result), "must-not-leak") || !strings.Contains(string(result.Result), "released") {
		t.Fatalf("public result leaked or omitted projection: %s", result.Result)
	}
	if strings.Contains(string(result.Step.Input), "must-not-leak") || strings.Contains(string(result.Chain.Steps[0].Input), "must-not-leak") {
		t.Fatalf("public wait snapshot leaked internal input: step=%s chain=%s", result.Step.Input, result.Chain.Steps[0].Input)
	}
	if !strings.Contains(string(result.Step.Input), "released") || !strings.Contains(string(result.Chain.Steps[0].Input), "released") {
		t.Fatalf("public wait snapshot did not use projection: step=%s chain=%s", result.Step.Input, result.Chain.Steps[0].Input)
	}
}

func TestAssistantServiceClientCurrentStepReturnsOnlyAllowListedFields(t *testing.T) {
	svc := openAssistantTestService(t)
	if err := svc.RegisterChain(ChainDefinition{Name: "client", Steps: []StepDefinition{{
		Name:         "form",
		Input:        map[string]any{"title": "Sichtbar", "secret": "must-not-leak"},
		ClientResult: ClientResultFromInputFields("title"),
	}}}); err != nil {
		t.Fatalf("register chain: %v", err)
	}
	step, chain, err := svc.ClientCurrentStep(context.Background(), "client", StateKey{ScopeKey: "scope:1"})
	if err != nil {
		t.Fatalf("load client current step: %v", err)
	}
	if step.Name != "form" || len(chain.Steps) != 1 {
		t.Fatalf("client snapshot = %#v / %#v", step, chain)
	}
	if strings.Contains(string(step.Input), "must-not-leak") || !strings.Contains(string(step.Input), "Sichtbar") {
		t.Fatalf("client input leaked or omitted projection: %s", step.Input)
	}
}
