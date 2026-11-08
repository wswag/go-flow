package assistants

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wswag/go-flow/workflows"

	"gorm.io/gorm"
)

type PrepareFunc func(ctx context.Context, prepare PrepareContext) (any, error)
type CommitFunc func(ctx context.Context, commit CommitContext) (*CommitOutcome, error)
type AutoCommitPredicate func(ctx context.Context, prepare PrepareContext) (bool, error)
type UpdatePolicyPredicate func(ctx context.Context, update UpdatePolicyContext) (bool, error)
type UpdatePolicyInvalidate func(ctx context.Context, update UpdatePolicyContext, following StepSnapshot) (bool, error)

// JumpToUnreachedPredicate permits a step to prove that it is reachable from
// already persisted assistant state although it has no own input patch or
// result yet. This is useful for workflow-wait steps whose parent reference is
// stored by the preceding transition.
type JumpToUnreachedPredicate func(ctx context.Context, prepare PrepareContext) (bool, error)
type ChainPreconditionFunc func(ctx context.Context, key StateKey) error

// ClientResultContext is the trusted server-side context from which a step
// explicitly projects the information its UI is allowed to receive.
// The raw workflow result is deliberately available only here; it is never
// serialized by the generic assistant service.
type ClientResultContext struct {
	Key      StateKey
	Chain    ChainSnapshot
	Step     StepSnapshot
	Workflow *workflows.WorkflowWaitResult
}

// ClientResultFunc defines the allow-list for one assistant step. Returning a
// value from this function is the only way workflow-derived data reaches a
// client response.
type ClientResultFunc func(ctx context.Context, result ClientResultContext) (any, error)

// WorkflowReferenceFunc identifies the single parent workflow a step waits
// for. It is intentionally step-local: normal synchronous steps simply omit
// it while retaining the same ClientResult contract.
type WorkflowReferenceFunc func(ctx context.Context, result ClientResultContext) (uint, error)

// ClientStepWaitResult is the safe transport contract for a step wait.
// It never exposes workflow metadata, token sets or the persisted assistant
// snapshot.
type ClientStepWaitResult struct {
	StepName string          `json:"stepName"`
	Pending  bool            `json:"pending"`
	Result   json.RawMessage `json:"result,omitempty"`
	// Chain und Step sind bereits über die ClientResult-Verträge projiziert.
	// Der HTTP-Rand kann damit nach einem Long-Poll dieselbe sichere Antwort
	// wie für einen normalen Assistant-Read ausliefern, ohne einen zweiten
	// internen Assistant-Read auszulösen.
	Chain ClientChainSnapshot `json:"-"`
	Step  ClientStepSnapshot  `json:"-"`
}

// ClientStepSnapshot contains the state metadata and explicitly projected UI
// model of one assistant step. It intentionally has no persisted input patch,
// commit result, timestamps or workflow details.
type ClientStepSnapshot struct {
	Name      string          `json:"name"`
	Sequence  int             `json:"sequence"`
	Input     json.RawMessage `json:"input"`
	Completed bool            `json:"completed"`
	Stale     bool            `json:"stale"`
}

// ClientChainSnapshot is the only chain representation intended for browser
// transports. Step contents originate exclusively from ClientResultFunc.
type ClientChainSnapshot struct {
	Name        string               `json:"name"`
	CurrentStep string               `json:"currentStep"`
	Completed   bool                 `json:"completed"`
	Steps       []ClientStepSnapshot `json:"steps"`
}

// ClientResultFromInput is the explicit contract for steps whose prepared
// input already is their complete UI model. It deliberately returns no chain
// result, input patch, workflow metadata or token data.
func ClientResultFromInput(_ context.Context, result ClientResultContext) (any, error) {
	if len(result.Step.Input) == 0 {
		return map[string]any{}, nil
	}
	var value any
	if err := json.Unmarshal(result.Step.Input, &value); err != nil {
		return nil, err
	}
	return value, nil
}

// ClientResultFromInputFields builds an explicit, stable allow-list for a
// client step. It is intended for ordinary form steps: callers name exactly
// the properties rendered or edited by their UI. Missing properties are
// omitted, and workflow references or server-only bookkeeping can therefore
// never leak accidentally through the assistant transport.
func ClientResultFromInputFields(fields ...string) ClientResultFunc {
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if normalized := strings.TrimSpace(field); normalized != "" {
			allowed[normalized] = struct{}{}
		}
	}
	return func(_ context.Context, result ClientResultContext) (any, error) {
		if len(result.Step.Input) == 0 {
			return map[string]any{}, nil
		}
		var input map[string]json.RawMessage
		if err := json.Unmarshal(result.Step.Input, &input); err != nil {
			return nil, err
		}
		output := make(map[string]json.RawMessage, len(allowed))
		for field := range allowed {
			if value, ok := input[field]; ok {
				output[field] = value
			}
		}
		return output, nil
	}
}

// AutoCommitDefinition beschreibt Schritte, die sich bei erfüllter Bedingung selbst weiterstellen.
type AutoCommitDefinition struct {
	Predicate AutoCommitPredicate
	Commit    CommitFunc
}

// UpdatePolicyDefinition beschreibt, ob und wie ein bereits erledigter Schritt nachträglich geändert werden darf.
type UpdatePolicyDefinition struct {
	CanUpdate           UpdatePolicyPredicate
	Commit              CommitFunc
	InvalidateFollowing UpdatePolicyInvalidate
}

type ChainCompletionBehavior string

const (
	ChainCompletionReset        ChainCompletionBehavior = "reset"
	ChainCompletionKeepComplete ChainCompletionBehavior = "keep_complete"
)

// StepDefinition beschreibt einen einzelnen Schritt innerhalb einer Kette.
type StepDefinition struct {
	Name                 string
	Sequence             int
	Input                any
	Prepare              PrepareFunc
	Commit               CommitFunc
	AutoCommit           AutoCommitDefinition
	UpdatePolicy         UpdatePolicyDefinition
	AllowJumpToUnreached bool
	CanJumpToUnreached   JumpToUnreachedPredicate
	WorkflowReference    WorkflowReferenceFunc
	ClientResult         ClientResultFunc
	// OncePerRun makes the first successful commit authoritative for this step
	// until the chain state is discarded and a new run is started.
	OncePerRun bool
}

// ChainDefinition registriert eine benannte Schrittkette mit sortierten Schritten.
type ChainDefinition struct {
	Name               string
	Steps              []StepDefinition
	Precondition       ChainPreconditionFunc
	CompletionBehavior ChainCompletionBehavior
}

// CommitOutcome erlaubt optional Ergebnisdaten und einen expliziten Folgeschritt.
type CommitOutcome struct {
	NextStep string
	Result   any
}

// StepSnapshot ist die gemergte Sicht eines Schritts für API und UI.
type StepSnapshot struct {
	Name              string          `json:"name"`
	Sequence          int             `json:"sequence"`
	Input             json.RawMessage `json:"input"`
	PreparedInput     json.RawMessage `json:"preparedInput,omitempty"`
	InputPatch        json.RawMessage `json:"inputPatch,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	Completed         bool            `json:"completed"`
	CompletedAt       *time.Time      `json:"completedAt,omitempty"`
	Stale             bool            `json:"stale"`
	StaleAt           *time.Time      `json:"staleAt,omitempty"`
	AutoCommitEnabled bool            `json:"autoCommitEnabled"`
}

// ChainSnapshot ist die aktuelle Sicht auf eine gesamte Schrittkette.
type ChainSnapshot struct {
	Name        string         `json:"name"`
	ScopeKey    string         `json:"scopeKey"`
	ActorKey    string         `json:"actorKey"`
	CurrentStep string         `json:"currentStep"`
	Completed   bool           `json:"completed"`
	CompletedAt *time.Time     `json:"completedAt,omitempty"`
	Steps       []StepSnapshot `json:"steps"`
}

// ChainEvent liefert einen serverseitig fortgeschriebenen Assistant-State an
// technische Event-Consumer. Scope und Actor bleiben bewusst generische Keys.
type ChainEvent struct {
	Chain ChainSnapshot
}

// PrepareContext stellt früheren Schritten und dem aktuellen Fachschlüssel gemergte Daten bereit.
type PrepareContext struct {
	ChainName         string
	Key               StateKey
	Current           string
	Steps             []StepSnapshot
	CurrentInputPatch json.RawMessage
	// CurrentResult is the persisted result of the currently prepared step.
	// Steps intentionally contains only earlier prepared steps, so a wait step
	// can otherwise not resolve a workflow reference it just replaced itself.
	CurrentResult json.RawMessage
}

// Step liefert einen bereits vorbereiteten Schritt aus dem aktuellen Kontext.
func (c PrepareContext) Step(name string) (StepSnapshot, bool) {
	needle := strings.TrimSpace(name)
	for _, step := range c.Steps {
		if step.Name == needle {
			return step, true
		}
	}
	return StepSnapshot{}, false
}

// CommitContext erweitert den PrepareContext um den tatsächlich zu quittierenden Schritt.
type CommitContext struct {
	PrepareContext
	Step StepSnapshot
}

// UpdatePolicyContext erweitert den PrepareContext für Entscheidungen über nachträgliche Schrittänderungen.
type UpdatePolicyContext struct {
	PrepareContext
	Step StepSnapshot
}

type Service struct {
	repo   *Repository
	chains map[string]compiledChain
	mu     sync.RWMutex
	// resolveMu serialisiert die Zustandsübergänge. CurrentStep darf
	// auto-commits auslösen; parallele GETs dürfen dabei nicht denselben
	// Übergang zweimal vorbereiten und committen.
	resolveMu sync.Mutex
	// commitMu serialisiert Commits inklusive fachlicher Workflow-Starts. Das
	// verhindert, dass parallele Requests denselben noch nicht referenzierten
	// Onboarding-Lauf doppelt anlegen.
	commitMu    sync.Mutex
	subscribers map[chan ChainEvent]struct{}
}

type compiledChain struct {
	name               string
	steps              []compiledStep
	stepByName         map[string]compiledStep
	precondition       ChainPreconditionFunc
	completionBehavior ChainCompletionBehavior
}

type compiledStep struct {
	Name                 string
	Sequence             int
	Input                json.RawMessage
	Prepare              PrepareFunc
	Commit               CommitFunc
	AutoCommit           AutoCommitDefinition
	UpdatePolicy         UpdatePolicyDefinition
	AllowJumpToUnreached bool
	CanJumpToUnreached   JumpToUnreachedPredicate
	WorkflowReference    WorkflowReferenceFunc
	ClientResult         ClientResultFunc
	OncePerRun           bool
}

type persistedChainState struct {
	CurrentStep string                        `json:"currentStep"`
	CompletedAt *time.Time                    `json:"completedAt,omitempty"`
	Steps       map[string]persistedStepState `json:"steps"`
}

type persistedStepState struct {
	InputPatch  json.RawMessage `json:"inputPatch,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
	StaleAt     *time.Time      `json:"staleAt,omitempty"`
}

func NewService(repo *Repository) *Service {
	return &Service{
		repo:        repo,
		chains:      map[string]compiledChain{},
		subscribers: map[chan ChainEvent]struct{}{},
	}
}

// NewRun creates an independent assistant run beneath a scope and actor.
// DraftID identifies the run; access metadata remains application-owned.
type NewRunOptions struct {
	DiscardOpen       bool
	ExpiresIn         time.Duration
	ExtendTTLOnAction bool
}

func (s *Service) NewRun(ctx context.Context, chainName string, key StateKey, accessRef, accessToken string, options ...NewRunOptions) (StateKey, error) {
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StateKey{}, err
	}
	if s.repo == nil {
		return StateKey{}, errors.New("assistant repository is not configured")
	}
	var idBytes [24]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return StateKey{}, err
	}
	key.ScopeKey = strings.TrimSpace(key.ScopeKey)
	key.ActorKey = strings.TrimSpace(key.ActorKey)
	key.DraftID = hex.EncodeToString(idBytes[:])
	state := persistedChainState{CurrentStep: s.firstStepName(chain), Steps: map[string]persistedStepState{}}
	data, err := json.Marshal(state)
	if err != nil {
		return StateKey{}, err
	}
	opt := NewRunOptions{}
	if len(options) > 0 {
		opt = options[0]
	}
	if opt.DiscardOpen {
		if err := s.repo.DiscardOpenRuns(ctx, chain.name, key); err != nil {
			return StateKey{}, err
		}
	}
	var expiresAt *time.Time
	if opt.ExpiresIn > 0 {
		expiry := time.Now().UTC().Add(opt.ExpiresIn)
		expiresAt = &expiry
	}
	err = s.repo.CreateRun(ctx, &ChainRecord{Name: chain.name, ScopeKey: key.ScopeKey, ActorKey: key.ActorKey, DraftID: key.DraftID, AccessRef: strings.TrimSpace(accessRef), AccessToken: accessToken, AccessExpiresAt: expiresAt, AutoExtendTTL: opt.ExtendTTLOnAction, RunTTLSeconds: int64(opt.ExpiresIn / time.Second), Status: string(RunStatusActive), StateJSON: string(data)})
	if err != nil {
		return StateKey{}, err
	}
	return key, nil
}

func (s *Service) extendRunAfterAction(ctx context.Context, name string, key StateKey) error {
	record, err := s.repo.FindRecord(ctx, name, key)
	if err != nil {
		return err
	}
	if record.AccessExpiresAt != nil && !record.AccessExpiresAt.After(time.Now().UTC()) {
		return ErrRunExpired
	}
	if !record.AutoExtendTTL || record.RunTTLSeconds <= 0 {
		return nil
	}
	return s.repo.SetRunExpiry(ctx, name, key, time.Now().UTC().Add(time.Duration(record.RunTTLSeconds)*time.Second))
}

// ResolveRun loads an unexpired run by its DraftID. The application validates AccessToken.
func (s *Service) ResolveRun(ctx context.Context, name string, key StateKey) (DraftSession, error) {
	record, err := s.repo.FindRecord(ctx, name, key)
	if err != nil {
		return DraftSession{}, err
	}
	if record.AccessExpiresAt != nil && !record.AccessExpiresAt.After(time.Now().UTC()) {
		return DraftSession{}, ErrRunExpired
	}
	return DraftSession{AssistantName: record.Name, Key: key, AccessRef: record.AccessRef, AccessToken: record.AccessToken, ExpiresAt: record.AccessExpiresAt}, nil
}

// ExtendRun refreshes the optional expiration deadline for a run.
func (s *Service) ExtendRun(ctx context.Context, name string, key StateKey, lifetime time.Duration) error {
	if lifetime <= 0 {
		return errors.New("run lifetime must be positive")
	}
	expiresAt := time.Now().UTC().Add(lifetime)
	return s.repo.SetRunExpiry(ctx, name, key, expiresAt)
}

// CleanupExpiredRuns removes expired runs and their action claims.
func (s *Service) CleanupExpiredRuns(ctx context.Context, limit int) (int64, error) {
	if s == nil || s.repo == nil {
		return 0, errors.New("assistant repository is not configured")
	}
	return s.repo.CleanupExpiredRuns(ctx, limit)
}

func (s *Service) CleanupRuns(ctx context.Context, options CleanupRunsOptions) (int64, error) {
	if s == nil || s.repo == nil {
		return 0, errors.New("assistant repository is not configured")
	}
	return s.repo.CleanupRuns(ctx, options)
}

// Subscribe liefert serverseitig fortgeschriebene Assistant-Chains.
func (s *Service) Subscribe() (chan ChainEvent, func()) {
	if s == nil {
		ch := make(chan ChainEvent)
		close(ch)
		return ch, func() {}
	}
	ch := make(chan ChainEvent, 16)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.subscribers, ch)
		s.mu.Unlock()
	}
}

// RegisterChain ergänzt eine neue Schrittkette im Service.
func (s *Service) RegisterChain(def ChainDefinition) error {
	name := strings.TrimSpace(def.Name)
	if name == "" {
		return errors.New("assistant chain name is required")
	}
	if len(def.Steps) == 0 {
		return fmt.Errorf("assistant chain %q requires at least one step", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.chains[name]; exists {
		return fmt.Errorf("assistant chain %q already registered", name)
	}
	completionBehavior := def.CompletionBehavior
	if completionBehavior == "" {
		completionBehavior = ChainCompletionReset
	}
	if completionBehavior != ChainCompletionReset && completionBehavior != ChainCompletionKeepComplete {
		return fmt.Errorf("assistant chain %q has unsupported completion behavior %q", name, completionBehavior)
	}
	steps := make([]compiledStep, 0, len(def.Steps))
	seen := map[string]struct{}{}
	for _, step := range def.Steps {
		stepName := strings.TrimSpace(step.Name)
		if stepName == "" {
			return fmt.Errorf("assistant chain %q has step without name", name)
		}
		if _, ok := seen[stepName]; ok {
			return fmt.Errorf("assistant chain %q has duplicate step %q", name, stepName)
		}
		seen[stepName] = struct{}{}
		defaultInput, err := marshalRaw(step.Input)
		if err != nil {
			return fmt.Errorf("assistant chain %q step %q default input: %w", name, stepName, err)
		}
		steps = append(steps, compiledStep{
			Name:                 stepName,
			Sequence:             step.Sequence,
			Input:                defaultInput,
			Prepare:              step.Prepare,
			Commit:               step.Commit,
			AutoCommit:           step.AutoCommit,
			UpdatePolicy:         step.UpdatePolicy,
			AllowJumpToUnreached: step.AllowJumpToUnreached,
			CanJumpToUnreached:   step.CanJumpToUnreached,
			WorkflowReference:    step.WorkflowReference,
			ClientResult:         step.ClientResult,
			OncePerRun:           step.OncePerRun,
		})
	}
	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].Sequence == steps[j].Sequence {
			return steps[i].Name < steps[j].Name
		}
		return steps[i].Sequence < steps[j].Sequence
	})
	for index, step := range steps {
		if step.Sequence <= 0 {
			steps[index].Sequence = (index + 1) * 10
		}
	}
	stepByName := make(map[string]compiledStep, len(steps))
	for _, step := range steps {
		stepByName[step.Name] = step
	}
	s.chains[name] = compiledChain{
		name:               name,
		steps:              steps,
		stepByName:         stepByName,
		precondition:       def.Precondition,
		completionBehavior: completionBehavior,
	}
	return nil
}

// CurrentStep liefert den aktuellen Schritt und quittiert Auto-Commit-Schritte bei Bedarf sofort durch.
func (s *Service) CurrentStep(ctx context.Context, chainName string, key StateKey) (StepSnapshot, ChainSnapshot, error) {
	snapshot, err := s.resolveCurrentChain(ctx, chainName, key, true)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	for _, step := range snapshot.Steps {
		if step.Name == snapshot.CurrentStep {
			s.logAction("current_step", chainName, key, step.Name)
			return step, snapshot, nil
		}
	}
	return StepSnapshot{}, snapshot, fmt.Errorf("assistant chain %q has no current step", strings.TrimSpace(chainName))
}

// AdvancePendingChains verarbeitet wartende Auto-Commit-Schritte nach einem
// externen Terminal-Event. Dadurch bleibt das Lesen eines Assistants frei von
// der Aufgabe, einen Workflow-Abschluss erst auszulösen.
func (s *Service) AdvancePendingChains(ctx context.Context, key StateKey, workflowID uint) ([]ChainSnapshot, error) {
	if s == nil || s.repo == nil {
		return nil, nil
	}
	records, err := s.repo.FindRecordsByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	updated := make([]ChainSnapshot, 0, len(records))
	for _, record := range records {
		chain, chainErr := s.compiledChain(record.Name)
		if chainErr != nil {
			continue
		}
		if preconditionErr := s.checkPrecondition(ctx, chain, key); preconditionErr != nil {
			continue
		}
		state, loadErr := s.loadState(ctx, chain.name, key)
		if loadErr != nil {
			return updated, loadErr
		}
		currentStepName := s.resolveCurrentStepName(chain, state)
		current, ok := chain.stepByName[currentStepName]
		if !ok || current.AutoCommit.Predicate == nil {
			continue
		}
		if workflowID > 0 {
			if current.WorkflowReference == nil {
				continue
			}
			step, snapshot, snapshotErr := s.unpreparedCurrentStepSnapshot(chain, key, state, currentStepName)
			if snapshotErr != nil {
				return updated, snapshotErr
			}
			clientContext := ClientResultContext{Key: key, Chain: snapshot, Step: step}
			waitingFor, referenceErr := current.WorkflowReference(ctx, clientContext)
			if referenceErr != nil {
				return updated, referenceErr
			}
			if waitingFor != workflowID {
				continue
			}
		}
		next, advanceErr := s.resolveCurrentChain(ctx, chain.name, key, true)
		if advanceErr != nil {
			return updated, advanceErr
		}
		updated = append(updated, next)
		s.broadcastChainEvent(ChainEvent{Chain: next})
	}
	return updated, nil
}

func (s *Service) unpreparedCurrentStepSnapshot(chain compiledChain, key StateKey, state persistedChainState, stepName string) (StepSnapshot, ChainSnapshot, error) {
	currentStep := s.resolveCurrentStepName(chain, state)
	snapshots := make([]StepSnapshot, 0, len(chain.steps))
	var currentSnapshot StepSnapshot
	for _, step := range chain.steps {
		persisted := state.Steps[step.Name]
		snapshot := StepSnapshot{
			Name:              step.Name,
			Sequence:          step.Sequence,
			Input:             mergeJSON(step.Input, persisted.InputPatch),
			InputPatch:        normalizeJSON(persisted.InputPatch),
			Result:            normalizeJSON(persisted.Result),
			Completed:         persisted.CompletedAt != nil,
			CompletedAt:       persisted.CompletedAt,
			Stale:             persisted.StaleAt != nil,
			StaleAt:           persisted.StaleAt,
			AutoCommitEnabled: step.AutoCommit.Predicate != nil,
		}
		snapshots = append(snapshots, snapshot)
		if step.Name == stepName {
			currentSnapshot = snapshot
		}
	}
	if currentSnapshot.Name == "" {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q could not be prepared", stepName)
	}
	return currentSnapshot, ChainSnapshot{
		Name:        chain.name,
		ScopeKey:    strings.TrimSpace(key.ScopeKey),
		ActorKey:    strings.TrimSpace(key.ActorKey),
		CurrentStep: currentStep,
		Completed:   state.CompletedAt != nil,
		CompletedAt: state.CompletedAt,
		Steps:       snapshots,
	}, nil
}

// ChainsForKey liefert gespeicherte Assistant-Snapshots für die Wiederaufnahme
// eines Event-Kanals. Die Methode löst bewusst keinen Auto-Commit aus.
func (s *Service) ChainsForKey(ctx context.Context, key StateKey) ([]ChainSnapshot, error) {
	if s == nil || s.repo == nil {
		return nil, nil
	}
	records, err := s.repo.FindRecordsByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	chains := make([]ChainSnapshot, 0, len(records))
	for _, record := range records {
		chain, chainErr := s.compiledChain(record.Name)
		if chainErr != nil {
			continue
		}
		if preconditionErr := s.checkPrecondition(ctx, chain, key); preconditionErr != nil {
			continue
		}
		state, loadErr := s.loadState(ctx, chain.name, key)
		if loadErr != nil {
			return chains, loadErr
		}
		snapshot, snapshotErr := s.buildChainSnapshot(ctx, chain, key, state)
		if snapshotErr != nil {
			return chains, snapshotErr
		}
		chains = append(chains, snapshot)
	}
	return chains, nil
}

// Chain liefert die aktuelle Kettensicht ohne Auto-Commit-Nachlauf.
func (s *Service) Chain(ctx context.Context, chainName string, key StateKey) (ChainSnapshot, error) {
	s.logAction("chain_snapshot", chainName, key, "")
	return s.resolveCurrentChain(ctx, chainName, key, false)
}

// Step liefert einen bestimmten vorbereiteten Schritt ohne den aktuellen Schritt zu verändern.
func (s *Service) Step(ctx context.Context, chainName string, key StateKey, stepName string) (StepSnapshot, ChainSnapshot, error) {
	s.logAction("step_snapshot", chainName, key, stepName)
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	name := strings.TrimSpace(stepName)
	if _, ok := chain.stepByName[name]; !ok {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", name)
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	snapshot, err := s.buildChainSnapshotPreparedThrough(ctx, chain, key, state, name)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	for _, step := range snapshot.Steps {
		if step.Name == name {
			return step, snapshot, nil
		}
	}
	return StepSnapshot{}, snapshot, fmt.Errorf("assistant step %q not found", name)
}

// WaitClientStep relays a single parent workflow wait through an assistant
// step. The service keeps the complete workflow snapshot private and returns
// only the step-specific allow-listed result.
//
// A client result definition is mandatory. This makes accidental exposure of
// workflow tokens, metadata or persisted assistant input impossible by
// default.
func (s *Service) WaitClientStep(ctx context.Context, chainName string, key StateKey, stepName string, waiter *workflows.Service, timeout time.Duration) (ClientStepWaitResult, error) {
	step, chain, err := s.CurrentStep(ctx, chainName, key)
	if err != nil {
		return ClientStepWaitResult{}, err
	}
	if strings.TrimSpace(step.Name) != strings.TrimSpace(stepName) {
		return ClientStepWaitResult{}, fmt.Errorf("assistant step %q is not current", strings.TrimSpace(stepName))
	}
	compiled, err := s.compiledChain(chainName)
	if err != nil {
		return ClientStepWaitResult{}, err
	}
	definition, ok := compiled.stepByName[step.Name]
	if !ok || definition.ClientResult == nil {
		return ClientStepWaitResult{}, fmt.Errorf("assistant step %q has no client result contract", step.Name)
	}

	clientContext := ClientResultContext{Key: key, Chain: chain, Step: step}
	pending := false
	if definition.WorkflowReference != nil {
		if waiter == nil {
			return ClientStepWaitResult{}, errors.New("workflow waiter is not configured")
		}
		workflowID, referenceErr := definition.WorkflowReference(ctx, clientContext)
		if referenceErr != nil {
			return ClientStepWaitResult{}, referenceErr
		}
		if workflowID > 0 {
			workflow, waitErr := waiter.WaitForWorkflow(ctx, workflowID, workflows.WorkflowWaitOptions{Timeout: timeout})
			if waitErr != nil {
				return ClientStepWaitResult{}, waitErr
			}
			clientContext.Workflow = &workflow
			pending = workflow.Status == workflows.FlowStatusRunning || workflow.Status == workflows.FlowStatusWaiting
			// Terminal workflow events may satisfy an auto-commit predicate. Read
			// once more before producing the projection, but never expose that
			// freshly loaded internal snapshot directly.
			if !pending {
				step, chain, err = s.CurrentStep(ctx, chainName, key)
				if err != nil {
					return ClientStepWaitResult{}, err
				}
				if strings.TrimSpace(step.Name) != strings.TrimSpace(stepName) {
					return s.clientStepWaitResult(ctx, key, chain, step, false, nil)
				}
				clientContext.Chain = chain
				clientContext.Step = step
			}
		}
	}
	result, resultErr := definition.ClientResult(ctx, clientContext)
	if resultErr != nil {
		return ClientStepWaitResult{}, resultErr
	}
	raw, marshalErr := marshalRaw(result)
	if marshalErr != nil {
		return ClientStepWaitResult{}, marshalErr
	}
	return s.clientStepWaitResult(ctx, key, chain, step, pending, raw)
}

func (s *Service) clientStepWaitResult(ctx context.Context, key StateKey, chain ChainSnapshot, step StepSnapshot, pending bool, result json.RawMessage) (ClientStepWaitResult, error) {
	clientChain, err := s.clientChainSnapshot(ctx, key, chain)
	if err != nil {
		return ClientStepWaitResult{}, err
	}
	clientStep, found := findClientStepSnapshot(clientChain.Steps, step.Name)
	if !found {
		return ClientStepWaitResult{}, fmt.Errorf("assistant step %q not found", step.Name)
	}
	if len(result) > 0 {
		clientStep.Input = append(json.RawMessage(nil), result...)
		for index := range clientChain.Steps {
			if clientChain.Steps[index].Name == clientStep.Name {
				clientChain.Steps[index].Input = append(json.RawMessage(nil), result...)
				break
			}
		}
	}
	return ClientStepWaitResult{
		StepName: step.Name,
		Pending:  pending,
		Result:   append(json.RawMessage(nil), result...),
		Chain:    clientChain,
		Step:     clientStep,
	}, nil
}

// ClientCurrentStep returns the current assistant state through its explicit
// ClientResult contracts. It is the normal browser-facing read path.
func (s *Service) ClientCurrentStep(ctx context.Context, chainName string, key StateKey) (ClientStepSnapshot, ClientChainSnapshot, error) {
	step, chain, err := s.CurrentStep(ctx, chainName, key)
	if err != nil {
		return ClientStepSnapshot{}, ClientChainSnapshot{}, err
	}
	clientChain, err := s.clientChainSnapshot(ctx, key, chain)
	if err != nil {
		return ClientStepSnapshot{}, ClientChainSnapshot{}, err
	}
	clientStep, found := findClientStepSnapshot(clientChain.Steps, step.Name)
	if !found {
		return ClientStepSnapshot{}, clientChain, fmt.Errorf("assistant step %q not found", step.Name)
	}
	return clientStep, clientChain, nil
}

// ClientStep returns one named step and the surrounding client-safe chain.
// It is deliberately available for earlier steps, so the SPA can rehydrate a
// form without receiving the persisted internal chain state.
func (s *Service) ClientStep(ctx context.Context, chainName string, key StateKey, stepName string) (ClientStepSnapshot, ClientChainSnapshot, error) {
	step, chain, err := s.Step(ctx, chainName, key, stepName)
	if err != nil {
		return ClientStepSnapshot{}, ClientChainSnapshot{}, err
	}
	clientChain, err := s.clientChainSnapshot(ctx, key, chain)
	if err != nil {
		return ClientStepSnapshot{}, ClientChainSnapshot{}, err
	}
	clientStep, found := findClientStepSnapshot(clientChain.Steps, step.Name)
	if !found {
		return ClientStepSnapshot{}, clientChain, fmt.Errorf("assistant step %q not found", step.Name)
	}
	return clientStep, clientChain, nil
}

func (s *Service) clientChainSnapshot(ctx context.Context, key StateKey, chain ChainSnapshot) (ClientChainSnapshot, error) {
	compiled, err := s.compiledChain(chain.Name)
	if err != nil {
		return ClientChainSnapshot{}, err
	}
	client := ClientChainSnapshot{
		Name:        chain.Name,
		CurrentStep: chain.CurrentStep,
		Completed:   chain.Completed,
		Steps:       make([]ClientStepSnapshot, 0, len(chain.Steps)),
	}
	for _, step := range chain.Steps {
		definition, found := compiled.stepByName[step.Name]
		if !found || definition.ClientResult == nil {
			return ClientChainSnapshot{}, fmt.Errorf("assistant step %q has no client result contract", step.Name)
		}
		projected, projectionErr := definition.ClientResult(ctx, ClientResultContext{Key: key, Chain: chain, Step: step})
		if projectionErr != nil {
			return ClientChainSnapshot{}, projectionErr
		}
		raw, marshalErr := marshalRaw(projected)
		if marshalErr != nil {
			return ClientChainSnapshot{}, marshalErr
		}
		client.Steps = append(client.Steps, ClientStepSnapshot{
			Name: step.Name, Sequence: step.Sequence, Input: raw, Completed: step.Completed, Stale: step.Stale,
		})
	}
	return client, nil
}

func findClientStepSnapshot(steps []ClientStepSnapshot, name string) (ClientStepSnapshot, bool) {
	for _, step := range steps {
		if step.Name == name {
			return step, true
		}
	}
	return ClientStepSnapshot{}, false
}

// UpdateStepInput merged einen JSON-Patch in den gespeicherten Eingabestatus des Schritts.
func (s *Service) UpdateStepInput(ctx context.Context, chainName string, key StateKey, stepName string, inputPatch any) (StepSnapshot, ChainSnapshot, error) {
	s.logAction("update_input", chainName, key, stepName)
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.extendRunAfterAction(ctx, chain.name, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	step, ok := chain.stepByName[strings.TrimSpace(stepName)]
	if !ok {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", strings.TrimSpace(stepName))
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if _, err := s.applyStepInputPatch(ctx, chain, key, &state, step.Name, inputPatch, true); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.saveState(ctx, chain.name, key, state); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	return s.Step(ctx, chain.name, key, step.Name)
}

func (s *Service) applyStepInputPatch(ctx context.Context, chain compiledChain, key StateKey, state *persistedChainState, stepName string, inputPatch any, validateAndInvalidateCommitted bool) (compiledStep, error) {
	step, ok := chain.stepByName[strings.TrimSpace(stepName)]
	if !ok {
		return compiledStep{}, fmt.Errorf("assistant step %q not found", strings.TrimSpace(stepName))
	}
	patchRaw, err := marshalRaw(inputPatch)
	if err != nil {
		return compiledStep{}, err
	}
	if state.Steps == nil {
		state.Steps = map[string]persistedStepState{}
	}
	stepState := state.Steps[step.Name]
	mergedInput := mergeJSON(stepState.InputPatch, patchRaw)
	inputChanged := !jsonRawEqual(stepState.InputPatch, mergedInput)
	// CommitStep prüft die Update-Policy anschließend auf dem bereits
	// zusammengeführten Input und invalidiert dort genau einmal.
	if stepState.CompletedAt != nil && validateAndInvalidateCommitted && inputChanged {
		chainSnapshot, buildErr := s.buildChainSnapshot(ctx, chain, key, *state)
		if buildErr != nil {
			return compiledStep{}, buildErr
		}
		currentStep, found := findStepSnapshot(chainSnapshot.Steps, step.Name)
		if !found {
			return compiledStep{}, fmt.Errorf("assistant step %q not found", step.Name)
		}
		allowed, allowErr := s.canUpdateCommittedStep(ctx, chain, chainSnapshot, key, currentStep)
		if allowErr != nil {
			return compiledStep{}, allowErr
		}
		if !allowed {
			return compiledStep{}, fmt.Errorf("assistant step %q does not allow updates after commit", step.Name)
		}
	}
	stepState.InputPatch = mergedInput
	state.Steps[step.Name] = stepState
	if stepState.CompletedAt != nil && validateAndInvalidateCommitted && inputChanged {
		s.logAction("revisit_step", chain.name, key, step.Name)
		invalidated := s.invalidateFollowingSteps(ctx, chain, key, state, step.Name)
		if invalidated {
			state.CurrentStep = step.Name
			state.CompletedAt = nil
		}
	} else if strings.TrimSpace(state.CurrentStep) == "" {
		state.CurrentStep = s.firstStepName(chain)
	}
	return step, nil
}

// CommitStep quittiert einen Schritt synchron und stellt die Kette auf den definierten Folgeschritt.
func (s *Service) CommitStep(ctx context.Context, chainName string, key StateKey, stepName string) (StepSnapshot, ChainSnapshot, error) {
	return s.commitStep(ctx, chainName, key, stepName, nil, false)
}

// CommitStepWithInput quittiert einen Schritt mit zusätzlichem transientem Input, ohne diesen zu persistieren.
func (s *Service) CommitStepWithInput(ctx context.Context, chainName string, key StateKey, stepName string, input any) (StepSnapshot, ChainSnapshot, error) {
	return s.commitStep(ctx, chainName, key, stepName, input, false)
}

// CommitStepWithInputPatch persistiert die Eingabe und quittiert den Schritt in einem Request.
func (s *Service) CommitStepWithInputPatch(ctx context.Context, chainName string, key StateKey, stepName string, inputPatch any) (StepSnapshot, ChainSnapshot, error) {
	return s.commitStep(ctx, chainName, key, stepName, inputPatch, true)
}

// PersistCommitOutcome speichert das Ergebnis eines bereits außerhalb der
// Assistant-Engine ausgeführten Commit-Schritts und setzt die Kette auf den
// angegebenen Folgeschritt. Das ist für Recovery-/Takeover-Pfade gedacht, die
// den fachlichen Workflow vorab starten müssen und daher den Commit-Callback
// nicht ein zweites Mal ausführen dürfen.
func (s *Service) PersistCommitOutcome(ctx context.Context, chainName string, key StateKey, stepName string, outcome CommitOutcome) (StepSnapshot, ChainSnapshot, error) {
	s.logAction("persist_commit_outcome", chainName, key, stepName)
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.extendRunAfterAction(ctx, chain.name, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	stepName = strings.TrimSpace(stepName)
	if _, ok := chain.stepByName[stepName]; !ok {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", stepName)
	}
	if state.Steps == nil {
		state.Steps = map[string]persistedStepState{}
	}
	stepState := state.Steps[stepName]
	if outcome.Result != nil {
		stepState.Result, err = marshalRaw(outcome.Result)
		if err != nil {
			return StepSnapshot{}, ChainSnapshot{}, err
		}
	}
	now := time.Now().UTC()
	stepState.CompletedAt = &now
	stepState.StaleAt = nil
	state.Steps[stepName] = stepState
	if err := s.applyCommitTransition(ctx, chain, key, &state, stepName, &outcome, now); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.saveState(ctx, chain.name, key, state); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	return s.CurrentStep(ctx, chain.name, key)
}

func (s *Service) commitStep(ctx context.Context, chainName string, key StateKey, stepName string, commitInput any, persistCommitInput bool) (StepSnapshot, ChainSnapshot, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.logAction("commit_step", chainName, key, stepName)
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	requestedStep := strings.TrimSpace(stepName)
	if requestedStep == "" {
		requestedStep = s.resolveCurrentStepName(chain, state)
	}
	inputChanged := false
	if persistCommitInput && commitInput != nil {
		previousInput := state.Steps[requestedStep].InputPatch
		if _, err := s.applyStepInputPatch(ctx, chain, key, &state, requestedStep, commitInput, false); err != nil {
			return StepSnapshot{}, ChainSnapshot{}, err
		}
		inputChanged = !jsonRawEqual(previousInput, state.Steps[requestedStep].InputPatch)
	}
	chainSnapshot, err := s.buildChainSnapshot(ctx, chain, key, state)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	currentStep, found := findStepSnapshot(chainSnapshot.Steps, requestedStep)
	if !found {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", requestedStep)
	}
	stepDef := chain.stepByName[currentStep.Name]
	if stepDef.OncePerRun && currentStep.Completed {
		// The persisted transition already points at the next step. A retried
		// request is therefore acknowledged by returning the current snapshot;
		// the action callback is never invoked twice in this run.
		return s.CurrentStep(ctx, chain.name, key)
	}
	claimedOnce := false
	if stepDef.OncePerRun {
		claimedOnce, err = s.repo.ClaimAction(ctx, chain.name, key, "step:"+stepDef.Name)
		if err != nil {
			return StepSnapshot{}, ChainSnapshot{}, err
		}
		if !claimedOnce {
			return s.CurrentStep(ctx, chain.name, key)
		}
	}
	if !persistCommitInput && commitInput != nil {
		transientRaw, marshalErr := marshalRaw(commitInput)
		if marshalErr != nil {
			return StepSnapshot{}, ChainSnapshot{}, marshalErr
		}
		currentStep.Input = normalizeJSON(mergeJSON(currentStep.Input, transientRaw))
	}
	isCommittedUpdate := currentStep.Completed
	if requestedStep != chainSnapshot.CurrentStep && !isCommittedUpdate {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q is not current", requestedStep)
	}
	if isCommittedUpdate {
		allowed, allowErr := s.canUpdateCommittedStep(ctx, chain, chainSnapshot, key, currentStep)
		if allowErr != nil {
			return StepSnapshot{}, ChainSnapshot{}, allowErr
		}
		if !allowed {
			return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q does not allow updates after commit", requestedStep)
		}
	}
	prepare := PrepareContext{
		ChainName: chain.name,
		Key:       key,
		Current:   chainSnapshot.CurrentStep,
		Steps:     chainSnapshot.Steps,
	}
	commitCtx := CommitContext{
		PrepareContext: prepare,
		Step:           currentStep,
	}
	var outcome *CommitOutcome
	commitFn := stepDef.Commit
	if isCommittedUpdate {
		commitFn = stepDef.UpdatePolicy.Commit
	}
	if commitFn != nil {
		outcome, err = commitFn(ctx, commitCtx)
		if err != nil {
			if claimedOnce {
				_ = s.repo.ReleaseAction(ctx, key.DraftID, "step:"+stepDef.Name)
			}
			return StepSnapshot{}, ChainSnapshot{}, err
		}
	}
	if outcome == nil {
		outcome = &CommitOutcome{}
	}
	if state.Steps == nil {
		state.Steps = map[string]persistedStepState{}
	}
	stepState := state.Steps[currentStep.Name]
	if outcome.Result != nil {
		stepState.Result, err = marshalRaw(outcome.Result)
		if err != nil {
			return StepSnapshot{}, ChainSnapshot{}, err
		}
	}
	now := time.Now().UTC()
	stepState.CompletedAt = &now
	stepState.StaleAt = nil
	state.Steps[currentStep.Name] = stepState
	if isCommittedUpdate && inputChanged {
		s.invalidateFollowingSteps(ctx, chain, key, &state, currentStep.Name)
	}
	if err := s.applyCommitTransition(ctx, chain, key, &state, currentStep.Name, outcome, now); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.saveState(ctx, chain.name, key, state); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	return s.CurrentStep(ctx, chain.name, key)
}

// ResetStep verwirft den Zustand ab einem bestimmten Schritt und stellt die Kette dorthin zurück.
func (s *Service) ResetStep(ctx context.Context, chainName string, key StateKey, stepName string) (StepSnapshot, ChainSnapshot, error) {
	s.logAction("reset_step", chainName, key, stepName)
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	target := strings.TrimSpace(stepName)
	stepDef, ok := chain.stepByName[target]
	if !ok {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", target)
	}
	if stepDef.OncePerRun {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q can only run once; discard this chain and start a new run", target)
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	nextStates := map[string]persistedStepState{}
	for _, step := range chain.steps {
		if step.Sequence < stepDef.Sequence {
			if persisted, ok := state.Steps[step.Name]; ok {
				nextStates[step.Name] = persisted
			}
		}
	}
	state.Steps = nextStates
	state.CurrentStep = target
	state.CompletedAt = nil
	if err := s.saveState(ctx, chain.name, key, state); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	return s.CurrentStep(ctx, chain.name, key)
}

// JumpToStep stellt die aktuelle Kette auf einen bereits erreichten Schritt, ohne Zustände zu verwerfen.
func (s *Service) JumpToStep(ctx context.Context, chainName string, key StateKey, stepName string) (StepSnapshot, ChainSnapshot, error) {
	s.logAction("jump_step", chainName, key, stepName)
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	target := strings.TrimSpace(stepName)
	if _, ok := chain.stepByName[target]; !ok {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", target)
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	snapshot, err := s.buildChainSnapshot(ctx, chain, key, state)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	targetStep, found := findStepSnapshot(snapshot.Steps, target)
	if !found {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", target)
	}
	if targetStep.Name != snapshot.CurrentStep && !targetStep.Completed && !hasStepProgress(targetStep) && !chain.stepByName[target].AllowJumpToUnreached {
		canJump := false
		if predicate := chain.stepByName[target].CanJumpToUnreached; predicate != nil {
			canJump, err = predicate(ctx, PrepareContext{
				ChainName:         chain.name,
				Key:               key,
				Current:           snapshot.CurrentStep,
				Steps:             snapshot.Steps,
				CurrentInputPatch: state.Steps[target].InputPatch,
			})
			if err != nil {
				return StepSnapshot{}, ChainSnapshot{}, err
			}
		}
		if !canJump {
			return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q has not been reached yet", target)
		}
	}
	state.CurrentStep = target
	if target != snapshot.CurrentStep {
		state.CompletedAt = nil
	}
	if err := s.saveState(ctx, chain.name, key, state); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	return s.Step(ctx, chain.name, key, target)
}

/*
func (s *Service) SetCurrentStepForRecovery(ctx context.Context, chainName string, key StateKey, stepName string) (StepSnapshot, ChainSnapshot, error) {
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	target := strings.TrimSpace(stepName)
	if _, ok := chain.stepByName[target]; !ok {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q not found", target)
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	snapshot, err := s.buildChainSnapshot(ctx, chain, key, state)
	if err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	targetStep, found := findStepSnapshot(snapshot.Steps, target)
	if !found || (!targetStep.Completed && !hasStepProgress(targetStep) && target != snapshot.CurrentStep) {
		return StepSnapshot{}, ChainSnapshot{}, fmt.Errorf("assistant step %q has not been reached yet", target)
	}
	state.CurrentStep = target
	state.CompletedAt = nil
	if err := s.saveState(ctx, chain.name, key, state); err != nil {
		return StepSnapshot{}, ChainSnapshot{}, err
	}
	return targetStep, snapshot, nil
}
*/

// ResetChain verwirft die persistierte Schrittkette vollständig.
func (s *Service) ResetChain(ctx context.Context, chainName string, key StateKey) error {
	return s.DiscardChain(ctx, chainName, key)
}

// DiscardChain removes one assistant run and its action claims. Starting the
// chain again with the same key creates a fresh run with fresh once-only steps.
func (s *Service) DiscardChain(ctx context.Context, chainName string, key StateKey) error {
	s.logAction("reset_chain", chainName, key, "")
	chain, err := s.compiledChain(chainName)
	if err != nil {
		return err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return err
	}
	if s.repo == nil {
		return nil
	}
	return s.repo.MarkDiscarded(ctx, chain.name, key)
}

// ClaimAction reserves an application-defined action once within this run.
func (s *Service) ClaimAction(ctx context.Context, chainName string, key StateKey, actionKey string) (bool, error) {
	claimed, err := s.repo.ClaimAction(ctx, chainName, key, actionKey)
	if err != nil || !claimed {
		return claimed, err
	}
	if err := s.extendRunAfterAction(ctx, chainName, key); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) resolveCurrentChain(ctx context.Context, chainName string, key StateKey, autoCommit bool) (ChainSnapshot, error) {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	chain, err := s.compiledChain(chainName)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if err := s.checkPrecondition(ctx, chain, key); err != nil {
		return ChainSnapshot{}, err
	}
	state, err := s.loadState(ctx, chain.name, key)
	if err != nil {
		return ChainSnapshot{}, err
	}
	for attempts := 0; attempts < len(chain.steps)+1; attempts++ {
		snapshot, buildErr := s.buildChainSnapshot(ctx, chain, key, state)
		if buildErr != nil {
			return ChainSnapshot{}, buildErr
		}
		if !autoCommit || snapshot.Completed {
			return snapshot, nil
		}
		currentStep, ok := chain.stepByName[snapshot.CurrentStep]
		if !ok || currentStep.AutoCommit.Predicate == nil {
			return snapshot, nil
		}
		prepare := PrepareContext{
			ChainName: chain.name,
			Key:       key,
			Current:   snapshot.CurrentStep,
			Steps:     snapshot.Steps,
		}
		shouldCommit, predicateErr := currentStep.AutoCommit.Predicate(ctx, prepare)
		if predicateErr != nil {
			return ChainSnapshot{}, predicateErr
		}
		if !shouldCommit {
			return snapshot, nil
		}
		s.logAction("auto_commit", chain.name, key, currentStep.Name)
		var commitFn CommitFunc
		if currentStep.AutoCommit.Commit != nil {
			commitFn = currentStep.AutoCommit.Commit
		} else {
			commitFn = currentStep.Commit
		}
		stepSnapshot, found := prepare.Step(currentStep.Name)
		if !found {
			return ChainSnapshot{}, fmt.Errorf("assistant step %q could not be prepared", currentStep.Name)
		}
		if state.Steps == nil {
			state.Steps = map[string]persistedStepState{}
		}
		stepState := state.Steps[currentStep.Name]
		var outcome *CommitOutcome
		if commitFn != nil {
			outcome, err = commitFn(ctx, CommitContext{
				PrepareContext: prepare,
				Step:           stepSnapshot,
			})
			if err != nil {
				return ChainSnapshot{}, err
			}
		}
		if outcome == nil {
			outcome = &CommitOutcome{}
		}
		if outcome.Result != nil {
			stepState.Result, err = marshalRaw(outcome.Result)
			if err != nil {
				return ChainSnapshot{}, err
			}
		}
		now := time.Now().UTC()
		stepState.CompletedAt = &now
		stepState.StaleAt = nil
		state.Steps[currentStep.Name] = stepState
		if err := s.applyCommitTransition(ctx, chain, key, &state, currentStep.Name, outcome, now); err != nil {
			return ChainSnapshot{}, err
		}
		if err := s.saveState(ctx, chain.name, key, state); err != nil {
			return ChainSnapshot{}, err
		}
	}
	return s.buildChainSnapshot(ctx, chain, key, state)
}

func (s *Service) buildChainSnapshot(ctx context.Context, chain compiledChain, key StateKey, state persistedChainState) (ChainSnapshot, error) {
	return s.buildChainSnapshotPreparedThrough(ctx, chain, key, state, "")
}

func (s *Service) buildChainSnapshotPreparedThrough(ctx context.Context, chain compiledChain, key StateKey, state persistedChainState, prepareThroughStepName string) (ChainSnapshot, error) {
	snapshot, err := s.buildChainSnapshotPreparedThroughUnchecked(ctx, chain, key, state, prepareThroughStepName)
	if !errors.Is(err, workflows.ErrFlowNotRegistered) {
		return snapshot, err
	}
	// Ein Assistant darf nicht an einem Persistenzzustand festhängen, dessen
	// Workflow-Blueprint inzwischen entfernt oder ersetzt wurde. Der Fehler
	// entsteht typischerweise beim Prepare eines Wait-Schritts. Wir verwerfen
	// deshalb die gesamte Kette und beginnen mit der aktuell registrierten
	// Schrittkette, statt technische Flow-Fehler bis zur UI durchzureichen.
	s.logAction("reset_unavailable_workflow", chain.name, key, s.resolveCurrentStepName(chain, state))
	if s.repo != nil {
		if resetErr := s.repo.DeleteRecord(ctx, chain.name, key); resetErr != nil {
			return ChainSnapshot{}, resetErr
		}
	}
	resetState := persistedChainState{CurrentStep: s.firstStepName(chain), Steps: map[string]persistedStepState{}}
	return s.buildChainSnapshotPreparedThroughUnchecked(ctx, chain, key, resetState, prepareThroughStepName)
}

func (s *Service) buildChainSnapshotPreparedThroughUnchecked(ctx context.Context, chain compiledChain, key StateKey, state persistedChainState, prepareThroughStepName string) (ChainSnapshot, error) {
	currentStep := s.resolveCurrentStepName(chain, state)
	prepareThroughSequence := chain.stepByName[currentStep].Sequence
	if requestedStep, ok := chain.stepByName[strings.TrimSpace(prepareThroughStepName)]; ok && requestedStep.Sequence > prepareThroughSequence {
		prepareThroughSequence = requestedStep.Sequence
	}
	snapshots := make([]StepSnapshot, 0, len(chain.steps))
	for _, step := range chain.steps {
		persisted := state.Steps[step.Name]
		prepareCtx := PrepareContext{
			ChainName:         chain.name,
			Key:               key,
			Current:           currentStep,
			Steps:             append([]StepSnapshot(nil), snapshots...),
			CurrentInputPatch: normalizeJSON(persisted.InputPatch),
			CurrentResult:     normalizeJSON(persisted.Result),
		}
		input := step.Input
		preparedInput := json.RawMessage(`{}`)
		if step.Prepare != nil && step.Sequence <= prepareThroughSequence {
			prepared, err := step.Prepare(ctx, prepareCtx)
			if err != nil {
				return ChainSnapshot{}, err
			}
			preparedRaw, err := marshalRaw(prepared)
			if err != nil {
				return ChainSnapshot{}, err
			}
			preparedInput = normalizeJSON(preparedRaw)
			input = mergeJSON(input, preparedRaw)
		}
		input = mergeJSON(input, persisted.InputPatch)
		snapshots = append(snapshots, StepSnapshot{
			Name:              step.Name,
			Sequence:          step.Sequence,
			Input:             normalizeJSON(input),
			PreparedInput:     preparedInput,
			InputPatch:        normalizeJSON(persisted.InputPatch),
			Result:            normalizeJSON(persisted.Result),
			Completed:         persisted.CompletedAt != nil,
			CompletedAt:       persisted.CompletedAt,
			Stale:             persisted.StaleAt != nil,
			StaleAt:           persisted.StaleAt,
			AutoCommitEnabled: step.AutoCommit.Predicate != nil,
		})
	}
	return ChainSnapshot{
		Name:        chain.name,
		ScopeKey:    strings.TrimSpace(key.ScopeKey),
		ActorKey:    strings.TrimSpace(key.ActorKey),
		CurrentStep: currentStep,
		Completed:   state.CompletedAt != nil,
		CompletedAt: state.CompletedAt,
		Steps:       snapshots,
	}, nil
}

func (s *Service) loadState(ctx context.Context, chainName string, key StateKey) (persistedChainState, error) {
	if s.repo == nil {
		return persistedChainState{Steps: map[string]persistedStepState{}}, nil
	}
	record, err := s.repo.FindRecord(ctx, chainName, key)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return persistedChainState{Steps: map[string]persistedStepState{}}, nil
		}
		return persistedChainState{}, err
	}
	state := persistedChainState{Steps: map[string]persistedStepState{}}
	if strings.TrimSpace(record.StateJSON) == "" {
		return state, nil
	}
	if err := json.Unmarshal([]byte(record.StateJSON), &state); err != nil {
		return persistedChainState{}, err
	}
	if state.Steps == nil {
		state.Steps = map[string]persistedStepState{}
	}
	return state, nil
}

func (s *Service) saveState(ctx context.Context, chainName string, key StateKey, state persistedChainState) error {
	if s.repo == nil {
		return nil
	}
	if state.Steps == nil {
		state.Steps = map[string]persistedStepState{}
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	status := RunStatusActive
	if state.CompletedAt != nil {
		status = RunStatusCompleted
	}
	return s.repo.SaveRecord(ctx, &ChainRecord{
		Name:      strings.TrimSpace(chainName),
		ScopeKey:  strings.TrimSpace(key.ScopeKey),
		ActorKey:  strings.TrimSpace(key.ActorKey),
		DraftID:   strings.TrimSpace(key.DraftID),
		Status:    string(status),
		StateJSON: string(data),
	})
}

func (s *Service) compiledChain(name string) (compiledChain, error) {
	s.mu.RLock()
	chain, ok := s.chains[strings.TrimSpace(name)]
	s.mu.RUnlock()
	if !ok {
		return compiledChain{}, fmt.Errorf("assistant chain %q not registered", strings.TrimSpace(name))
	}
	return chain, nil
}

func (s *Service) broadcastChainEvent(event ChainEvent) {
	if s == nil {
		return
	}
	s.mu.RLock()
	subscribers := make([]chan ChainEvent, 0, len(s.subscribers))
	for ch := range s.subscribers {
		subscribers = append(subscribers, ch)
	}
	s.mu.RUnlock()
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s *Service) checkPrecondition(ctx context.Context, chain compiledChain, key StateKey) error {
	if chain.precondition == nil {
		return nil
	}
	return chain.precondition(ctx, key)
}

func (s *Service) resolveCurrentStepName(chain compiledChain, state persistedChainState) string {
	if state.CompletedAt != nil && strings.TrimSpace(state.CurrentStep) != "" {
		if _, ok := chain.stepByName[strings.TrimSpace(state.CurrentStep)]; ok {
			return strings.TrimSpace(state.CurrentStep)
		}
	}
	if stepName := strings.TrimSpace(state.CurrentStep); stepName != "" {
		if _, ok := chain.stepByName[stepName]; ok {
			return stepName
		}
	}
	for _, step := range chain.steps {
		if persisted := state.Steps[step.Name]; persisted.CompletedAt == nil {
			return step.Name
		}
	}
	if len(chain.steps) == 0 {
		return ""
	}
	return chain.steps[len(chain.steps)-1].Name
}

func (s *Service) nextStepName(chain compiledChain, currentStep string) string {
	for index, step := range chain.steps {
		if step.Name != currentStep {
			continue
		}
		if index+1 < len(chain.steps) {
			return chain.steps[index+1].Name
		}
		return ""
	}
	return ""
}

func (s *Service) firstStepName(chain compiledChain) string {
	if len(chain.steps) == 0 {
		return ""
	}
	return chain.steps[0].Name
}

// logAction schreibt eine einheitliche Logzeile für Assistant-Aktionen zur späteren Nachverfolgung.
func (s *Service) logAction(action string, chainName string, key StateKey, stepName string) {
	log.Printf(
		"[assistant] action=%s chain=%s step=%s scope=%s actor=%s",
		strings.TrimSpace(action),
		strings.TrimSpace(chainName),
		strings.TrimSpace(stepName),
		strings.TrimSpace(key.ScopeKey),
		strings.TrimSpace(key.ActorKey),
	)
}

func (s *Service) applyCommitTransition(ctx context.Context, chain compiledChain, key StateKey, state *persistedChainState, currentStepName string, outcome *CommitOutcome, completedAt time.Time) error {
	nextStep := strings.TrimSpace(outcome.NextStep)
	if nextStep != "" {
		if _, ok := chain.stepByName[nextStep]; !ok {
			return fmt.Errorf("assistant step %q not found", nextStep)
		}
		state.CurrentStep = nextStep
		state.CompletedAt = nil
		return nil
	}
	nextStep = s.nextStepName(chain, currentStepName)
	if nextStep != "" {
		state.CurrentStep = nextStep
		state.CompletedAt = nil
		return nil
	}
	if chain.completionBehavior == ChainCompletionKeepComplete {
		s.logAction("complete_chain", chain.name, key, currentStepName)
		state.CurrentStep = currentStepName
		state.CompletedAt = &completedAt
		return nil
	}
	s.logAction("complete_chain_reset", chain.name, key, currentStepName)
	if s.repo == nil {
		*state = persistedChainState{CurrentStep: s.firstStepName(chain), Steps: map[string]persistedStepState{}}
		return nil
	}
	if err := s.repo.DeleteRecord(ctx, chain.name, key); err != nil {
		return err
	}
	*state = persistedChainState{CurrentStep: s.firstStepName(chain), Steps: map[string]persistedStepState{}}
	return nil
}

func (s *Service) canUpdateCommittedStep(ctx context.Context, chain compiledChain, snapshot ChainSnapshot, key StateKey, step StepSnapshot) (bool, error) {
	stepDef, ok := chain.stepByName[step.Name]
	if !ok {
		return false, fmt.Errorf("assistant step %q not found", step.Name)
	}
	if stepDef.UpdatePolicy.CanUpdate == nil {
		return false, nil
	}
	return stepDef.UpdatePolicy.CanUpdate(ctx, UpdatePolicyContext{
		PrepareContext: PrepareContext{
			ChainName: chain.name,
			Key:       key,
			Current:   snapshot.CurrentStep,
			Steps:     snapshot.Steps,
		},
		Step: step,
	})
}

func (s *Service) invalidateFollowingSteps(ctx context.Context, chain compiledChain, key StateKey, state *persistedChainState, stepName string) bool {
	snapshot, err := s.buildChainSnapshot(ctx, chain, key, *state)
	if err != nil {
		return false
	}
	sourceStep, found := findStepSnapshot(snapshot.Steps, stepName)
	if !found {
		return false
	}
	stepDef := chain.stepByName[stepName]
	now := time.Now().UTC()
	invalidated := false
	for _, following := range snapshot.Steps {
		if following.Sequence <= sourceStep.Sequence {
			continue
		}
		shouldInvalidate := true
		if stepDef.UpdatePolicy.InvalidateFollowing != nil {
			allowed, policyErr := stepDef.UpdatePolicy.InvalidateFollowing(ctx, UpdatePolicyContext{
				PrepareContext: PrepareContext{
					ChainName: chain.name,
					Key:       key,
					Current:   snapshot.CurrentStep,
					Steps:     snapshot.Steps,
				},
				Step: sourceStep,
			}, following)
			if policyErr != nil {
				continue
			}
			shouldInvalidate = allowed
		}
		if !shouldInvalidate {
			continue
		}
		followingState := state.Steps[following.Name]
		followingState.StaleAt = &now
		state.Steps[following.Name] = followingState
		s.logAction("mark_stale", chain.name, key, following.Name)
		invalidated = true
	}
	return invalidated
}

func findStepSnapshot(steps []StepSnapshot, name string) (StepSnapshot, bool) {
	needle := strings.TrimSpace(name)
	for _, step := range steps {
		if step.Name == needle {
			return step, true
		}
	}
	return StepSnapshot{}, false
}

func hasStepProgress(step StepSnapshot) bool {
	// Ein Ergebnis bleibt eine tatsächlich erreichte, weiterhin lesbare
	// Zwischenstation. Auch wenn spätere Eingaben inzwischen als veraltet
	// markiert sind, darf ein Assistant dorthin zurückspringen, um etwa ein
	// bereits erzeugtes Ergebnis wieder anzuzeigen. Reine Eingabepatches
	// bleiben bei Stale bewusst nicht navigierbar.
	return hasJSONPayload(step.Result) || (!step.Stale && hasJSONPayload(step.InputPatch))
}

func hasJSONPayload(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null" && trimmed != "{}"
}

func marshalRaw(value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage(`{}`), nil
	}
	switch typed := value.(type) {
	case json.RawMessage:
		return normalizeJSON(typed), nil
	case []byte:
		return normalizeJSON(json.RawMessage(typed)), nil
	case string:
		return normalizeJSON(json.RawMessage(typed)), nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return normalizeJSON(json.RawMessage(data)), nil
	}
}

func normalizeJSON(raw json.RawMessage) json.RawMessage {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(trimmed)
}

// jsonRawEqual ignores harmless JSON formatting and object-key ordering.
func jsonRawEqual(left, right json.RawMessage) bool {
	var leftValue any
	if err := json.Unmarshal(normalizeJSON(left), &leftValue); err != nil {
		return string(normalizeJSON(left)) == string(normalizeJSON(right))
	}
	var rightValue any
	if err := json.Unmarshal(normalizeJSON(right), &rightValue); err != nil {
		return false
	}
	leftCanonical, leftErr := json.Marshal(leftValue)
	rightCanonical, rightErr := json.Marshal(rightValue)
	return leftErr == nil && rightErr == nil && string(leftCanonical) == string(rightCanonical)
}

func mergeJSON(base json.RawMessage, patch json.RawMessage) json.RawMessage {
	base = normalizeJSON(base)
	patch = normalizeJSON(patch)
	if len(patch) == 0 {
		return base
	}
	var baseValue any
	if err := json.Unmarshal(base, &baseValue); err != nil {
		return patch
	}
	var patchValue any
	if err := json.Unmarshal(patch, &patchValue); err != nil {
		return base
	}
	merged := mergeJSONValue(baseValue, patchValue)
	data, err := json.Marshal(merged)
	if err != nil {
		return patch
	}
	return normalizeJSON(data)
}

func mergeJSONValue(base any, patch any) any {
	patchMap, patchIsMap := patch.(map[string]any)
	if !patchIsMap {
		return patch
	}
	baseMap, baseIsMap := base.(map[string]any)
	if !baseIsMap {
		baseMap = map[string]any{}
	}
	merged := map[string]any{}
	for key, value := range baseMap {
		merged[key] = value
	}
	for key, value := range patchMap {
		if value == nil {
			delete(merged, key)
			continue
		}
		if current, ok := merged[key]; ok {
			merged[key] = mergeJSONValue(current, value)
			continue
		}
		merged[key] = value
	}
	return merged
}
