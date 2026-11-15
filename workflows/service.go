package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wswag/go-flow/tokens"

	"gorm.io/gorm"
)

// ErrFlowNotRegistered identifies persisted workflow instances whose
// blueprint is no longer part of the running application. Callers can use it
// to discard stale references without matching an implementation-specific
// error string.
var ErrFlowNotRegistered = errors.New("flow is not registered")

type Service struct {
	mu                sync.RWMutex
	idempotencyMu     sync.Mutex
	store             *Repository
	flows             map[string]Flow
	userInterfaces    []any
	active            map[uint]*FlowExecutionContext
	activeKeys        map[string]*FlowExecutionContext
	subscribers       map[chan FlowEvent]struct{}
	failureHooks      []CorrelationFailureHook
	terminalHooks     []TerminalHook
	startMiddlewares  []StartMiddleware
	actionMiddlewares []ActionMiddleware
}

// NewService baut den Workflow-Dienst über dem persistierten Repository.
// `store` hält Token-, Flow- und Node-Zustände über Neustarts hinweg fest.
func NewService(store *Repository) *Service {
	return &Service{
		store:       store,
		flows:       map[string]Flow{},
		active:      map[uint]*FlowExecutionContext{},
		activeKeys:  map[string]*FlowExecutionContext{},
		subscribers: map[chan FlowEvent]struct{}{},
	}
}

// RegisterFlow hinterlegt einen ausführbaren Workflow-Blueprint.
// Ein Flow ist unter seinem Namen eindeutig registriert.
func (s *Service) RegisterFlow(flow Flow) error {
	name := strings.TrimSpace(flow.Name)
	if name == "" {
		return errors.New("flow name is required")
	}
	if len(flow.Nodes) == 0 {
		return errors.New("flow blueprint is empty")
	}
	for _, node := range flow.Nodes {
		if strings.TrimSpace(node.Name) == "" {
			return fmt.Errorf("flow %q contains an unnamed node", name)
		}
		if node.Action == nil {
			return fmt.Errorf("flow %q node %q has no action", name, strings.TrimSpace(node.Name))
		}
	}
	for _, retryNodeName := range flow.RetryNodeNames {
		name := strings.TrimSpace(retryNodeName)
		if name == "" {
			return fmt.Errorf("flow %q contains an empty retry node", flow.Name)
		}
		found := false
		for _, node := range flow.Nodes {
			if strings.TrimSpace(node.Name) == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("flow %q retry node %q is not defined", flow.Name, name)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.flows[name]; exists {
		return fmt.Errorf("flow %q is already registered", name)
	}
	flow.Name = name
	flow.StartMiddlewares = append([]StartMiddleware(nil), flow.StartMiddlewares...)
	flow.UserInterfaces = append([]any(nil), flow.UserInterfaces...)
	s.flows[name] = flow
	return nil
}

// RegisterUserInterface registers an application dependency at engine scope.
// Engine dependencies are the fallback after call-specific and flow-level
// dependencies. The same instance is shared by all flows using this service.
func (s *Service) RegisterUserInterface(value any) error {
	return s.RegisterUserInterfaces(value)
}

// RegisterUserInterfaces registers multiple application dependencies at
// engine scope. Nil values are rejected; duplicate matching types are reported
// as ambiguous when resolved from a workflow.
func (s *Service) RegisterUserInterfaces(values ...any) error {
	if s == nil {
		return errors.New("flow service is not configured")
	}
	if len(values) == 0 {
		return nil
	}
	for _, value := range values {
		if isNilUserInterface(value) {
			return errors.New("workflow user interface cannot be nil")
		}
	}
	s.mu.Lock()
	s.userInterfaces = append(s.userInterfaces, values...)
	s.mu.Unlock()
	return nil
}

// Subscribe liefert Live-Events zu Workflow-Schritten und Zustandswechseln.
func (s *Service) Subscribe() (chan FlowEvent, func()) {
	if s == nil {
		ch := make(chan FlowEvent)
		close(ch)
		return ch, func() {}
	}
	ch := make(chan FlowEvent, 32)
	s.mu.Lock()
	if s.subscribers == nil {
		s.subscribers = map[chan FlowEvent]struct{}{}
	}
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.subscribers, ch)
		s.mu.Unlock()
	}
}

// RegisterCorrelationFailureHook registriert einen best-effort Listener für terminal fehlgeschlagene Correlations.
// Der Workflow-Status ist vor dem Hook bereits persistent gespeichert; Fehler im Hook dürfen den Flow nicht verändern.
func (s *Service) RegisterCorrelationFailureHook(hook CorrelationFailureHook) {
	if s == nil || hook == nil {
		return
	}
	s.mu.Lock()
	s.failureHooks = append(s.failureHooks, hook)
	s.mu.Unlock()
}

// RegisterTerminalHook registriert einen best-effort Listener für persistierte
// Abschluss- und Fehlerereignisse. Der Flow ist zu diesem Zeitpunkt bereits
// abgeschlossen und bleibt unabhängig vom Hook konsistent.
func (s *Service) RegisterTerminalHook(hook TerminalHook) {
	if s == nil || hook == nil {
		return
	}
	s.mu.Lock()
	s.terminalHooks = append(s.terminalHooks, hook)
	s.mu.Unlock()
}

// RegisterStartMiddleware adds an engine-level middleware with sequence 0.
// Engine, flow-definition, and per-call middleware are sorted together by
// sequence; ties retain source order.
func (s *Service) RegisterStartMiddleware(middleware StartMiddleware) {
	if s == nil || middleware == nil {
		return
	}
	s.mu.Lock()
	s.startMiddlewares = append(s.startMiddlewares, middleware)
	s.mu.Unlock()
}

// RegisterStartMiddlewareAt registers engine-level middleware with an
// explicit sequence number. Lower sequence values run first.
func (s *Service) RegisterStartMiddlewareAt(sequence int, middleware StartMiddleware) {
	s.RegisterStartMiddleware(NewSequencedStartMiddleware(sequence, middleware))
}

// IdempotencyMiddleware returns a start middleware that reuses an active flow
// with the same flow name and StartOptions.IdempotencyKey. Register it with
// RegisterStartMiddleware or pass it in StartOptions.Middlewares at the point
// in the chain where idempotency should be applied. If no idempotency middleware
// for this service is present in the chain, StartFlow adds it as the final
// middleware automatically.
//
// Starts using this middleware are serialized while the lookup and downstream
// start run, so concurrent calls in this service do not create duplicate runs.
func (s *Service) IdempotencyMiddleware() StartMiddleware {
	return idempotencyStartMiddleware{service: s}
}

type idempotencyStartMiddleware struct {
	service *Service
}

func (m idempotencyStartMiddleware) Start(ctx context.Context, request StartRequest, next StartFlowNext) (*FlowExecutionContext, error) {
	s := m.service
	key := strings.TrimSpace(request.Options.IdempotencyKey)
	if s == nil || key == "" {
		return next(ctx, request)
	}
	if active, _ := ctx.Value(idempotencyMiddlewareContextKey{}).(idempotencyMiddlewareContext); active.service == s {
		return next(ctx, request)
	}

	s.idempotencyMu.Lock()
	defer s.idempotencyMu.Unlock()

	existing, err := s.FindActiveFlowByIdempotencyKey(ctx, request.FlowName, key)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		existing.Reused = true
		return existing, nil
	}
	ctx = context.WithValue(ctx, idempotencyMiddlewareContextKey{}, idempotencyMiddlewareContext{service: s})
	return next(ctx, request)
}

type idempotencyMiddlewareContextKey struct{}

type idempotencyMiddlewareContext struct {
	service *Service
}

// RegisterActionMiddleware adds an engine-level action middleware. Registered
// middleware wraps actions in registration order, with the first registered
// middleware as the outermost wrapper.
func (s *Service) RegisterActionMiddleware(middleware ActionMiddleware) {
	if s == nil || middleware == nil {
		return
	}
	s.mu.Lock()
	s.actionMiddlewares = append(s.actionMiddlewares, middleware)
	s.mu.Unlock()
}

// StartFlow starts a workflow in the background and returns its serializable
// instance reference. At most one StartOptions value may be provided.
func (s *Service) StartFlow(ctx context.Context, name string, startToken string, options ...StartOptions) (FlowRef, error) {
	exec, err := s.startFlowExecution(ctx, name, startToken, options...)
	if err != nil {
		return 0, err
	}
	return FlowRef(exec.FlowInstanceID), nil
}

// startFlowExecution runs the start middleware pipeline and returns its
// process-local execution handle.
func (s *Service) startFlowExecution(ctx context.Context, name string, startToken string, options ...StartOptions) (*FlowExecutionContext, error) {
	if s == nil {
		return nil, errors.New("flow service is not configured")
	}
	if len(options) > 1 {
		return nil, errors.New("StartFlow accepts at most one StartOptions value")
	}
	var startOptions StartOptions
	if len(options) == 1 {
		startOptions = options[0]
	}
	s.mu.RLock()
	middlewares := append([]StartMiddleware(nil), s.startMiddlewares...)
	engineUserInterfaces := append([]any(nil), s.userInterfaces...)
	if flow, ok := s.flows[strings.TrimSpace(name)]; ok {
		middlewares = append(middlewares, flow.StartMiddlewares...)
	}
	s.mu.RUnlock()
	startOptions.engineUserInterfaces = engineUserInterfaces
	startOptions.engineUserInterfacesSet = true
	middlewares = append(middlewares, startOptions.Middlewares...)
	sort.SliceStable(middlewares, func(i, j int) bool {
		return startMiddlewareSequence(middlewares[i]) < startMiddlewareSequence(middlewares[j])
	})
	if !hasIdempotencyMiddleware(middlewares, s) {
		middlewares = append(middlewares, s.IdempotencyMiddleware())
	}
	request := StartRequest{FlowName: name, StartToken: startToken, Options: startOptions}
	var invoke func(int, context.Context, StartRequest) (*FlowExecutionContext, error)
	invoke = func(index int, ctx context.Context, request StartRequest) (*FlowExecutionContext, error) {
		request.Options.engineUserInterfaces = engineUserInterfaces
		request.UserInterfaces = request.Options.UserInterfaces
		request.FlowUserInterfaces = s.flowUserInterfaces(request.FlowName)
		request.EngineUserInterfaces = engineUserInterfaces
		if index == len(middlewares) {
			return s.startFlowCore(ctx, request)
		}
		return middlewares[index].Start(ctx, request, func(nextCtx context.Context, nextRequest StartRequest) (*FlowExecutionContext, error) {
			return invoke(index+1, nextCtx, nextRequest)
		})
	}
	return invoke(0, ctx, request)
}

func (s *Service) flowUserInterfaces(flowName string) []any {
	s.mu.RLock()
	flow, ok := s.flows[strings.TrimSpace(flowName)]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return flow.UserInterfaces
}

func startMiddlewareSequence(middleware StartMiddleware) int {
	if sequenced, ok := middleware.(interface{ Sequence() int }); ok {
		return sequenced.Sequence()
	}
	return 0
}

func hasIdempotencyMiddleware(middlewares []StartMiddleware, service *Service) bool {
	for _, middleware := range middlewares {
		for middleware != nil {
			if idempotency, ok := middleware.(idempotencyStartMiddleware); ok && idempotency.service == service {
				return true
			}
			unwrapper, ok := middleware.(interface{ UnwrapStartMiddleware() StartMiddleware })
			if !ok {
				break
			}
			middleware = unwrapper.UnwrapStartMiddleware()
		}
	}
	return false
}

func (s *Service) startFlowCore(ctx context.Context, request StartRequest) (*FlowExecutionContext, error) {
	name := request.FlowName
	startToken := request.StartToken
	startOptions := request.Options
	exec, err := s.startFlowAtNode(
		ctx, name, startToken, 0, startOptions, true,
	)
	if err != nil {
		return nil, err
	}
	return exec, nil
}

// startFlowAtNode validates, persists, and optionally launches a flow at a
// selected node with its start options.
func (s *Service) startFlowAtNode(ctx context.Context, name string, startToken string, startNodeIndex int, options StartOptions, launch bool) (*FlowExecutionContext, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	flow, err := s.flowDefinition(name)
	if err != nil {
		return nil, err
	}
	if startNodeIndex < 0 || startNodeIndex >= len(flow.Nodes) {
		return nil, fmt.Errorf("flow %q has no node at index %d", flow.Name, startNodeIndex)
	}
	if !options.engineUserInterfacesSet {
		s.mu.RLock()
		options.engineUserInterfaces = append([]any(nil), s.userInterfaces...)
		s.mu.RUnlock()
		options.engineUserInterfacesSet = true
	}

	now := time.Now().UTC()
	var instanceID uint
	metadataJSON := ""
	idempotencyKey := strings.TrimSpace(options.IdempotencyKey)
	metadata := EnsureCorrelationMetadata(options.Metadata, flow.Label, flow.Description)
	if len(metadata) > 0 {
		encoded, encodeErr := json.Marshal(metadata)
		if encodeErr != nil {
			return nil, encodeErr
		}
		metadataJSON = string(encoded)
	}
	s.mu.Lock()
	if err := s.store.Transaction(ctx, func(tx *Repository) error {
		startTokenRecord := &Token{RawData: strings.TrimSpace(startToken)}
		if err := tx.CreateToken(ctx, startTokenRecord); err != nil {
			return err
		}
		instance := &FlowInstance{
			Name:             flow.Name,
			Status:           FlowStatusRunning,
			IdempotencyKey:   idempotencyKey,
			CurrentNodeIndex: startNodeIndex,
			InputTokenID:     &startTokenRecord.ID,
			MetadataJSON:     metadataJSON,
			StartedAt:        now,
			UpdatedAt:        now,
		}
		if err := tx.CreateFlowInstance(ctx, instance); err != nil {
			return err
		}
		firstNode := &NodeInstance{
			Name:           flow.Nodes[startNodeIndex].Name,
			FlowInstanceID: instance.ID,
			NodeIndex:      startNodeIndex,
			Status:         NodeStatusQueued,
			InputTokenID:   &startTokenRecord.ID,
			MetadataJSON:   metadataJSON,
			Active:         true,
			StartedAt:      now,
			UpdatedAt:      now,
		}
		if err := tx.CreateNodeInstance(ctx, firstNode); err != nil {
			return err
		}
		instanceID = instance.ID
		return nil
	}); err != nil {
		s.mu.Unlock()
		return nil, err
	}

	exec := &FlowExecutionContext{
		FlowName:             flow.Name,
		FlowInstanceID:       instanceID,
		Status:               FlowStatusRunning,
		StartToken:           strings.TrimSpace(startToken),
		MetadataJSON:         metadataJSON,
		IdempotencyKey:       idempotencyKey,
		done:                 make(chan struct{}),
		startMiddlewares:     append([]StartMiddleware(nil), options.Middlewares...),
		actionMiddlewares:    append([]ActionMiddleware(nil), options.ActionMiddlewares...),
		userInterfaces:       append([]any(nil), options.UserInterfaces...),
		engineUserInterfaces: append([]any(nil), options.engineUserInterfaces...),
	}
	s.active[instanceID] = exec
	if idempotencyKey != "" {
		s.activeKeys[activeKey(flow.Name, idempotencyKey)] = exec
	}
	s.mu.Unlock()
	if launch {
		s.launchExecution(exec)
	}
	return exec, nil
}

// FindActiveFlowByIdempotencyKey lädt einen aktiven Lauf für denselben fachlichen Schlüssel.
func (s *Service) FindActiveFlowByIdempotencyKey(ctx context.Context, name string, idempotencyKey string) (*FlowExecutionContext, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	flowName := strings.TrimSpace(name)
	key := strings.TrimSpace(idempotencyKey)
	if flowName == "" || key == "" {
		return nil, nil
	}
	s.mu.RLock()
	if existing := s.findActiveExecutionLocked(flowName, key); existing != nil {
		s.mu.RUnlock()
		return existing, nil
	}
	s.mu.RUnlock()
	instance, err := s.store.FindActiveFlowInstanceByNameAndIdempotencyKey(ctx, flowName, key)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.ResumeFlowInstance(ctx, instance.ID)
}

// FindLatestFlowByNameAndCorrelationID lädt den jüngsten Lauf einer
// Correlation-Kette und liefert ihn als resume-fähigen Execution-Handle.
func (s *Service) FindLatestFlowByNameAndCorrelationID(ctx context.Context, name string, correlationID string) (*FlowExecutionContext, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	instance, err := s.store.FindLatestFlowInstanceByNameAndCorrelationID(ctx, name, correlationID)
	if err != nil {
		return nil, err
	}
	return s.ResumeFlowInstance(ctx, instance.ID)
}

// ListLatestFlowInstancesByName returns recent persisted instances without
// starting or resuming them. This is useful for app-level, token-contract based
// recovery paths that must not accidentally execute an old workflow.
func (s *Service) ListLatestFlowInstancesByName(ctx context.Context, name string, limit int) ([]FlowInstance, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	return s.store.ListLatestFlowInstancesByName(ctx, name, limit)
}

// ListFlowInstancesByCorrelationID liefert alle gespeicherten Läufe derselben Correlation-Kette.
func (s *Service) ListFlowInstancesByCorrelationID(ctx context.Context, correlationID string) ([]FlowInstance, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	return s.store.ListFlowInstancesByCorrelationID(ctx, correlationID)
}

// ListResumableFlowInstances gibt pausierte oder noch laufende Flows für Live-Clients zurück.
func (s *Service) ListResumableFlowInstances(ctx context.Context, limit int) ([]FlowInstance, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	return s.store.ListResumableFlowInstances(ctx, limit)
}

// FlowEventForInstance baut einen UI-tauglichen Snapshot für einen gespeicherten Flow.
// Der Snapshot kann für Ticker, Banner und Resume-Ansichten verwendet werden.
func (s *Service) FlowEventForInstance(ctx context.Context, flowInstanceID uint) (FlowEvent, error) {
	if s == nil || s.store == nil {
		return FlowEvent{}, errors.New("flow store is not configured")
	}
	instance, flow, err := s.loadExecutionState(ctx, flowInstanceID)
	if err != nil {
		return FlowEvent{}, err
	}
	var node *NodeInstance
	if current := findNodeInstanceByIndex(instance.NodeInstances, instance.CurrentNodeIndex); current.ID != 0 {
		node = &current
	}
	status := instance.Status
	var execErr error
	if strings.TrimSpace(instance.ErrorMessage) != "" && status == FlowStatusRunning {
		status = FlowStatusFailed
	}
	if status == FlowStatusFailed && strings.TrimSpace(instance.ErrorMessage) != "" {
		execErr = errors.New(strings.TrimSpace(instance.ErrorMessage))
	}
	return s.buildFlowEvent(instance, flow, node, status, "", execErr, status == FlowStatusFailed || status == FlowStatusFinished), nil
}

// ResumeStoppedFlows sucht in der Persistenz nach pausierten oder nach einem Neustart
// liegen gebliebenen Flows und startet alle passenden Ausführungen erneut.
func (s *Service) ResumeStoppedFlows(ctx context.Context) ([]*FlowExecutionContext, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	instances, err := s.store.ListResumableFlowInstances(ctx, 200)
	if err != nil {
		return nil, err
	}
	resumed := make([]*FlowExecutionContext, 0, len(instances))
	var firstErr error
	for _, instance := range instances {
		exec, resumeErr := s.ResumeFlowInstance(ctx, instance.ID)
		if resumeErr != nil {
			if firstErr == nil {
				firstErr = resumeErr
			}
			continue
		}
		if exec != nil {
			resumed = append(resumed, exec)
		}
	}
	return resumed, firstErr
}

// FlowBlueprint lädt die registrierte Flow-Definition für reine UI- oder
// Diagnosezwecke, ohne die Persistenz zu berühren.
// Rückgabe ist die registrierte Blueprint-Definition samt Label und Beschreibung.
func (s *Service) FlowBlueprint(name string) (Flow, bool) {
	if s == nil {
		return Flow{}, false
	}
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return Flow{}, false
	}
	s.mu.RLock()
	flow, ok := s.flows[trimmed]
	s.mu.RUnlock()
	if !ok {
		return Flow{}, false
	}
	if len(flow.Nodes) == 0 {
		return Flow{}, false
	}
	return flow, true
}

// ResumeFlowInstance setzt einen einzelnen gespeicherten Flow-Lauf fort.
// Bereits aktive Flows werden nicht doppelt gestartet.
func (s *Service) ResumeFlowInstance(ctx context.Context, flowInstanceID uint) (*FlowExecutionContext, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	instance, err := s.store.FindFlowInstanceByID(ctx, flowInstanceID)
	if err != nil {
		return nil, err
	}
	if _, err := s.flowDefinition(instance.Name); err != nil {
		return nil, err
	}
	if instance.Status == FlowStatusFinished || instance.Status == FlowStatusFailed {
		return s.executionFromInstance(instance), nil
	}

	s.mu.Lock()
	if existing, ok := s.active[instance.ID]; ok {
		s.mu.Unlock()
		return existing, nil
	}
	exec := s.activeExecutionFromInstance(instance)
	s.active[instance.ID] = exec
	s.mu.Unlock()

	s.launchExecution(exec)
	return exec, nil
}

// GetExecutionContextForFlow returns the local execution handle for a workflow
// instance, or a snapshot reconstructed from persistence when no local handle
// is active.
func (s *Service) GetExecutionContextForFlow(ctx context.Context, flowInstanceID uint) (*FlowExecutionContext, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("flow store is not configured")
	}
	if flowInstanceID == 0 {
		return nil, errors.New("workflow instance is required")
	}
	s.mu.RLock()
	exec := s.active[flowInstanceID]
	s.mu.RUnlock()
	if exec != nil {
		return exec, nil
	}
	instance, err := s.store.FindFlowInstanceByID(ctx, flowInstanceID)
	if err != nil {
		return nil, err
	}
	return s.executionFromInstance(instance), nil
}

// RetryFlow starts a configured retry as a fresh workflow instance.
//
// Reusing the failed instance is only safe when all side effects before the
// current checkpoint are still present. Some workflows compensate those
// effects when a downstream branch fails. A fresh run therefore starts at the
// configured persisted checkpoint, reusing the already generated input token.
func (s *Service) RetryFlow(ctx context.Context, flowInstanceID uint, nodeID uint, setKey string, value string) (FlowRef, error) {
	if s == nil || s.store == nil {
		return 0, errors.New("flow store is not configured")
	}
	if flowInstanceID == 0 || nodeID == 0 {
		return 0, errors.New("flow instance and node are required")
	}
	if s.isActive(flowInstanceID) {
		return 0, errors.New("workflow is still running")
	}
	instance, err := s.store.FindFlowInstanceByID(ctx, flowInstanceID)
	if err != nil {
		return 0, err
	}
	definition, err := s.flowDefinition(instance.Name)
	if err != nil {
		return 0, err
	}
	if instance.Status != FlowStatusFailed {
		return 0, errors.New("nur fehlgeschlagene workflows können als neue instanz neu gestartet werden")
	}
	node, err := s.store.FindNodeInstanceByID(ctx, nodeID)
	if err != nil {
		return 0, err
	}
	if node.FlowInstanceID != instance.ID {
		return 0, errors.New("node does not belong to flow instance")
	}
	selectedCheckpoint := isRetryCheckpointNode(definition, node.NodeIndex) && node.NodeIndex <= instance.CurrentNodeIndex
	if !selectedCheckpoint && node.NodeIndex != instance.CurrentNodeIndex {
		return 0, errors.New("nur der aktuelle knoten oder ein erreichter retry-checkpoint kann neu gestartet werden")
	}
	startNodeIndex := node.NodeIndex
	if !selectedCheckpoint {
		var ok bool
		startNodeIndex, ok = nearestRetryNodeIndex(definition, instance.CurrentNodeIndex)
		if !ok {
			return 0, fmt.Errorf("workflow %q has no retry checkpoint at or before node %d", definition.Name, instance.CurrentNodeIndex)
		}
	}
	startNode := findNodeInstanceByIndex(instance.NodeInstances, startNodeIndex)
	if startNode.ID == 0 || startNode.InputTokenID == nil {
		return 0, fmt.Errorf("workflow is missing retry checkpoint node %d", startNodeIndex)
	}
	startTokenRecord, err := s.store.FindTokenByID(ctx, *startNode.InputTokenID)
	if err != nil {
		return 0, err
	}
	startToken := startTokenRecord.RawData
	// Editing the retry checkpoint remains supported. For a later failed node
	// the edited value belongs to a downstream checkpoint and cannot safely be
	// used as the new workflow input; the generated upstream result is reused.
	if selectedCheckpoint || node.NodeIndex == startNodeIndex {
		setKey = strings.TrimSpace(setKey)
		if setKey != "" {
			startToken, err = tokens.RewriteTokenPayloadSet(startToken, setKey, strings.TrimSpace(value))
		} else {
			startToken, err = rewritePrimaryTokenPayloadSet(startToken, value)
		}
		if err != nil {
			return 0, err
		}
	}

	metadata := parseFlowMetadata(instance.MetadataJSON)
	freshMetadata := metadata.Raw
	if freshMetadata == nil {
		freshMetadata = map[string]any{}
	}
	delete(freshMetadata, metadataCorrelationIDKey)
	delete(freshMetadata, metadataParentFlowInstanceIDKey)
	delete(freshMetadata, metadataParentNodeIDKey)
	delete(freshMetadata, metadataParentNodeIndexKey)
	freshMetadata["retryOfFlowInstanceId"] = instance.ID
	freshMetadata = EnsureCorrelationMetadata(freshMetadata, metadata.CorrelationLabel, metadata.CorrelationDescription)

	started, err := s.startFlowAtNode(ctx, instance.Name, startToken, startNodeIndex, StartOptions{Metadata: freshMetadata}, false)
	if err != nil {
		return 0, err
	}
	// Hide the superseded failure from user-facing "latest failure" lookups,
	// while retaining the complete failed run for diagnostics and correlation.
	if err := retryFlowSQLiteBusy(func() error { return s.DiscardFlowInstance(ctx, instance.ID) }); err != nil {
		s.clearActive(started.FlowInstanceID)
		return 0, err
	}
	s.launchExecution(started)
	return FlowRef(started.FlowInstanceID), nil
}

// UpdateCurrentNodeStartToken ersetzt den editierbaren Tokenwert des aktuellen Knotens.
// Der Flow wird dabei auf den aktuellen Checkpoint zurückgesetzt, damit ein Resume den neuen Wert nutzt.
func (s *Service) UpdateCurrentNodeStartToken(ctx context.Context, flowInstanceID uint, nodeID uint, value string) error {
	return s.updateCurrentNodeStartToken(ctx, flowInstanceID, nodeID, "", value)
}

// UpdateCurrentNodeStartTokenSet ersetzt ein explizit ausgewaehltes Set im Start-Token.
// Der vorherige vollstaendige Token wird in der Rewrite-Historie gesichert.
func (s *Service) UpdateCurrentNodeStartTokenSet(ctx context.Context, flowInstanceID uint, nodeID uint, setKey string, value string) error {
	return s.updateCurrentNodeStartToken(ctx, flowInstanceID, nodeID, setKey, value)
}

// updateCurrentNodeStartToken persists an edit to the current node's input
// token and records the old and new token values for reversal.
func (s *Service) updateCurrentNodeStartToken(ctx context.Context, flowInstanceID uint, nodeID uint, setKey string, value string) error {
	if s == nil || s.store == nil {
		return errors.New("flow store is not configured")
	}
	if flowInstanceID == 0 || nodeID == 0 {
		return errors.New("flow instance and node are required")
	}
	if s.isActive(flowInstanceID) {
		return errors.New("workflow is still running")
	}

	now := time.Now().UTC()
	if err := s.store.Transaction(ctx, func(tx *Repository) error {
		instance, err := tx.FindFlowInstanceByID(ctx, flowInstanceID)
		if err != nil {
			return err
		}
		node, err := tx.FindNodeInstanceByID(ctx, nodeID)
		if err != nil {
			return err
		}
		if node.FlowInstanceID != instance.ID {
			return errors.New("node does not belong to flow instance")
		}
		if node.NodeIndex != instance.CurrentNodeIndex {
			return errors.New("only the current node can be edited")
		}
		if node.InputTokenID == nil {
			return errors.New("node is missing its start token")
		}
		inputToken, err := tx.FindTokenByID(ctx, *node.InputTokenID)
		if err != nil {
			return err
		}
		metadata := parseFlowMetadata(instance.MetadataJSON)
		freshMetadata := metadata.Raw
		if freshMetadata == nil {
			freshMetadata = map[string]any{}
		}
		// Ein Retry ist ein neuer fachlicher Lauf. Die alte Correlation enthält
		// weiterhin die fehlgeschlagenen Kindläufe und darf deshalb nicht mit dem
		// neuen Versuch vermischt werden.
		delete(freshMetadata, metadataCorrelationIDKey)
		freshMetadata = EnsureCorrelationMetadata(freshMetadata, metadata.CorrelationLabel, metadata.CorrelationDescription)
		freshMetadataJSON, err := json.Marshal(freshMetadata)
		if err != nil {
			return err
		}
		setKey = strings.TrimSpace(setKey)
		beforeValue := ""
		var updatedRaw string
		if setKey != "" {
			payload, parseErr := tokens.ParseTokenPayload(inputToken.RawData)
			if parseErr != nil {
				return parseErr
			}
			beforeValue = tokens.TokenPayloadSetString(payload, setKey)
			updatedRaw, err = tokens.RewriteTokenPayloadSet(inputToken.RawData, setKey, strings.TrimSpace(value))
		} else {
			beforeValue = workflowTokenEditableValue(inputToken.RawData)
			updatedRaw, err = rewritePrimaryTokenPayloadSet(inputToken.RawData, value)
		}
		if err != nil {
			return err
		}
		replacement := &Token{RawData: updatedRaw}
		if err := tx.CreateToken(ctx, replacement); err != nil {
			return err
		}
		rewrite := &TokenRewrite{
			FlowInstanceID: instance.ID,
			NodeInstanceID: node.ID,
			OldTokenID:     inputToken.ID,
			NewTokenID:     replacement.ID,
			SetKey:         setKey,
			BeforeToken:    inputToken.RawData,
			AfterToken:     updatedRaw,
			BeforeValue:    beforeValue,
			AfterValue:     strings.TrimSpace(value),
			CreatedAt:      now,
		}
		if err := tx.db.WithContext(ctx).Create(rewrite).Error; err != nil {
			return err
		}
		node.InputTokenID = &replacement.ID
		node.InputToken = replacement
		node.OutputTokenID = nil
		node.OutputToken = nil
		node.Status = NodeStatusQueued
		node.ErrorMessage = ""
		node.ErrorCode = 0
		node.Active = true
		node.StartedAt = now
		node.UpdatedAt = now
		node.FinishedAt = nil
		node.MetadataJSON = string(freshMetadataJSON)
		if err := tx.UpdateNodeInstance(ctx, &node); err != nil {
			return err
		}

		instance.InputTokenID = &replacement.ID
		instance.InputToken = replacement
		instance.OutputTokenID = nil
		instance.OutputToken = nil
		instance.Status = FlowStatusRunning
		instance.CurrentNodeIndex = node.NodeIndex
		instance.ErrorMessage = ""
		instance.ErrorCode = 0
		instance.FinishedAt = nil
		instance.UpdatedAt = now
		instance.MetadataJSON = string(freshMetadataJSON)
		return tx.UpdateFlowInstance(ctx, &instance)
	}); err != nil {
		return err
	}
	return nil
}

// workflowTokenEditableValue returns the primary editable set's value, or the
// original token text when the payload has no editable set.
func workflowTokenEditableValue(raw string) string {
	payload, err := tokens.ParseTokenPayload(raw)
	if err != nil {
		return strings.TrimSpace(raw)
	}
	key := tokens.TokenPayloadPrimarySet(payload)
	if key == "" {
		return strings.TrimSpace(raw)
	}
	return tokens.TokenPayloadSetString(payload, key)
}

// rewritePrimaryTokenPayloadSet updates the set selected by the payload's
// primary editable-set metadata.
func rewritePrimaryTokenPayloadSet(raw string, value string) (string, error) {
	payload, err := tokens.ParseTokenPayload(raw)
	if err != nil {
		return "", err
	}
	key := tokens.TokenPayloadPrimarySet(payload)
	if key == "" {
		return "", errors.New("token payload has no primary editable set")
	}
	return tokens.RewriteTokenPayloadSet(raw, key, strings.TrimSpace(value))
}

// DiscardFlowInstance markiert einen fehlgeschlagenen Flow als verworfen.
// Dadurch verschwindet der Lauf aus der Nutzeransicht, bleibt aber historisch erhalten.
func (s *Service) DiscardFlowInstance(ctx context.Context, flowInstanceID uint) error {
	if s == nil || s.store == nil {
		return errors.New("flow store is not configured")
	}
	if flowInstanceID == 0 {
		return errors.New("flow instance is required")
	}
	now := time.Now().UTC()
	err := s.store.Transaction(ctx, func(tx *Repository) error {
		instance, err := tx.FindFlowInstanceByID(ctx, flowInstanceID)
		if err != nil {
			return err
		}
		if instance.DiscardedAt != nil {
			return nil
		}
		instance.DiscardedAt = &now
		instance.UpdatedAt = now
		return tx.UpdateFlowInstance(ctx, &instance)
	})
	if err == nil {
		if event, eventErr := s.FlowEventForInstance(ctx, flowInstanceID); eventErr == nil {
			event.BannerAvailable = false
			event.Message = "Workflow verworfen"
			s.broadcastFlowEvent(event)
		}
	}
	return err
}

// executionFromInstance baut einen Laufzeit-Handle aus einem gespeicherten Flow-Lauf.
func (s *Service) executionFromInstance(instance FlowInstance) *FlowExecutionContext {
	exec := &FlowExecutionContext{
		FlowName:         strings.TrimSpace(instance.Name),
		FlowInstanceID:   instance.ID,
		Status:           instance.Status,
		CurrentNodeIndex: instance.CurrentNodeIndex,
		MetadataJSON:     strings.TrimSpace(instance.MetadataJSON),
		IdempotencyKey:   strings.TrimSpace(instance.IdempotencyKey),
		done:             closedDoneChannel(),
	}
	if instance.InputToken != nil {
		exec.StartToken = instance.InputToken.RawData
	}
	if instance.OutputToken != nil {
		exec.EndToken = instance.OutputToken.RawData
	}
	if strings.TrimSpace(instance.ErrorMessage) != "" {
		exec.Error = errors.New(strings.TrimSpace(instance.ErrorMessage))
	}
	return exec
}

// activeExecutionFromInstance returns an active in-memory handle when present,
// otherwise it creates one from the persisted flow instance.
func (s *Service) activeExecutionFromInstance(instance FlowInstance) *FlowExecutionContext {
	exec := s.executionFromInstance(instance)
	exec.done = make(chan struct{})
	// ResumeFlowInstance calls this while holding s.mu.
	exec.engineUserInterfaces = append([]any(nil), s.userInterfaces...)
	return exec
}

// launchExecution registers an execution as active and runs it asynchronously.
func (s *Service) launchExecution(exec *FlowExecutionContext) {
	if exec == nil {
		return
	}
	s.mu.Lock()
	if exec.FlowInstanceID != 0 {
		if _, exists := s.active[exec.FlowInstanceID]; !exists {
			s.active[exec.FlowInstanceID] = exec
		}
		if strings.TrimSpace(exec.IdempotencyKey) != "" {
			s.activeKeys[activeKey(exec.FlowName, exec.IdempotencyKey)] = exec
		}
	}
	s.mu.Unlock()
	if exec.done == nil {
		exec.done = make(chan struct{})
	}
	go s.execute(exec)
}

// execute advances a flow through persisted nodes until it waits, fails, or
// completes.
func (s *Service) execute(exec *FlowExecutionContext) {
	defer close(exec.done)
	defer s.clearActive(exec.FlowInstanceID)

	ctx := context.Background()
	for {
		// Reload state on each pass because child completion can advance a
		// waiting flow while this execution handle remains alive.
		instance, flow, err := s.loadExecutionState(ctx, exec.FlowInstanceID)
		if err != nil {
			s.failExecution(exec, err)
			return
		}
		// Terminal states publish once and synchronize any parent system node.
		if instance.Status == FlowStatusFinished {
			s.finishExecution(exec, instance)
			s.broadcastExecutionEventFromInstance(ctx, exec.FlowInstanceID, instance, flow, nil, FlowStatusFinished, "", nil, true)
			s.notifyTerminalFromInstance(ctx, exec.FlowInstanceID, instance, flow, nil, FlowStatusFinished, nil)
			return
		}
		if instance.Status == FlowStatusFailed {
			s.failExecution(exec, errors.New(strings.TrimSpace(instance.ErrorMessage)))
			s.broadcastExecutionEventFromInstance(ctx, exec.FlowInstanceID, instance, flow, nil, FlowStatusFailed, "", exec.Error, true)
			s.notifyTerminalFromInstance(ctx, exec.FlowInstanceID, instance, flow, nil, FlowStatusFailed, exec.Error)
			return
		}
		if instance.Status == FlowStatusWaiting {
			// Wartende Flows bleiben als derselbe resumierbare Lauf aktiv. Die
			// Kindabschluss-Synchronisierung wechselt ihn atomar auf "stopped";
			// dann setzt diese Schleife am bereits angelegten Folgeknoten fort.
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if instance.CurrentNodeIndex >= len(flow.Nodes) {
			if err := s.finalizeCompletedFlow(ctx, instance); err != nil {
				s.failExecution(exec, err)
				return
			}
			s.finishExecution(exec, instance)
			s.broadcastExecutionEventFromInstance(ctx, exec.FlowInstanceID, instance, flow, nil, FlowStatusFinished, "", nil, true)
			s.notifyTerminalFromInstance(ctx, exec.FlowInstanceID, instance, flow, nil, FlowStatusFinished, nil)
			return
		}

		// Prepare and announce the checkpoint before calling user-supplied code.
		node, input, err := s.prepareNodeForExecution(ctx, instance, flow)
		if err != nil {
			s.failExecution(exec, err)
			return
		}
		nodeCtx := flowNodeContext(node.MetadataJSON)
		s.broadcastExecutionEventFromInstance(ctx, exec.FlowInstanceID, instance, flow, &node, FlowStatusRunning, "", nil, false)
		exec.FlowName = flow.Name
		exec.Status = FlowStatusRunning
		exec.CurrentNodeIndex = node.NodeIndex
		exec.CurrentNodeName = node.Name
		exec.StartToken = input

		currentEvent := s.buildFlowEvent(instance, flow, &node, FlowStatusRunning, "", nil, false)
		runtime := s.runtimeFor(exec.FlowInstanceID, node.ID, node.NodeIndex, flow.Name, flow.UserInterfaces, exec.userInterfaces, exec.engineUserInterfaces, currentEvent)
		action := flow.Nodes[node.NodeIndex].Action
		s.mu.RLock()
		actionMiddlewares := append([]ActionMiddleware(nil), s.actionMiddlewares...)
		s.mu.RUnlock()
		actionMiddlewares = append(actionMiddlewares, exec.actionMiddlewares...)
		for index := len(actionMiddlewares) - 1; index >= 0; index-- {
			action = actionMiddlewares[index].Wrap(action)
		}
		// Middleware wraps the action at the engine boundary; its result is
		// persisted below before the next node is allowed to run.
		output, actionErr := action(nodeCtx, input, runtime)
		if actionErr != nil {
			if len(actionMiddlewares) == 0 {
				output = ""
			}
			if updateErr := s.markExecutionFailed(ctx, instance.ID, node.ID, actionErr, output); updateErr != nil {
				s.failExecution(exec, updateErr)
				return
			}
			s.failExecution(exec, actionErr)
			s.broadcastExecutionEventFromInstance(ctx, exec.FlowInstanceID, instance, flow, &node, FlowStatusFailed, "", actionErr, true)
			s.notifyTerminalFromInstance(ctx, exec.FlowInstanceID, instance, flow, &node, FlowStatusFailed, actionErr)
			return
		}
		if err := s.commitNodeOutput(ctx, instance.ID, node.ID, node.NodeIndex, flow, output); err != nil {
			s.failExecution(exec, err)
			return
		}
		exec.EndToken = output
		if node.NodeIndex+1 >= len(flow.Nodes) {
			if freshInstance, freshFlow, loadErr := s.loadExecutionState(ctx, exec.FlowInstanceID); loadErr == nil {
				instance = freshInstance
				flow = freshFlow
			}
			s.finishExecution(exec, instance)
			s.broadcastExecutionEventFromInstance(ctx, exec.FlowInstanceID, instance, flow, &node, FlowStatusFinished, "", nil, true)
			s.notifyTerminalFromInstance(ctx, exec.FlowInstanceID, instance, flow, &node, FlowStatusFinished, nil)
			return
		}
		exec.Status = FlowStatusStopped
		exec.CurrentNodeIndex = node.NodeIndex + 1
		exec.CurrentNodeName = flow.Nodes[node.NodeIndex+1].Name
	}
}

// broadcastExecutionEventFromInstance builds and publishes a status event from
// persisted execution state.
func (s *Service) broadcastExecutionEventFromInstance(ctx context.Context, flowInstanceID uint, instance FlowInstance, flow Flow, node *NodeInstance, status FlowStatus, message string, execErr error, terminal bool) {
	if s == nil {
		return
	}
	event := s.buildFlowEvent(instance, flow, node, status, message, execErr, terminal)
	if event.FlowInstanceID == 0 {
		event.FlowInstanceID = flowInstanceID
	}
	s.broadcastFlowEvent(event)
	if terminal {
		s.broadcastCorrelationFailure(ctx, event)
	}
}

// notifyTerminalFromInstance publishes a terminal event and invokes terminal
// listeners after the flow state has been persisted.
func (s *Service) notifyTerminalFromInstance(ctx context.Context, flowInstanceID uint, instance FlowInstance, flow Flow, node *NodeInstance, status FlowStatus, execErr error) {
	if s == nil {
		return
	}
	event := s.buildFlowEvent(instance, flow, node, status, "", execErr, true)
	if event.FlowInstanceID == 0 {
		event.FlowInstanceID = flowInstanceID
	}
	s.broadcastTerminal(ctx, event)
}

// broadcastTerminal sends a terminal event to subscribers and registered hooks.
func (s *Service) broadcastTerminal(ctx context.Context, event FlowEvent) {
	if s == nil {
		return
	}
	s.mu.RLock()
	hooks := append([]TerminalHook(nil), s.terminalHooks...)
	s.mu.RUnlock()
	for _, hook := range hooks {
		hook(context.WithoutCancel(ctx), s, event)
	}
}

// FlowStartToken returns the raw input token supplied when a flow instance
// started. The returned token can be decoded with the generic helpers in the
// tokens package.
func (s *Service) FlowStartToken(ctx context.Context, flowInstanceID uint) (string, error) {
	if s == nil || s.store == nil {
		return "", errors.New("flow store is not configured")
	}
	instance, err := s.store.FindFlowInstanceByID(ctx, flowInstanceID)
	if err != nil {
		return "", err
	}
	if instance.InputToken == nil {
		return "", fmt.Errorf("flow instance %d has no start token", flowInstanceID)
	}
	return instance.InputToken.RawData, nil
}

// DecodeFlowStartTokenSet decodes a named set from a flow instance's start
// token. Use DecodeFlowStartTokenMetaSet when the set name is stored in token
// metadata.
func (s *Service) DecodeFlowStartTokenSet(ctx context.Context, flowInstanceID uint, setKey string, target any) error {
	raw, err := s.FlowStartToken(ctx, flowInstanceID)
	if err != nil {
		return err
	}
	return tokens.DecodeTokenPayloadSet(raw, setKey, target)
}

// DecodeFlowStartTokenMetaSet decodes the set whose name is referenced by the
// given metadata key in a flow instance's start token.
func (s *Service) DecodeFlowStartTokenMetaSet(ctx context.Context, flowInstanceID uint, metaKey string, target any) error {
	raw, err := s.FlowStartToken(ctx, flowInstanceID)
	if err != nil {
		return err
	}
	return tokens.DecodeTokenPayloadMetaSet(raw, metaKey, target)
}

// runtimeFor builds the per-node runtime supplied to an action.
func (s *Service) runtimeFor(flowInstanceID uint, nodeID uint, nodeIndex int, flowName string, flowInterfaces []any, callInterfaces []any, engineInterfaces []any, currentEvent FlowEvent) *executionRuntime {
	return &executionRuntime{
		service:          s,
		flowInstanceID:   flowInstanceID,
		nodeID:           nodeID,
		nodeIndex:        nodeIndex,
		flowName:         strings.TrimSpace(flowName),
		flowInterfaces:   flowInterfaces,
		callInterfaces:   callInterfaces,
		engineInterfaces: engineInterfaces,
		currentEvent:     currentEvent,
	}
}

// buildFlowEvent creates the public status snapshot for a flow and its nodes.
func (s *Service) buildFlowEvent(instance FlowInstance, flow Flow, node *NodeInstance, status FlowStatus, message string, execErr error, terminal bool) FlowEvent {
	meta := parseFlowMetadata(instance.MetadataJSON)
	flowLabel := strings.TrimSpace(flow.Label)
	if flowLabel == "" {
		flowLabel = humanizeIdentifier(flow.Name)
	}
	flowDescription := strings.TrimSpace(flow.Description)
	currentNodeIndex := instance.CurrentNodeIndex
	if node != nil {
		currentNodeIndex = node.NodeIndex
	}
	totalSteps := len(flow.Nodes)
	stepNumber := currentNodeIndex + 1
	currentNodeName := ""
	currentNodeLabel := ""
	stepDescription := ""
	nodeID := uint(0)
	nodeStatus := NodeStatusQueued
	if node != nil {
		currentNodeName = strings.TrimSpace(node.Name)
		nodeID = node.ID
		nodeStatus = node.Status
	}
	if currentNodeLabel == "" && currentNodeIndex >= 0 && currentNodeIndex < len(flow.Nodes) {
		currentNodeLabel = strings.TrimSpace(flow.Nodes[currentNodeIndex].Label)
		if currentNodeLabel == "" {
			currentNodeLabel = humanizeIdentifier(flow.Nodes[currentNodeIndex].Name)
		}
		if stepDescription == "" {
			stepDescription = strings.TrimSpace(flow.Nodes[currentNodeIndex].Description)
		}
		if currentNodeName == "" {
			currentNodeName = strings.TrimSpace(flow.Nodes[currentNodeIndex].Name)
		}
	}
	if stepDescription == "" {
		stepDescription = currentNodeLabel
	}
	if stepNumber < 1 {
		stepNumber = 1
	}
	if totalSteps < stepNumber {
		totalSteps = stepNumber
	}
	return FlowEvent{
		FlowInstanceID:         instance.ID,
		FlowName:               strings.TrimSpace(flow.Name),
		FlowLabel:              flowLabel,
		FlowDescription:        flowDescription,
		CorrelationID:          strings.TrimSpace(meta.CorrelationID),
		CorrelationLabel:       strings.TrimSpace(meta.CorrelationLabel),
		CorrelationDescription: strings.TrimSpace(meta.CorrelationDescription),
		ParentFlowInstanceID:   meta.ParentFlowInstanceID,
		ParentNodeID:           meta.ParentNodeID,
		ParentNodeIndex:        meta.ParentNodeIndex,
		Status:                 status,
		CurrentNodeIndex:       currentNodeIndex,
		CurrentNodeCount:       totalSteps,
		CurrentNodeName:        currentNodeName,
		CurrentNodeLabel:       currentNodeLabel,
		StepLabel:              currentNodeLabel,
		StepDescription:        stepDescription,
		Message:                strings.TrimSpace(message),
		ErrorMessage:           errorMessageFrom(execErr),
		UpdatedAt:              instance.UpdatedAt,
		CreatedAt:              instance.StartedAt,
		TotalSteps:             totalSteps,
		StepNumber:             stepNumber,
		NodeID:                 nodeID,
		NodeStatus:             nodeStatus,
		IsTerminal:             terminal,
		BannerAvailable:        status == FlowStatusFailed && instance.DiscardedAt == nil,
		Discarded:              instance.DiscardedAt != nil,
		Nodes:                  buildFlowEventNodes(flow, currentNodeIndex, status, node, totalSteps),
	}
}

// buildFlowEventNodes combines the flow definition with persisted node state
// to produce an ordered status list.
func buildFlowEventNodes(flow Flow, currentNodeIndex int, workflowStatus FlowStatus, currentNode *NodeInstance, totalSteps int) []FlowEventNode {
	if len(flow.Nodes) == 0 && totalSteps <= 0 {
		return nil
	}
	count := len(flow.Nodes)
	if totalSteps > count {
		count = totalSteps
	}
	nodes := make([]FlowEventNode, 0, count)
	for index := 0; index < count; index++ {
		definitionAvailable := index < len(flow.Nodes)
		nodeName := ""
		nodeLabel := ""
		nodeDescription := ""
		if definitionAvailable {
			nodeName = strings.TrimSpace(flow.Nodes[index].Name)
			nodeLabel = strings.TrimSpace(flow.Nodes[index].Label)
			nodeDescription = strings.TrimSpace(flow.Nodes[index].Description)
		}
		if nodeName == "" {
			nodeName = humanizeIdentifier(nodeLabel)
		}
		if nodeName == "" {
			nodeName = humanizeIdentifier("schritt")
		}
		if nodeLabel == "" {
			nodeLabel = nodeName
		}
		nodeStatus := NodeStatusQueued
		switch workflowStatus {
		case FlowStatusFinished:
			nodeStatus = NodeStatusFinished
		case FlowStatusFailed:
			if index < currentNodeIndex {
				nodeStatus = NodeStatusFinished
			} else if index == currentNodeIndex {
				nodeStatus = NodeStatusFailed
			}
		case FlowStatusRunning:
			if index < currentNodeIndex {
				nodeStatus = NodeStatusFinished
			} else if index == currentNodeIndex {
				nodeStatus = NodeStatusRunning
			}
		case FlowStatusStopped, FlowStatusPending:
			if index < currentNodeIndex {
				nodeStatus = NodeStatusFinished
			} else if index == currentNodeIndex {
				nodeStatus = NodeStatusQueued
			}
		default:
			if index < currentNodeIndex {
				nodeStatus = NodeStatusFinished
			} else if index == currentNodeIndex {
				nodeStatus = NodeStatusRunning
			}
		}
		eventNode := FlowEventNode{
			NodeIndex:   index,
			Name:        nodeName,
			Label:       nodeLabel,
			Description: nodeDescription,
			Status:      nodeStatus,
			Active:      index == currentNodeIndex,
		}
		if currentNode != nil && index == currentNodeIndex {
			eventNode.ID = currentNode.ID
			if trimmedName := strings.TrimSpace(currentNode.Name); trimmedName != "" {
				eventNode.Name = trimmedName
			}
			if definitionAvailable && eventNode.Label == "" {
				eventNode.Label = eventNode.Name
			}
			if workflowStatus != FlowStatusFinished && workflowStatus != FlowStatusFailed && currentNode.Status != "" {
				eventNode.Status = currentNode.Status
			}
		}
		nodes = append(nodes, eventNode)
	}
	return nodes
}

// flowDefinition returns a registered flow blueprint or an error if it cannot
// be executed.
func (s *Service) flowDefinition(name string) (Flow, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return Flow{}, errors.New("flow name is required")
	}
	s.mu.RLock()
	flow, ok := s.flows[trimmed]
	s.mu.RUnlock()
	if !ok {
		return Flow{}, fmt.Errorf("%w: %q", ErrFlowNotRegistered, trimmed)
	}
	if len(flow.Nodes) == 0 {
		return Flow{}, fmt.Errorf("flow %q blueprint is empty", trimmed)
	}
	return flow, nil
}

// loadExecutionState loads a persisted flow instance and its registered
// blueprint.
func (s *Service) loadExecutionState(ctx context.Context, flowInstanceID uint) (FlowInstance, Flow, error) {
	instance, err := s.store.FindFlowInstanceByID(ctx, flowInstanceID)
	if err != nil {
		return FlowInstance{}, Flow{}, err
	}
	flow, err := s.flowDefinition(instance.Name)
	if err != nil {
		return FlowInstance{}, Flow{}, err
	}
	return instance, flow, nil
}

// prepareNodeForExecution loads or creates the current node checkpoint and
// returns the input token for its action.
func (s *Service) prepareNodeForExecution(ctx context.Context, instance FlowInstance, flow Flow) (NodeInstance, string, error) {
	if instance.CurrentNodeIndex < 0 || instance.CurrentNodeIndex >= len(flow.Nodes) {
		return NodeInstance{}, "", fmt.Errorf("flow %q has no node at index %d", flow.Name, instance.CurrentNodeIndex)
	}

	var prepared NodeInstance
	var input string
	now := time.Now().UTC()
	if err := s.store.Transaction(ctx, func(tx *Repository) error {
		// Create or recover the node checkpoint inside one transaction so two
		// concurrent resumptions cannot execute different inputs.
		fresh, err := tx.FindFlowInstanceByID(ctx, instance.ID)
		if err != nil {
			return err
		}
		current := findNodeInstanceByIndex(fresh.NodeInstances, instance.CurrentNodeIndex)
		// A checkpoint is created only once; its input comes from the flow start
		// token or the preceding node's output token.
		if current.ID == 0 {
			current = NodeInstance{
				Name:           flow.Nodes[instance.CurrentNodeIndex].Name,
				FlowInstanceID: fresh.ID,
				NodeIndex:      instance.CurrentNodeIndex,
				Status:         NodeStatusQueued,
				MetadataJSON:   strings.TrimSpace(fresh.MetadataJSON),
				Active:         true,
				StartedAt:      now,
				UpdatedAt:      now,
			}
			switch instance.CurrentNodeIndex {
			case 0:
				current.InputTokenID = fresh.InputTokenID
			default:
				prev := findNodeInstanceByIndex(fresh.NodeInstances, instance.CurrentNodeIndex-1)
				current.InputTokenID = prev.OutputTokenID
			}
			if err := tx.CreateNodeInstance(ctx, &current); err != nil {
				return err
			}
		}
		if current.InputToken == nil && current.InputTokenID != nil {
			token, tokenErr := tx.FindTokenByID(ctx, *current.InputTokenID)
			if tokenErr != nil {
				return tokenErr
			}
			current.InputToken = &token
		}
		if strings.TrimSpace(current.MetadataJSON) == "" {
			current.MetadataJSON = strings.TrimSpace(fresh.MetadataJSON)
		}
		// Actions always receive decoded caller-facing token data, regardless
		// of whether storage compressed the token.
		if current.InputToken == nil {
			return fmt.Errorf("flow %q node %q is missing its input token", flow.Name, current.Name)
		}
		decodedInput, decodeErr := tokens.Decode(current.InputToken.RawData)
		if decodeErr != nil {
			return decodeErr
		}
		current.InputToken.RawData = decodedInput
		current.Status = NodeStatusRunning
		current.Active = true
		if current.StartedAt.IsZero() {
			current.StartedAt = now
		}
		current.UpdatedAt = now
		if err := tx.UpdateNodeInstance(ctx, &current); err != nil {
			return err
		}
		fresh.Status = FlowStatusRunning
		fresh.UpdatedAt = now
		if err := tx.UpdateFlowInstance(ctx, &fresh); err != nil {
			return err
		}
		prepared = current
		input = decodedInput
		return nil
	}); err != nil {
		return NodeInstance{}, "", err
	}
	return prepared, input, nil
}

// commitNodeOutput persists a node result and advances the flow.
func (s *Service) commitNodeOutput(ctx context.Context, flowInstanceID uint, nodeID uint, nodeIndex int, flow Flow, output string) error {
	return s.store.Transaction(ctx, func(tx *Repository) error {
		// Persist output, node completion, and the next checkpoint atomically.
		now := time.Now().UTC()
		fresh, err := tx.FindFlowInstanceByID(ctx, flowInstanceID)
		if err != nil {
			return err
		}
		node, err := tx.FindNodeInstanceByID(ctx, nodeID)
		if err != nil {
			return err
		}
		// The same token becomes both the node output and the next node input.
		token := &Token{RawData: output}
		if err := tx.CreateToken(ctx, token); err != nil {
			return err
		}
		node.OutputTokenID = &token.ID
		node.OutputToken = token
		node.Status = NodeStatusFinished
		node.Active = false
		node.UpdatedAt = now
		node.FinishedAt = &now
		if err := tx.UpdateNodeInstance(ctx, &node); err != nil {
			return err
		}
		fresh.OutputTokenID = &token.ID
		fresh.OutputToken = token
		fresh.UpdatedAt = now
		fresh.CurrentNodeIndex = nodeIndex + 1
		// Create the next node now so resume always has a durable checkpoint.
		if nodeIndex+1 < len(flow.Nodes) {
			fresh.Status = FlowStatusStopped
			next := &NodeInstance{
				Name:                   flow.Nodes[nodeIndex+1].Name,
				FlowInstanceID:         fresh.ID,
				NodeIndex:              nodeIndex + 1,
				Status:                 NodeStatusQueued,
				MetadataJSON:           strings.TrimSpace(fresh.MetadataJSON),
				InputTokenID:           &token.ID,
				Active:                 true,
				StartedAt:              now,
				UpdatedAt:              now,
				PreviousNodeInstanceID: &node.ID,
			}
			if err := tx.CreateNodeInstance(ctx, next); err != nil {
				return err
			}
			node.NextNodeInstanceID = &next.ID
			if err := tx.UpdateNodeInstance(ctx, &node); err != nil {
				return err
			}
		} else {
			fresh.Status = FlowStatusFinished
			fresh.FinishedAt = &now
		}
		return tx.UpdateFlowInstance(ctx, &fresh)
	})
}

// markExecutionFailed persists the failure state and output for a node and its
// containing flow.
func (s *Service) markExecutionFailed(ctx context.Context, flowInstanceID uint, nodeID uint, execErr error, output string) error {
	output, err := ensureWorkflowErrorOutput(output, execErr)
	if err != nil {
		return err
	}
	return s.store.Transaction(ctx, func(tx *Repository) error {
		now := time.Now().UTC()
		fresh, err := tx.FindFlowInstanceByID(ctx, flowInstanceID)
		if err != nil {
			return err
		}
		node, err := tx.FindNodeInstanceByID(ctx, nodeID)
		if err != nil {
			return err
		}
		node.Status = NodeStatusFailed
		node.Active = false
		node.ErrorMessage = strings.TrimSpace(execErr.Error())
		node.ErrorCode = -1
		node.UpdatedAt = now
		node.FinishedAt = &now
		token := Token{RawData: output}
		if err := tx.CreateToken(ctx, &token); err != nil {
			return err
		}
		node.OutputTokenID = &token.ID
		node.OutputToken = &token
		if err := tx.UpdateNodeInstance(ctx, &node); err != nil {
			return err
		}
		fresh.Status = FlowStatusFailed
		fresh.ErrorMessage = strings.TrimSpace(execErr.Error())
		fresh.ErrorCode = -1
		fresh.UpdatedAt = now
		fresh.FinishedAt = &now
		if err := tx.UpdateFlowInstance(ctx, &fresh); err != nil {
			return err
		}
		return nil
	})
}

func ensureWorkflowErrorOutput(output string, execErr error) (string, error) {
	var payload tokens.TokenPayload
	if strings.TrimSpace(output) != "" {
		if parsed, err := tokens.ParseTokenPayload(output); err == nil {
			payload = parsed
			if _, exists := payload.Sets[tokens.TokenSetError]; exists {
				return output, nil
			}
		}
	}

	var flowError tokens.FlowError
	if !errors.As(execErr, &flowError) {
		flowError = tokens.FlowError{
			Message: strings.TrimSpace(execErr.Error()),
			Code:    "node_error",
			Data:    map[string]any{},
		}
	}
	if payload.Schema != "" {
		return tokens.NewTokenBuilderFrom(output).SetError(flowError).Build()
	}
	return tokens.NewTokenBuilder().SetError(flowError).Build()
}

// finalizeCompletedFlow persists successful completion of a flow instance.
func (s *Service) finalizeCompletedFlow(ctx context.Context, instance FlowInstance) error {
	now := time.Now().UTC()
	instance.Status = FlowStatusFinished
	instance.UpdatedAt = now
	instance.FinishedAt = &now
	return s.store.UpdateFlowInstance(ctx, &instance)
}

// finishExecution copies terminal persisted state into the execution handle
// and closes its completion signal.
func (s *Service) finishExecution(exec *FlowExecutionContext, instance FlowInstance) {
	if exec == nil {
		return
	}
	exec.Status = FlowStatusFinished
	if exec.EndToken == "" && instance.OutputToken != nil {
		exec.EndToken = instance.OutputToken.RawData
	}
}

// broadcastFlowEvent sends an event to current subscribers without blocking on
// a subscriber that is not ready to receive it.
func (s *Service) broadcastFlowEvent(event FlowEvent) {
	if s == nil {
		return
	}
	s.mu.RLock()
	subscribers := make([]chan FlowEvent, 0, len(s.subscribers))
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

// broadcastCorrelationFailure notifies listeners when a flow failure makes
// its correlation chain terminally unsuccessful.
func (s *Service) broadcastCorrelationFailure(ctx context.Context, event FlowEvent) {
	if s == nil || s.store == nil || strings.TrimSpace(event.CorrelationID) == "" {
		return
	}
	s.mu.RLock()
	hooks := append([]CorrelationFailureHook(nil), s.failureHooks...)
	s.mu.RUnlock()
	if len(hooks) == 0 {
		return
	}
	instances, err := s.store.listFlowInstanceSummariesByCorrelationID(ctx, event.CorrelationID)
	if err != nil {
		return
	}
	var failedInstance *FlowInstance
	for index := range instances {
		instance := &instances[index]
		if instance.Status == FlowStatusPending || instance.Status == FlowStatusRunning || instance.Status == FlowStatusWaiting || instance.Status == FlowStatusStopped {
			return
		}
		if failedInstance == nil && instance.Status == FlowStatusFailed {
			failedInstance = instance
		}
	}
	if failedInstance == nil {
		return
	}
	if event.Status != FlowStatusFailed || event.FlowInstanceID != failedInstance.ID {
		if failedFlow, definitionErr := s.flowDefinition(failedInstance.Name); definitionErr == nil {
			execErr := errors.New(strings.TrimSpace(failedInstance.ErrorMessage))
			event = s.buildFlowEvent(*failedInstance, failedFlow, nil, FlowStatusFailed, "", execErr, true)
		} else {
			event.FlowInstanceID = failedInstance.ID
			event.FlowName = strings.TrimSpace(failedInstance.Name)
			event.Status = FlowStatusFailed
			event.ErrorMessage = strings.TrimSpace(failedInstance.ErrorMessage)
		}
	}
	failed := CorrelationFailureEvent{
		CorrelationID: strings.TrimSpace(event.CorrelationID),
		FlowEvent:     event,
	}
	failed.Workflows = make([]CorrelationWorkflowStatus, 0, len(instances))
	var correlationStart time.Time
	for _, instance := range instances {
		if correlationStart.IsZero() || instance.StartedAt.Before(correlationStart) {
			correlationStart = instance.StartedAt
		}
		duration := time.Duration(0)
		if instance.FinishedAt != nil {
			duration = instance.FinishedAt.Sub(instance.StartedAt)
		}
		failed.Workflows = append(failed.Workflows, CorrelationWorkflowStatus{FlowInstanceID: instance.ID, FlowName: instance.Name, Status: instance.Status, ErrorMessage: instance.ErrorMessage, CreatedAt: instance.StartedAt, FinishedAt: instance.FinishedAt, Duration: duration})
	}
	failed.StartedAt = correlationStart
	failed.FailedAt = event.UpdatedAt
	for _, hook := range hooks {
		hook(ctx, s, failed)
	}
}

// failExecution records an execution error on its handle and closes its done
// signal.
func (s *Service) failExecution(exec *FlowExecutionContext, err error) {
	if exec == nil {
		return
	}
	exec.Status = FlowStatusFailed
	exec.Error = err
}

// clearActive removes a terminal execution from the in-memory active index.
func (s *Service) clearActive(flowInstanceID uint) {
	if flowInstanceID == 0 {
		return
	}
	s.mu.Lock()
	if exec, ok := s.active[flowInstanceID]; ok {
		delete(s.activeKeys, activeKey(exec.FlowName, exec.IdempotencyKey))
	}
	delete(s.active, flowInstanceID)
	s.mu.Unlock()
}

// findActiveExecutionLocked finds a run by flow and idempotency key while the
// caller holds the service mutex.
func (s *Service) findActiveExecutionLocked(name string, idempotencyKey string) *FlowExecutionContext {
	if s == nil || strings.TrimSpace(name) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return nil
	}
	return s.activeKeys[activeKey(name, idempotencyKey)]
}

// activeKey constructs the in-memory lookup key for an idempotent flow run.
func activeKey(name string, idempotencyKey string) string {
	return strings.TrimSpace(name) + "\x00" + strings.TrimSpace(idempotencyKey)
}

// parseFlowMetadata decodes persisted metadata and extracts engine correlation
// and parent-child linkage fields.
func parseFlowMetadata(raw string) workflowMetadata {
	meta := workflowMetadata{}
	if strings.TrimSpace(raw) == "" {
		return meta
	}
	_ = json.Unmarshal([]byte(raw), &meta.Raw)
	meta.CorrelationID = metadataString(meta.Raw[metadataCorrelationIDKey])
	meta.CorrelationLabel = metadataString(meta.Raw[metadataCorrelationLabelKey])
	meta.CorrelationDescription = metadataString(meta.Raw[metadataCorrelationDescriptionKey])
	meta.ParentFlowInstanceID = metadataUint(meta.Raw[metadataParentFlowInstanceIDKey])
	meta.ParentNodeID = metadataUint(meta.Raw[metadataParentNodeIDKey])
	meta.ParentNodeIndex = metadataInt(meta.Raw[metadataParentNodeIndexKey])
	return meta
}

// flowNodeContext attaches persisted flow metadata to the context used by node
// actions.
func flowNodeContext(metadataJSON string) context.Context {
	meta := parseFlowMetadata(metadataJSON)
	return WithFlowMetadata(context.Background(), meta.Raw)
}

type workflowMetadata struct {
	Raw                    map[string]any
	CorrelationID          string
	CorrelationLabel       string
	CorrelationDescription string
	ParentFlowInstanceID   uint
	ParentNodeID           uint
	ParentNodeIndex        int
}

// metadataUint converts a JSON-compatible metadata value to an unsigned ID.
func metadataUint(value any) uint {
	switch typed := value.(type) {
	case float64:
		if typed > 0 {
			return uint(typed)
		}
	case int:
		if typed > 0 {
			return uint(typed)
		}
	case int64:
		if typed > 0 {
			return uint(typed)
		}
	case uint:
		return typed
	case string:
		var out uint
		if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &out); err == nil {
			return out
		}
	}
	return 0
}

// metadataInt converts a JSON-compatible metadata value to an integer.
func metadataInt(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	case uint:
		return int(typed)
	case string:
		var out int
		if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &out); err == nil {
			return out
		}
	}
	return 0
}

// metadataBool converts a JSON-compatible metadata value to a boolean.
func metadataBool(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	case float64:
		return typed != 0
	case int:
		return typed != 0
	case int64:
		return typed != 0
	}
	return false
}

// humanizeIdentifier converts an identifier into a readable display label.
func humanizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "_", " ")
	value = strings.ReplaceAll(value, "-", " ")
	parts := strings.Fields(value)
	for i, part := range parts {
		lower := strings.ToLower(part)
		if len(lower) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(lower[:1]) + lower[1:]
	}
	return strings.Join(parts, " ")
}

// errorMessageFrom returns a trimmed error message, or an empty string for nil.
func errorMessageFrom(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

type executionRuntime struct {
	service          *Service
	flowInstanceID   uint
	nodeID           uint
	nodeIndex        int
	flowName         string
	flowInterfaces   []any
	callInterfaces   []any
	engineInterfaces []any
	currentEvent     FlowEvent
}

// NewTokenBuilder creates a workflow token builder tagged with the current
// flow and node names.
func (r *executionRuntime) NewTokenBuilder(state any) *tokens.TokenBuilder {
	if r == nil {
		return nil
	}
	return NewFlowTokenBuilder(r.flowName, r.currentEvent.CurrentNodeName, state)
}

// CurrentEvent returns the status snapshot for the node executing this runtime.
func (r *executionRuntime) CurrentEvent() FlowEvent {
	if r == nil {
		return FlowEvent{}
	}
	return r.currentEvent
}

// StartFlow starts a child workflow and returns its unique serializable
// instance reference. The child inherits correlation metadata and records this
// node and workflow as its parent.
func (r *executionRuntime) StartFlow(ctx context.Context, name string, startToken string) (FlowRef, error) {
	if r == nil || r.service == nil || r.service.store == nil {
		return 0, errors.New("flow runtime is not configured")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, errors.New("flow name is required")
	}
	parent, err := r.service.store.FindFlowInstanceByID(ctx, r.flowInstanceID)
	if err != nil {
		return 0, err
	}
	metadata := parseFlowMetadata(parent.MetadataJSON).Raw
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata = EnsureCorrelationMetadata(metadata, r.currentEvent.CorrelationLabel, r.currentEvent.CorrelationDescription)
	metadata = InheritCorrelationMetadata(metadata, r.flowInstanceID, r.nodeID, r.nodeIndex)
	return r.service.StartFlow(ctx, name, strings.TrimSpace(startToken), StartOptions{
		Metadata:       metadata,
		Middlewares:    r.service.executionStartMiddlewares(r.flowInstanceID),
		UserInterfaces: r.service.executionUserInterfaces(r.flowInstanceID),
	})
}

// AwaitFlow waits until the referenced workflow completes and returns its
// result as raw JSON. If the workflow fails, its structured error set is
// returned as an error.
func (r *executionRuntime) AwaitFlow(ctx context.Context, ref FlowRef) (json.RawMessage, error) {
	if r == nil || r.service == nil {
		return nil, errors.New("flow runtime is not configured")
	}
	return r.service.AwaitFlow(ctx, ref)
}

// GetExecutionContextForFlow resolves an instance ID to this service's local
// execution handle, or to a snapshot loaded from persistence.
func (r *executionRuntime) GetExecutionContextForFlow(ctx context.Context, flowInstanceID uint) (*FlowExecutionContext, error) {
	if r == nil || r.service == nil {
		return nil, errors.New("flow runtime is not configured")
	}
	return r.service.GetExecutionContextForFlow(ctx, flowInstanceID)
}

func (s *Service) executionStartMiddlewares(flowInstanceID uint) []StartMiddleware {
	s.mu.RLock()
	exec := s.active[flowInstanceID]
	if exec == nil {
		s.mu.RUnlock()
		return nil
	}
	middlewares := append([]StartMiddleware(nil), exec.startMiddlewares...)
	s.mu.RUnlock()
	return middlewares
}

func (s *Service) executionUserInterfaces(flowInstanceID uint) []any {
	s.mu.RLock()
	exec := s.active[flowInstanceID]
	if exec == nil {
		s.mu.RUnlock()
		return nil
	}
	userInterfaces := append([]any(nil), exec.userInterfaces...)
	s.mu.RUnlock()
	return userInterfaces
}

func (s *Service) isActive(flowInstanceID uint) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.active[flowInstanceID]
	return ok
}

// retryFlowSQLiteBusy retries a database operation when SQLite reports a
// transient busy or locked error.
func retryFlowSQLiteBusy(operation func() error) error {
	if operation == nil {
		return nil
	}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = operation()
		if err == nil || !isFlowSQLiteBusyError(err) {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 20 * time.Millisecond)
	}
	return err
}

// isFlowSQLiteBusyError reports whether err represents a transient SQLite
// lock contention error.
func isFlowSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked") || strings.Contains(message, "sqlite_busy")
}

// findNodeInstanceByIndex returns the persisted node at index, or its zero
// value when no node has that index.
func findNodeInstanceByIndex(nodes []NodeInstance, index int) NodeInstance {
	for _, node := range nodes {
		if node.NodeIndex == index {
			return node
		}
	}
	return NodeInstance{}
}

// nearestRetryNodeIndex selects the closest configured retry checkpoint at or
// before the current node.
func nearestRetryNodeIndex(definition Flow, currentNodeIndex int) (int, bool) {
	nearest := -1
	for _, retryName := range definition.RetryNodeNames {
		retryName = strings.TrimSpace(retryName)
		for index, node := range definition.Nodes {
			if strings.TrimSpace(node.Name) != retryName || index > currentNodeIndex {
				continue
			}
			if index > nearest {
				nearest = index
			}
		}
	}
	return nearest, nearest >= 0
}

// isRetryCheckpointNode reports whether the definition marks nodeIndex as a
// retry checkpoint.
func isRetryCheckpointNode(definition Flow, nodeIndex int) bool {
	if nodeIndex < 0 || nodeIndex >= len(definition.Nodes) {
		return false
	}
	name := strings.TrimSpace(definition.Nodes[nodeIndex].Name)
	for _, retryName := range definition.RetryNodeNames {
		if strings.TrimSpace(retryName) == name {
			return true
		}
	}
	return false
}

// closedDoneChannel returns a completion channel that is already signaled.
func closedDoneChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
