package workflows

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wswag/go-flow/tokens"
)

type FlowStatus string

const (
	FlowStatusPending  FlowStatus = "pending"
	FlowStatusRunning  FlowStatus = "running"
	FlowStatusWaiting  FlowStatus = "waiting"
	FlowStatusStopped  FlowStatus = "stopped"
	FlowStatusFinished FlowStatus = "finished"
	FlowStatusFailed   FlowStatus = "failed"
)

type NodeStatus string

const (
	NodeStatusQueued   NodeStatus = "queued"
	NodeStatusRunning  NodeStatus = "running"
	NodeStatusWaiting  NodeStatus = "waiting"
	NodeStatusFinished NodeStatus = "finished"
	NodeStatusFailed   NodeStatus = "failed"
)

// FlowRef is the serializable identifier of one workflow instance.
type FlowRef uint

// Flow beschreibt den ausführbaren Blueprint eines Workflows.
type Flow struct {
	Name        string
	Label       string
	Description string
	// StartMiddlewares are included when this workflow is started. Sequence
	// wrappers order them together with engine-level and per-call middleware.
	StartMiddlewares []StartMiddleware
	// UserInterfaces contains application-provided dependencies available to
	// every node in this workflow through GetInterface[T]. Instances are shared
	// by all executions of this registered flow.
	UserInterfaces []any
	// RetryNodeNames declares persisted checkpoints from which a failed run may
	// be restarted. The engine chooses the nearest declared node at or before
	// the failed/current node and starts a fresh workflow instance there.
	RetryNodeNames []string
	Nodes          []FlowNode
}

// FlowNode beschreibt eine fachliche Schrittgrenze innerhalb eines Workflows.
type FlowNode struct {
	Name        string
	Label       string
	Description string
	Action      Action
}

// Runtime beschreibt die Fähigkeiten, die ein Node während seiner Ausführung
// für Folge-Workflows nutzen darf.
type Runtime interface {
	// NewTokenBuilder creates a workflow token builder tagged with the current
	// flow and node names. A non-nil state is stored in the "state" set.
	NewTokenBuilder(state any) *tokens.TokenBuilder
	CurrentEvent() FlowEvent
	// StartFlow starts a child flow and returns its serializable reference.
	StartFlow(ctx context.Context, name string, startToken string) (FlowRef, error)
	// AwaitFlow waits for that reference and returns result JSON or a flow error.
	AwaitFlow(ctx context.Context, ref FlowRef) (json.RawMessage, error)
	// GetExecutionContextForFlow resolves an instance ID to its execution context.
	GetExecutionContextForFlow(ctx context.Context, flowInstanceID uint) (*FlowExecutionContext, error)
}

// Action führt einen einzelnen Workflow-Schritt aus.
// `rawInput` ist der persistierte Input-Token des Knotens, `runtime` kann
// aus dem laufenden Knoten heraus weitere Flows anstoßen.
type Action func(ctx context.Context, rawInput string, runtime Runtime) (string, error)

// FlowExecutionContext hält den Laufzeitstatus eines gestarteten oder wiederaufgenommenen Flows.
type FlowExecutionContext struct {
	FlowName         string
	FlowInstanceID   uint
	Status           FlowStatus
	CurrentNodeIndex int
	CurrentNodeName  string
	StartToken       string
	EndToken         string
	MetadataJSON     string
	IdempotencyKey   string
	Reused           bool
	Error            error

	done                 chan struct{}
	startMiddlewares     []StartMiddleware
	actionMiddlewares    []ActionMiddleware
	userInterfaces       []any
	engineUserInterfaces []any
}

// StartOptions describes optional behavior for StartFlow.
type StartOptions struct {
	// Metadata is persisted with the flow instance and its initial node.
	Metadata map[string]any
	// IdempotencyKey is used by Service.IdempotencyMiddleware to find and reuse
	// an active execution with the same flow name and key. StartFlow adds this
	// middleware at the end of the chain unless it is already present.
	IdempotencyKey string

	// Middlewares adds middleware for this StartFlow call after engine-level
	// middleware. The chain is fixed when StartFlow is called; changes to this
	// slice from within a middleware do not alter the active chain. Child
	// workflows started from this execution inherit these middleware.
	Middlewares []StartMiddleware
	// ActionMiddlewares wraps each node action in this execution after
	// engine-level action middleware. These functions are process-local and are
	// not persisted for a later resume.
	ActionMiddlewares []ActionMiddleware
	// UserInterfaces supplies call-specific dependencies. They take precedence
	// over matching dependencies in Flow.UserInterfaces and are process-local,
	// so they are not restored after a process restart.
	UserInterfaces          []any
	engineUserInterfaces    []any
	engineUserInterfacesSet bool
}

// CorrelationFailureEvent beschreibt den terminalen Fehler eines Laufs innerhalb
// einer fachlichen Workflow-Correlation. Fachliche Listener können damit die
// gesamte Kette als fehlgeschlagen behandeln.
type CorrelationFailureEvent struct {
	CorrelationID string
	FlowEvent     FlowEvent
	Workflows     []CorrelationWorkflowStatus
	StartedAt     time.Time
	FailedAt      time.Time
}

// CorrelationWorkflowStatus is a generic, app-independent summary of one
// workflow in a failed correlation.
type CorrelationWorkflowStatus struct {
	FlowInstanceID uint
	FlowName       string
	Status         FlowStatus
	ErrorMessage   string
	CreatedAt      time.Time
	FinishedAt     *time.Time
	Duration       time.Duration
}

// CorrelationFailureHook verarbeitet terminale Fehler einer Workflow-Correlation.
// Der Hook darf keine fachlichen Annahmen über den konkreten Flow treffen.
type CorrelationFailureHook func(context.Context, *Service, CorrelationFailureEvent)

// TerminalHook beobachtet einen bereits persistent abgeschlossenen Workflow.
// Fehler im Hook dürfen den Abschluss des Workflows nicht verändern.
type TerminalHook func(context.Context, *Service, FlowEvent)

// FlowEvent beschreibt einen live gemeldeten Workflow-Schritt für Ticker und Banner.
type FlowEvent struct {
	FlowInstanceID         uint            `json:"flowInstanceId"`
	FlowName               string          `json:"flowName"`
	FlowLabel              string          `json:"flowLabel"`
	FlowDescription        string          `json:"flowDescription,omitempty"`
	CorrelationID          string          `json:"correlationId,omitempty"`
	CorrelationLabel       string          `json:"correlationLabel,omitempty"`
	CorrelationDescription string          `json:"correlationDescription,omitempty"`
	ParentFlowInstanceID   uint            `json:"parentFlowInstanceId,omitempty"`
	ParentNodeID           uint            `json:"parentNodeId,omitempty"`
	ParentNodeIndex        int             `json:"parentNodeIndex,omitempty"`
	Status                 FlowStatus      `json:"status"`
	CurrentNodeIndex       int             `json:"currentNodeIndex"`
	CurrentNodeCount       int             `json:"currentNodeCount"`
	CurrentNodeName        string          `json:"currentNodeName"`
	CurrentNodeLabel       string          `json:"currentNodeLabel"`
	StepLabel              string          `json:"stepLabel"`
	StepDescription        string          `json:"stepDescription"`
	Message                string          `json:"message,omitempty"`
	ErrorMessage           string          `json:"errorMessage,omitempty"`
	UpdatedAt              time.Time       `json:"updatedAt"`
	CreatedAt              time.Time       `json:"createdAt,omitempty"`
	TotalSteps             int             `json:"totalSteps"`
	StepNumber             int             `json:"stepNumber"`
	NodeID                 uint            `json:"nodeId,omitempty"`
	NodeStatus             NodeStatus      `json:"nodeStatus,omitempty"`
	IsTerminal             bool            `json:"isTerminal,omitempty"`
	BannerAvailable        bool            `json:"bannerAvailable,omitempty"`
	Discarded              bool            `json:"discarded,omitempty"`
	Nodes                  []FlowEventNode `json:"nodes,omitempty"`
}

// FlowEventNode beschreibt einen einzelnen Workflow-Schritt für Live-Statusanzeigen.
type FlowEventNode struct {
	ID          uint       `json:"id,omitempty"`
	NodeIndex   int        `json:"nodeIndex"`
	Name        string     `json:"name"`
	Label       string     `json:"label,omitempty"`
	Description string     `json:"description,omitempty"`
	Status      NodeStatus `json:"status"`
	Active      bool       `json:"active"`
}

// Done returns a channel that closes when the workflow execution terminates.
func (c *FlowExecutionContext) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

// FlowInstance speichert einen gesamten Workflow-Lauf mit Checkpoints.
type FlowInstance struct {
	ID               uint       `gorm:"primaryKey"`
	Name             string     `gorm:"index"`
	Status           FlowStatus `gorm:"index"`
	IdempotencyKey   string     `gorm:"index"`
	CurrentNodeIndex int        `gorm:"index"`
	InputTokenID     *uint      `gorm:"index"`
	OutputTokenID    *uint      `gorm:"index"`
	MetadataJSON     string     `gorm:"type:text"`
	ErrorMessage     string     `gorm:"type:text"`
	ErrorCode        int
	DiscardedAt      *time.Time     `gorm:"index"`
	StartedAt        time.Time      `gorm:"index"`
	UpdatedAt        time.Time      `gorm:"index"`
	FinishedAt       *time.Time     `gorm:"index"`
	InputToken       *Token         `gorm:"foreignKey:InputTokenID"`
	OutputToken      *Token         `gorm:"foreignKey:OutputTokenID"`
	NodeInstances    []NodeInstance `gorm:"foreignKey:FlowInstanceID"`
}

// NodeInstance speichert einen einzelnen Knoten innerhalb eines Flow-Laufs.
type NodeInstance struct {
	ID                     uint       `gorm:"primaryKey"`
	Name                   string     `gorm:"index"`
	FlowInstanceID         uint       `gorm:"index;uniqueIndex:idx_flow_node_index,priority:1"`
	NodeIndex              int        `gorm:"index;uniqueIndex:idx_flow_node_index,priority:2"`
	Status                 NodeStatus `gorm:"index"`
	InputTokenID           *uint      `gorm:"index"`
	OutputTokenID          *uint      `gorm:"index"`
	MetadataJSON           string     `gorm:"type:text"`
	ErrorMessage           string     `gorm:"type:text"`
	ErrorCode              int
	PreviousNodeInstanceID *uint      `gorm:"index"`
	NextNodeInstanceID     *uint      `gorm:"index"`
	StartedAt              time.Time  `gorm:"index"`
	UpdatedAt              time.Time  `gorm:"index"`
	FinishedAt             *time.Time `gorm:"index"`
	Active                 bool

	InputToken           *Token        `gorm:"foreignKey:InputTokenID"`
	OutputToken          *Token        `gorm:"foreignKey:OutputTokenID"`
	PreviousNodeInstance *NodeInstance `gorm:"foreignKey:PreviousNodeInstanceID"`
	NextNodeInstance     *NodeInstance `gorm:"foreignKey:NextNodeInstanceID"`
	FlowInstance         *FlowInstance `gorm:"foreignKey:FlowInstanceID"`
}

// Token speichert einen persistierten Input- oder Output-Wert zwischen Workflow-Knoten.
type Token struct {
	ID        uint      `gorm:"primaryKey"`
	RawData   string    `gorm:"type:longtext"`
	CreatedAt time.Time `gorm:"index"`
}

// TokenRewrite protokolliert reversible Admin-/Retry-Aenderungen an Start-Tokens.
// Der vollstaendige Vorher-/Nachher-Token bleibt erhalten, damit fehlerhafte
// Rewrites ohne manuelle Datenbankreparatur zurueckgenommen werden koennen.
type TokenRewrite struct {
	ID             uint       `gorm:"primaryKey"`
	FlowInstanceID uint       `gorm:"index"`
	NodeInstanceID uint       `gorm:"index"`
	OldTokenID     uint       `gorm:"index"`
	NewTokenID     uint       `gorm:"index"`
	SetKey         string     `gorm:"type:varchar(255);index"`
	BeforeToken    string     `gorm:"type:longtext"`
	AfterToken     string     `gorm:"type:longtext"`
	BeforeValue    string     `gorm:"type:longtext"`
	AfterValue     string     `gorm:"type:longtext"`
	CreatedAt      time.Time  `gorm:"index"`
	RevertedAt     *time.Time `gorm:"index"`
}

// TokenPayload stores generic named sets and optional metadata.
type TokenPayload = tokens.TokenPayload
