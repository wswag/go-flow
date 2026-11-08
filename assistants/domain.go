package assistants

import (
	"errors"
	"time"

	"github.com/wswag/go-flow/tokens"
	_ "github.com/wswag/go-flow/tokens/gzip"
	"gorm.io/gorm"
)

var ErrRunExpired = errors.New("assistant run expired")

type DraftSession struct {
	AssistantName string
	Key           StateKey
	AccessRef     string
	AccessToken   string
	ExpiresAt     *time.Time
}

// ChainRecord speichert den persistierten UI-Zustand einer Schrittkette pro Schlüssel.
// `ScopeKey` und `ActorKey` bleiben generisch, damit Fachkontexte außerhalb des Packages definiert werden.
type ChainRecord struct {
	ID       uint   `gorm:"primaryKey"`
	Name     string `gorm:"type:varchar(120);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:1"`
	ScopeKey string `gorm:"type:varchar(255);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:2"`
	ActorKey string `gorm:"type:varchar(120);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:3"`
	DraftID  string `gorm:"type:varchar(64);index;uniqueIndex:idx_assistant_chain_scope_actor,priority:4"`
	// AccessRef is an application-defined, non-secret selector for this state.
	AccessRef string `gorm:"type:varchar(255);index:idx_assistant_access_ref"`
	// AccessToken is opaque to this package; applications define its meaning and validation.
	AccessToken     string     `gorm:"type:longtext"`
	AccessExpiresAt *time.Time `gorm:"index"`
	AutoExtendTTL   bool       `gorm:"not null;default:false"`
	RunTTLSeconds   int64      `gorm:"not null;default:0"`
	Status          string     `gorm:"type:varchar(24);index"`
	StateJSON       string     `gorm:"type:longtext"`
	CreatedAt       time.Time  `gorm:"index"`
	UpdatedAt       time.Time  `gorm:"index"`
}

type RunStatus string

const (
	RunStatusActive    RunStatus = "active"
	RunStatusCompleted RunStatus = "completed"
	RunStatusDiscarded RunStatus = "discarded"
)

type CleanupRunsOptions struct {
	Expired    bool
	MinimumAge time.Duration
	Statuses   []RunStatus
	Limit      int
}

func (r *ChainRecord) BeforeSave(_ *gorm.DB) error {
	encoded, err := tokens.Encode(r.StateJSON)
	if err == nil {
		r.StateJSON = encoded
	}
	return err
}

func (r *ChainRecord) AfterSave(_ *gorm.DB) error {
	decoded, err := tokens.Decode(r.StateJSON)
	if err == nil {
		r.StateJSON = decoded
	}
	return err
}

func (r *ChainRecord) AfterFind(_ *gorm.DB) error {
	decoded, err := tokens.Decode(r.StateJSON)
	if err == nil {
		r.StateJSON = decoded
	}
	return err
}

// StateKey beschreibt den fachlichen Zielkontext einer Schrittkette.
// Die konkreten Schlüssel werden vom jeweiligen Aufrufer definiert.
type StateKey struct {
	ScopeKey string
	ActorKey string
	DraftID  string
}

// ActionClaim reserves an application-defined action once for one assistant run.
type ActionClaim struct {
	ID        uint      `gorm:"primaryKey"`
	DraftID   string    `gorm:"type:varchar(64);uniqueIndex:idx_assistant_action_claim,priority:1"`
	ActionKey string    `gorm:"type:varchar(120);uniqueIndex:idx_assistant_action_claim,priority:2"`
	CreatedAt time.Time `gorm:"index"`
}

func (ActionClaim) TableName() string { return "assistant_action_claims" }
