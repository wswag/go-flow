package workflows

import (
	"github.com/wswag/go-flow/tokens"
	_ "github.com/wswag/go-flow/tokens/gzip"
	"gorm.io/gorm"
)

const compressedTokenPrefix = "gz:"

func (t *Token) BeforeSave(_ *gorm.DB) error {
	encoded, err := tokens.Encode(t.RawData)
	if err == nil {
		t.RawData = encoded
	}
	return err
}

func (t *Token) AfterFind(_ *gorm.DB) error {
	decoded, err := tokens.Decode(t.RawData)
	if err == nil {
		t.RawData = decoded
	}
	return err
}

func encodeTokenRawData(raw string) (string, error) { return tokens.Encode(raw) }
func decodeTokenRawData(raw string) (string, error) { return tokens.Decode(raw) }
