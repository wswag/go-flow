package flow

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
	"strings"

	"gorm.io/gorm"
)

const compressedTokenPrefix = "gz:"

// BeforeSave compresses a token's raw data when compression reduces its size.
func (t *Token) BeforeSave(_ *gorm.DB) error {
	encoded, err := encodeTokenRawData(t.RawData)
	if err != nil {
		return err
	}
	t.RawData = encoded
	return nil
}

// AfterFind restores compressed token data after GORM loads a token.
func (t *Token) AfterFind(_ *gorm.DB) error {
	decoded, err := decodeTokenRawData(t.RawData)
	if err != nil {
		return err
	}
	t.RawData = decoded
	return nil
}

// encodeTokenRawData gzip-compresses raw data and prefixes it with a marker
// when the encoded form is shorter than the original.
func encodeTokenRawData(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}

	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write([]byte(raw)); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}

	// Keep small or incompressible values readable and avoid storage overhead.
	compressed := compressedTokenPrefix + base64.StdEncoding.EncodeToString(buffer.Bytes())
	if len(compressed) >= len(raw) {
		return raw, nil
	}
	return compressed, nil
}

// decodeTokenRawData decompresses marked values and leaves unmarked legacy
// storage values unchanged.
func decodeTokenRawData(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, compressedTokenPrefix) {
		return raw, nil
	}
	// The marker distinguishes compressed records from previously stored text.
	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(raw, compressedTokenPrefix))
	if err != nil {
		return "", err
	}
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer reader.Close()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}
