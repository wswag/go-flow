// Package gzip registers a gzip storage codec with tokens.
package gzip

import (
	"bytes"
	stdgzip "compress/gzip"
	"encoding/base64"
	"io"
	"strings"

	"github.com/wswag/go-flow/tokens"
)

const prefix = "gz:"

type codec struct{}

func init() { _ = tokens.RegisterCodec(prefix, codec{}) }

func (codec) Encode(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return raw, nil
	}
	var buffer bytes.Buffer
	w := stdgzip.NewWriter(&buffer)
	if _, err := w.Write([]byte(raw)); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	encoded := prefix + base64.StdEncoding.EncodeToString(buffer.Bytes())
	if len(encoded) >= len(raw) {
		return raw, nil
	}
	return encoded, nil
}

func (codec) Decode(stored string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, prefix))
	if err != nil {
		return "", err
	}
	r, err := stdgzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	defer r.Close()
	decoded, err := io.ReadAll(r)
	return string(decoded), err
}
