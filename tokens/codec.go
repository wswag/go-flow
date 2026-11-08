// Package tokens provides a shared token payload format and extensible storage codecs.
package tokens

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

// Codec encodes token text for storage and decodes values identified by its prefix.
type Codec interface {
	Encode(string) (string, error)
	Decode(string) (string, error)
}

var codecRegistry = struct {
	sync.RWMutex
	codecs map[string]Codec
}{codecs: make(map[string]Codec)}

// RegisterCodec registers a storage codec under a unique non-empty prefix.
func RegisterCodec(prefix string, codec Codec) error {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || codec == nil {
		return errors.New("codec prefix and implementation are required")
	}
	codecRegistry.Lock()
	defer codecRegistry.Unlock()
	if _, exists := codecRegistry.codecs[prefix]; exists {
		return errors.New("codec prefix is already registered")
	}
	codecRegistry.codecs[prefix] = codec
	return nil
}

// Encode applies registered codecs in prefix order; codecs may leave text unchanged.
func Encode(raw string) (string, error) {
	codecRegistry.RLock()
	prefixes := make([]string, 0, len(codecRegistry.codecs))
	for prefix := range codecRegistry.codecs {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	codecs := make(map[string]Codec, len(prefixes))
	for _, prefix := range prefixes {
		codecs[prefix] = codecRegistry.codecs[prefix]
	}
	codecRegistry.RUnlock()
	for _, prefix := range prefixes {
		encoded, err := codecs[prefix].Encode(raw)
		if err != nil {
			return "", err
		}
		if strings.HasPrefix(encoded, prefix) {
			return encoded, nil
		}
	}
	return raw, nil
}

// Decode expands a value with a registered prefix and preserves legacy plain text.
func Decode(stored string) (string, error) {
	codecRegistry.RLock()
	prefixes := make([]string, 0, len(codecRegistry.codecs))
	for prefix := range codecRegistry.codecs {
		prefixes = append(prefixes, prefix)
	}
	sort.Slice(prefixes, func(i, j int) bool { return len(prefixes[i]) > len(prefixes[j]) })
	for _, prefix := range prefixes {
		if strings.HasPrefix(stored, prefix) {
			codec := codecRegistry.codecs[prefix]
			codecRegistry.RUnlock()
			return codec.Decode(stored)
		}
	}
	codecRegistry.RUnlock()
	return stored, nil
}
