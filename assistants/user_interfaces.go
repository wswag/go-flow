package assistants

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrUserInterfaceNotFound  = errors.New("assistant user interface not registered")
	ErrAmbiguousUserInterface = errors.New("multiple assistant user interfaces match the requested type")
)

type assistantUserInterfaceResolver interface {
	resolveUserInterface(reflect.Type) (any, error)
}

type assistantUserInterfacesContextKey struct{}
type assistantUserInterfaceResolverContextKey struct{}

type assistantUserInterfaceDependencies struct {
	call      []any
	assistant []any
	service   []any
}

func (d assistantUserInterfaceDependencies) resolveUserInterface(targetType reflect.Type) (any, error) {
	for _, dependencies := range [][]any{d.call, d.assistant, d.service} {
		resolved, err := resolveAssistantUserInterfaceIn(dependencies, targetType)
		if err == nil || !errors.Is(err, ErrUserInterfaceNotFound) {
			return resolved, err
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrUserInterfaceNotFound, targetType)
}

// WithUserInterfaces adds call-specific dependencies to ctx. These values
// override matching dependencies on the assistant definition and service.
// Values are process-local and are never persisted with an assistant run.
func WithUserInterfaces(ctx context.Context, values ...any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, assistantUserInterfacesContextKey{}, append([]any(nil), values...))
}

// GetInterface resolves a dependency from an assistant callback context or
// from a context.Context wrapped with WithUserInterfaces. Resolution order is
// call-specific, assistant-definition, then service-level.
func GetInterface[T any](source any) (T, error) {
	var zero T
	if source == nil {
		return zero, errors.New("assistant context is not configured")
	}
	resolver, ok := source.(assistantUserInterfaceResolver)
	if !ok {
		if ctx, isContext := source.(context.Context); isContext && ctx != nil {
			call, _ := ctx.Value(assistantUserInterfacesContextKey{}).([]any)
			resolver, ok = ctx.Value(assistantUserInterfaceResolverContextKey{}).(assistantUserInterfaceResolver)
			if dependencies, isAssistantResolver := resolver.(assistantUserInterfaceDependencies); isAssistantResolver {
				dependencies.call = call
				resolver = dependencies
			} else if !ok {
				resolver = assistantUserInterfaceDependencies{call: call}
				ok = true
			}
		}
	}
	if !ok {
		return zero, errors.New("source does not support assistant user interface lookup")
	}
	targetType := reflect.TypeOf((*T)(nil)).Elem()
	resolved, err := resolver.resolveUserInterface(targetType)
	if err != nil {
		return zero, err
	}
	value, ok := resolved.(T)
	if !ok {
		return zero, fmt.Errorf("registered assistant dependency %T cannot be used as %s", resolved, targetType)
	}
	return value, nil
}

func (s *Service) assistantInterfaceDependencies(ctx context.Context, chain compiledChain) assistantUserInterfaceDependencies {
	if ctx == nil {
		ctx = context.Background()
	}
	call, _ := ctx.Value(assistantUserInterfacesContextKey{}).([]any)
	s.mu.RLock()
	serviceInterfaces := append([]any(nil), s.userInterfaces...)
	s.mu.RUnlock()
	return assistantUserInterfaceDependencies{
		call:      append([]any(nil), call...),
		assistant: append([]any(nil), chain.userInterfaces...),
		service:   serviceInterfaces,
	}
}

func (s *Service) withAssistantUserInterfaces(ctx context.Context, chain compiledChain) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	dependencies := s.assistantInterfaceDependencies(ctx, chain)
	ctx = context.WithValue(ctx, assistantUserInterfaceResolverContextKey{}, dependencies)
	return ctx
}

func isNilAssistantDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return isNilAssistantUserInterface(reflected.Kind()) && reflected.IsNil()
}

func resolveAssistantUserInterfaceIn(dependencies []any, targetType reflect.Type) (any, error) {
	var match any
	for _, dependency := range dependencies {
		if dependency == nil {
			continue
		}
		value := reflect.ValueOf(dependency)
		if isNilAssistantUserInterface(value.Kind()) && value.IsNil() {
			continue
		}
		if !value.Type().AssignableTo(targetType) {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("%w: %s", ErrAmbiguousUserInterface, targetType)
		}
		match = dependency
	}
	if match == nil {
		return nil, fmt.Errorf("%w: %s", ErrUserInterfaceNotFound, targetType)
	}
	return match, nil
}

func isNilAssistantUserInterface(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}
