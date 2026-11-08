package workflows

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrUserInterfaceNotFound  = errors.New("workflow user interface not registered")
	ErrAmbiguousUserInterface = errors.New("multiple workflow user interfaces match the requested type")
)

type userInterfaceResolver interface {
	resolveUserInterface(reflect.Type) (any, error)
}

// GetInterface resolves an application dependency registered in the current
// flow definition. source may be a Runtime (for node actions and action
// middlewares) or a StartRequest (for start middlewares). Go does not support
// generic methods, so this is a package function used as
// workflows.GetInterface[MyInterface](runtime) or
// workflows.GetInterface[MyInterface](request).
//
// The registered value must be assignable to T. No match and multiple matches
// are reported as errors rather than selecting a dependency unpredictably.
func GetInterface[T any](source any) (T, error) {
	var zero T
	if source == nil {
		return zero, errors.New("workflow runtime is not configured")
	}
	resolver, ok := source.(userInterfaceResolver)
	if !ok {
		return zero, errors.New("source does not support workflow user interface lookup")
	}
	targetType := reflect.TypeOf((*T)(nil)).Elem()
	resolved, err := resolver.resolveUserInterface(targetType)
	if err != nil {
		return zero, err
	}
	value, ok := resolved.(T)
	if !ok {
		return zero, fmt.Errorf("registered workflow dependency %T cannot be used as %s", resolved, targetType)
	}
	return value, nil
}

func (r *executionRuntime) resolveUserInterface(targetType reflect.Type) (any, error) {
	if r == nil {
		return nil, errors.New("workflow runtime is not configured")
	}
	return resolveUserInterfaceFrom(r.callInterfaces, r.flowInterfaces, targetType)
}

func resolveUserInterfaceFrom(preferred, fallback []any, targetType reflect.Type) (any, error) {
	resolved, err := resolveUserInterfaceIn(preferred, targetType)
	if err == nil || !errors.Is(err, ErrUserInterfaceNotFound) {
		return resolved, err
	}
	return resolveUserInterfaceIn(fallback, targetType)
}

func resolveUserInterfaceIn(dependencies []any, targetType reflect.Type) (any, error) {
	var match any
	for _, dependency := range dependencies {
		if dependency == nil {
			continue
		}
		value := reflect.ValueOf(dependency)
		if isNilable(value.Kind()) && value.IsNil() {
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

func isNilable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}
