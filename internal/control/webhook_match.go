package control

import (
	"fmt"
	"reflect"
	"strings"

	"remount.dev/remount/internal/proto"
)

const (
	maxTimerMatchFields = 32
	maxTimerMatchKey    = 128
	maxTimerMatchValue  = 512
)

func validateTimerMatch(onEvent string, match map[string]string) error {
	if len(match) == 0 {
		return nil
	}
	if onEvent == "" {
		return proto.Err(proto.CodeBadRequest, "timer match requires an event")
	}
	if len(match) > maxTimerMatchFields {
		return proto.Err(proto.CodeBadRequest, "timer match exceeds %d fields", maxTimerMatchFields)
	}
	for key, value := range match {
		if key == "" || len(key) > maxTimerMatchKey || len(value) > maxTimerMatchValue {
			return proto.Err(proto.CodeBadRequest, "timer match fields must have a 1-%d byte key and at most %d byte value", maxTimerMatchKey, maxTimerMatchValue)
		}
		for _, part := range strings.Split(key, ".") {
			if part == "" {
				return proto.Err(proto.CodeBadRequest, "timer match path %q is invalid", key)
			}
		}
	}
	return nil
}

func timerMatchesEvent(timer *proto.Timer, event proto.Event) bool {
	if timer.OnEvent == "" || timer.OnEvent != event.Type {
		return false
	}
	if len(timer.Match) == 0 {
		return true
	}
	var payload any
	if len(event.Payload) == 0 || proto.Unmarshal(event.Payload, &payload) != nil {
		return false
	}
	for key, want := range timer.Match {
		if !payloadFieldMatches(payload, key, want) {
			return false
		}
	}
	return true
}

func payloadFieldMatches(payload any, key, want string) bool {
	paths := []string{key}
	switch key {
	case "repo":
		paths = []string{"repository.full_name", "repository.name", "repo"}
	case "label":
		paths = []string{"label.name", "issue.labels", "labels", "label"}
	}
	for _, path := range paths {
		for _, value := range payloadValuesAt(payload, strings.Split(path, ".")) {
			if scalarText(value) == want {
				return true
			}
		}
	}
	return false
}

func payloadValuesAt(value any, path []string) []any {
	if len(path) == 0 {
		return leafValues(value)
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil
	}
	if rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		return payloadValuesAt(rv.Elem().Interface(), path)
	}
	switch rv.Kind() {
	case reflect.Map:
		for _, mapKey := range rv.MapKeys() {
			if fmt.Sprint(mapKey.Interface()) == path[0] {
				return payloadValuesAt(rv.MapIndex(mapKey).Interface(), path[1:])
			}
		}
	case reflect.Slice, reflect.Array:
		var out []any
		for i := 0; i < rv.Len(); i++ {
			out = append(out, payloadValuesAt(rv.Index(i).Interface(), path)...)
		}
	}
	return nil
}

func leafValues(value any) []any {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil
	}
	if rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		return leafValues(rv.Elem().Interface())
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		// GitHub label lists contain objects. Treat their name as the scalar
		// while retaining direct scalar lists for generic adapters.
		if rv.Kind() == reflect.Map {
			for _, key := range rv.MapKeys() {
				if fmt.Sprint(key.Interface()) == "name" {
					return leafValues(rv.MapIndex(key).Interface())
				}
			}
		}
		return []any{value}
	}
	var out []any
	for i := 0; i < rv.Len(); i++ {
		out = append(out, leafValues(rv.Index(i).Interface())...)
	}
	return out
}

func scalarText(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case fmt.Stringer:
		return value.String()
	default:
		rv := reflect.ValueOf(value)
		if !rv.IsValid() || rv.Kind() == reflect.Map || rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			return ""
		}
		return fmt.Sprint(value)
	}
}
