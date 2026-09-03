package control

import (
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestTimerPayloadMatch(t *testing.T) {
	payload := map[string]any{
		"action":     "created",
		"repository": map[string]any{"full_name": "acme/widgets"},
		"issue":      map[string]any{"labels": []any{map[string]any{"name": "agent"}, map[string]any{"name": "urgent"}}},
		"reviews":    []any{map[string]any{"author": map[string]any{"login": "alice"}}},
	}
	event := proto.Event{Type: "webhook.github.issue_comment", Payload: proto.MustMarshal(payload)}
	for name, timer := range map[string]*proto.Timer{
		"exact":        {OnEvent: event.Type, Match: map[string]string{"repository.full_name": "acme/widgets", "action": "created"}},
		"aliases":      {OnEvent: event.Type, Match: map[string]string{"repo": "acme/widgets", "label": "agent"}},
		"nested array": {OnEvent: event.Type, Match: map[string]string{"reviews.author.login": "alice"}},
	} {
		if !timerMatchesEvent(timer, event) {
			t.Fatalf("%s did not match", name)
		}
	}
	for name, timer := range map[string]*proto.Timer{
		"wrong type":  {OnEvent: "webhook.github.push"},
		"wrong repo":  {OnEvent: event.Type, Match: map[string]string{"repo": "other/repo"}},
		"wrong label": {OnEvent: event.Type, Match: map[string]string{"label": "backlog"}},
		"bad payload": {OnEvent: event.Type, Match: map[string]string{"repo": "acme/widgets"}},
	} {
		candidate := event
		if name == "bad payload" {
			candidate.Payload = []byte{0xff}
		}
		if timerMatchesEvent(timer, candidate) {
			t.Fatalf("%s unexpectedly matched", name)
		}
	}
}

func TestTimerPayloadMatchValidationIsBounded(t *testing.T) {
	if err := validateTimerMatch("", map[string]string{"repo": "x"}); err == nil {
		t.Fatal("match without event accepted")
	}
	if err := validateTimerMatch("event", map[string]string{"repository..name": "x"}); err == nil {
		t.Fatal("invalid path accepted")
	}
	if err := validateTimerMatch("event", map[string]string{strings.Repeat("k", maxTimerMatchKey+1): "x"}); err == nil {
		t.Fatal("oversized key accepted")
	}
	tooMany := make(map[string]string, maxTimerMatchFields+1)
	for i := 0; i < maxTimerMatchFields+1; i++ {
		tooMany[string(rune('a'+i))] = "x"
	}
	if err := validateTimerMatch("event", tooMany); err == nil {
		t.Fatal("too many match fields accepted")
	}
}
