package server

import (
	"context"
	"strings"
	"testing"

	"remount.dev/remount/internal/notifier"
	"remount.dev/remount/internal/proto"
)

func TestServerNotifierLifecycleAndStartupFailure(t *testing.T) {
	bad := Options{
		ArtifactGCInterval: -1, EventGCInterval: -1, RecordGCInterval: -1,
		Notifications: []notifier.Subscription{{
			ID: "bad", Tenant: "local", Events: []string{proto.EvEgressPending},
			Destination: notifier.Destination{Kind: notifier.GenericWebhook, URL: "http://127.0.0.1/hook", AllowedHosts: []string{"127.0.0.1"}},
		}},
	}
	if server, err := New(bad); err == nil {
		_ = server.Close()
		t.Fatal("unsafe notifier destination was accepted")
	} else if !strings.Contains(err.Error(), "invalid notifier configuration") {
		t.Fatalf("startup error = %v", err)
	}

	s, err := New(Options{
		ArtifactGCInterval: -1, EventGCInterval: -1, RecordGCInterval: -1,
		NotifierDeadLetterGCInterval: -1,
		Notifications: []notifier.Subscription{{
			ID: "approvals", Tenant: "local", Events: []string{proto.EvEgressPending},
			Destination: notifier.Destination{Kind: notifier.SlackWebhook, URL: "https://hooks.slack.com/services/T/B/test"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.notifiers) != 1 || s.notifierDLQ == nil {
		t.Fatal("configured notifier was not started")
	}
	letters, err := s.NotificationDeadLetters(context.Background(), "local", 0, 10)
	if err != nil || len(letters) != 0 {
		t.Fatalf("dead letters = %#v, %v", letters, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
