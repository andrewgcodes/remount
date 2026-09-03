package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/notifier"
	"remount.dev/remount/internal/server"
)

func TestLoadNotificationsResolvesSecretsOnlyFromEnvironment(t *testing.T) {
	t.Setenv("TEST_GITHUB_WEBHOOK_SECRET", "github-secret-value")
	t.Setenv("TEST_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/path-secret")
	t.Setenv("TEST_GENERIC_BEARER", "generic-secret-value")
	path := filepath.Join(t.TempDir(), "notifications.json")
	raw := `{
  "webhooks": {
    "github_secret_env": "TEST_GITHUB_WEBHOOK_SECRET",
    "generic_bearer_env": "TEST_GENERIC_BEARER",
    "slack_replay_window": "4m"
  },
  "subscriptions": [{
    "id": "security", "tenant": "tenant-a", "events": ["egress.pending", "pool.*"],
    "kind": "slack_webhook", "url_env": "TEST_SLACK_WEBHOOK_URL"
  }],
  "limits": {"attempts": 3, "retry_base": "250ms", "retry_max": "2s", "dead_letter_retention": "168h"}
}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	settings, err := loadNotifications(path)
	if err != nil {
		t.Fatal(err)
	}
	if settings.providers.GitHubSecret != "github-secret-value" || settings.providers.GenericBearer != "generic-secret-value" ||
		settings.providers.SlackReplayWindow != 4*time.Minute {
		t.Fatalf("providers were not resolved: %+v", settings.providers)
	}
	if len(settings.subscriptions) != 1 || settings.subscriptions[0].Destination.Kind != notifier.SlackWebhook ||
		settings.subscriptions[0].Destination.URL != "https://hooks.slack.com/services/T/B/path-secret" {
		t.Fatalf("subscriptions = %+v", settings.subscriptions)
	}
	var options server.Options
	settings.apply(&options)
	if options.NotifierAttempts != 3 || options.NotifierRetryBase != 250*time.Millisecond ||
		options.NotifierRetryMax != 2*time.Second || options.NotifierDeadLetterRetention != 7*24*time.Hour {
		t.Fatalf("notification limits = %+v", options)
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"github-secret-value", "generic-secret-value", "path-secret"} {
		if strings.Contains(string(stored), secret) {
			t.Fatalf("credential value %q entered notification configuration", secret)
		}
	}
}

func TestLoadNotificationsFailsClosedWithoutCredentialEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.json")
	if err := os.WriteFile(path, []byte(`{"webhooks":{"linear_secret_env":"MISSING_LINEAR_SECRET"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadNotifications(path)
	if err == nil || !strings.Contains(err.Error(), "MISSING_LINEAR_SECRET") {
		t.Fatalf("missing credential error = %v", err)
	}
}

func TestLoadNotificationsRefusesLiteralSlackSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.json")
	raw := `{"subscriptions":[{"id":"s","tenant":"t","events":["run.finished"],"kind":"slack_webhook","url":"https://hooks.slack.com/services/T/B/secret"}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadNotifications(path)
	if err == nil || !strings.Contains(err.Error(), "must use url_env") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("literal Slack URL error = %v", err)
	}
}

func TestLoadNotificationsRejectsUnknownFieldsAndBadDurations(t *testing.T) {
	tests := []string{
		`{"unknown":true}`,
		`{"limits":{"retry_base":"forever"}}`,
		`{"limits":{"http_timeout":"-1s"}}`,
	}
	for _, raw := range tests {
		path := filepath.Join(t.TempDir(), "notifications.json")
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadNotifications(path); err == nil {
			t.Fatalf("loadNotifications(%s) succeeded", raw)
		}
	}
}
