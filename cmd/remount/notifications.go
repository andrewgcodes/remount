package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"remount.dev/remount/internal/notifier"
	"remount.dev/remount/internal/server"
)

type notificationFile struct {
	Webhooks      webhookProviderFile    `json:"webhooks"`
	Subscriptions []notificationEntry    `json:"subscriptions"`
	Limits        notificationLimitsFile `json:"limits"`
}

type webhookProviderFile struct {
	GitHubSecretEnv       string `json:"github_secret_env,omitempty"`
	SlackSigningSecretEnv string `json:"slack_signing_secret_env,omitempty"`
	LinearSecretEnv       string `json:"linear_secret_env,omitempty"`
	GenericBearerEnv      string `json:"generic_bearer_env,omitempty"`
	SlackReplayWindow     string `json:"slack_replay_window,omitempty"`
}

type notificationEntry struct {
	ID             string   `json:"id"`
	Tenant         string   `json:"tenant"`
	Events         []string `json:"events"`
	Kind           string   `json:"kind"`
	URL            string   `json:"url,omitempty"`
	URLEnv         string   `json:"url_env,omitempty"`
	BearerTokenEnv string   `json:"bearer_token_env,omitempty"`
	AllowedHosts   []string `json:"allowed_hosts,omitempty"`
}

type notificationLimitsFile struct {
	BatchEvents             int    `json:"batch_events,omitempty"`
	BatchBytes              int    `json:"batch_bytes,omitempty"`
	Attempts                int    `json:"attempts,omitempty"`
	RetryBase               string `json:"retry_base,omitempty"`
	RetryMax                string `json:"retry_max,omitempty"`
	HTTPTimeout             string `json:"http_timeout,omitempty"`
	MaxSubscriptions        int    `json:"max_subscriptions,omitempty"`
	MaxDeadLettersPerTenant int    `json:"max_dead_letters_per_tenant,omitempty"`
	DeadLetterRetention     string `json:"dead_letter_retention,omitempty"`
	DeadLetterGCInterval    string `json:"dead_letter_gc_interval,omitempty"`
}

type notificationSettings struct {
	providers     server.WebhookProviderConfig
	subscriptions []notifier.Subscription
	limits        notificationLimits
}

type notificationLimits struct {
	batchEvents, batchBytes, attempts         int
	retryBase, retryMax, httpTimeout          time.Duration
	maxSubscriptions, maxDeadLettersPerTenant int
	deadLetterRetention, deadLetterGCInterval time.Duration
}

func loadNotifications(path string) (notificationSettings, error) {
	if path == "" {
		return notificationSettings{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return notificationSettings{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config notificationFile
	if err := decoder.Decode(&config); err != nil {
		return notificationSettings{}, fmt.Errorf("decode notifications: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return notificationSettings{}, errors.New("decode notifications: multiple JSON values")
		}
		return notificationSettings{}, fmt.Errorf("decode notifications: %w", err)
	}

	github, err := optionalNotificationEnv(config.Webhooks.GitHubSecretEnv)
	if err != nil {
		return notificationSettings{}, err
	}
	slack, err := optionalNotificationEnv(config.Webhooks.SlackSigningSecretEnv)
	if err != nil {
		return notificationSettings{}, err
	}
	linear, err := optionalNotificationEnv(config.Webhooks.LinearSecretEnv)
	if err != nil {
		return notificationSettings{}, err
	}
	generic, err := optionalNotificationEnv(config.Webhooks.GenericBearerEnv)
	if err != nil {
		return notificationSettings{}, err
	}
	replayWindow, err := optionalNotificationDuration("slack_replay_window", config.Webhooks.SlackReplayWindow)
	if err != nil {
		return notificationSettings{}, err
	}

	subscriptions := make([]notifier.Subscription, 0, len(config.Subscriptions))
	for index, entry := range config.Subscriptions {
		kind := notifier.Kind(strings.TrimSpace(entry.Kind))
		if entry.URL != "" && entry.URLEnv != "" {
			return notificationSettings{}, fmt.Errorf("notification %d: exactly one of url and url_env is allowed", index)
		}
		if kind == notifier.SlackWebhook && entry.URL != "" {
			return notificationSettings{}, fmt.Errorf("notification %d: Slack webhook URLs must use url_env", index)
		}
		endpoint := strings.TrimSpace(entry.URL)
		if entry.URLEnv != "" {
			endpoint, err = requiredNotificationEnv(entry.URLEnv)
			if err != nil {
				return notificationSettings{}, fmt.Errorf("notification %d: %w", index, err)
			}
		}
		bearer, err := optionalNotificationEnv(entry.BearerTokenEnv)
		if err != nil {
			return notificationSettings{}, fmt.Errorf("notification %d: %w", index, err)
		}
		subscriptions = append(subscriptions, notifier.Subscription{
			ID: entry.ID, Tenant: entry.Tenant, Events: append([]string(nil), entry.Events...),
			Destination: notifier.Destination{
				Kind: kind, URL: endpoint, BearerToken: bearer,
				AllowedHosts: append([]string(nil), entry.AllowedHosts...),
			},
		})
	}

	limits, err := parseNotificationLimits(config.Limits)
	if err != nil {
		return notificationSettings{}, err
	}
	return notificationSettings{
		providers: server.WebhookProviderConfig{
			GitHubSecret: github, SlackSigningSecret: slack, LinearSecret: linear,
			GenericBearer: generic, SlackReplayWindow: replayWindow,
		},
		subscriptions: subscriptions,
		limits:        limits,
	}, nil
}

func parseNotificationLimits(config notificationLimitsFile) (notificationLimits, error) {
	retryBase, err := optionalNotificationDuration("retry_base", config.RetryBase)
	if err != nil {
		return notificationLimits{}, err
	}
	retryMax, err := optionalNotificationDuration("retry_max", config.RetryMax)
	if err != nil {
		return notificationLimits{}, err
	}
	httpTimeout, err := optionalNotificationDuration("http_timeout", config.HTTPTimeout)
	if err != nil {
		return notificationLimits{}, err
	}
	retention, err := optionalNotificationDuration("dead_letter_retention", config.DeadLetterRetention)
	if err != nil {
		return notificationLimits{}, err
	}
	gcInterval, err := optionalNotificationDuration("dead_letter_gc_interval", config.DeadLetterGCInterval)
	if err != nil {
		return notificationLimits{}, err
	}
	return notificationLimits{
		batchEvents: config.BatchEvents, batchBytes: config.BatchBytes, attempts: config.Attempts,
		retryBase: retryBase, retryMax: retryMax, httpTimeout: httpTimeout,
		maxSubscriptions: config.MaxSubscriptions, maxDeadLettersPerTenant: config.MaxDeadLettersPerTenant,
		deadLetterRetention: retention, deadLetterGCInterval: gcInterval,
	}, nil
}

func optionalNotificationDuration(field, value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("notification %s must be a positive duration", field)
	}
	return duration, nil
}

func optionalNotificationEnv(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", nil
	}
	return requiredNotificationEnv(name)
}

func requiredNotificationEnv(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("credential environment variable name is required")
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("credential environment variable %s is not set", name)
	}
	return value, nil
}

func (settings notificationSettings) apply(options *server.Options) {
	options.WebhookProviders = settings.providers
	options.Notifications = settings.subscriptions
	options.NotifierBatchEvents = settings.limits.batchEvents
	options.NotifierBatchBytes = settings.limits.batchBytes
	options.NotifierAttempts = settings.limits.attempts
	options.NotifierRetryBase = settings.limits.retryBase
	options.NotifierRetryMax = settings.limits.retryMax
	options.NotifierHTTPTimeout = settings.limits.httpTimeout
	options.MaxNotifierSubscriptions = settings.limits.maxSubscriptions
	options.MaxNotifierDeadLettersPerTenant = settings.limits.maxDeadLettersPerTenant
	options.NotifierDeadLetterRetention = settings.limits.deadLetterRetention
	options.NotifierDeadLetterGCInterval = settings.limits.deadLetterGCInterval
}
