package node

import (
	"strings"
	"testing"

	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/computer"
	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/redact"
)

// capabilityCanary stands in for a workspace broker capability. It is
// synthetic and never a real credential; it is long enough that the redactor's
// literal floor cannot silently ignore it.
const capabilityCanary = "cap_synthetic_workspace_capability_0123456789"

// TestProxyCredentialsComeFromTheBrokerEnvironment pins the two halves
// together: whatever Broker.EnvForCapability writes for a workspace is exactly
// what the node will answer a proxy challenge with. A second credential path
// would drift from this the first time either side changed.
func TestProxyCredentialsComeFromTheBrokerEnvironment(t *testing.T) {
	b := broker.New(broker.Options{WS: "w_1", Generation: 1, Principal: "p"})
	if _, err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	defer b.Close()

	env := b.EnvForCapability(capabilityCanary)
	creds := proxyCredentialsFromEnv(env)
	if creds.Username != capabilityCanary {
		t.Fatalf("proxy username = %q, want the capability the broker advertised", creds.Username)
	}
	if creds.Password != "" {
		t.Fatalf("proxy password = %q, want empty", creds.Password)
	}
	if !creds.Set() {
		t.Fatal("a credential read out of the broker environment reports itself unset")
	}
}

func TestProxyCredentialsFromEnvEdges(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		want computer.ProxyCredentials
	}{
		{name: "no broker at all", env: []string{"HOME=/work", "PATH=/usr/bin"}},
		{
			name: "a proxy with no user-info authorizes nothing",
			env:  []string{"HTTPS_PROXY=http://127.0.0.1:9000"},
		},
		{
			name: "an empty value is not a credential",
			env:  []string{"HTTPS_PROXY="},
		},
		{
			name: "https wins over http when both are present",
			env: []string{
				"HTTP_PROXY=http://plain@127.0.0.1:9000",
				"HTTPS_PROXY=http://secure@127.0.0.1:9000",
			},
			want: computer.ProxyCredentials{Username: "secure"},
		},
		{
			name: "the last assignment of a name wins",
			env: []string{
				"HTTPS_PROXY=http://first@127.0.0.1:9000",
				"HTTPS_PROXY=http://second@127.0.0.1:9000",
			},
			want: computer.ProxyCredentials{Username: "second"},
		},
		{
			name: "a password is carried through",
			env:  []string{"HTTPS_PROXY=http://user:secretvalue@127.0.0.1:9000"},
			want: computer.ProxyCredentials{Username: "user", Password: "secretvalue"},
		},
		{
			name: "lower case is read when upper case is absent",
			env:  []string{"https_proxy=http://lower@127.0.0.1:9000"},
			want: computer.ProxyCredentials{Username: "lower"},
		},
		{
			name: "an unparseable proxy is not guessed at",
			env:  []string{"HTTPS_PROXY=://%%%"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := proxyCredentialsFromEnv(tc.env); got != tc.want {
				t.Fatalf("proxyCredentialsFromEnv = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestLaunchProgramIsRedactedBeforeItIsEmitted proves the scan by planting the
// canary in the very words the node is about to publish. A run where the
// canary was absent from the input would prove nothing at all.
func TestLaunchProgramIsRedactedBeforeItIsEmitted(t *testing.T) {
	creds := computer.ProxyCredentials{Username: capabilityCanary}
	program := []string{
		"chromium",
		"--proxy-server=http://" + capabilityCanary + ":@10.0.0.1:9000",
		"about:blank",
	}
	got := redactProxyCredential(program, creds)
	joined := strings.Join(got, " ")
	if strings.Contains(joined, capabilityCanary) {
		t.Fatalf("the emitted program still names the capability: %s", joined)
	}
	if !strings.Contains(joined, redact.Mark) {
		t.Fatalf("nothing was redacted, so the scan cannot be working: %s", joined)
	}
	if got[0] != "chromium" || got[2] != "about:blank" {
		t.Fatalf("redaction rewrote words that carried no credential: %v", got)
	}
	// Without a credential there is nothing to scrub and the words are the
	// caller's own, unchanged.
	plain := []string{"chromium", "about:blank"}
	if same := redactProxyCredential(plain, computer.ProxyCredentials{}); same[0] != "chromium" || same[1] != "about:blank" {
		t.Fatalf("an uncredentialed program was rewritten: %v", same)
	}
}

// TestDefaultLaunchSilencesBrowserBackgroundTraffic pins the flags that keep
// Chromium's own component, sync and metrics traffic off the broker. That
// traffic is issued outside any page target, so CDP interception cannot
// authenticate it: every request of it is refused as `unauthenticated` and
// lands in the workspace's durable egress record looking like a proxy-auth
// defect. Dropping one of these flags would quietly bring the noise back.
func TestDefaultLaunchSilencesBrowserBackgroundTraffic(t *testing.T) {
	argv := DefaultBrowserProgram("127.0.0.1", 9222, "/ws/.remount/browser/default",
		proto.ComputerViewport{Width: 1280, Height: 720})
	joined := strings.Join(argv, " ")
	for _, want := range []string{
		"--disable-background-networking",
		"--disable-component-update",
		"--disable-default-apps",
		"--disable-sync",
		"--metrics-recording-only",
		"--no-first-run",
		"--no-default-browser-check",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("the default launch does not carry %s: %s", want, joined)
		}
	}
}

// TestComputerGetNeverCarriesTheProxyCredential is a shape assertion on the
// response type itself: there is no field a credential could ride out on.
func TestComputerGetNeverCarriesTheProxyCredential(t *testing.T) {
	res := proto.ComputerGetRes{
		Computer: "cmp_1", State: proto.ComputerStateReady,
		Viewport: proto.ComputerViewport{Width: 800, Height: 600}, Session: "s_1",
	}
	raw, err := proto.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, forbidden := range []string{"proxy", "credential", "password", "capability"} {
		if strings.Contains(strings.ToLower(string(raw)), forbidden) {
			t.Fatalf("computer.get response has a %q-shaped field: %s", forbidden, raw)
		}
	}
}
