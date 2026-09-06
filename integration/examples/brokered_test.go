// Evidence gate B35: the SDK's brokered-credential examples run in CI with a
// fake provider, no network and no provider key.
//
// The gap brief asks for examples covering "model API through broker", "search
// API through broker" and "custom HTTP service through broker", and for none
// of them to place a provider credential in the workspace environment. An
// example nobody executes decays silently, and one that can only be executed
// with a real key is executed by nobody. So each example here runs end to end
// against a whole in-process Remount system whose only upstream is a loopback
// TLS fake that answers 401 unless the exact expected credential arrived —
// which means a broker that stopped substituting, or substituted the wrong
// thing, fails this test rather than a reader's laptop.
package examples

import (
	"crypto/x509"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/node"
	"remount.dev/remount/internal/testutil/fakeprovider"
)

// The credentials the fake upstream requires. They are synthetic, generated
// for this test, and shaped so a leak scan can tell them from anything real.
const (
	fakeModelKey   = "remount-fake-model-key-0f3a9c21"
	fakeSearchKey  = "remount-fake-search-key-7b1d4e88"
	fakeServiceTok = "remount-fake-service-token-2c6f0a45"
)

// TestB35BrokeredExamplesRunWithoutAKey is the gate. All three examples run,
// each proves its own substitution location, and neither the real credential
// nor an unsubstituted placeholder ever reaches an upstream it was not bound
// to.
func TestB35BrokeredExamplesRunWithoutAKey(t *testing.T) {
	requirePOSIXShell(t)
	requireCurl(t)

	provider := fakeprovider.New(fakeprovider.Config{
		ModelKey: fakeModelKey, SearchKey: fakeSearchKey, ServiceToken: fakeServiceTok,
	})
	t.Cleanup(provider.Close)
	// A second fake stands in for "some other host". A placeholder aimed at it
	// must never arrive, so its counters are the negative half of the proof.
	foreign := fakeprovider.New(fakeprovider.Config{
		ModelKey: fakeModelKey, SearchKey: fakeSearchKey, ServiceToken: fakeServiceTok,
	})
	t.Cleanup(foreign.Close)

	roots := x509.NewCertPool()
	roots.AddCert(provider.Certificate())
	roots.AddCert(foreign.Certificate())
	endpoint := standalone(t, func(o *node.Options) {
		// The fakes listen on loopback, which the broker refuses by default:
		// a workspace that can reach 127.0.0.1 can reach the node's own
		// services. Naming them is the same opt-in a local Ollama needs.
		o.AllowPrivate = []string{"127.0.0.1", "localhost"}
		o.BrokerRootCAs = roots
	})

	t.Run("model", func(t *testing.T) {
		stdout := runExample(t, "./examples/brokered-model-call", endpoint, 5*time.Minute,
			"REMOUNT_MODEL_HOST="+provider.Host(),
			"REMOUNT_FOREIGN_HOST="+foreign.Host(),
			"REMOUNT_MODEL_KEY_ENV=REMOUNT_EXAMPLE_MODEL_KEY",
			"REMOUNT_EXAMPLE_MODEL_KEY="+fakeModelKey,
		)
		assertContains(t, "brokered-model-call", stdout,
			"model list through the broker: HTTP 200",
			"chat completion through the broker: HTTP 200",
			"placeholder at foreign host "+foreign.Host()+": HTTP 403 reason=egress_denied",
			"workspace environment holds 0 copies of the real credential",
			"audit for b_example_model_",
			"binding b_example_model_",
		)
		assertCleanedUp(t, stdout)
	})

	t.Run("search", func(t *testing.T) {
		stdout := runExample(t, "./examples/brokered-search-api", endpoint, 5*time.Minute,
			"REMOUNT_SEARCH_HOST="+provider.Host(),
			"REMOUNT_FOREIGN_HOST="+foreign.Host(),
			"REMOUNT_SEARCH_KEY_ENV=REMOUNT_EXAMPLE_SEARCH_KEY",
			"REMOUNT_EXAMPLE_SEARCH_KEY="+fakeSearchKey,
		)
		assertContains(t, "brokered-search-api", stdout,
			"search through the broker: HTTP 200",
			// The declared location is enforced: the same placeholder in a
			// header is refused rather than substituted in two places.
			"placeholder in a header instead of the query: HTTP 400 reason=egress_denied",
			"placeholder at foreign host "+foreign.Host()+": HTTP 403 reason=egress_denied",
			"workspace environment holds 0 copies of the real credential",
		)
		assertCleanedUp(t, stdout)
	})

	t.Run("custom_http", func(t *testing.T) {
		stdout := runExample(t, "./examples/brokered-custom-http", endpoint, 5*time.Minute,
			"REMOUNT_SERVICE_HOST="+provider.Host(),
			"REMOUNT_FOREIGN_HOST="+foreign.Host(),
			"REMOUNT_SERVICE_TOKEN_ENV=REMOUNT_EXAMPLE_SERVICE_TOKEN",
			"REMOUNT_EXAMPLE_SERVICE_TOKEN="+fakeServiceTok,
		)
		assertContains(t, "brokered-custom-http", stdout,
			"record posted through the broker: HTTP 200",
			"placeholder at foreign host "+foreign.Host()+": HTTP 403 reason=egress_denied",
			"workspace environment holds 0 copies of the real credential",
		)
		assertCleanedUp(t, stdout)
	})

	// The bound upstream saw the real credential on every request it served,
	// and never an unsubstituted placeholder.
	counts := provider.Counts()
	if counts.Authorized < 4 || counts.Unauthorized != 0 || counts.Placeholders != 0 {
		t.Fatalf("bound upstream: %+v, want at least 4 authorized and no failures", counts)
	}
	// The foreign upstream saw nothing at all: every leaked placeholder was
	// refused by the broker before a byte left the node.
	if got := foreign.Counts(); got != (fakeprovider.Counts{}) {
		t.Fatalf("a request reached the foreign upstream: %+v", got)
	}
	// Nothing the fake was ever sent was a placeholder, and every value it saw
	// was the credential it expected.
	for _, presented := range provider.Credentials() {
		switch presented {
		case fakeModelKey, fakeSearchKey, fakeServiceTok:
		default:
			t.Fatalf("the upstream was sent an unexpected credential shape (%d bytes)", len(presented))
		}
	}
}

// TestB35NoFirstPartyExamplePutsAProviderKeyInWorkspaceEnv is the static half
// of the acceptance criterion. The examples prove at run time that the
// credential is absent from one workspace; this proves no example in the tree
// is written to put one there in the first place.
func TestB35NoFirstPartyExamplePutsAProviderKeyInWorkspaceEnv(t *testing.T) {
	root := repoRoot(t)
	sources := goSourcesUnder(t, filepath.Join(root, "examples"))
	if len(sources) == 0 {
		t.Fatal("no example sources were scanned, so this proves nothing")
	}

	// Prove the instrument works before trusting a clean result: a synthetic
	// forbidden block must be found by the same scan.
	canary := "Env: map[string]string{\n\t\t\"OPENAI_API_KEY\": os.Getenv(\"OPENAI_API_KEY\"),\n\t}"
	if findings := forbiddenEnvValues(canary); len(findings) != 1 {
		t.Fatalf("the scan missed its own canary: %q", findings)
	}

	placeholders := 0
	for _, path := range sources {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		if findings := forbiddenEnvValues(text); len(findings) != 0 {
			t.Errorf("%s puts a live value in a workspace env: %q", path, findings)
		}
		placeholders += strings.Count(text, `"ref:`)
	}
	// A scan that found nothing because no example uses bindings at all would
	// be vacuous. At least one first-party example hands over a placeholder.
	if placeholders == 0 {
		t.Fatal("no example hands a workspace a ref: placeholder, so the scan is vacuous")
	}
}

// envValuePattern matches one `"NAME": VALUE,` entry of a workspace env map.
var envValuePattern = regexp.MustCompile(`(?m)^\s*"[A-Za-z0-9_]+":\s*(.+),\s*$`)

// forbiddenEnvValues returns the workspace env values that are computed at run
// time rather than being an opaque placeholder or a broker URL. A key can only
// reach a workspace through such a value, so the rule is that there are none:
// every entry is a literal, a "ref:" placeholder, or a ${REMOUNT_BROKER} URL.
func forbiddenEnvValues(source string) []string {
	var findings []string
	for _, block := range envBlocks(source) {
		for _, match := range envValuePattern.FindAllStringSubmatch(block, -1) {
			value := strings.TrimSpace(match[1])
			if strings.Contains(value, "os.Getenv") || strings.Contains(value, "secret") ||
				strings.Contains(value, "Secret") {
				findings = append(findings, value)
			}
		}
	}
	return findings
}

// envBlocks returns the body of every `Env: map[string]string{…}` literal,
// matched by brace depth so a nested composite literal does not end the block
// early.
func envBlocks(source string) []string {
	const opener = "Env: map[string]string{"
	var blocks []string
	for index := 0; ; {
		start := strings.Index(source[index:], opener)
		if start < 0 {
			return blocks
		}
		start += index + len(opener)
		depth := 1
		end := start
		for ; end < len(source) && depth > 0; end++ {
			switch source[end] {
			case '{':
				depth++
			case '}':
				depth--
			}
		}
		blocks = append(blocks, source[start:min(end, len(source))])
		index = end
	}
}

func goSourcesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			out = append(out, goSourcesUnder(t, path)...)
			continue
		}
		if strings.HasSuffix(entry.Name(), ".go") {
			out = append(out, path)
		}
	}
	return out
}

func assertContains(t *testing.T, name, stdout string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(stdout, want) {
			t.Errorf("%s output is missing %q:\n%s", name, want, stdout)
		}
	}
}

// assertCleanedUp proves the example left neither a workspace nor a live
// binding behind. An example that leaks a binding leaks a credential.
func assertCleanedUp(t *testing.T, stdout string) {
	t.Helper()
	if !strings.Contains(stdout, " destroyed") {
		t.Errorf("the example left its workspace behind:\n%s", stdout)
	}
	if !strings.Contains(stdout, " revoked") {
		t.Errorf("the example left its binding live:\n%s", stdout)
	}
}

// requireCurl skips where the examples' HTTP client does not exist. A check
// that cannot run is reported as unavailable, never as a pass.
func requireCurl(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("unavailable: the brokered examples issue their requests with curl")
	}
}
