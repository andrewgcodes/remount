package evidence

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Canary is the synthetic credential-shaped string a leak scan plants to prove
// the scan works. A scan that finds nothing is worthless until it has
// demonstrably found this; the repository has already been bitten by a leak
// scan that reported clean because its pipeline always exited zero.
const Canary = "sk-remount-planted-canary-0000000000000000000000"

// envNamePattern accepts an environment variable name and nothing else, so a
// record cannot smuggle a value into a field that claims to hold a name.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// envRefPattern finds the variable names a record mentions in free text, so a
// record that names a variable is scanned for that variable's value even when
// it never listed it in RequiredEnv.
var envRefPattern = regexp.MustCompile(`\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b`)

// minEnvValueLen keeps the scan from matching on values like "1" or "/" that
// occur in every string. Anything shorter is not a credential, and treating it
// as one would make the validator useless through false positives.
const minEnvValueLen = 6

type secretRule struct {
	name string
	re   *regexp.Regexp
}

// secretRules are shapes that are credentials wherever they appear. They are
// deliberately narrow: a rule that fires on ordinary prose gets suppressed, and
// a suppressed scan proves nothing.
var secretRules = []secretRule{
	{"openai-key", regexp.MustCompile(`\bsk-[A-Za-z0-9][A-Za-z0-9_-]{19,}`)},
	{"e2b-key", regexp.MustCompile(`\be2b_[A-Za-z0-9]{16,}`)},
	{"aws-access-key-id", regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)},
	{"github-token", regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}`)},
	{"slack-token", regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`)},
	{"private-key-block", regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
	{"bearer-header", regexp.MustCompile(`(?i)authorization:\s*bearer\s+\S{12,}`)},
	{"assigned-secret", regexp.MustCompile(`(?i)\b(?:api[_-]?key|secret|token|password)\b["']?\s*[:=]\s*["']?[A-Za-z0-9/+=_-]{20,}`)},
}

// Finding names a leak without reproducing it. Printing the matched text would
// copy the secret into the very report the scan exists to keep clean.
type Finding struct {
	Rule  string `json:"rule"`
	Where string `json:"where"`
	Hint  string `json:"hint"`
}

func (f Finding) String() string { return fmt.Sprintf("%s: %s (%s)", f.Where, f.Rule, f.Hint) }

// redact describes a match by prefix and length only.
func redact(match string) string {
	prefix := match
	if len(prefix) > 4 {
		prefix = prefix[:4]
	}
	return fmt.Sprintf("%q…, %d bytes", prefix, len(match))
}

// ScanText reports credential-shaped strings in one piece of text.
func ScanText(where, text string) []Finding {
	var out []Finding
	for _, rule := range secretRules {
		for _, match := range rule.re.FindAllString(text, -1) {
			out = append(out, Finding{Rule: rule.name, Where: where, Hint: redact(match)})
		}
	}
	return out
}

// Lookup resolves an environment variable name to its value. Tests supply a
// fake so no real credential is ever needed to prove the scan works.
type Lookup func(name string) (string, bool)

// OSLookup reads the process environment.
func OSLookup(name string) (string, bool) { return os.LookupEnv(name) }

// ScanEnvValues reports any case where text contains the value of an
// environment variable that text itself names. This is the rule from the
// evidence model: a record may name a variable, never its value.
func ScanEnvValues(where, text string, extra []string, lookup Lookup) []Finding {
	named := map[string]bool{}
	for _, name := range extra {
		named[name] = true
	}
	for _, name := range envRefPattern.FindAllString(text, -1) {
		named[name] = true
	}
	names := make([]string, 0, len(named))
	for name := range named {
		names = append(names, name)
	}
	sort.Strings(names)

	var out []Finding
	for _, name := range names {
		value, ok := lookup(name)
		if !ok || len(value) < minEnvValueLen {
			continue
		}
		if strings.Contains(text, value) {
			out = append(out, Finding{Rule: "env-value:" + name, Where: where, Hint: redact(value)})
		}
	}
	return out
}

// ScanSecrets refuses a record that carries a credential: the value of any
// environment variable it names, or any credential-shaped string.
func (r Record) ScanSecrets(lookup Lookup) []Finding {
	var out []Finding
	for i, field := range r.Strings() {
		if field == "" {
			continue
		}
		where := fmt.Sprintf("%s field %d", r.Scenario, i)
		out = append(out, ScanEnvValues(where, field, r.RequiredEnv, lookup)...)
		out = append(out, ScanText(where, field)...)
	}
	return out
}

// maxScanBytes bounds a single file read. Evidence outputs are small; a large
// file in the output directory is not evidence and is reported as unscanned
// rather than silently skipped.
const maxScanBytes = 8 << 20

// ScanTree reports credential-shaped strings under root. It returns an error
// rather than a clean result when a file cannot be read, because an unscanned
// file must never render as a clean one.
func ScanTree(root string, lookup Lookup, named []string) ([]Finding, error) {
	var out []Finding
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() > maxScanBytes {
			return fmt.Errorf("evidence: %s is %d bytes, too large to scan", path, info.Size())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = path
		}
		text := string(data)
		out = append(out, ScanEnvValues(rel, text, named, lookup)...)
		out = append(out, ScanText(rel, text)...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
