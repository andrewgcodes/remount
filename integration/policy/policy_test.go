// Package policy proves that Remount's deployment surfaces keep one owner per
// fact.
//
// Plan B phase B5 §13.7 asks for tests that reject configurations in which two
// systems each believe they decide the same thing: Terraform moving a live
// workspace, or provider metadata carrying a reusable credential. Prose cannot
// reject anything, so every rule here is a scanner or an external validator
// run for its exit status, and every rule has a control that feeds it the bad
// configuration it exists to catch.
//
// The controls matter more than the rules. A scan written with `grep | head`
// exits zero whether or not it matched, and this repository has already paid
// for that once; see AGENTS.md. So each scanner is additionally pointed at
// testdata/canary, where the violation is planted deliberately, and the test
// fails if the scanner comes back clean.
package policy

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// repoRoot walks up from the test's directory to the module root, so the tests
// address deploy/ and packaging/ by their real paths rather than by a relative
// path that breaks the moment the package moves.
// itoa keeps the diff helper free of a strconv import at each call site.
func itoa(n int) string { return strconv.Itoa(n) }

func repoRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("no go.mod above the test directory")
		}
		directory = parent
	}
}

// configFile is one machine-executed configuration file: its repo-relative path
// and its body with comments removed.
type configFile struct {
	path string
	// body still contains every line, so a violation's line number is honest.
	// Comment content is blanked in place rather than deleted.
	body string
}

var (
	hclLineComment  = regexp.MustCompile(`(?m)(^|\s)(#|//).*$`)
	hclBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	tplComment      = regexp.MustCompile(`(?s)\{\{-?\s*/\*.*?\*/\s*-?\}\}`)
	yamlLineComment = regexp.MustCompile(`(?m)^\s*#.*$`)
	// error_message and description are operator-facing prose. HCL never
	// executes them, and they have to be allowed to name the hazard they
	// explain: the most useful rejection message for an operator who set
	// node_assignments is the one that tells them to run `remount ws move`
	// instead.
	hclMessage = regexp.MustCompile(`(?m)(error_message|description)(\s*=\s*)".*"$`)
)

// stripComments blanks comment text while preserving line structure.
//
// Scanning raw bytes would flag this package's own explanations: the modules
// and templates describe the boundary they enforce, in the words the scanners
// look for. What a machine executes is the only thing a policy scan may judge.
func stripComments(path, body string) string {
	blank := func(match string) string {
		return strings.Repeat(" ", len(match)-strings.Count(match, "\n")) + strings.Repeat("\n", strings.Count(match, "\n"))
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".tf", ".tfvars":
		body = hclBlockComment.ReplaceAllStringFunc(body, blank)
		body = hclLineComment.ReplaceAllStringFunc(body, blank)
		body = hclMessage.ReplaceAllString(body, "$1$2\"\"")
	case ".yaml", ".yml", ".tpl":
		body = tplComment.ReplaceAllStringFunc(body, blank)
		body = yamlLineComment.ReplaceAllStringFunc(body, blank)
	case "":
		// Dockerfiles.
		body = yamlLineComment.ReplaceAllStringFunc(body, blank)
	}
	return body
}

// scannedExtensions is what a machine executes. Markdown and NOTES.txt are
// excluded on purpose: they are read by people, they necessarily quote the
// commands the rules forbid, and forbidding the documentation from naming the
// hazard would make the documentation worse.
var scannedExtensions = map[string]bool{".tf": true, ".tfvars": true, ".yaml": true, ".yml": true, ".tpl": true, "": true}

// collect gathers every machine-executed configuration file under the given
// repo-relative roots. A root may be a directory or a single file.
func collect(t *testing.T, roots ...string) []configFile {
	t.Helper()
	root := repoRoot(t)
	var files []configFile
	read := func(path string) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, configFile{path: rel, body: stripComments(path, string(raw))})
	}
	for _, relative := range roots {
		base := filepath.Join(root, relative)
		info, err := os.Stat(base)
		if err != nil {
			t.Fatalf("policy scope %s: %v", relative, err)
		}
		if !info.IsDir() {
			read(base)
			continue
		}
		err = filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				// A provider working directory holds downloaded plugins and
				// state, neither of which this repository authored.
				if entry.Name() == ".terraform" || entry.Name() == ".git" {
					return filepath.SkipDir
				}
				return nil
			}
			if !scanned(entry.Name()) {
				return nil
			}
			read(path)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(files) == 0 {
		t.Fatalf("policy scan collected no files from %v; a scan with nothing to scan is not a passing scan", roots)
	}
	return files
}

// scanned reports whether a file name is one a machine executes.
func scanned(name string) bool {
	extension := strings.ToLower(filepath.Ext(name))
	if extension == "" {
		return strings.Contains(strings.ToLower(name), "dockerfile")
	}
	return scannedExtensions[extension]
}

// finding is one rule violation, named so a failure says what to fix.
type finding struct {
	path string
	line int
	rule string
	text string
}

func (f finding) String() string {
	return f.path + ":" + strconv.Itoa(f.line) + ": " + f.rule + ": " + strings.TrimSpace(f.text)
}

// scan applies every rule to every line and returns what it found. It returns
// the findings rather than failing, so the same function can be pointed at the
// real tree (expecting none) and at the canary (expecting several).
func scan(files []configFile, rules []rule) []finding {
	var found []finding
	for _, file := range files {
		for index, line := range strings.Split(file.body, "\n") {
			for _, r := range rules {
				if r.violates(file.path, line) {
					found = append(found, finding{path: file.path, line: index + 1, rule: r.name, text: line})
				}
			}
		}
	}
	return found
}

// rule is one named policy check over a single line.
type rule struct {
	name     string
	why      string
	violates func(path, line string) bool
}

// matchRule builds a rule from a regexp.
func matchRule(name, why string, pattern *regexp.Regexp) rule {
	return rule{name: name, why: why, violates: func(_, line string) bool { return pattern.MatchString(line) }}
}

// hasTool reports whether an external validator is on PATH. A missing tool is
// unavailable with a name, never a silent pass.
func hasTool(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("unavailable: %s is not on PATH, so this gate cannot run here: %v", name, err)
	}
	return path
}
