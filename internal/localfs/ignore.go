package localfs

import (
	"bufio"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// IgnoreFiles are the per-directory ignore files Pack honors, in the order
// they are consulted. A later file's patterns take precedence over an earlier
// one's at the same directory, so .remountignore can re-include what
// .gitignore excludes.
var IgnoreFiles = []string{".gitignore", ".remountignore"}

// pattern is one parsed ignore rule, anchored to the directory whose ignore
// file declared it.
type pattern struct {
	re      *regexp.Regexp
	negate  bool
	dirOnly bool
}

// ignoreDir is the compiled rule set of one directory's ignore files.
type ignoreDir struct {
	rules []pattern
}

// ignorer evaluates gitignore semantics lazily over a tree rooted at an
// os.Root. Rules are loaded per directory on first use and cached.
type ignorer struct {
	root *os.Root
	dirs map[string]*ignoreDir // key: slash-separated dir, "" for root
}

func newIgnorer(root *os.Root) *ignorer {
	return &ignorer{root: root, dirs: map[string]*ignoreDir{}}
}

// ignored reports whether rel (slash-separated, relative to root) is excluded
// by an ignore file in its own directory or any ancestor. Deeper files win
// over shallower ones and later rules win over earlier ones, as in git.
func (ig *ignorer) ignored(rel string, isDir bool) bool {
	decided, matched := false, false
	dir := path.Dir(rel)
	if dir == "." {
		dir = ""
	}
	// Walk from the deepest directory outward; the first directory with a
	// matching rule decides, because git applies deeper ignore files last.
	for {
		rules := ig.load(dir)
		for i := len(rules.rules) - 1; i >= 0; i-- {
			p := rules.rules[i]
			if p.dirOnly && !isDir {
				continue
			}
			sub := rel
			if dir != "" {
				sub = strings.TrimPrefix(rel, dir+"/")
			}
			if p.re.MatchString(sub) {
				decided, matched = true, !p.negate
				break
			}
		}
		if decided || dir == "" {
			break
		}
		if i := strings.LastIndexByte(dir, '/'); i >= 0 {
			dir = dir[:i]
		} else {
			dir = ""
		}
	}
	return decided && matched
}

func (ig *ignorer) load(dir string) *ignoreDir {
	if d, ok := ig.dirs[dir]; ok {
		return d
	}
	d := &ignoreDir{}
	for _, name := range IgnoreFiles {
		p := name
		if dir != "" {
			p = filepath.Join(filepath.FromSlash(dir), name)
		}
		f, err := ig.root.Open(p)
		if err != nil {
			continue
		}
		st, err := f.Stat()
		if err != nil || !st.Mode().IsRegular() || st.Size() > maxIgnoreFileBytes {
			f.Close()
			continue
		}
		d.rules = append(d.rules, parseIgnore(bufio.NewScanner(f))...)
		f.Close()
	}
	ig.dirs[dir] = d
	return d
}

// maxIgnoreFileBytes bounds one ignore file; anything larger is not a hand
// written ignore list and is skipped rather than compiled.
const maxIgnoreFileBytes = 1 << 20

func parseIgnore(sc *bufio.Scanner) []pattern {
	var out []pattern
	for sc.Scan() {
		line := sc.Text()
		if p, ok := parseLine(line); ok {
			out = append(out, p)
		}
	}
	return out
}

// parseLine compiles one gitignore line. The rules implemented are the
// documented ones: blank and # lines are skipped, trailing unescaped spaces
// are dropped, a leading ! negates, a trailing / matches directories only, a
// pattern containing a / (other than the trailing one) is anchored to the
// ignore file's directory, ** spans directories, * and ? do not cross /.
func parseLine(line string) (pattern, bool) {
	var p pattern
	if line == "" || strings.HasPrefix(line, "#") {
		return p, false
	}
	line = trimTrailingSpace(line)
	if line == "" {
		return p, false
	}
	if strings.HasPrefix(line, "!") {
		p.negate = true
		line = line[1:]
	} else if strings.HasPrefix(line, `\!`) || strings.HasPrefix(line, `\#`) {
		line = line[1:]
	}
	if strings.HasSuffix(line, "/") {
		p.dirOnly = true
		line = strings.TrimSuffix(line, "/")
	}
	if line == "" {
		return p, false
	}
	anchored := strings.Contains(line, "/")
	line = strings.TrimPrefix(line, "/")
	re, err := globToRegexp(line, anchored)
	if err != nil {
		return p, false
	}
	p.re = re
	return p, true
}

func trimTrailingSpace(s string) string {
	for strings.HasSuffix(s, " ") && !strings.HasSuffix(s, `\ `) {
		s = strings.TrimSuffix(s, " ")
	}
	return strings.ReplaceAll(s, `\ `, " ")
}

// globToRegexp translates a gitignore glob into a regexp over a
// slash-separated path relative to the ignore file's directory. An
// unanchored pattern may match at any depth and a match on a directory
// implicitly covers everything below it (Pack skips the subtree).
func globToRegexp(glob string, anchored bool) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	if !anchored {
		b.WriteString("(?:.*/)?")
	}
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch c {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				i++
				switch {
				case i+1 < len(glob) && glob[i+1] == '/':
					// "**/" matches zero or more directories.
					i++
					b.WriteString("(?:.*/)?")
				default:
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '[':
			end := strings.IndexByte(glob[i:], ']')
			if end <= 1 {
				b.WriteString(`\[`)
				continue
			}
			class := glob[i+1 : i+end]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			b.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
			i += end
		case '\\':
			if i+1 < len(glob) {
				i++
				b.WriteString(regexp.QuoteMeta(string(glob[i])))
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	// A directory match covers its descendants, which matters when a pattern
	// is evaluated against a file whose ancestor directory was named.
	b.WriteString("(?:/.*)?$")
	return regexp.Compile(b.String())
}
