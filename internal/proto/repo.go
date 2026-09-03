package proto

import (
	"net"
	"net/url"
	"strings"
)

// EgressConnectorGit identifies the managed git smart-HTTP surface: only
// info/refs, git-upload-pack and git-receive-pack of the repositories a rule
// names, with push as a separate grant (ADR 0054).
const EgressConnectorGit = "git"

// EvRepoCloned records that the node cloned Spec.Repo before ws.ready;
// payload {url, ref, commit, depth}.
const EvRepoCloned = "repo.cloned"

// RepoSpec names a git repository to clone at first materialization.
type RepoSpec struct {
	// URL is the canonical https URL without a trailing .git; it is also what
	// the tree's origin remote records, so a snapshot never carries a broker
	// address.
	URL string `cbor:"url,omitempty" json:"url,omitempty"`
	// Ref is the branch or tag to check out; "" means the remote's default.
	Ref string `cbor:"ref,omitempty" json:"ref,omitempty"`
	// Depth truncates history to this many commits; 0 clones everything.
	Depth int `cbor:"depth,omitempty" json:"depth,omitempty"`
}

// MaxRepoRefSize bounds a ref name as hosting services do in practice.
const MaxRepoRefSize = 255

// ParseRepoURL accepts "host/owner/name", "https://host/owner/name" or either
// with a .git suffix, and returns the canonical https URL plus the pieces a
// connector rule and a clone need. Anything else — userinfo, a query, a
// scheme other than https, a path that is not exactly owner/name — is refused.
func ParseRepoURL(raw string) (canonical, host, ownerRepo string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", "", Err(CodeBadRequest, "repo url is required")
	}
	if len(raw) > 1024 {
		return "", "", "", Err(CodeBadRequest, "repo url is longer than 1024 bytes")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", "", Err(CodeBadRequest, "repo url %q: %v", raw, perr)
	}
	if u.Scheme != "https" {
		return "", "", "", Err(CodeBadRequest, "repo url %q must use https", raw)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", "", "", Err(CodeBadRequest, "repo url %q must not carry userinfo, a query or a fragment", raw)
	}
	host = strings.ToLower(u.Host)
	if host == "" || strings.ContainsAny(host, " \t\r\n\\%") {
		return "", "", "", Err(CodeBadRequest, "repo url %q has no host", raw)
	}
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", "", "", Err(CodeBadRequest, "repo url %q must name exactly owner/name", raw)
	}
	parts[1] = strings.TrimSuffix(parts[1], ".git")
	for _, part := range parts {
		if !ValidRepoSegment(part) {
			return "", "", "", Err(CodeBadRequest, "repo url %q has an invalid path segment %q", raw, part)
		}
	}
	ownerRepo = parts[0] + "/" + parts[1]
	return "https://" + host + "/" + ownerRepo, host, ownerRepo, nil
}

// ValidRepoSegment reports whether s is an owner or repository name a hosting
// service would accept: ASCII letters, digits, '.', '_' and '-', not starting
// with '.' or '-', at most 255 bytes.
func ValidRepoSegment(s string) bool {
	if s == "" || len(s) > 255 || s[0] == '.' || s[0] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// ValidateRepoRef rejects a ref git would refuse or misread as an option.
func ValidateRepoRef(ref string) error {
	if ref == "" {
		return nil
	}
	if len(ref) > MaxRepoRefSize {
		return Err(CodeBadRequest, "repo ref is longer than %d bytes", MaxRepoRefSize)
	}
	if ref[0] == '-' || ref[0] == '/' || strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".lock") ||
		strings.Contains(ref, "..") || strings.Contains(ref, "@{") || strings.Contains(ref, "//") {
		return Err(CodeBadRequest, "repo ref %q is not a valid git ref", ref)
	}
	for _, r := range ref {
		if r <= 0x20 || r == 0x7f || strings.ContainsRune("~^:?*[\\", r) {
			return Err(CodeBadRequest, "repo ref %q is not a valid git ref", ref)
		}
	}
	return nil
}

// Normalize canonicalizes the URL and checks the ref and depth. An empty
// RepoSpec is valid and means "no clone".
func (r RepoSpec) Normalize() (RepoSpec, error) {
	if r.URL == "" {
		if r.Ref != "" || r.Depth != 0 {
			return RepoSpec{}, Err(CodeBadRequest, "repo ref and depth need a repo url")
		}
		return RepoSpec{}, nil
	}
	canonical, _, _, err := ParseRepoURL(r.URL)
	if err != nil {
		return RepoSpec{}, err
	}
	if err := ValidateRepoRef(r.Ref); err != nil {
		return RepoSpec{}, err
	}
	if r.Depth < 0 {
		return RepoSpec{}, Err(CodeBadRequest, "repo depth must not be negative")
	}
	return RepoSpec{URL: canonical, Ref: r.Ref, Depth: r.Depth}, nil
}

// ValidateRepoPattern checks a connector rule's repos entry: "owner/name" or
// "owner/*".
func ValidateRepoPattern(pattern string) error {
	owner, name, ok := strings.Cut(pattern, "/")
	if !ok || !ValidRepoSegment(owner) || (name != "*" && !ValidRepoSegment(name)) {
		return Err(CodeBadRequest, "repo pattern %q must be owner/name or owner/*", pattern)
	}
	return nil
}

// MatchRepo reports whether pattern ("owner/name" or "owner/*") covers
// ownerRepo. Matching is case-insensitive, as hosting services are.
func MatchRepo(pattern, ownerRepo string) bool {
	pattern, ownerRepo = strings.ToLower(pattern), strings.ToLower(ownerRepo)
	if owner, ok := strings.CutSuffix(pattern, "/*"); ok {
		rest, found := strings.CutPrefix(ownerRepo, owner+"/")
		return found && rest != "" && !strings.Contains(rest, "/")
	}
	return pattern == ownerRepo
}

// GitRuleCovers reports whether a normalized git connector rule authorizes
// fetching (and, when push is set, pushing) the repository at host.
func GitRuleCovers(rule EgressRule, host, ownerRepo string, push bool) bool {
	if rule.Connector != EgressConnectorGit || (push && !rule.Push) {
		return false
	}
	if !HostMatchesAny(host, rule.Hosts) {
		return false
	}
	for _, p := range rule.Repos {
		if MatchRepo(p, ownerRepo) {
			return true
		}
	}
	return false
}

// HostMatchesAny reports whether host (optionally host:port) is covered by
// one of patterns: an exact host, "*.example.com" for subdomains (never the
// apex itself), or "*". A pattern with a port matches only that port.
func HostMatchesAny(host string, patterns []string) bool {
	host = strings.ToLower(host)
	hport := ""
	if h, p, err := net.SplitHostPort(host); err == nil {
		host, hport = h, p
	}
	for _, p := range patterns {
		p = strings.ToLower(p)
		pport := ""
		if ph, pp, err := net.SplitHostPort(p); err == nil {
			p, pport = ph, pp
		}
		if pport != "" && pport != hport {
			continue
		}
		if p == "*" {
			return true
		}
		if strings.HasPrefix(p, "*.") {
			if strings.HasSuffix(host, p[1:]) && host != p[2:] {
				return true
			}
			continue
		}
		if host == p {
			return true
		}
	}
	return false
}

// ParseRepoFlag parses the CLI form URL[@REF]: the last '@' separates the
// ref, so an accidental userinfo '@' inside the URL is still reported as a
// URL error rather than silently becoming a ref.
func ParseRepoFlag(raw string, depth int) (RepoSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return RepoSpec{}, Err(CodeBadRequest, "repo is required")
	}
	spec := RepoSpec{URL: raw, Depth: depth}
	if i := strings.LastIndexByte(raw, '@'); i > 0 && !strings.Contains(raw[i+1:], "/") {
		spec.URL, spec.Ref = raw[:i], raw[i+1:]
		if spec.Ref == "" {
			return RepoSpec{}, Err(CodeBadRequest, "repo %q: empty ref after '@'", raw)
		}
	}
	return spec.Normalize()
}
