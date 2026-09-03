// Package webui serves the reproducibly built operator console embedded in the
// Remount binary.
package webui

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

//go:embed dist dist/assets/* dist.sha256
var files embed.FS

const contentSecurityPolicy = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' wss: ws:; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"

// NewHandler verifies the committed distribution manifest and returns a
// handler for /console/. Verifying at server construction prevents a stale or
// partially rebuilt UI from becoming operator-facing authority.
func NewHandler() (http.Handler, error) {
	manifest, err := fs.ReadFile(files, "dist.sha256")
	if err != nil {
		return nil, fmt.Errorf("console manifest: %w", err)
	}
	for lineNo, line := range strings.Split(strings.TrimSpace(string(manifest)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || len(fields[0]) != sha256.Size*2 || !strings.HasPrefix(fields[1], "dist/") {
			return nil, fmt.Errorf("console manifest line %d is invalid", lineNo+1)
		}
		want, err := hex.DecodeString(fields[0])
		if err != nil {
			return nil, fmt.Errorf("console manifest line %d digest: %w", lineNo+1, err)
		}
		body, err := fs.ReadFile(files, fields[1])
		if err != nil {
			return nil, fmt.Errorf("console asset %q: %w", fields[1], err)
		}
		got := sha256.Sum256(body)
		if !bytes.Equal(got[:], want) {
			return nil, fmt.Errorf("console asset %q does not match manifest", fields[1])
		}
	}
	return http.HandlerFunc(serve), nil
}

func serve(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w.Header())
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rel := strings.TrimPrefix(r.URL.Path, "/console/")
	if rel == r.URL.Path || rel == "" {
		rel = "index.html"
	}
	clean := path.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "\\") {
		http.NotFound(w, r)
		return
	}
	body, err := fs.ReadFile(files, "dist/"+clean)
	if err != nil {
		// Client-side routes fall back to the application shell. Asset and
		// config misses stay 404 so a proxy never caches HTML as executable.
		if strings.HasPrefix(clean, "assets/") || clean == "config.json" || path.Ext(clean) != "" {
			http.NotFound(w, r)
			return
		}
		clean = "index.html"
		body, err = fs.ReadFile(files, "dist/index.html")
		if err != nil {
			http.Error(w, "console unavailable", http.StatusInternalServerError)
			return
		}
	}
	switch clean {
	case "index.html":
		w.Header().Set("Cache-Control", "no-cache")
	case "config.json":
		w.Header().Set("Cache-Control", "no-store")
	default:
		if strings.HasPrefix(clean, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
	}
	if typ := mime.TypeByExtension(path.Ext(clean)); typ != "" {
		w.Header().Set("Content-Type", typ)
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(body)
}

func setSecurityHeaders(h http.Header) {
	h.Set("Content-Security-Policy", contentSecurityPolicy)
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
}
