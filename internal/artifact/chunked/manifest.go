package chunked

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"remount.dev/remount/internal/artifact"
)

const manifestVersion = 1

// Limits bound both snapshot construction and hostile manifest decoding. Zero
// fields receive finite defaults chosen to permit artifacts far above 8 GiB.
type Limits struct {
	MaxEntries         int
	MaxChunks          int
	MaxManifestBytes   int64
	MaxPathBytes       int
	MaxXattrsPerEntry  int
	MaxXattrBytes      int
	MaxTotalXattrBytes int64
	MaxFileBytes       int64
	MaxSnapshotBytes   int64
}

// DefaultLimits bound manifest metadata while allowing multi-terabyte trees.
var DefaultLimits = Limits{
	MaxEntries: 2_000_000, MaxChunks: 4_000_000,
	MaxManifestBytes: 512 << 20, MaxPathBytes: 4096,
	MaxXattrsPerEntry: 128, MaxXattrBytes: 1 << 20, MaxTotalXattrBytes: 256 << 20,
	MaxFileBytes: 1 << 40, MaxSnapshotBytes: 16 << 40,
}

// Manifest is the canonical plaintext snapshot descriptor.
type Manifest struct {
	Version    int       `json:"version"`
	Chunker    ChunkSpec `json:"chunker"`
	Entries    []Entry   `json:"entries"`
	Hot        []string  `json:"hot"`
	TotalBytes int64     `json:"total_bytes"`
	Chunks     int       `json:"chunks"`
}

// ChunkSpec records the content-defined chunking parameters as format state.
type ChunkSpec struct {
	Algorithm string `json:"algorithm"`
	Min       int    `json:"min"`
	Average   int    `json:"average"`
	Max       int    `json:"max"`
}

// Entry describes one directory, regular file, or symlink.
type Entry struct {
	Path        string     `json:"path"`
	Type        string     `json:"type"`
	Mode        uint32     `json:"mode"`
	ModTimeUnix int64      `json:"mtime_unix_nano"`
	Size        int64      `json:"size,omitempty"`
	Link        string     `json:"link,omitempty"`
	Xattrs      []Xattr    `json:"xattrs,omitempty"`
	Chunks      []ChunkRef `json:"chunks,omitempty"`
}

// Xattr is one sorted extended attribute.
type Xattr struct {
	Name  string `json:"name"`
	Value []byte `json:"value"`
}

// ChunkRef names one plaintext content chunk and its exact byte length.
type ChunkRef struct {
	ID   string `json:"id"`
	Size int    `json:"size"`
}

func normalizedLimits(l Limits) Limits {
	d := DefaultLimits
	if l.MaxEntries > 0 {
		d.MaxEntries = l.MaxEntries
	}
	if l.MaxChunks > 0 {
		d.MaxChunks = l.MaxChunks
	}
	if l.MaxManifestBytes > 0 {
		d.MaxManifestBytes = l.MaxManifestBytes
	}
	if l.MaxPathBytes > 0 {
		d.MaxPathBytes = l.MaxPathBytes
	}
	if l.MaxXattrsPerEntry > 0 {
		d.MaxXattrsPerEntry = l.MaxXattrsPerEntry
	}
	if l.MaxXattrBytes > 0 {
		d.MaxXattrBytes = l.MaxXattrBytes
	}
	if l.MaxTotalXattrBytes > 0 {
		d.MaxTotalXattrBytes = l.MaxTotalXattrBytes
	}
	if l.MaxFileBytes > 0 {
		d.MaxFileBytes = l.MaxFileBytes
	}
	if l.MaxSnapshotBytes > 0 {
		d.MaxSnapshotBytes = l.MaxSnapshotBytes
	}
	return d
}

func canonicalManifest(manifest Manifest, limits Limits) ([]byte, error) {
	if err := validateManifest(manifest, limits); err != nil {
		return nil, err
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limits.MaxManifestBytes {
		return nil, fmt.Errorf("chunked artifact: manifest exceeds %d bytes", limits.MaxManifestBytes)
	}
	return body, nil
}

func decodeManifest(r io.Reader, expectedID string, limits Limits) (Manifest, []byte, error) {
	var manifest Manifest
	body, err := io.ReadAll(io.LimitReader(r, limits.MaxManifestBytes+1))
	if err != nil {
		return manifest, nil, err
	}
	if int64(len(body)) > limits.MaxManifestBytes {
		return manifest, nil, fmt.Errorf("chunked artifact: manifest exceeds %d bytes", limits.MaxManifestBytes)
	}
	if digestID(body) != expectedID {
		return manifest, nil, fmt.Errorf("%w", artifact.ErrDigestMismatch)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, nil, fmt.Errorf("chunked artifact: malformed manifest")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return manifest, nil, fmt.Errorf("chunked artifact: malformed manifest")
	}
	canonical, err := canonicalManifest(manifest, limits)
	if err != nil {
		return manifest, nil, err
	}
	if !bytes.Equal(canonical, body) {
		return manifest, nil, fmt.Errorf("chunked artifact: non-canonical manifest")
	}
	return manifest, body, nil
}

func validateManifest(manifest Manifest, limits Limits) error {
	if manifest.Version != manifestVersion || manifest.Chunker != (ChunkSpec{Algorithm: "fastcdc-v1", Min: MinChunkSize, Average: AvgChunkSize, Max: MaxChunkSize}) {
		return errors.New("chunked artifact: unsupported manifest format")
	}
	if len(manifest.Entries) > limits.MaxEntries || manifest.Chunks > limits.MaxChunks || manifest.TotalBytes < 0 || manifest.TotalBytes > limits.MaxSnapshotBytes {
		return errors.New("chunked artifact: manifest resource limit exceeded")
	}
	seen := make(map[string]string, len(manifest.Entries))
	var prior string
	var total, xattrBytes int64
	var chunks int
	for _, entry := range manifest.Entries {
		if err := validatePath(entry.Path, limits.MaxPathBytes); err != nil {
			return err
		}
		if prior != "" && entry.Path <= prior {
			return errors.New("chunked artifact: entries are not uniquely sorted")
		}
		prior = entry.Path
		if entry.Mode&^0o7777 != 0 {
			return errors.New("chunked artifact: invalid entry mode")
		}
		if parent := path.Dir(entry.Path); parent != "." {
			if kind, ok := seen[parent]; !ok || kind != "dir" {
				return errors.New("chunked artifact: missing directory parent")
			}
		}
		if len(entry.Xattrs) > limits.MaxXattrsPerEntry {
			return errors.New("chunked artifact: too many extended attributes")
		}
		var lastXattr string
		for _, attr := range entry.Xattrs {
			if attr.Name == "" || !utf8.ValidString(attr.Name) || strings.IndexByte(attr.Name, 0) >= 0 || len(attr.Name) > limits.MaxPathBytes || len(attr.Value) > limits.MaxXattrBytes || (lastXattr != "" && attr.Name <= lastXattr) {
				return errors.New("chunked artifact: invalid extended attribute")
			}
			lastXattr = attr.Name
			xattrBytes += int64(len(attr.Name) + len(attr.Value))
			if xattrBytes > limits.MaxTotalXattrBytes {
				return errors.New("chunked artifact: extended attributes exceed limit")
			}
		}
		switch entry.Type {
		case "dir":
			if entry.Size != 0 || entry.Link != "" || len(entry.Chunks) != 0 {
				return errors.New("chunked artifact: invalid directory metadata")
			}
		case "symlink":
			if entry.Size != 0 || entry.Link == "" || len(entry.Chunks) != 0 || len(entry.Xattrs) != 0 || !safeLink(entry.Path, entry.Link) {
				return errors.New("chunked artifact: invalid symlink metadata")
			}
		case "file":
			if entry.Link != "" || entry.Size < 0 || entry.Size > limits.MaxFileBytes {
				return errors.New("chunked artifact: invalid file metadata")
			}
			var fileBytes int64
			for _, chunk := range entry.Chunks {
				if _, err := artifact.Digest(chunk.ID); err != nil || chunk.Size <= 0 || chunk.Size > MaxChunkSize {
					return errors.New("chunked artifact: invalid chunk reference")
				}
				fileBytes += int64(chunk.Size)
				chunks++
				if chunks > limits.MaxChunks || fileBytes > limits.MaxFileBytes {
					return errors.New("chunked artifact: chunk resource limit exceeded")
				}
			}
			if fileBytes != entry.Size || (entry.Size == 0 && len(entry.Chunks) != 0) || (entry.Size != 0 && len(entry.Chunks) == 0) {
				return errors.New("chunked artifact: file size does not match chunks")
			}
			total += entry.Size
			if total > limits.MaxSnapshotBytes {
				return errors.New("chunked artifact: snapshot exceeds limit")
			}
		default:
			return errors.New("chunked artifact: invalid entry type")
		}
		seen[entry.Path] = entry.Type
	}
	if total != manifest.TotalBytes || chunks != manifest.Chunks {
		return errors.New("chunked artifact: manifest totals do not match entries")
	}
	if !sort.StringsAreSorted(manifest.Hot) {
		return errors.New("chunked artifact: hot set is not sorted")
	}
	lastHot := ""
	for _, hot := range manifest.Hot {
		if hot == lastHot || seen[hot] != "file" {
			return errors.New("chunked artifact: invalid hot-set path")
		}
		lastHot = hot
	}
	return nil
}

func validatePath(name string, max int) error {
	if name == "" || !utf8.ValidString(name) || len(name) > max || name != path.Clean(name) || name == "." || name == ".." || strings.HasPrefix(name, "../") || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") || strings.IndexByte(name, 0) >= 0 {
		return errors.New("chunked artifact: invalid entry path")
	}
	return nil
}

func safeLink(name, target string) bool {
	if target == "" || !utf8.ValidString(target) || strings.HasPrefix(target, "/") || strings.Contains(target, "\\") || strings.IndexByte(target, 0) >= 0 {
		return false
	}
	resolved := path.Clean(path.Join(path.Dir(name), target))
	return resolved != ".." && !strings.HasPrefix(resolved, "../")
}
