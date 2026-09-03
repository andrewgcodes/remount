package firecracker

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"remount.dev/remount/internal/proto"
)

const (
	bundleFormat       = "remount-firecracker-full-v1"
	defaultBundleBytes = int64(512 << 30)
)

// CoWVolumeOptions configure full disk+memory checkpoint storage.
type CoWVolumeOptions struct {
	Dir            string
	Images         *ReflinkStore
	Firecracker    string
	MaxBundleBytes int64
	ExpectedGuest  uint16
}

// CoWVolumeProvider combines bounded reflink root images with full snapshot
// bundles. It never mounts an image concurrently with the VM; filesystem
// access goes through GuestBridge.
type CoWVolumeProvider struct {
	opts   CoWVolumeOptions
	compat Compatibility
}

// NewCoWVolumeProvider constructs a concrete block volume provider.
func NewCoWVolumeProvider(opts CoWVolumeOptions) (*CoWVolumeProvider, error) {
	if opts.Dir == "" || opts.Images == nil || opts.Firecracker == "" {
		return nil, errors.New("firecracker: volume directory, reflink store and VMM path are required")
	}
	if opts.MaxBundleBytes <= 0 {
		opts.MaxBundleBytes = defaultBundleBytes
	}
	if opts.ExpectedGuest == 0 {
		opts.ExpectedGuest = GuestProtocolVersion
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if st, err := os.Lstat(dir); err != nil || !st.IsDir() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("firecracker: snapshot directory %q must be a private real directory", dir)
	}
	opts.Dir = dir
	return &CoWVolumeProvider{opts: opts}, nil
}

func (p *CoWVolumeProvider) Probe(ctx context.Context, base string) error {
	if err := p.opts.Images.Probe(ctx, base); err != nil {
		return err
	}
	compat, err := DetectCompatibility(ctx, p.opts.Firecracker)
	if err != nil {
		return err
	}
	if compat.GuestProtocol != p.opts.ExpectedGuest {
		return fmt.Errorf("guest protocol %d does not match %d", compat.GuestProtocol, p.opts.ExpectedGuest)
	}
	p.compat = compat
	return nil
}

func (p *CoWVolumeProvider) Create(ctx context.Context, id, base string, restore io.Reader) (_ Volume, err error) {
	if err := p.compat.validate(); err != nil {
		return nil, errors.New("firecracker: volume provider has not passed Probe")
	}
	image, err := p.opts.Images.Create(ctx, id, base)
	if err != nil {
		return nil, err
	}
	volume := &cowVolume{provider: p, id: id, image: image}
	committed := false
	defer func() {
		if !committed {
			cleanupErr := volume.Destroy(context.Background())
			err = errors.Join(err, cleanupErr)
		}
	}()
	if restore != nil {
		if err := volume.restore(ctx, restore); err != nil {
			return nil, err
		}
	}
	committed = true
	return volume, nil
}

func (p *CoWVolumeProvider) Adopt(ctx context.Context, id string) (Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	image, err := p.opts.Images.Adopt(id)
	if err != nil {
		return nil, proto.Err(proto.CodeNotFound, "%v", err)
	}
	volume := &cowVolume{provider: p, id: id, image: image}
	manifestPath := filepath.Join(p.snapshotDir(id), "manifest.cbor")
	data, err := os.ReadFile(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return volume, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest bundleManifest
	if err := proto.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if err := p.validateManifest(manifest); err != nil {
		return nil, err
	}
	volume.files = SnapshotFiles{State: filepath.Join(p.snapshotDir(id), "vm.state"), Memory: filepath.Join(p.snapshotDir(id), "vm.mem"), Compatibility: manifest.Compatibility}
	volume.generation = manifest.Generation
	volume.hasMemory = true
	return volume, nil
}

func (p *CoWVolumeProvider) snapshotDir(id string) string {
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(p.opts.Dir, fmt.Sprintf("vm-%x-%s", sum[:8], id))
}

func (p *CoWVolumeProvider) validateManifest(manifest bundleManifest) error {
	if manifest.Format != bundleFormat || manifest.Generation == 0 {
		return proto.Err(proto.CodeBadRequest, "invalid Firecracker full checkpoint manifest")
	}
	if err := manifest.Compatibility.validate(); err != nil {
		return proto.Err(proto.CodeBadRequest, "%v", err)
	}
	if manifest.Compatibility != p.compat {
		return proto.Err(proto.CodeConflict, "Firecracker checkpoint is incompatible with this host")
	}
	return nil
}

type bundleManifest struct {
	Format        string        `cbor:"format"`
	Generation    uint64        `cbor:"generation"`
	Compatibility Compatibility `cbor:"compatibility"`
	Files         []bundleFile  `cbor:"files"`
}

type bundleFile struct {
	Name   string   `cbor:"name"`
	Size   int64    `cbor:"size"`
	SHA256 [32]byte `cbor:"sha256"`
}

type cowVolume struct {
	provider   *CoWVolumeProvider
	id         string
	image      *RootImage
	files      SnapshotFiles
	generation uint64
	hasMemory  bool
}

func (v *cowVolume) ImagePath() string { return v.image.Path() }
func (v *cowVolume) MemorySnapshot() (SnapshotFiles, uint64, bool) {
	return v.files, v.generation, v.hasMemory
}

func (v *cowVolume) Checkpoint(ctx context.Context, excludes []string, files SnapshotFiles, generation uint64, out io.Writer) error {
	for _, exclude := range excludes {
		if exclude != ".remount" && exclude != ".remount/env" {
			return proto.Err(proto.CodeUnsupported, "process-preserving Firecracker checkpoints cannot omit %q", exclude)
		}
	}
	if generation == 0 || files.State == "" || files.Memory == "" {
		return errors.New("firecracker: incomplete full checkpoint inputs")
	}
	if files.Compatibility != v.provider.compat {
		return proto.Err(proto.CodeConflict, "Firecracker snapshot compatibility changed while checkpointing")
	}
	sources := map[string]string{"disk.ext4": v.image.Path(), "vm.state": files.State, "vm.mem": files.Memory}
	manifest := bundleManifest{Format: bundleFormat, Generation: generation, Compatibility: files.Compatibility}
	var total int64
	for name, path := range sources {
		info, err := regularFile(path)
		if err != nil {
			return err
		}
		if info.Size() > v.provider.opts.MaxBundleBytes-total {
			return proto.Err(proto.CodeResourceExhausted, "Firecracker checkpoint exceeds %d bytes", v.provider.opts.MaxBundleBytes)
		}
		digest, err := hashFile(ctx, path)
		if err != nil {
			return err
		}
		total += info.Size()
		manifest.Files = append(manifest.Files, bundleFile{Name: name, Size: info.Size(), SHA256: digest})
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Name < manifest.Files[j].Name })
	if err := v.persistSnapshot(ctx, files, manifest); err != nil {
		return err
	}
	if err := writeBundle(ctx, out, manifest, sources); err != nil {
		return err
	}
	v.files = SnapshotFiles{State: filepath.Join(v.provider.snapshotDir(v.id), "vm.state"), Memory: filepath.Join(v.provider.snapshotDir(v.id), "vm.mem"), Compatibility: manifest.Compatibility}
	v.generation, v.hasMemory = generation, true
	return nil
}

func (v *cowVolume) persistSnapshot(ctx context.Context, files SnapshotFiles, manifest bundleManifest) error {
	dir := v.provider.snapshotDir(v.id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for name, source := range map[string]string{"vm.state": files.State, "vm.mem": files.Memory} {
		if err := atomicCopy(ctx, source, filepath.Join(dir, name), 0o600); err != nil {
			return err
		}
	}
	data, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := atomicBytes(filepath.Join(dir, "manifest.cbor"), data, 0o600); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func (v *cowVolume) restore(ctx context.Context, in io.Reader) error {
	stage, err := os.MkdirTemp(v.provider.opts.Dir, ".restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	manifest, err := readBundle(ctx, in, stage, v.provider.opts.MaxBundleBytes)
	if err != nil {
		return err
	}
	if err := v.provider.validateManifest(manifest); err != nil {
		return err
	}
	if err := atomicCopy(ctx, filepath.Join(stage, "disk.ext4"), v.image.Path(), 0o600); err != nil {
		return err
	}
	files := SnapshotFiles{State: filepath.Join(stage, "vm.state"), Memory: filepath.Join(stage, "vm.mem"), Compatibility: manifest.Compatibility}
	if err := v.persistSnapshot(ctx, files, manifest); err != nil {
		return err
	}
	v.files = SnapshotFiles{State: filepath.Join(v.provider.snapshotDir(v.id), "vm.state"), Memory: filepath.Join(v.provider.snapshotDir(v.id), "vm.mem"), Compatibility: manifest.Compatibility}
	v.generation, v.hasMemory = manifest.Generation, true
	return nil
}

func (v *cowVolume) Destroy(context.Context) error {
	if err := v.image.Destroy(); err != nil {
		return err
	}
	dir := v.provider.snapshotDir(v.id)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDirectory(v.provider.opts.Dir)
}

func writeBundle(ctx context.Context, out io.Writer, manifest bundleManifest, sources map[string]string) (err error) {
	gz := gzip.NewWriter(out)
	tw := tar.NewWriter(gz)
	defer func() { err = errors.Join(err, tw.Close(), gz.Close()) }()
	data, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: "manifest.cbor", Mode: 0o600, Size: int64(len(data))}); err != nil {
		return err
	}
	if err := writeAll(tw, data); err != nil {
		return err
	}
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: file.Name, Mode: 0o600, Size: file.Size}); err != nil {
			return err
		}
		input, err := os.Open(sources[file.Name])
		if err != nil {
			return err
		}
		_, copyErr := io.CopyN(tw, input, file.Size)
		closeErr := input.Close()
		if copyErr != nil || closeErr != nil {
			return errors.Join(copyErr, closeErr)
		}
	}
	return nil
}

func readBundle(ctx context.Context, in io.Reader, dir string, maxBytes int64) (bundleManifest, error) {
	gz, err := gzip.NewReader(in)
	if err != nil {
		return bundleManifest{}, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	seen := map[string]bool{}
	var manifest bundleManifest
	var total int64
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return bundleManifest{}, err
		}
		if err := ctx.Err(); err != nil {
			return bundleManifest{}, err
		}
		if header.Typeflag != tar.TypeReg || seen[header.Name] {
			return bundleManifest{}, proto.Err(proto.CodeBadRequest, "invalid Firecracker bundle entry %q", header.Name)
		}
		seen[header.Name] = true
		if header.Size < 0 || header.Size > maxBytes-total {
			return bundleManifest{}, proto.Err(proto.CodeResourceExhausted, "Firecracker bundle exceeds %d bytes", maxBytes)
		}
		total += header.Size
		if header.Name == "manifest.cbor" {
			if header.Size > 1<<20 {
				return bundleManifest{}, proto.Err(proto.CodeResourceExhausted, "Firecracker manifest is too large")
			}
			data := make([]byte, header.Size)
			if _, err := io.ReadFull(tr, data); err != nil {
				return bundleManifest{}, err
			}
			if err := proto.Unmarshal(data, &manifest); err != nil {
				return bundleManifest{}, err
			}
			continue
		}
		if header.Name != "disk.ext4" && header.Name != "vm.state" && header.Name != "vm.mem" {
			return bundleManifest{}, proto.Err(proto.CodeBadRequest, "unknown Firecracker bundle entry %q", header.Name)
		}
		path := filepath.Join(dir, header.Name)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return bundleManifest{}, err
		}
		_, copyErr := io.CopyN(file, tr, header.Size)
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil {
			return bundleManifest{}, errors.Join(copyErr, syncErr, closeErr)
		}
	}
	if !seen["manifest.cbor"] || !seen["disk.ext4"] || !seen["vm.state"] || !seen["vm.mem"] {
		return bundleManifest{}, proto.Err(proto.CodeBadRequest, "incomplete Firecracker bundle")
	}
	for _, file := range manifest.Files {
		if !seen[file.Name] {
			return bundleManifest{}, proto.Err(proto.CodeBadRequest, "manifest references missing %q", file.Name)
		}
		info, err := regularFile(filepath.Join(dir, file.Name))
		if err != nil || info.Size() != file.Size {
			return bundleManifest{}, proto.Err(proto.CodeBadRequest, "bundle size mismatch for %q", file.Name)
		}
		digest, err := hashFile(ctx, filepath.Join(dir, file.Name))
		if err != nil || digest != file.SHA256 {
			return bundleManifest{}, proto.Err(proto.CodeBadRequest, "bundle digest mismatch for %q", file.Name)
		}
	}
	return manifest, nil
}

func hashFile(ctx context.Context, path string) ([32]byte, error) {
	var zero [32]byte
	file, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	defer file.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		n, err := file.Read(buf)
		if n > 0 {
			_, _ = h.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return zero, err
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func atomicCopy(ctx context.Context, source, target string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	tmp, err := os.CreateTemp(filepath.Dir(target), ".copy-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := io.Copy(tmp, &contextReader{ctx: ctx, reader: input}); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, target); err != nil {
		return err
	}
	ok = true
	return syncDirectory(filepath.Dir(target))
}

func atomicBytes(target string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(target), ".bytes-")
	if err != nil {
		return err
	}
	path := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(path)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := writeAll(tmp, data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(path, target); err != nil {
		return err
	}
	ok = true
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

var _ VolumeProvider = (*CoWVolumeProvider)(nil)
var _ Volume = (*cowVolume)(nil)
