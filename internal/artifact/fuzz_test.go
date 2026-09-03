package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"testing"
)

func FuzzRestore(f *testing.F) {
	f.Add(fuzzArchiveSeed())
	f.Add([]byte("not a gzip stream"))
	f.Add([]byte{0x1f, 0x8b, 0x08})
	f.Fuzz(func(t *testing.T, archive []byte) {
		if len(archive) > 1<<20 {
			t.Skip()
		}
		root := t.TempDir()
		limits := RestoreLimits{
			MaxCompressedBytes: 1 << 20, MaxExpandedBytes: 2 << 20,
			MaxFileBytes: 1 << 20, MaxEntries: 128, MaxPathBytes: 256,
			MaxDepth: 16, MaxCompressionRatio: 100,
		}
		_ = RestoreWithLimits(root, bytes.NewReader(archive), limits)
		if info, err := os.Lstat(root); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("restore root lost containment: info=%v err=%v", info, err)
		}
	})
}

func fuzzArchiveSeed() []byte {
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	body := []byte("seed")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "dir/file.txt", Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		panic(err)
	}
	if _, err := tarWriter.Write(body); err != nil {
		panic(err)
	}
	if err := tarWriter.Close(); err != nil {
		panic(err)
	}
	if err := gzipWriter.Close(); err != nil {
		panic(err)
	}
	return output.Bytes()
}
