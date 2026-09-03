package bench

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"remount.dev/remount/internal/artifact"
	"remount.dev/remount/internal/artifact/chunked"
	"remount.dev/remount/internal/broker"
	"remount.dev/remount/internal/proto"
)

const chunkFixtureBytes = 8 << 20

func writeFixture(path string, size int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	block := make([]byte, 64<<10)
	random := rand.New(rand.NewSource(1))
	for written := 0; written < size; {
		if _, err := random.Read(block); err != nil {
			_ = file.Close()
			return err
		}
		part := block
		if len(part) > size-written {
			part = part[:size-written]
		}
		n, err := file.Write(part)
		if err != nil {
			_ = file.Close()
			return err
		}
		written += n
	}
	return file.Close()
}

func BenchmarkChunkedSnapshotFull8MiB(b *testing.B) {
	var uploaded int64
	for range b.N {
		b.StopTimer()
		root := b.TempDir()
		if err := writeFixture(filepath.Join(root, "fixture.bin"), chunkFixtureBytes); err != nil {
			b.Fatal(err)
		}
		store, err := artifact.NewStore(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := chunked.Snapshot(context.Background(), store, root, chunked.SnapshotOptions{})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		uploaded += result.BytesUploaded
	}
	b.ReportMetric(float64(uploaded)/float64(b.N), "upload-B/op")
	b.SetBytes(chunkFixtureBytes)
}

func BenchmarkChunkedSnapshotDelta4KiBOf8MiB(b *testing.B) {
	var uploaded int64
	for range b.N {
		b.StopTimer()
		root := b.TempDir()
		path := filepath.Join(root, "fixture.bin")
		if err := writeFixture(path, chunkFixtureBytes); err != nil {
			b.Fatal(err)
		}
		store, err := artifact.NewStore(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		if _, err := chunked.Snapshot(context.Background(), store, root, chunked.SnapshotOptions{}); err != nil {
			b.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := file.WriteAt(make([]byte, 4096), chunkFixtureBytes/2); err != nil {
			b.Fatal(err)
		}
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := chunked.Snapshot(context.Background(), store, root, chunked.SnapshotOptions{})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		uploaded += result.BytesUploaded
	}
	b.ReportMetric(float64(uploaded)/float64(b.N), "upload-B/op")
	b.ReportMetric(float64(chunkFixtureBytes)/(float64(uploaded)/float64(b.N)), "dedupe-ratio")
}

type brokerFixture struct {
	client *http.Client
	url    string
	close  func()
}

func newBrokerFixture(b *testing.B, proxied bool) brokerFixture {
	b.Helper()
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "2")
		_, _ = io.WriteString(w, "ok")
	}))
	pool := x509.NewCertPool()
	pool.AddCert(upstream.Certificate())
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		b.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	if !proxied {
		return brokerFixture{client: client, url: upstream.URL, close: func() { transport.CloseIdleConnections(); upstream.Close() }}
	}
	gateway := broker.New(broker.Options{
		WS: "ws_bench", Generation: 1, Principal: "bench", Tenant: "bench",
		RootCAs: pool, AllowPrivate: []string{"127.0.0.1"},
		Network: proto.NetworkPolicy{Default: proto.NetworkDefaultDeny, Rules: []proto.EgressRule{{
			ID: "bench", Protocol: proto.EgressProtocolHTTPS, Hosts: []string{upstreamURL.Host},
		}}},
	})
	base, err := gateway.Start()
	if err != nil {
		upstream.Close()
		b.Fatal(err)
	}
	return brokerFixture{client: client, url: broker.DestURL(base, upstreamURL.Host), close: func() { _ = gateway.Close(); transport.CloseIdleConnections(); upstream.Close() }}
}

func benchmarkHTTPRoundTrip(b *testing.B, proxied bool) {
	fixture := newBrokerFixture(b, proxied)
	defer fixture.close()
	response, err := fixture.client.Get(fixture.url)
	if err != nil {
		b.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		response, err := fixture.client.Get(fixture.url)
		if err != nil {
			b.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			b.Fatalf("status %d", response.StatusCode)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			b.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectTLSRoundTrip(b *testing.B)     { benchmarkHTTPRoundTrip(b, false) }
func BenchmarkBrokerAllowedRoundTrip(b *testing.B) { benchmarkHTTPRoundTrip(b, true) }

func TestBenchmarkFixtureSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture")
	if err := writeFixture(path, chunkFixtureBytes); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != chunkFixtureBytes {
		t.Fatalf("fixture = %v, %v; want %s", info, err, fmt.Sprint(chunkFixtureBytes))
	}
}
