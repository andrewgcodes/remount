package node

import (
	"testing"

	"remount.dev/remount/internal/proto"
)

// A release aborted at one generation must not block a release at a later
// one: control re-placing the workspace at a newer generation is the
// authority that retires the older abort record.
func TestNewerGenerationSupersedesAnAbortPublishedRelease(t *testing.T) {
	aborted := durableRelease{State: releasePublished, Request: proto.WSReleaseReq{WS: "ws_1", Gen: 5, ReleaseEpoch: 3, OperationID: "rel_a"}}
	cases := []struct {
		name string
		req  proto.WSReleaseReq
		want bool
	}{
		{"newer generation", proto.WSReleaseReq{WS: "ws_1", Gen: 7, ReleaseEpoch: 4, OperationID: "rel_b"}, true},
		{"same generation, newer epoch", proto.WSReleaseReq{WS: "ws_1", Gen: 5, ReleaseEpoch: 4, OperationID: "rel_b"}, true},
		{"same generation, same epoch", proto.WSReleaseReq{WS: "ws_1", Gen: 5, ReleaseEpoch: 3, OperationID: "rel_b"}, false},
		{"older generation", proto.WSReleaseReq{WS: "ws_1", Gen: 4, ReleaseEpoch: 9, OperationID: "rel_b"}, false},
	}
	for _, c := range cases {
		if got := startsNewReleaseCycle(aborted, c.req); got != c.want {
			t.Errorf("%s: startsNewReleaseCycle = %v, want %v", c.name, got, c.want)
		}
	}
	aborting := durableRelease{State: releaseAborting, Request: aborted.Request}
	if startsNewReleaseCycle(aborting, proto.WSReleaseReq{WS: "ws_1", Gen: 7, ReleaseEpoch: 4, OperationID: "rel_b"}) {
		t.Fatal("an abort still in progress must keep refusing a new cycle")
	}
}
