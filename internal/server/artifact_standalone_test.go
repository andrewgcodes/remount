package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestStandaloneArtifactsAcceptABearerTheSameAsNone pins a bug where adding a
// credential made a caller LESS authorized.
//
// A standalone server with no shared token and no authenticator has nothing to
// check a bearer against, and the relay hello and console both admit any bearer
// in that configuration. The artifact endpoint admitted only the absence of
// one, so an operator with REMOUNT_TOKEN exported got 401 from `remount push`
// against their own laptop, and bench/move_matrix.py — which requires
// REMOUNT_TOKEN precisely so it never puts a credential on argv — could not run
// against a standalone server at all.
//
// The two answers must agree: either the mode authenticates or it does not.
func TestStandaloneArtifactsAcceptABearerTheSameAsNone(t *testing.T) {
	s := &Server{opts: Options{Mode: ModeStandalone}}

	withoutHeader := httptest.NewRequest(http.MethodGet, "/v1/artifacts/art_sha256:"+
		"0000000000000000000000000000000000000000000000000000000000000000", nil)
	bare, _, okBare := s.artifactSubject(withoutHeader)
	if !okBare {
		t.Fatal("standalone refused a request carrying no credential; that is the configuration's whole point")
	}

	withHeader := withoutHeader.Clone(withoutHeader.Context())
	withHeader.Header.Set("Authorization", "Bearer any-value-at-all")
	presented, _, okPresented := s.artifactSubject(withHeader)
	if !okPresented {
		t.Fatal("standalone refused a request that presented a bearer while accepting the same request without one; presenting a credential must never reduce authority")
	}
	if presented.ID != bare.ID || presented.Tenant != bare.Tenant {
		t.Fatalf("the same standalone caller resolved to two different subjects: %+v with a bearer, %+v without", presented, bare)
	}
}

// TestConfiguredTokenStillRejectsAWrongBearer is the control. The fix above
// must not turn every mode into an open door: once a shared token exists, a
// wrong one is still refused and an absent one is still refused.
func TestConfiguredTokenStillRejectsAWrongBearer(t *testing.T) {
	s := &Server{opts: Options{Mode: ModeStandalone, Token: "the-real-token"}}
	base := httptest.NewRequest(http.MethodGet, "/v1/artifacts/art_sha256:"+
		"0000000000000000000000000000000000000000000000000000000000000000", nil)

	if _, _, ok := s.artifactSubject(base); ok {
		t.Fatal("a server with a configured token accepted a request with no credential")
	}

	wrong := base.Clone(base.Context())
	wrong.Header.Set("Authorization", "Bearer not-the-real-token")
	if _, _, ok := s.artifactSubject(wrong); ok {
		t.Fatal("a server with a configured token accepted the wrong bearer")
	}

	right := base.Clone(base.Context())
	right.Header.Set("Authorization", "Bearer the-real-token")
	if _, _, ok := s.artifactSubject(right); !ok {
		t.Fatal("the configured token was refused")
	}
}
