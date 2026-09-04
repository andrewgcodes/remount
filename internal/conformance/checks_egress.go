package conformance

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// ---- binding placeholders, destination scoping and leak blocking ---------

// envFilePath is where §10.2 and §9 say a node records the broker address and
// the workspace's placeholders. It is the one file a harness is told to read,
// so it is also the one file a conformance run may assume exists.
const envFilePath = ".remount/env"

// bindingFixture makes a workspace that leases the target's binding and
// returns it with the broker base URL the node advertised.
func (s *Session) bindingFixture(ctx context.Context) (*fixture, string, error) {
	f, err := s.NewFixture(ctx, WorkspaceSpec{
		Name:     "conformance-binding",
		Bindings: []string{s.Target.Binding.ID},
	})
	if err != nil {
		return nil, "", err
	}
	var info WSInfoRes
	if err := s.CallNode(ctx, f.Node, "ws.info", WSGetReq{ID: f.WS.ID, Grant: &f.Grant}, &info); err != nil {
		return nil, "", err
	}
	if info.Broker == "" {
		return nil, "", Unavailablef("the node advertised no broker for %s, so the egress surface of §9 is not reachable", f.WS.ID)
	}
	return f, strings.TrimSuffix(info.Broker, "/"), nil
}

// brokerGet issues one reverse-proxy request through the broker, optionally
// carrying the binding's placeholder in a header. The broker is a public
// surface: §9 defines its URL shape and its decisions, so observing it is
// black-box.
func (s *Session) brokerGet(ctx context.Context, base, host, path, placeholder string) (int, []byte, error) {
	url := base + "/d/" + host + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	if placeholder != "" {
		req.Header.Set("Authorization", "Bearer "+placeholder)
	}
	res, err := s.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, body, err
}

func checkBindPlaceholderNotSecret(ctx context.Context, s *Session) error {
	f, _, err := s.bindingFixture(ctx)
	if err != nil {
		return err
	}
	var res FSReadRes
	if err := s.CallNode(ctx, f.Node, "fs.read", FSReadReq{WS: f.WS.ID, Path: envFilePath, Grant: &f.Grant}, &res); err != nil {
		return failf("the node wrote no %s for a workspace holding binding %s: %v", envFilePath, s.Target.Binding.ID, err)
	}
	secret := s.Target.Binding.Secret
	if secret != "" && bytes.Contains(res.Data, []byte(secret)) {
		return failf("%s contains the binding's secret; the workspace is trusted with nothing", envFilePath)
	}
	if ph := s.Target.Binding.Placeholder; ph != "" && !bytes.Contains(res.Data, []byte(ph)) {
		return failf("%s carries no placeholder for binding %s, so a harness has no reference to send", envFilePath, s.Target.Binding.ID)
	}
	if !bytes.Contains(res.Data, []byte("REMOUNT_BROKER=")) {
		return failf("%s names no broker, so a harness could not discover the egress path after a move", envFilePath)
	}
	return nil
}

func checkBindLeakBlocked(ctx context.Context, s *Session) error {
	b := s.Target.Binding
	if b.Placeholder == "" || b.UnboundHost == "" {
		return Unavailablef("the target's binding fixture names no placeholder and unbound host to aim it at")
	}
	f, base, err := s.bindingFixture(ctx)
	if err != nil {
		return err
	}
	from, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	status, body, err := s.brokerGet(ctx, base, b.UnboundHost, "/leak", b.Placeholder)
	if err != nil {
		return failf("the broker did not answer a request carrying a misaimed placeholder: %v", err)
	}
	if status >= 200 && status < 400 {
		return failf("the broker answered %d for a placeholder aimed at %s, a host its binding does not cover; §9 rule 1 blocks the request", status, b.UnboundHost)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := s.Tail(ctx, from+1, f.WS.ID)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.Type == "egress.denied" && payloadContains(e.Payload, "leak_blocked") {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return failf("the broker refused the request (%d %s) but recorded no egress.denied with decision leak_blocked on %s's stream", status, strings.TrimSpace(string(body)), f.WS.ID)
}

func checkBindUnboundDestinationDenied(ctx context.Context, s *Session) error {
	f, base, err := s.bindingFixture(ctx)
	if err != nil {
		return err
	}
	from, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	// No placeholder at all: nothing authorizes this destination, so nothing
	// may leave.
	status, body, err := s.brokerGet(ctx, base, "denied.conformance.invalid", "/", "")
	if err != nil {
		return failf("the broker did not answer a request to an unauthorized destination: %v", err)
	}
	if status >= 200 && status < 400 {
		return failf("the broker answered %d for a destination no rule and no binding covers", status)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := s.Tail(ctx, from+1, f.WS.ID)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.Type == "egress.denied" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return failf("the broker refused the request (%d %s) but recorded no egress.denied on %s's stream; a decision that emits no event is unauditable", status, strings.TrimSpace(string(body)), f.WS.ID)
}

func checkBindPrivateAddressRefused(ctx context.Context, s *Session) error {
	_, base, err := s.bindingFixture(ctx)
	if err != nil {
		return err
	}
	for _, host := range []string{"127.0.0.1", "169.254.169.254", "10.0.0.1"} {
		status, _, err := s.brokerGet(ctx, base, host, "/", "")
		if err != nil {
			return failf("the broker did not answer a request to %s: %v", host, err)
		}
		if status >= 200 && status < 400 {
			return failf("the broker answered %d for %s; §9 rule 5 refuses loopback, private, link-local and multicast destinations unless explicitly allowed", status, host)
		}
	}
	return nil
}

func checkBindNoSecretInEvents(ctx context.Context, s *Session) error {
	secret := s.Target.Binding.Secret
	if secret == "" {
		return Unavailablef("the target's binding fixture names no secret, so there is nothing to search the log for")
	}
	f, base, err := s.bindingFixture(ctx)
	if err != nil {
		return err
	}
	from, err := s.headSequence(ctx)
	if err != nil {
		return err
	}
	// Produce audit records first, so the search has something to search.
	if _, _, err := s.brokerGet(ctx, base, s.Target.Binding.UnboundHost, "/leak", s.Target.Binding.Placeholder); err != nil {
		return err
	}
	if _, _, err := s.brokerGet(ctx, base, "denied.conformance.invalid", "/", ""); err != nil {
		return err
	}
	events, err := s.Tail(ctx, from+1, f.WS.ID)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return failf("two refused egress requests produced no events at all on %s's stream", f.WS.ID)
	}
	for _, e := range events {
		if payloadContains(e.Payload, secret) {
			return failf("event %d of type %s carries the binding's secret in its payload", e.Seq, e.Type)
		}
	}
	return nil
}

// payloadContains searches a CBOR payload for a literal substring. The
// payload is opaque to a black-box reader, so the search is over the raw
// bytes: a secret encoded anywhere inside is still a leak.
func payloadContains(payload []byte, needle string) bool {
	return bytes.Contains(payload, []byte(needle))
}

// ---- approval durability and request fingerprinting ----------------------

func checkAppUnknownApproval(ctx context.Context, s *Session) error {
	err := s.Call(ctx, "approval.get", ApprovalGetReq{ID: "ap_conformance_absent"}, nil)
	if err == nil {
		return failf("approval.get of an absent id succeeded")
	}
	if code := CodeOf(err); code != "not_found" {
		return failf("approval.get of an absent id answered %q, not %q (%v)", code, "not_found", err)
	}
	return nil
}

func checkAppListIsPendingOnly(ctx context.Context, s *Session) error {
	var res ApprovalListRes
	if err := s.Call(ctx, "approval.list", ApprovalListReq{}, &res); err != nil {
		return err
	}
	for _, a := range res.Approvals {
		if a.Status != "pending" {
			return failf("approval.list with no status returned %s in status %q; a caller polling for work would treat a settled row as outstanding", a.ID, a.Status)
		}
	}
	return nil
}

// The remaining approval requirements need an approve-mode egress rule, which
// is workspace policy the runner does not configure for the reference target.
// They are written so a target that declares the prerequisite is held to
// them.

func checkAppFingerprintIdempotent(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares an approve-mode egress rule, but the runner has no configured rule to drive a fingerprinted request through", s.Target.Name)
}

func checkAppSecondDecisionConflicts(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares an approve-mode egress rule, but no approval row exists to decide twice", s.Target.Name)
}

func checkAppDecisionSurvivesRestart(_ context.Context, s *Session) error {
	return Unavailablef("target %q declares a restartable control plane, but the runner does not restart a target it did not start", s.Target.Name)
}
