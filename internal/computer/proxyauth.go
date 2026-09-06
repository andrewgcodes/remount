package computer

import (
	"context"
	"encoding/json"
)

// ProxyCredentials are what the node answers a proxy authentication challenge
// with on the browser's behalf.
//
// The value is the workspace's broker capability, which authorizes every
// egress that workspace has. It is a real secret: it is written into exactly
// one CDP call, Fetch.continueWithAuth, and never into a log line, an event,
// an error, a computer.get response or a computer.eval result. See ADR 0095.
type ProxyCredentials struct {
	Username string
	Password string
}

// Set reports whether a credential was configured at all. An unset credential
// leaves request interception off, so a browser with no broker in its path
// pays nothing for this.
func (p ProxyCredentials) Set() bool { return p.Username != "" || p.Password != "" }

// authSourceProxy is the CDP challenge source that names the proxy itself.
// Anything else is an origin asking the *user* for a password.
const authSourceProxy = "Proxy"

// interceptPatterns are the URLs whose requests are paused. Only the schemes
// that can traverse a forward proxy are listed: a file://, data: or blob: load
// never meets the broker, so pausing one would buy a round trip and nothing
// else.
var interceptPatterns = []any{
	map[string]any{"urlPattern": "http://*"},
	map[string]any{"urlPattern": "https://*"},
}

// challengeTableCap bounds the table of requests that have already been given
// the credential. An entry is added per challenged request and there is no
// event that reliably retires one, so the table is dropped whole at the cap
// rather than grown for the life of the browser. The only consequence of a
// drop is that a request challenged again much later may be answered again,
// which is the behaviour a fresh conversation would have had anyway.
const challengeTableCap = 1024

// fetchEnableParams builds the Fetch.enable arguments. It is one function so
// the setup call and the call that arms a newly attached target cannot drift.
func fetchEnableParams() map[string]any {
	return map[string]any{
		"handleAuthRequests": true,
		"patterns":           interceptPatterns,
	}
}

func autoAttachParams() map[string]any {
	// waitForDebuggerOnStart pauses a new target until we have armed it. Its
	// first request is the one most likely to be challenged, so arming after
	// it had already started would be a race the page loses silently.
	return map[string]any{
		"autoAttach": true, "waitForDebuggerOnStart": true, "flatten": true,
	}
}

// armSession makes one CDP session answer proxy authentication, pass every
// other paused request through, and hand the same treatment to every target it
// spawns. It is the blocking form, used during setup where a failure must fail
// computer.create rather than be discovered later.
func (c *Client) armSession(ctx context.Context, session string) error {
	if err := c.conn.call(ctx, session, "Target.setAutoAttach", autoAttachParams(), nil); err != nil {
		return err
	}
	return c.conn.call(ctx, session, "Fetch.enable", fetchEnableParams(), nil)
}

// dispatch issues one fire-and-forget CDP call from the read loop's event
// handler. Nothing is logged on failure: the parameters can carry the
// capability, and a socket that will not take a write is already being
// reported once by watchClose.
func (c *Client) dispatch(session, method string, params map[string]any) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), c.opts.CallTimeout)
	defer cancel()
	if len(params) == 0 {
		// A nil map would marshal to `null`, which is not the same wire shape
		// as a call that carries no parameters at all.
		_ = c.conn.send(ctx, session, method, nil)
		return
	}
	_ = c.conn.send(ctx, session, method, params)
}

// answerAuthChallenge answers one authentication challenge.
//
// A proxy challenge is the workspace's own broker asking the browser to
// identify itself, and the node is holding the answer. Anything else is a site
// asking a *user* for a password. The broker capability is not that, and
// handing it to an origin would post the workspace's entire egress authority
// to a third party, so an origin challenge is cancelled — which is what a
// headless browser with no user in front of it does anyway.
func (c *Client) answerAuthChallenge(m message) {
	var p struct {
		RequestID     string `json:"requestId"`
		AuthChallenge struct {
			Source string `json:"source"`
		} `json:"authChallenge"`
	}
	if json.Unmarshal(m.Params, &p) != nil || p.RequestID == "" {
		return
	}
	answer := map[string]any{"response": "CancelAuth"}
	if p.AuthChallenge.Source == authSourceProxy && c.claimChallenge(p.RequestID) {
		answer = map[string]any{
			"response": "ProvideCredentials",
			"username": c.opts.ProxyAuth.Username,
			"password": c.opts.ProxyAuth.Password,
		}
	}
	c.dispatch(m.SessionID, "Fetch.continueWithAuth", map[string]any{
		"requestId": p.RequestID, "authChallengeResponse": answer,
	})
}

// claimChallenge records that this request has been given the credential and
// reports whether it is the first time.
//
// A proxy that refuses the capability challenges the same request again.
// Answering a second time would retry forever against a broker that will never
// take it, so the second answer cancels and the failure reaches the page as
// the browser's own proxy error.
func (c *Client) claimChallenge(requestID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.challenged == nil {
		c.challenged = map[string]bool{}
	}
	if c.challenged[requestID] {
		return false
	}
	if len(c.challenged) >= challengeTableCap {
		c.challenged = map[string]bool{}
	}
	c.challenged[requestID] = true
	return true
}

// continuePaused releases a paused request exactly as it arrived. The
// interception exists to answer a challenge, not to rewrite traffic: naming a
// url, method, header or body here would change a request nobody asked to
// change.
func (c *Client) continuePaused(m message) {
	var p struct {
		RequestID string `json:"requestId"`
	}
	if json.Unmarshal(m.Params, &p) != nil || p.RequestID == "" {
		return
	}
	c.dispatch(m.SessionID, "Fetch.continueRequest", map[string]any{"requestId": p.RequestID})
}

// armAttachedTarget gives a target that attached after setup the same
// interception the page has. An out-of-process iframe is its own target and
// its requests meet the same proxy challenge the page's do.
//
// Runtime.runIfWaitingForDebugger is sent last and unconditionally: the target
// is paused waiting for it, and a target left paused is a frame that never
// loads. Liveness fails open here even if arming it did not.
func (c *Client) armAttachedTarget(m message) {
	var p struct {
		SessionID string `json:"sessionId"`
	}
	if json.Unmarshal(m.Params, &p) != nil || p.SessionID == "" {
		return
	}
	c.dispatch(p.SessionID, "Target.setAutoAttach", autoAttachParams())
	c.dispatch(p.SessionID, "Fetch.enable", fetchEnableParams())
	c.dispatch(p.SessionID, "Runtime.runIfWaitingForDebugger", nil)
}
