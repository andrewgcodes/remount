package sim

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
	"remount.dev/remount/internal/server"
)

// A provider-native GitHub delivery is committed as a canonical event before
// its matching durable timer resumes the workspace. Nonmatching deliveries
// neither wake the workspace nor consume its timer.
func TestSignedGitHubIssueCommentWakesMatchingSleepingWorkspace(t *testing.T) {
	const secret = "github-sim-secret"
	w := newWorldWith(t, func(o *server.Options) {
		o.WebhookProviders.GitHubSecret = secret
	})
	w.node("n1", nil)
	c := w.client("c1")
	ws := mustWS(t, c, proto.WorkspaceSpec{})
	ctx := ctxT(t, 60*time.Second)
	if _, err := c.SleepWorkspace(ctx, proto.WSSleepReq{
		ID: ws.ID, OnEvent: "webhook.github.issue_comment",
		Match: map[string]string{"repo": "acme/widgets", "label": "agent"},
	}); err != nil {
		t.Fatal(err)
	}

	post := func(body string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.http.URL+"/v1/events", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(body))
		req.Header.Set("X-GitHub-Event", "issue_comment")
		req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp
	}

	nonmatch := `{"action":"created","repository":{"full_name":"acme/other"},"issue":{"labels":[{"name":"agent"}]},"comment":{"body":"wake"}}`
	if resp := post(nonmatch); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("nonmatching webhook status = %d", resp.StatusCode)
	}
	if current, err := c.GetWorkspace(context.Background(), ws.ID); err != nil || current.State != proto.WSPaused {
		t.Fatalf("nonmatching webhook woke workspace: %+v, %v", current, err)
	}

	matching := `{"action":"created","repository":{"full_name":"acme/widgets"},"issue":{"labels":[{"name":"agent"}]},"comment":{"body":"wake"}}`
	if resp := post(matching); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("matching webhook status = %d", resp.StatusCode)
	}
	if _, err := c.WaitClaimed(ctx, ws.ID); err != nil {
		t.Fatal(err)
	}
	events, err := w.srv.Log.Read(ctx, 1, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	var webhookSeq, resumedSeq uint64
	for _, event := range events {
		if event.Type == "webhook.github.issue_comment" && event.Seq > webhookSeq {
			webhookSeq = event.Seq
		}
		if event.Type == proto.EvWSResumed {
			resumedSeq = event.Seq
		}
	}
	if webhookSeq == 0 || resumedSeq == 0 || webhookSeq >= resumedSeq {
		t.Fatalf("commit order webhook=%d resumed=%d", webhookSeq, resumedSeq)
	}
}
