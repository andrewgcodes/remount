package e2bfake

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The fake is the foundation every keyless provider proof stands on. A fake
// that is itself wrong does not fail loudly; it silently agrees with whatever
// the driver does, and the suite above it reports confidence it has not
// earned. These tests exercise the fake directly, without the driver, so its
// own behavior is pinned rather than inferred.

func post(t *testing.T, s *Service, key string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, s.URL()+"/sandboxes", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-API-Key", key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func listPage(t *testing.T, s *Service, key string, filters url.Values, token string) ([]map[string]any, string) {
	t.Helper()
	query := url.Values{"metadata": {filters.Encode()}, "limit": {"100"}}
	if token != "" {
		query.Set("nextToken", token)
	}
	request, err := http.NewRequest(http.MethodGet, s.URL()+"/v2/sandboxes?"+query.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-API-Key", key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("list = %d %s", response.StatusCode, body)
	}
	var page []map[string]any
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	return page, response.Header.Get("X-Next-Token")
}

func create(t *testing.T, s *Service, key, name string) {
	t.Helper()
	response := post(t, s, key, map[string]any{
		"templateID": "remount-node",
		"metadata":   map[string]string{"remount_managed": "true", "remount_name": name, "remount_tenant": "tenant-a"},
	})
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create %s = %d", name, response.StatusCode)
	}
}

func TestPaginationCoversEveryResultExactlyOnce(t *testing.T) {
	s := New(Options{APIKey: "k", PageSize: 3})
	defer s.Close()
	for i := 0; i < 10; i++ {
		create(t, s, "k", "node-"+strconv.Itoa(i))
	}
	filters := url.Values{"remount_managed": {"true"}}
	seen := map[string]int{}
	token := ""
	for pages := 0; pages < 20; pages++ {
		page, next := listPage(t, s, "k", filters, token)
		if next != "" && len(page) != 3 {
			t.Fatalf("a non-final page returned %d results, want the page size 3", len(page))
		}
		for _, item := range page {
			seen[item["sandboxID"].(string)]++
		}
		if next == "" {
			if len(seen) != 10 {
				t.Fatalf("pagination surfaced %d of 10 sandboxes", len(seen))
			}
			for id, count := range seen {
				if count != 1 {
					t.Fatalf("sandbox %s appeared %d times across pages", id, count)
				}
			}
			return
		}
		token = next
	}
	t.Fatal("pagination never terminated")
}

func TestFilterRequiresEveryKeyToMatch(t *testing.T) {
	s := New(Options{APIKey: "k"})
	defer s.Close()
	response := post(t, s, "k", map[string]any{"metadata": map[string]string{
		"remount_managed": "true", "remount_name": "a", "remount_tenant": "tenant-a", "remount_pool": "iad",
	}})
	response.Body.Close()

	matching, _ := listPage(t, s, "k", url.Values{"remount_managed": {"true"}, "remount_tenant": {"tenant-a"}}, "")
	if len(matching) != 1 {
		t.Fatalf("an exactly matching filter returned %d results", len(matching))
	}
	// One wrong value must exclude the object. A filter that ORs its terms
	// would let a tenant see another tenant's fleet.
	wrong, _ := listPage(t, s, "k", url.Values{"remount_managed": {"true"}, "remount_tenant": {"tenant-b"}}, "")
	if len(wrong) != 0 {
		t.Fatalf("a non-matching tenant filter returned %d results", len(wrong))
	}
	// A key absent from the object cannot match either.
	absent, _ := listPage(t, s, "k", url.Values{"remount_absent_key": {"x"}}, "")
	if len(absent) != 0 {
		t.Fatalf("a filter on an absent key returned %d results", len(absent))
	}
}

func TestMissingOrWrongAPIKeyIsRefused(t *testing.T) {
	s := New(Options{APIKey: "correct"})
	defer s.Close()
	response := post(t, s, "wrong", map[string]any{"metadata": map[string]string{}})
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong key = %d, want 401", response.StatusCode)
	}
	if live := s.Live(); len(live) != 0 {
		t.Fatalf("an unauthorized create still produced %v", live)
	}
}

func TestUnexpectedRouteIsLoud(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	response, err := http.Get(s.URL() + "/v1/sandboxes")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotImplemented {
		t.Fatalf("unexpected route = %d, want 501 so a driver cannot learn a habit the real API refuses", response.StatusCode)
	}
}

func TestDeletionLagCountsDownAndThenTheSandboxIsGone(t *testing.T) {
	s := New(Options{APIKey: "k", DeletionLagLists: 2})
	defer s.Close()
	create(t, s, "k", "doomed")
	id := s.Live()[0]

	request, err := http.NewRequest(http.MethodDelete, s.URL()+"/sandboxes/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-API-Key", "k")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("destroy = %d", response.StatusCode)
	}
	if live := s.Live(); len(live) != 0 {
		t.Fatalf("ground truth still holds %v after destroy", live)
	}

	filters := url.Values{"remount_managed": {"true"}}
	for i := 1; i <= 2; i++ {
		page, _ := listPage(t, s, "k", filters, "")
		if len(page) != 1 {
			t.Fatalf("list %d during the deletion lag returned %d, want 1", i, len(page))
		}
	}
	page, _ := listPage(t, s, "k", filters, "")
	if len(page) != 0 {
		t.Fatalf("the sandbox never left inventory after its lag elapsed: %d remain", len(page))
	}

	// A repeated destroy is 404, which the driver treats as already gone.
	request2, _ := http.NewRequest(http.MethodDelete, s.URL()+"/sandboxes/"+id, nil)
	request2.Header.Set("X-API-Key", "k")
	repeat, err := http.DefaultClient.Do(request2)
	if err != nil {
		t.Fatal(err)
	}
	repeat.Body.Close()
	if repeat.StatusCode != http.StatusNotFound {
		t.Fatalf("repeated destroy = %d, want 404", repeat.StatusCode)
	}
}

func TestMaxLifetimeReclaimsWithoutADelete(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	s := New(Options{APIKey: "k", MaxLifetime: time.Hour, Now: func() time.Time { return now }})
	defer s.Close()
	create(t, s, "k", "short-lived")
	filters := url.Values{"remount_managed": {"true"}}
	if page, _ := listPage(t, s, "k", filters, ""); len(page) != 1 {
		t.Fatalf("before expiry the fake listed %d", len(page))
	}
	now = now.Add(time.Hour + time.Second)
	if page, _ := listPage(t, s, "k", filters, ""); len(page) != 0 {
		t.Fatalf("a reclaimed sandbox is still listed: %d", len(page))
	}
	if live := s.Live(); len(live) != 0 {
		t.Fatalf("ground truth still holds %v after reclamation", live)
	}
}

func TestLostCreateResponseStillCreatesTheSandbox(t *testing.T) {
	s := New(Options{APIKey: "k", LoseCreateResponses: 1})
	defer s.Close()
	response := post(t, s, "k", map[string]any{"metadata": map[string]string{
		"remount_managed": "true", "remount_name": "ambiguous", "remount_tenant": "tenant-a",
	}})
	response.Body.Close()
	if response.StatusCode < 500 {
		t.Fatalf("a lost create response returned %d, want a server error", response.StatusCode)
	}
	// The whole point: the effect happened even though the caller was told it
	// did not. Without this the ambiguity being tested does not exist.
	if live := s.Live(); len(live) != 1 {
		t.Fatalf("a lost create response left %d sandboxes, want 1", len(live))
	}
	// The next create returns normally again.
	create(t, s, "k", "ordinary")
	if live := s.Live(); len(live) != 2 {
		t.Fatalf("after the lost response the fake holds %d, want 2", len(live))
	}
}

func TestContractsAreValidAndNameTheFieldsTheDriverReads(t *testing.T) {
	item := ListItemContract()
	for _, field := range []string{"sandboxID", "startedAt", "state", "metadata", "_ignorable"} {
		if _, ok := item[field]; !ok {
			t.Fatalf("contract/list_item.json omits %q", field)
		}
	}
	response := CreateResponseContract()
	for _, field := range []string{"sandboxID", "clientID", "envdAccessToken"} {
		if _, ok := response[field]; !ok {
			t.Fatalf("contract/create_response.json omits %q", field)
		}
	}
	for _, absent := range []string{"metadata", "startedAt", "state"} {
		if _, present := response[absent]; present {
			t.Fatalf("contract/create_response.json carries %q, which the real service does not return", absent)
		}
	}
	request := CreateRequestContract()
	for _, field := range []string{"templateID", "timeout", "secure", "metadata"} {
		if _, ok := request[field]; !ok {
			t.Fatalf("contract/create_request.json omits %q", field)
		}
	}
	// Bootstrap values never ride the create body: E2B resumes sandboxes from
	// a post-start snapshot, so creation-time envVars would not reach the
	// template's start command anyway.
	if _, present := request["envVars"]; present {
		t.Fatal("contract/create_request.json carries envVars; the bootstrap goes through the envd file route")
	}
}

// TestCreateResponseIsAsSparseAsTheRealService is the fake guarding itself. The
// value of a fake is that it refuses what production refuses; if this drifts,
// every provider test above it quietly starts proving less than it claims.
func TestCreateResponseIsAsSparseAsTheRealService(t *testing.T) {
	s := New(Options{APIKey: "k"})
	defer s.Close()
	response := post(t, s, "k", map[string]any{"metadata": map[string]string{
		"remount_managed": "true", "remount_name": "sparse", "remount_tenant": "tenant-a",
	}})
	defer response.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["sandboxID"] == "" || body["sandboxID"] == nil {
		t.Fatal("create response has no sandboxID")
	}
	for _, absent := range []string{"metadata", "startedAt", "state"} {
		if _, present := body[absent]; present {
			t.Fatalf("the fake volunteered %q on create; the real service does not, so a driver could depend on it and fail only in production", absent)
		}
	}
}
