package e2b

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/e2b/e2bfake"
)

func contractRequest(name string) provision.Request {
	return provision.Request{
		Name: name, Tenant: "tenant-a",
		Labels: map[string]string{provision.PoolLabel: "iad", "region": "iad"},
		Bootstrap: provision.Bootstrap{
			ServerURL: "https://control.example", BinaryURL: "https://control.example/remount",
			EnrollmentToken: "enroll-once", Backend: "process", DataDir: "/var/lib/remount",
		},
	}
}

func contractDriver(t *testing.T, opts e2bfake.Options) (*Driver, *e2bfake.Service) {
	t.Helper()
	if opts.APIKey == "" {
		opts.APIKey = "fake-key"
	}
	service := e2bfake.New(opts)
	t.Cleanup(service.Close)
	driver, err := New(Config{Endpoint: service.URL(), EnvdEndpoint: service.URL(), APIKey: opts.APIKey, Template: "remount-node"})
	if err != nil {
		t.Fatal(err)
	}
	return driver, service
}

func listOptions() provision.ListOptions {
	return provision.ListOptions{Tenant: "tenant-a", Pool: "iad"}
}

// TestB8ContractCoversCreatePaginatedListFilterAndIdempotentDestroy is Plan B's
// B8. It drives the real driver against the checked-in contract rather than an
// inline handler, so a provider field rename fails in one place.
func TestB8ContractCoversCreatePaginatedListFilterAndIdempotentDestroy(t *testing.T) {
	// A page size below the number of sandboxes forces the driver to follow
	// X-Next-Token rather than silently reporting one page as the whole fleet.
	driver, service := contractDriver(t, e2bfake.Options{PageSize: 2})
	ctx := context.Background()

	var ids []string
	for _, name := range []string{"node-1", "node-2", "node-3", "node-4", "node-5"} {
		machine, err := driver.Create(ctx, contractRequest(name))
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if machine.ID == "" || machine.Name != name || machine.Tenant != "tenant-a" {
			t.Fatalf("create %s returned %#v", name, machine)
		}
		ids = append(ids, machine.ID)
	}

	// The create body must match the contract's field names exactly.
	contract := e2bfake.CreateRequestContract()
	bodies := service.CreateBodies()
	if len(bodies) == 0 {
		t.Fatal("no create body was recorded")
	}
	for field := range contract {
		if field == "_comment" {
			continue
		}
		if _, ok := bodies[0][field]; !ok {
			t.Fatalf("create request omits contract field %q; the driver and contract/create_request.json have drifted", field)
		}
	}
	// The bootstrap, enrollment token included, reaches the sandbox through
	// the envd file route and only that route.
	for _, id := range ids {
		files := service.Files(id)
		content, ok := files[DefaultBootstrapPath]
		if !ok || !strings.Contains(content, "REMOUNT_ENROLL_TOKEN='") {
			t.Fatalf("sandbox %s received bootstrap files %v", id, files)
		}
	}
	for _, body := range bodies {
		encoded, _ := json.Marshal(body)
		if strings.Contains(string(encoded), "REMOUNT_ENROLL_TOKEN") {
			t.Fatalf("enrollment token in create body: %s", encoded)
		}
	}

	machines, err := driver.List(ctx, listOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 5 {
		t.Fatalf("paginated inventory returned %d sandboxes, want 5; pagination was not followed", len(machines))
	}

	// A tenant filter is a boundary, not a hint.
	if other, err := driver.List(ctx, provision.ListOptions{Tenant: "tenant-b", Pool: "iad"}); err != nil {
		t.Fatal(err)
	} else if len(other) != 0 {
		t.Fatalf("tenant-b saw %d of tenant-a's sandboxes", len(other))
	}

	// Destroy is idempotent: the second call must also succeed, because a
	// retry after a dropped response is the ordinary case.
	if err := driver.Destroy(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := driver.Destroy(ctx, ids[0]); err != nil {
		t.Fatalf("repeated destroy must be idempotent: %v", err)
	}
	remaining, err := driver.List(ctx, listOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 4 {
		t.Fatalf("after one destroy the inventory has %d sandboxes, want 4", len(remaining))
	}
}

// TestB9AmbiguousCreateIsRecoveredByNameWithoutDuplicates is Plan B's B9. The
// provider accepted the sandbox and then lost the response, so the caller
// cannot tell "created" from "nothing happened". Retrying blindly would leak a
// second machine that nobody tracks and nobody reaps. The driver resolves the
// ambiguity inside the same call by looking the sandbox up under its unique
// name, so the caller never sees the ambiguity at all.
func TestB9AmbiguousCreateIsRecoveredByNameWithoutDuplicates(t *testing.T) {
	driver, service := contractDriver(t, e2bfake.Options{LoseCreateResponses: 1})
	ctx := context.Background()
	request := contractRequest("ambiguous-node")

	machine, err := driver.Create(ctx, request)
	if err != nil {
		t.Fatalf("an ambiguous create must be resolved by name, not surfaced: %v", err)
	}
	if machine.Name != request.Name || machine.ID == "" {
		t.Fatalf("recovered machine = %#v", machine)
	}
	live := service.Live()
	if len(live) != 1 {
		t.Fatalf("provider holds %d sandboxes after the ambiguous create, want 1", len(live))
	}
	if machine.ID != live[0] {
		t.Fatalf("recovered id %q is not the sandbox that exists (%q)", machine.ID, live[0])
	}

	// Repeating the request must adopt the same sandbox rather than add one.
	again, err := driver.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != machine.ID {
		t.Fatalf("repeat create returned %q, want the existing %q", again.ID, machine.ID)
	}
	if live := service.Live(); len(live) != 1 {
		t.Fatalf("repeat create leaked a duplicate: provider holds %d sandboxes, want 1", len(live))
	}
}

// TestAmbiguousCreateWithUnreadableInventoryStaysAmbiguous is the other half of
// B9. When the response is lost *and* inventory cannot be read, the outcome is
// genuinely unknown. The driver must say so: reporting success would strand a
// machine the caller believes it owns, and reporting a clean failure would
// strand one it believes does not exist.
func TestAmbiguousCreateWithUnreadableInventoryStaysAmbiguous(t *testing.T) {
	driver, service := contractDriver(t, e2bfake.Options{LoseCreateResponses: 1, FailListsAfter: 1})
	if _, err := driver.Create(context.Background(), contractRequest("unknowable")); err == nil {
		t.Fatal("an unresolvable ambiguous create reported success")
	}
	// The sandbox really does exist, which is exactly why the error matters:
	// a caller that treats this as "nothing happened" leaks it.
	if live := service.Live(); len(live) != 1 {
		t.Fatalf("provider holds %d sandboxes, want the 1 that was actually created", len(live))
	}
}

// TestInventoryLagDoesNotHideOrDuplicateASandbox covers the eventual-deletion
// case Plan B names. A destroyed sandbox that lingers in inventory must not be
// mistaken for a live one that needs reaping again, and it must eventually
// disappear rather than be trusted forever.
func TestInventoryLagDoesNotHideOrDuplicateASandbox(t *testing.T) {
	driver, service := contractDriver(t, e2bfake.Options{DeletionLagLists: 2})
	ctx := context.Background()
	machine, err := driver.Create(ctx, contractRequest("lagging-node"))
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Destroy(ctx, machine.ID); err != nil {
		t.Fatal(err)
	}
	if live := service.Live(); len(live) != 0 {
		t.Fatalf("provider still holds %v after destroy", live)
	}
	// Inventory lags: the sandbox is still listed while the provider catches
	// up. A caller polling for absence must keep polling, not conclude the
	// delete failed.
	seenWhileLagging := false
	for i := 0; i < 5; i++ {
		machines, listErr := driver.List(ctx, listOptions())
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(machines) == 0 {
			if !seenWhileLagging {
				t.Fatal("the fake never exercised the deletion lag")
			}
			return
		}
		seenWhileLagging = true
	}
	t.Fatal("a destroyed sandbox never left the inventory")
}

// TestProviderInventoryOutageIsAnErrorNotAnEmptyFleet is the failure Plan B
// calls "provider inventory temporarily unavailable". Reporting an outage as an
// empty inventory would tell the pool every node had vanished, and the pool
// would answer by provisioning a replacement fleet.
func TestProviderInventoryOutageIsAnErrorNotAnEmptyFleet(t *testing.T) {
	driver, _ := contractDriver(t, e2bfake.Options{FailNextLists: 64})
	if machines, err := driver.List(context.Background(), listOptions()); err == nil {
		t.Fatalf("inventory outage reported %d machines and no error", len(machines))
	}
}

// TestMaxLifetimeReplacementIsVisibleAsAbsence covers a provider reclaiming
// capacity underneath the pool. The sandbox is simply gone; nothing destroyed
// it locally, and the driver must report that honestly.
func TestMaxLifetimeReplacementIsVisibleAsAbsence(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	driver, _ := contractDriver(t, e2bfake.Options{
		MaxLifetime: time.Hour,
		Now:         func() time.Time { return now },
	})
	ctx := context.Background()
	if _, err := driver.Create(ctx, contractRequest("short-lived")); err != nil {
		t.Fatal(err)
	}
	if machines, err := driver.List(ctx, listOptions()); err != nil || len(machines) != 1 {
		t.Fatalf("before expiry: %d machines, err=%v", len(machines), err)
	}
	now = now.Add(2 * time.Hour)
	machines, err := driver.List(ctx, listOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 0 {
		t.Fatalf("a reclaimed sandbox is still reported live: %#v", machines)
	}
}

// TestDriverRefusesAnUnexpectedProviderRoute proves the fake is strict. A
// permissive fake would let the driver acquire habits the real API does not
// honor, so an unimplemented interaction must fail loudly here.
func TestDriverRefusesAnUnexpectedProviderRoute(t *testing.T) {
	service := e2bfake.New(e2bfake.Options{})
	defer service.Close()
	driver, err := New(Config{Endpoint: service.URL(), EnvdEndpoint: service.URL(), APIKey: "fake-key", Template: "remount-node"})
	if err != nil {
		t.Fatal(err)
	}
	// An id containing a path separator must be refused before it can reach
	// the provider as a different route.
	if err := driver.Destroy(context.Background(), "../sandboxes"); err == nil {
		t.Fatal("a traversal-shaped sandbox id was accepted")
	}
}

// TestContractShapesAreWhatTheDriverReads pins the decode contract against the
// shapes captured from the real service: the list item carries what the driver
// reads, the create response deliberately does not, and neither one's extra
// provider fields may change behavior.
func TestContractShapesAreWhatTheDriverReads(t *testing.T) {
	contract := e2bfake.ListItemContract()
	for _, field := range []string{"sandboxID", "startedAt", "state", "metadata"} {
		if _, ok := contract[field]; !ok {
			t.Fatalf("contract/sandbox.json omits %q, which the driver reads", field)
		}
	}
	ignorable, ok := contract["_ignorable"].(map[string]any)
	if !ok || len(ignorable) == 0 {
		t.Fatal("contract/sandbox.json declares no ignorable provider fields")
	}

	driver, _ := contractDriver(t, e2bfake.Options{})
	machine, err := driver.Create(context.Background(), contractRequest("shaped"))
	if err != nil {
		t.Fatal(err)
	}
	// The fake serves the ignorable fields on every object; the driver must
	// still produce a correct machine from the contractual ones alone.
	if machine.Provider != "e2b" || machine.State != "running" || machine.Labels["region"] != "iad" {
		t.Fatalf("ignorable provider fields changed the decoded machine: %#v", machine)
	}
}

// TestCreateResponseCarriesOnlyAnIdAndTheDriverSurvivesIt is the lesson from
// probing the real api.e2b.app: POST /sandboxes returns no metadata, no
// startedAt and no state. A fake that volunteered them would let the driver
// grow a dependency the real service never satisfies, and the failure would
// appear only against production.
func TestCreateResponseCarriesOnlyAnIdAndTheDriverSurvivesIt(t *testing.T) {
	response := e2bfake.CreateResponseContract()
	for _, absent := range []string{"metadata", "startedAt", "state"} {
		if _, present := response[absent]; present {
			t.Fatalf("contract/create_response.json declares %q, which the real service does not return", absent)
		}
	}
	if _, ok := response["sandboxID"]; !ok {
		t.Fatal("contract/create_response.json omits sandboxID, the only field the driver needs from a create")
	}

	// Everything the driver reports about a freshly created machine other than
	// its id therefore has to come from the request it already holds.
	driver, _ := contractDriver(t, e2bfake.Options{})
	request := contractRequest("sparse-create")
	machine, err := driver.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if machine.ID == "" {
		t.Fatal("create produced no id")
	}
	if machine.Name != request.Name || machine.Tenant != request.Tenant {
		t.Fatalf("create response shape leaked into identity: %#v", machine)
	}
	if machine.Labels[provision.PoolLabel] != "iad" || machine.Labels["region"] != "iad" {
		t.Fatalf("labels must come from the request, not the sparse create response: %#v", machine.Labels)
	}
}

// TestListItemContractMatchesWhatWasCapturedLive keeps the fixture honest about
// its provenance: these field names came from a real sandbox on 2026-09-03, and
// the fake must serve every one of them.
func TestListItemContractMatchesWhatWasCapturedLive(t *testing.T) {
	item := e2bfake.ListItemContract()
	reads, ok := item["_driver_reads"].([]any)
	if !ok || len(reads) == 0 {
		t.Fatal("contract/list_item.json does not declare which fields the driver reads")
	}
	for _, field := range reads {
		name, _ := field.(string)
		if _, present := item[name]; !present {
			t.Fatalf("contract declares the driver reads %q but does not carry it", name)
		}
	}
	ignorable, ok := item["_ignorable"].(map[string]any)
	if !ok {
		t.Fatal("contract/list_item.json declares no ignorable provider fields")
	}
	for _, field := range []string{"alias", "clientID", "cpuCount", "endAt", "envdVersion", "memoryMB", "templateID", "volumeMounts"} {
		if _, present := ignorable[field]; !present {
			t.Fatalf("the live capture included %q; the contract must list it as ignorable", field)
		}
	}
}
