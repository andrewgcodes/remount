package publicconsumer

import (
	"context"
	"errors"
	"testing"

	"remount.dev/remount/api"
	"remount.dev/remount/client"
)

// This package intentionally lives in a separate module. Merely compiling it
// proves the supported SDK does not require consumers to import internal
// protocol or transport packages.
func TestPublicSurfaceCompilesForExternalModule(t *testing.T) {
	c, err := client.New(client.Options{Server: "http://127.0.0.1:7443", Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	spec := api.WorkspaceSpec{
		Name: "external-consumer",
		Security: api.SecuritySpec{
			Profile: api.SecurityLocal,
		},
	}
	_ = spec
	_ = client.WithIdempotencyKey("logical-operation-1")
	_ = api.IsErrorCode(&api.Error{Code: api.CodeConflict}, api.CodeConflict)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.CreateWorkspace(ctx, spec); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call = %v", err)
	}
}
