package temporalexample

import (
	"errors"
	"os"

	remountclient "remount.dev/remount/client"
)

// NewEnvironmentClient creates the public Remount SDK client used by the
// Activity. The token remains in process environment and is never serialized
// into Temporal inputs, results, or heartbeat details.
func NewEnvironmentClient() (Remount, error) {
	server := os.Getenv("REMOUNT_SERVER")
	if server == "" {
		return nil, errors.New("REMOUNT_SERVER is required")
	}
	client, err := remountclient.New(remountclient.Options{
		Server: server,
		Token:  os.Getenv("REMOUNT_TOKEN"),
	})
	if err != nil {
		return nil, err
	}
	return sdkClient{client}, nil
}
