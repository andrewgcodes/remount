package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	example "remount.dev/remount/examples/temporal"
)

func main() {
	c, err := dialTemporal()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	w := worker.New(c, example.TaskQueue, worker.Options{})
	w.RegisterWorkflow(example.Workflow)
	w.RegisterActivityWithOptions((&example.Activities{Logf: log.Printf}).RunStep, activity.RegisterOptions{Name: "RunStep"})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}

func dialTemporal() (client.Client, error) {
	options := client.Options{HostPort: os.Getenv("TEMPORAL_ADDRESS"), Namespace: os.Getenv("TEMPORAL_NAMESPACE")}
	if key := temporalAPIKey(); key != "" {
		options.Credentials = client.NewAPIKeyStaticCredentials(key)
	}
	return client.Dial(options)
}

func temporalAPIKey() string {
	for _, name := range []string{"TEMPORAL_API_KEY", "LAKESIDE_TEMPORAL_API_KEY", "NEW_LAKESIDE_TEMPORAL_API_KEY"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
