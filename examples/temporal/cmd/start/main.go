package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"

	example "remount.dev/remount/examples/temporal"
)

func main() {
	workflowID := flag.String("workflow", "", "stable Temporal workflow id (required)")
	workspace := flag.String("workspace", "", "existing Remount workspace id (required)")
	cwd := flag.String("cwd", "", "command working directory inside the workspace")
	flag.Parse()
	if *workflowID == "" || *workspace == "" || flag.NArg() == 0 {
		log.Fatal("usage: start -workflow ID -workspace WS [-cwd PATH] PROGRAM [ARG...]")
	}

	c, err := dialTemporal()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	handle, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID: *workflowID, TaskQueue: example.TaskQueue,
	}, example.Workflow, example.StepInput{Workspace: *workspace, Program: flag.Args(), Cwd: *cwd})
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()
	var result example.StepResult
	if err := handle.Get(ctx, &result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("session=%s next=%d exit=%d signal=%s error=%s reason=%s\n",
		result.Session, result.Next, result.Exit, result.Signal, result.Error, result.Reason)
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
