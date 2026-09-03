package temporalexample

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is shared by the example worker and starter.
const TaskQueue = "remount-step"

// Workflow runs one Remount-backed Activity with retry and heartbeat timeouts.
func Workflow(ctx workflow.Context, input StepInput) (StepResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
		},
	})
	var result StepResult
	err := workflow.ExecuteActivity(ctx, "RunStep", input).Get(ctx, &result)
	return result, err
}
