/*
	Unique error type definitions for the drone mesos scheduler
*/

package mesos_engine

import "fmt"

// This is the base error type for the scheduler
type SchedulerError struct {
	message string
}

func (this *SchedulerError) Error() string {
	return fmt.Sprintf("Mesos Scheduler Error: %s", this.message)
}

// Error raised when a task ID is re-used before being finished
type SchedulerErrorDuplicateTaskId struct {
	SchedulerError
}
