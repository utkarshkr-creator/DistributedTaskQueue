package models

import (
	"time"
)

// Workflow represents a row in the 'workflows' table
type Workflow struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"` // PENDING, RUNNING, COMPLETED, FAILED
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ProcessedJob represents a row in the 'processed_jobs' table
type ProcessedJob struct {
	ID             string     `json:"id"`
	WorkflowID     string     `json:"workflow_id"`
	TaskType       string     `json:"task_type"`
	Status         string     `json:"status"`
	MaxRetries     int        `json:"max_retries"`
	CurrentRetries int        `json:"current_retries"`
	TaskDetails    []byte     `json:"task_details"`
	ErrorLog       *string    `json:"error_log"`
	ProcessedAt    *time.Time `json:"processed_at"`
	CreatedAt      time.Time  `json:"created_at"`
}

// JobDependency represents a row in the 'job_dependencies' table
type JobDependency struct {
	ChildJobID  string `json:"child_job_id"`
	ParentJobID string `json:"parent_job_id"`
}
