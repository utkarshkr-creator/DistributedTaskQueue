// Package metrics defines all Prometheus collectors for the distributed
// queue. Collectors are created once via promauto (which auto-registers
// them against the default registry) and exposed as package-level vars so
// both the redis store and the worker/producer/janitor code can record
// against them without passing a registry around.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// --- Gauges: point-in-time snapshots of Redis-backed queue state.
	// These are NOT incremented inline — they're set periodically by a
	// collector goroutine that polls LLen/HLen/ZCard, since "queue depth"
	// is a fact about Redis state, not a discrete event in Go code.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_depth",
		Help: "Number of tasks currently waiting in the main queue, by queue name.",
	}, []string{"queue"})

	InflightDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_inflight_depth",
		Help: "Number of tasks currently claimed by a worker but not yet acknowledged, by queue name.",
	}, []string{"queue"})

	DelayedDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_delayed_depth",
		Help: "Number of tasks waiting in the delayed-retry set, by queue name.",
	}, []string{"queue"})

	DLQDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_dlq_depth",
		Help: "Number of tasks currently sitting in the dead letter queue, by queue name.",
	}, []string{"queue"})

	// --- Counters: discrete events, incremented exactly once where they occur.
	TasksEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_enqueued_total",
		Help: "Total tasks pushed onto the main queue.",
	}, []string{"queue"})

	TasksConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_consumed_total",
		Help: "Total tasks successfully claimed by a worker (moved into inflight).",
	}, []string{"queue"})

	TasksAcknowledged = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_acknowledged_total",
		Help: "Total tasks successfully processed and acknowledged.",
	}, []string{"queue"})

	TasksRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_retried_total",
		Help: "Total tasks scheduled for retry with backoff after a processing failure.",
	}, []string{"queue"})

	TasksDeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_dead_lettered_total",
		Help: "Total tasks moved to the dead letter queue after exceeding max retries.",
	}, []string{"queue"})

	TasksSwept = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_swept_total",
		Help: "Total tasks reclaimed by the janitor after sitting inflight longer than holdTTL (likely worker crash).",
	}, []string{"queue"})

	// StoreErrors covers Redis-operation failures, labeled by which store
	// method raised them so a dashboard can show e.g. "ConsumeTask errors
	// spiking" without needing to grep logs.
	StoreErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_store_errors_total",
		Help: "Total errors from RedisStore operations, labeled by operation.",
	}, []string{"queue", "operation"})

	// ProcessingFailures counts task-processing failures (distinct from
	// store/Redis errors above) — i.e. the task was claimed fine but the
	// actual work raised an error.
	ProcessingFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_processing_failures_total",
		Help: "Total task processing failures (work itself failed, not a Redis error).",
	}, []string{"queue"})

	// --- Histogram: processing duration, recorded by the worker around
	// whatever does the real work.
	ProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "queue_processing_duration_seconds",
		Help:    "Time spent processing a single task, from claim to processing completion (success or failure).",
		Buckets: prometheus.DefBuckets, // 5ms .. 10s; adjust if real task durations differ a lot
	}, []string{"queue"})

	// --- PostgreSQL / job-repository metrics. Same shape as the Redis store
	// metrics above: errors labeled by operation, discrete events as counters.

	// JobRepoErrors covers JobRepository method failures (DB connection
	// issues, query errors), labeled by which method raised them.
	JobRepoErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_job_repo_errors_total",
		Help: "Total errors from JobRepository (PostgreSQL) operations, labeled by operation.",
	}, []string{"queue", "operation"})

	// JobsDuplicateSkipped counts tasks dropped because TryStartJob found
	// an existing COMPLETED row — i.e. idempotency actually doing its job.
	JobsDuplicateSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_jobs_duplicate_skipped_total",
		Help: "Total tasks skipped because the job was already marked COMPLETED in PostgreSQL.",
	}, []string{"queue"})

	// JobsResumed counts tasks where TryStartJob found a pre-existing
	// non-completed row (RUNNING/PENDING) rather than creating a fresh one —
	// i.e. recovery of a job abandoned by a crashed worker.
	JobsResumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_jobs_resumed_total",
		Help: "Total tasks resumed from a pre-existing non-completed row rather than freshly claimed.",
	}, []string{"queue"})

	// JobsCompleted / JobsFailed mirror Redis's TasksAcknowledged /
	// TasksDeadLettered but reflect the PostgreSQL row's terminal state,
	// which is the durable source of truth distinct from Redis's ephemeral
	// queue bookkeeping.
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_jobs_completed_total",
		Help: "Total jobs marked COMPLETED in PostgreSQL.",
	}, []string{"queue"})

	JobsFailedTerminal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_jobs_failed_terminal_total",
		Help: "Total jobs marked FAILED in PostgreSQL after exhausting max_retries.",
	}, []string{"queue"})

	// --- DAG orchestration metrics

	// ChildrenEnqueued counts how many child tasks were triggered when a
	// parent completed. This shows DAG fan-out activity.
	ChildrenEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_children_enqueued_total",
		Help: "Total child tasks enqueued after parent completion (DAG orchestration).",
	}, []string{"queue"})

	// ChildEnqueueFailures counts failures to enqueue ready children. This
	// is a critical failure path — parent is marked done but children never run.
	ChildEnqueueFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_child_enqueue_failures_total",
		Help: "Total failures enqueueing child tasks after parent completion.",
	}, []string{"queue"})

	// --- Data consistency metrics

	// TasksOrphaned counts tasks found in Redis with no DB row. This
	// indicates a bug or data inconsistency and results in DLQ.
	TasksOrphaned = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_orphaned_total",
		Help: "Total tasks found in Redis without corresponding DB row (data inconsistency).",
	}, []string{"queue"})

	// WorkflowsCreated tracks workflow creation for capacity planning.
	WorkflowsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "queue_workflows_created_total",
		Help: "Total workflows created.",
	})

	// TasksCancelledCascade counts tasks cancelled due to parent failure.
	// High values indicate many blocked DAG branches.
	TasksCancelledCascade = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_tasks_cancelled_cascade_total",
		Help: "Total tasks cancelled due to parent task failure (cascading cancellation).",
	}, []string{"queue"})
)
