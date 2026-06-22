# Metrics Guide - How to Add Observability

## 🎯 Core Concepts

### **3 Types of Metrics**

1. **Counter** - Always increases (events, errors, requests)
2. **Gauge** - Current value that goes up/down (queue depth, active workers)
3. **Histogram** - Distribution of values (latency, duration)

---

## 📝 Step-by-Step: Adding a New Metric

### **Step 1: Define the Metric**

Location: `pkg/redis/metrics/metrics.go`

#### **Counter** (for events that happen)
```go
TasksOrphaned = promauto.NewCounterVec(prometheus.CounterOpts{
    Name: "queue_tasks_orphaned_total",  // Must end with _total
    Help: "Total tasks found in Redis without DB row",
}, []string{"queue"})  // Labels: add context like queue name
```

#### **Gauge** (for current state)
```go
QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
    Name: "queue_depth",  // No _total suffix
    Help: "Number of tasks currently in queue",
}, []string{"queue"})
```

#### **Histogram** (for durations)
```go
ProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
    Name:    "queue_processing_duration_seconds",
    Help:    "Time to process a task",
    Buckets: prometheus.DefBuckets,  // 5ms, 10ms, 25ms, 50ms, 100ms...
}, []string{"queue"})
```

#### **No Labels? Use plain Counter**
```go
WorkflowsCreated = promauto.NewCounter(prometheus.CounterOpts{
    Name: "queue_workflows_created_total",
    Help: "Total workflows created",
})
```

---

### **Step 2: Use the Metric in Code**

#### **Counter - Increment when event happens**
```go
if err != nil {
    slog.Error("task orphaned", "taskId", id, "error", err)
    metrics.TasksOrphaned.WithLabelValues(queueName).Inc()  // ← Add this
    return
}
```

#### **Gauge - Set current value**
```go
depth := len(queue)
metrics.QueueDepth.WithLabelValues(queueName).Set(float64(depth))
```

#### **Histogram - Measure duration**
```go
start := time.Now()
err := processTask(task)
metrics.ProcessingDuration.WithLabelValues(queueName).Observe(time.Since(start).Seconds())
```

#### **No Labels - Direct call**
```go
metrics.WorkflowsCreated.Inc()  // No WithLabelValues()
```

---

## 🎨 Real Examples from This Project

### **Example 1: Tracking Data Inconsistency**

```go
// Step 1: Define in metrics.go
TasksOrphaned = promauto.NewCounterVec(prometheus.CounterOpts{
    Name: "queue_tasks_orphaned_total",
    Help: "Total tasks found in Redis without corresponding DB row",
}, []string{"queue"})

// Step 2: Use in main.go
if errors.Is(err, repository.ErrJobNotFound) {
    slog.Error("task in Redis but no DB row", "taskId", taskId)
    metrics.TasksOrphaned.WithLabelValues(queueName).Inc()  // ← Track it
    store.MoveToDeadLetterQueue(ctx, queueName, taskId)
}
```

### **Example 2: Tracking DAG Fan-Out**

```go
// Step 1: Define in metrics.go
ChildrenEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
    Name: "queue_children_enqueued_total",
    Help: "Total child tasks triggered by parent completion",
}, []string{"queue"})

ChildEnqueueFailures = promauto.NewCounterVec(prometheus.CounterOpts{
    Name: "queue_child_enqueue_failures_total",
    Help: "Total failures enqueueing children",
}, []string{"queue"})

// Step 2: Use in main.go
for _, childID := range children {
    if err := store.EnqueueTask(ctx, queueName, childID); err != nil {
        slog.Error("child enqueue failed", "childId", childID, "error", err)
        metrics.ChildEnqueueFailures.WithLabelValues(queueName).Inc()  // ← Failure
    } else {
        metrics.ChildrenEnqueued.WithLabelValues(queueName).Inc()  // ← Success
    }
}
```

---

## 🚀 Quick Reference

| Situation | Metric Type | Method |
|-----------|-------------|--------|
| Event happened | Counter | `.Inc()` |
| Event happened N times | Counter | `.Add(n)` |
| Current state changed | Gauge | `.Set(value)` |
| State increased | Gauge | `.Inc()` |
| State decreased | Gauge | `.Dec()` |
| Measure duration | Histogram | `.Observe(seconds)` |

---

## ✅ What We Added

1. **`TasksOrphaned`** - Tracks data inconsistencies (Redis task without DB row)
2. **`ChildrenEnqueued`** - Tracks DAG orchestration success
3. **`ChildEnqueueFailures`** - Tracks DAG orchestration failures
4. **`WorkflowsCreated`** - Tracks workflow creation

---

## 📊 Viewing Metrics

1. **Start your app**: `./distributed-queue`
2. **Open browser**: http://localhost:9090/metrics
3. **Search for**: `queue_tasks_orphaned_total`, `queue_children_enqueued_total`, etc.

### **Example Output**
```
queue_tasks_orphaned_total{queue="queue:video_processing"} 3
queue_children_enqueued_total{queue="queue:video_processing"} 42
queue_child_enqueue_failures_total{queue="queue:video_processing"} 1
queue_workflows_created_total 5
```

---

## 🎯 Best Practices

✅ **DO**
- Add metrics for failures (errors, retries, DLQ)
- Add metrics for business events (tasks completed, workflows started)
- Use descriptive names: `queue_tasks_orphaned_total` not `orphans`
- Keep both `slog` and `metrics` - they serve different purposes

❌ **DON'T**
- Don't add metrics for every single line of code
- Don't use metrics instead of logs
- Don't add labels with high cardinality (user_id, task_id) - causes memory issues

---

## 🔧 Practice Exercise

**Task**: Add a metric to track when janitor recovers tasks

```go
// Step 1: Add to metrics.go
TasksRecovered = promauto.NewCounterVec(prometheus.CounterOpts{
    Name: "queue_tasks_recovered_total",
    Help: "Total tasks recovered by janitor after timeout",
}, []string{"queue"})

// Step 2: Find in store.go where janitor moves task back to queue
// Step 3: Add after successful recovery:
metrics.TasksRecovered.WithLabelValues(queueName).Inc()
```

Try it yourself!
