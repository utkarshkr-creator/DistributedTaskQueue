### Distributed Task Queue

#### Understanding the Problem
**What is a Distributed Task Queue?** Every modern application needs to handle work asynchronously, such as sending emails without blocking HTTP responses or calling slow external APIs. A distributed task queue manages these background jobs across competing workers. When a producer pushes tasks to the queue, workers pull them and process them. However, building a reliable queue requires handling complex realities: what happens when a worker crashes mid-task, how to prevent priority starvation, and how to safely orchestrate jobs that depend on each other (DAG dependencies).

#### Requirements
Before designing the system, we should ask questions of our interviewer to turn the vague prompt into a concrete specification.

##### Clarifying Questions
**You:** "What happens when a worker crashes between dequeueing a task and completing it?"
**Interviewer:** "We can't lose the task forever. Use a visibility timeout pattern. If a task stays active for too long, assume the worker died and requeue it."

**You:** "If multiple workers process jobs concurrently, does strict First-In-First-Out (FIFO) ordering apply?"
**Interviewer:** "Distributed parallelism breaks strict processing order. If tasks require specific ordering, we need to implement Directed Acyclic Graph (DAG) dependencies where a child job only runs after its parent completes."

**You:** "What happens if a job constantly fails? Do we retry immediately?"
**Interviewer:** "Implement an exponential backoff retry mechanism. But make sure to handle scenarios where thousands of tasks fail simultaneously so we don't overload the database on the retry."

**You:** "Should we worry about graceful shutdowns when a server restarts?"
**Interviewer:** "Yes, the sequence matters. Workers should stop accepting new tasks, wait for in-flight tasks to complete, and close connections safely."

##### Final Requirements
*   **Decoupled Execution:** Producers submit workflows; workers pull and execute tasks.
*   **DAG Orchestration:** A child task must only be queued when its parent successfully completes.
*   **In-Flight Safeguard:** Tasks must use a visibility timeout to prevent loss during worker crashes.
*   **Resilient Retries:** Failures must trigger exponential backoff with jitter.
*   **Idempotency:** Tasks must not be processed twice, even if network delays cause duplicate ACKs.

#### Core Entities and Relationships
We need to identify the core objects that will make up our system. For a distributed task queue, we can extract these responsibilities:

| Entity | Responsibility |
| :--- | :--- |
| **Job** | Represents a single executable unit within a workflow. It tracks its own state, retry counts, and DAG relationships (parent IDs). |
| **QueueBroker** (Redis) | The low-latency transport layer. It manages active queues, in-flight tracking lists, and delayed sorted sets for retries. |
| **Worker** | The consumer that constantly polls the broker. It executes the business logic dynamically based on task type and reports terminal states. |
| **DAG Orchestrator** | The brain of the dependency graph. It acts as an Observer: when a worker finishes a job, it evaluates the graph and promotes waiting child tasks into active queues. |
| **Janitor** | A background daemon that monitors in-flight lists. It sweeps abandoned tasks that exceed their visibility timeout back to the active queue. |

#### Class Design
Let's design the core models and interfaces, starting with the `Job` entity and the `RedisStore`. 

##### Job
The `Job` needs to hold enough state to track its place in the dependency graph and its retry lifecycle.
**Job State:**
*   `id` (UUID): Unique identifier.
*   `parent_id` (UUID): Links to the task that must run before this one.
*   `task_type` (String): Defines the execution strategy.
*   `status` (Enum): PENDING, QUEUED, RUNNING, COMPLETED, FAILED.
*   `current_retries` / `max_retries` (Integer): For backoff tracking.

##### RedisStore (Queue Broker)
The `RedisStore` acts as our queue manager. From the outside, it needs to support a small set of actions:
*   `EnqueueTask(queueName, taskId)`: Pushes a task to the active queue.
*   `ConsumeTask(queueName)`: Blocks and retrieves a task.
*   `AcknowledgeTask(queueName, taskId)`: Clears a successful task.
*   `RetryTaskWithBackoff(queueName, taskId, attempt)`: Moves a failure to a delayed set.
*   `PolledDelayedTasks(queueName)`: Sweeps matured retries back to active.

#### Implementation
Let's drill into the movement and extraction logic, specifically `ConsumeTask` and the Janitor's `PolledDelayedTasks`.

##### Bad Solution: Basic FIFO (`BRPOP` without In-Flight)
The simplest movement strategy is fetching jobs with `BRPOP`.
**Challenges:** `BRPOP` deletes the message on extraction. If the worker crashes immediately after dequeueing, the task is lost forever.

##### Great Solution: In-Flight Safeguard (`BLMOVE`)
**Approach:** We upgrade our logic to use `BLMOVE` (which replaced the deprecated `BRPOPLPUSH` in Redis 6.2.0). 
We instruct Redis to pop from the **RIGHT** of the main active queue and push it to the **LEFT** of an `:inflight` tracking list atomically. 
```go
func (s *RedisStore) ConsumeTask(ctx context.Context, queueName string) (string, error) {
    inflightKey := queueName + ":inflight"
    // BLMove atomically pops from RIGHT of queueName and pushes to LEFT of inflightKey
    result, err := s.rdb.BLMove(ctx, queueName, inflightKey, "RIGHT", "LEFT", 0).Result()
    // ... record start timestamp in a Redis Hash for the Janitor ...
}
```
If the worker dies, the job safely remains inside the in-flight list. 

##### Polling Delayed Tasks (The Janitor)
When handling retries, we put failed tasks in a Redis Sorted Set (`ZSET`) where the "Score" is the future Unix timestamp. The Janitor must poll this to restore tasks.
**Challenges:** If the Janitor uses an inclusive/closed time boundary, a task expiring at the *exact* current millisecond could be matched twice during rapid sweeps, creating an accidental duplication race condition. Furthermore, if the network drops between deleting the item (`ZRem`) and re-enqueuing it, the task vanishes permanently.

**Optimal Approach:** We use an **exclusive upper boundary** by prefixing the score with `(`, and we execute the Compare-And-Swap (CAS) check entirely inside a **Redis Lua Script**. Lua scripts run atomically inside Redis memory, ensuring the task is perfectly evaluated, removed, and pushed without race conditions.

#### Verification
Let's trace a concrete scenario to verify the workflow and error recovery:
1. **Worker Crashes:** A worker pops Job A. It is atomically moved to the `inflight` list. The physical server loses power, and the worker dies.
2. **Janitor Sweeps:** The background Janitor scans the `inflight` list and checks the start time hash. It realizes Job A has exceeded its 30-second visibility timeout.
3. **Recovery:** The Janitor atomically moves Job A back to the active queue. A healthy worker picks it up and processes it successfully.

#### Extensibility

##### 1. "How would you handle priority queues without starving low-priority tasks?"
"A naive approach is to use multiple Redis lists and always pop high-priority first, but a flood of high-priority tasks starves everything else. Real systems need weighted processing. I would use a Redis Sorted Set and compute a score: `(maxPriority - priority) * 10^12 + timestamp_nanoseconds`. Higher priority tasks get lower scores and are dequeued first, but within the same priority, earlier timestamps win. This ensures elegant fairness."

##### 2. "How would you prevent a 'thundering herd' when a database outage causes thousands of tasks to retry?"
"If 10,000 tasks fail and use pure exponential backoff, they will all retry at exactly the same time, immediately overloading the database again. The fix is adding **jitter**—randomness to spread retries across time. We calculate the delay as `min(base * 2^attempt + random(0, delay/2), maxDelay)`. This spreads the retries over a window instead of firing simultaneously."

##### 3. "How would you implement graceful shutdown?"
"Graceful shutdown requires a strict sequence. First, I would trap the `SIGTERM` interrupt signal. The workers would stop accepting new tasks but continue processing current ones. We wait for in-flight tasks to complete using a sync channel like `workerDone` with a maximum timeout of 30 seconds. Only then do we safely close the Redis database connections and exit."
