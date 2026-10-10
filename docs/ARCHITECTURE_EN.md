[한국어](ARCHITECTURE.md) / [English](ARCHITECTURE_EN.md)

# Worker Architecture

The Worker claims durable Jobs in OpenSQL and performs document indexing or chunk deletion.
APIs, search interfaces, source object removal, and shared schema migrations belong to other components.

## Components

```mermaid
flowchart TD
    Q[(indexing_job)] --> W[DbJobWorker]
    W --> R[IndexingPipelineRunner]
    W --> H[Separate heartbeat thread]
    H --> Q
    R --> A[IndexingAttemptProcessor]
    A --> D[Download / Hash / Parse / Chunk]
    D --> E[EmbeddingIndexingProcessor]
    E --> X[ExternalApiOperations]
    X --> API[Embedding API]
    E --> P[IndexingPublicationService]
    R --> DEL[DocumentDeletionService]
    P --> DB[(Documents / versions / chunks / Jobs)]
    DEL --> DB
    B[DatabaseOperations / ExecutionOperations] --> Q
    B --> DB
```

| Component | Responsibility |
|---|---|
| `DbJobWorker` | Reserve slots, claim/recover loop, execution pool, separate heartbeat, shutdown |
| `JobQueueStore` | Short SQL transactions for claims, ownership snapshots, conditional renewal, retry resumption |
| `DatabaseOperations` | Connection failure classification, shared availability state, probes and waits outside transactions |
| `ExecutionOperations` | Recheck token, status, and lease after recovery; conditionally renew and retry the suspended call |
| `ExternalApiOperations` | 429 backoff, shared deadlines, retry serialization within a Worker |
| `IndexingPipelineRunner` | Validate claimed Job data, dispatch INDEX/DELETE, retry general failures |
| `IndexingAttemptProcessor` | Download, verify hash, parse/chunk, record fenced phases |
| `IndexingPublicationService` | Guard ownership and save complete results atomically |
| `DocumentDeletionService` | Check deleted document, delete chunks and complete DELETE Job atomically; complete INDEX Jobs of deleted documents as `SKIPPED_DOCUMENT_DELETED` |

## Database contract

`indexing_job` stores `source_event_id` as an idempotency key, document IDs, `job_type`, status,
attempt count, worker ID, execution epoch, lease expiry, retry deadline, and error/trace data.
`INDEX` requires a version ID; `DELETE` uses a null version ID and deletes at document scope.
Timestamps use TIMESTAMPTZ and database time.

The execution token is `job_id + worker_id + execution_epoch`. Claims and recovery increment
the epoch; heartbeat and retry within the same execution retain it. Document `version_no`
is a separate safeguard against searchable pointer regression. The shared schema's
`latest_embedding_version_no` remains metadata.

Chunks UPSERT on `(document_version_id, chunk_no)` and store JSONB metadata, `vector(1536)`,
and Nori `content_tokens`. Publication locks the Job and document rows and commits chunks,
version state, searchable pointer, and Job completion together. External calls finish outside that transaction.

## Deployment dependencies

The [contract SQL](sql/DB_JOB_QUEUE_CONTRACT.sql) is a schema-owner proposal, not an automatic migration.
Stop Workers without fencing, apply the contract, then deploy this Worker.
`recover_expired_indexing_jobs()` is required and called by the poll loop. A database scheduler
may also call it; competing renewal and recovery use conditional row updates.

Server producers must lock the document and enqueue Jobs in the document mutation transaction.
Per-document Job ID order must match version/deletion order. Reject historical retries and INDEX
Jobs for deleted documents. This repository does not implement those Server paths.
`JobQueueStore.enqueue()` supplies registration SQL and an integration-test entry point;
the running Worker does not automatically produce Jobs.

## Scope of guarantees

The Worker supports concurrency across documents, fencing of stale owners, atomic result saves,
and waiting at failed database calls. Process termination loses in-memory files/chunks/embeddings;
a new execution computes them again. Active Job limits and retry serialization are per Worker.
A global external request limiter and an external API circuit breaker are outside this implementation.

[Processing model](PROCESSING_MODEL_EN.md) · [Failure handling](FAILURE_HANDLING_EN.md) · [README](../README_EN.md)
