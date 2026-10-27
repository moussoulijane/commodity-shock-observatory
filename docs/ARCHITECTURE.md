# V1 architecture and operating boundaries — archived reference

For the active implementation, read [V2 architecture](ARCHITECTURE_V2.md). This document describes the preserved V1 design and its original limits.

Python adapters → Kafka topics → two Spark Structured Streaming queries → Delta Lake / PostgreSQL → read-only FastAPI / React.

## Ingestion query

Kafka coordinates `(topic, partition, offset)` identify a delivery. Delta bronze merges on these coordinates. PostgreSQL atomically records the delivery, validates the Pydantic contract, inserts new observation identities, and rebuilds the serving signals. A crash between these systems is repaired by retry; this is not a distributed atomic transaction. A valid event identity contains provider, instrument, observation time and revision. The mode is part of every database key. Re-delivery does not change a price. Conflicting values under one identity are quarantined.

Original payloads remain in local bronze and PostgreSQL quarantine. The Kafka dead-letter projection contains only the coordinate and reason, is at least once, and is independently deduplicable by its key. No payloads or API keys are logged. Different revisions are retained; the latest observed revision forms the current analyst snapshot. This historical price display may contain revised values and is never described as point-in-time evidence.

## Window query

Spark computes daily event-time delivery counts, mean, minimum and maximum, using a seven-day watermark. It deliberately counts contract-valid DELIVERIES, including duplicates. It is operational telemetry, not canonical price bars. Late records can disappear from this bounded state, while the ingestion query retains them. A global watermark can advance because another instrument or mode is more recent: historical backfills belong in the unbounded bronze path. Watermark progress and dropped rows are exposed in the engineering console.

## Local scale

One KRaft broker, PostgreSQL, one Spark driver with `local[2]`, two shuffle partitions, maximum 2,000 offsets per trigger. Spark performs Kafka ingestion, Delta persistence and stateful event-time aggregation. Signal calculation currently uses a bounded pandas job on the driver and replaces the serving snapshot in one PostgreSQL transaction. This is a documented scalability limit, not a distributed real-time ML engine. Sustained high volume would require incremental partitioned signals, bulk sinks, separate instrument clocks and an actual multi-node deployment.

## Recovery

Keep `kafka-data`, `postgres-data`, `lake` and `checkpoints` volumes. Never delete a checkpoint to fix a failed query casually. `failOnDataLoss=true` fails if Kafka retention has removed required offsets. A deliberate full rebuild needs fresh state plus a complete archive/replay. The two queries have independent checkpoints and eventually converge; their outputs are not committed atomically together.

## Security boundary

Everything is bound to localhost. The API is read only, contains no order execution, and has no authentication. Do not expose it remotely. Production needs SSO/RBAC, TLS/SASL, secret management, network isolation, retention policies, data licence review, backups and high availability. Local demonstration credentials in `.env.example` are not production credentials. No API key is bundled.

## References

- https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html
- https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html
- https://docs.delta.io/releases/ (Spark 3.5.x / Delta 3.3.x compatibility)
- https://fred.stlouisfed.org/docs/api/fred/series_observations.html
- https://www.worldbank.org/en/research/commodity-markets
