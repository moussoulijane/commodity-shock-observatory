"""Two independent checkpointed queries: lossless audit + bounded event-time telemetry.

The canonical observation store keeps all revisions, including events too late for the
seven-day operational watermark. Telemetry counts VALID DELIVERIES, including duplicates;
analyst signals use deduplicated economic observations instead.
"""

import json
import os
import time
from datetime import UTC, datetime

from observatory.config import ARTIFACTS, BROKERS, TOPICS
from observatory.contracts import Quote
from observatory.store import connect, ingest_rows, init, observation_frame, save_report


def session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("commodity-shock-observatory")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
                    "io.delta:delta-spark_2.12:3.3.2",
                ]
            ),
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def valid_contract(payload, topic):
    try:
        quote = Quote.model_validate_json(payload)
        return TOPICS[quote.mode] == topic
    except (ValueError, TypeError):
        return False


def raw_sink(frame, batch_id):
    from delta.tables import DeltaTable

    if frame.isEmpty():
        return
    start = time.perf_counter()
    frame = frame.persist()
    try:
        path = "/app/lake/bronze"
        if DeltaTable.isDeltaTable(frame.sparkSession, path):
            (
                DeltaTable.forPath(frame.sparkSession, path)
                .alias("t")
                .merge(
                    frame.alias("s"),
                    "t.topic=s.topic AND t.partition_id=s.partition_id AND t.offset_id=s.offset_id",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            frame.write.format("delta").mode("overwrite").save(path)
        rows = [row.asDict() for row in frame.collect()]
        result = ingest_rows(rows, batch_id)
        with connect() as conn:
            snapshot = observation_frame(conn)
        # Recoverable mirror; bronze and PostgreSQL remain independently replayable.
        if not snapshot.empty:
            snapshot["payload"] = snapshot.apply(
                lambda r: json.dumps({k: str(v) for k, v in r.to_dict().items()}), axis=1
            )
            mirror = frame.sparkSession.createDataFrame(
                snapshot[["mode", "instrument", "event_id", "payload"]]
            )
            mirror.write.format("delta").mode("overwrite").save("/app/lake/observations")
        # DLQ is an at-least-once projection of durable quarantine, keyed by Kafka coordinate.
        with connect() as conn:
            # Match only this batch; a retried batch republishes the same durable keys.
            bad = conn.execute(
                "SELECT d.* FROM deliveries d JOIN "
                "unnest(%s::text[],%s::int[],%s::bigint[]) AS c(topic,partition_id,offset_id) "
                "USING(topic,partition_id,offset_id) WHERE NOT d.valid",
                (
                    [r["topic"] for r in rows],
                    [r["partition_id"] for r in rows],
                    [r["offset_id"] for r in rows],
                ),
            ).fetchall()
        if bad:
            from confluent_kafka import Producer

            producer = Producer(
                {
                    "bootstrap.servers": BROKERS,
                    "enable.idempotence": True,
                    "broker.address.family": "v4",
                }
            )
            delivery_errors = []

            def acknowledge(error, _):
                if error:
                    delivery_errors.append(str(error))

            for row in bad:
                producer.produce(
                    "cso.dead-letter.v1",
                    key=f"{row['topic']}:{row['partition_id']}:{row['offset_id']}",
                    on_delivery=acknowledge,
                    value=json.dumps(
                        {
                            "reason": row["reason"],
                            "topic": row["topic"],
                            "partition": row["partition_id"],
                            "offset": row["offset_id"],
                        }
                    ),
                )
            if producer.flush(30) or delivery_errors:
                raise RuntimeError("DLQ publication incomplete; batch will retry")
        elapsed = time.perf_counter() - start
        times = [r["kafka_time"].replace(tzinfo=UTC) for r in rows]
        result.update(
            {
                "completed_at": datetime.now(UTC).isoformat(),
                "processing_seconds": elapsed,
                "oldest_delivery_age_seconds": max(
                    (datetime.now(UTC) - t).total_seconds() for t in times
                ),
            }
        )
        with connect() as conn:
            from psycopg.types.json import Jsonb

            conn.execute(
                "UPDATE pipeline_runs SET payload=%s WHERE query_name='ingest' AND batch_id=%s",
                (Jsonb(result), batch_id),
            )
        save_report(ARTIFACTS / "latest-ingest.json", result)
    finally:
        frame.unpersist()


def window_sink(frame, batch_id):
    from psycopg.types.json import Jsonb

    rows = frame.collect()
    with connect() as conn:
        for row in rows:
            conn.execute(
                "INSERT INTO delivery_windows VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(mode,instrument,window_start) DO UPDATE SET "
                "delivery_count=excluded.delivery_count,mean_value=excluded.mean_value,"
                "min_value=excluded.min_value,max_value=excluded.max_value",
                (
                    row.mode,
                    row.instrument,
                    row.window.start.replace(tzinfo=UTC),
                    row.window.end.replace(tzinfo=UTC),
                    row.delivery_count,
                    row.mean_value,
                    row.min_value,
                    row.max_value,
                ),
            )
        conn.execute(
            "INSERT INTO pipeline_runs VALUES ('windows',%s,%s) "
            "ON CONFLICT(query_name,batch_id) DO UPDATE SET payload=excluded.payload",
            (
                batch_id,
                Jsonb(
                    {"windows_updated": len(rows), "completed_at": datetime.now(UTC).isoformat()}
                ),
            ),
        )


def run(available_now=False):
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType

    from observatory.producer import create_topics

    init()
    create_topics()
    spark = session()
    modes = os.getenv("SPARK_MODES", "REPLAY,SYNTHETIC,LIVE").split(",")
    # One Kafka partition per instrument preserves source order; cross-instrument disorder
    # is expected. The raw path has no watermark and never drops late records.
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", ",".join(TOPICS[m] for m in modes))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", 2000)
        .load()
        .selectExpr(
            "topic",
            "partition AS partition_id",
            "offset AS offset_id",
            "CAST(value AS STRING) AS payload",
            "timestamp AS kafka_time",
        )
    )
    valid = F.udf(valid_contract, BooleanType())
    accepted = raw.filter(valid("payload", "topic")).select(
        F.get_json_object("payload", "$.mode").alias("mode"),
        F.get_json_object("payload", "$.instrument").alias("instrument"),
        F.to_timestamp(F.get_json_object("payload", "$.event_time")).alias("event_time"),
        F.get_json_object("payload", "$.value").cast("double").alias("value"),
    )
    windows = (
        accepted.withWatermark("event_time", "7 days")
        .groupBy("mode", "instrument", F.window("event_time", "1 day"))
        .agg(
            F.count("*").alias("delivery_count"),
            F.avg("value").alias("mean_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
        )
    )
    queries = []
    for name, df, sink, output in [
        ("ingest", raw, raw_sink, "append"),
        ("windows", windows, window_sink, "update"),
    ]:
        writer = (
            df.writeStream.queryName(name)
            .foreachBatch(sink)
            .outputMode(output)
            .option("checkpointLocation", f"/app/checkpoints/{name}")
        )
        writer = (
            writer.trigger(availableNow=True)
            if available_now
            else writer.trigger(processingTime="5 seconds")
        )
        queries.append(writer.start())
    try:
        while any(q.isActive for q in queries):
            for query in queries:
                if query.exception():
                    raise RuntimeError(str(query.exception()))
                if query.lastProgress:
                    save_report(ARTIFACTS / f"spark-{query.name}.json", query.lastProgress)
            time.sleep(1)
        for query in queries:
            if query.exception():
                raise RuntimeError(str(query.exception()))
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    import sys

    run("--available-now" in sys.argv)
