"""V2 Spark micro-batches: bounded context, incremental features and isolated clocks."""

import json
import os
import re
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

from observatory.config import ARTIFACTS, BROKERS, TOPICS
from observatory.contracts import Quote
from observatory.incremental_store import (
    bars_context,
    complete_batch,
    feature_context,
    ingest_batch,
    init,
    write_bars,
    write_signals,
)
from observatory.store import connect, save_report


def session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("commodity-shock-observatory-v2")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,io.delta:delta-spark_2.12:3.3.2",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
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


def delta_merge(df, path, condition):
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(df.sparkSession, path):
        (
            DeltaTable.forPath(df.sparkSession, path)
            .alias("t")
            .merge(df.alias("s"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)


def publish_rejections(rejections):
    if not rejections:
        return
    from confluent_kafka import Producer

    errors = []
    producer = Producer(
        {"bootstrap.servers": BROKERS, "enable.idempotence": True, "broker.address.family": "v4"}
    )

    def acknowledged(error, _):
        if error:
            errors.append(str(error))

    for row in rejections:
        producer.produce(
            "cso.dead-letter.v1",
            key=f"{row['topic']}:{row['part']}:{row['off']}",
            value=json.dumps(
                {
                    "topic": row["topic"],
                    "partition": row["part"],
                    "offset": row["off"],
                    "reason": row["reason"],
                }
            ),
            on_delivery=acknowledged,
        )
    if producer.flush(30) or errors:
        raise RuntimeError("Quarantine projection not acknowledged; batch retry is safe")


def failure_point(stage, batch_id):
    setting = os.getenv("CSO_FAILPOINT", "")
    if not setting:
        return
    point, _, token = setting.partition(":")
    if point != stage:
        return
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", token):
        raise ValueError("Invalid failure-injection token")
    path = Path("/app/checkpoints/v2/faults") / (point + "-" + token)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(str(batch_id), encoding="utf-8")
        raise RuntimeError(f"Intentional V2 recovery exercise at {stage}")


def raw_sink(df, batch_id):
    from observatory.incremental_store import materialization_lease

    with materialization_lease() as generation:
        _materialize(df, batch_id, generation)


def _materialize(df, batch_id, generation):
    from observatory.spark_features import bars, frame, score

    if df.isEmpty():
        return
    with connect() as conn:
        if conn.execute(
            "SELECT 1 FROM completed_batches_v2 WHERE query_name='ingest-v2' AND batch_id=%s",
            (batch_id,),
        ).fetchone():
            return
    start = time.perf_counter()
    df = df.persist()
    try:
        delta_merge(
            df,
            "/app/lake/v2/bronze",
            "t.topic=s.topic AND t.partition_id=s.partition_id AND t.offset_id=s.offset_id",
        )
        # Bounded 2,000-offset bridge; Spark, not pandas, computes feature windows.
        rows = [r.asDict() for r in df.toLocalIterator()]
        accepted, rejections, stats = ingest_batch(rows, batch_id)
        stats["generation"] = generation
        failure_point("after_bulk", batch_id)
        if accepted:
            silver = (
                frame(df.sparkSession, accepted).drop("emit").dropDuplicates(["mode", "event_id"])
            )
            delta_merge(silver, "/app/lake/v2/silver", "t.mode=s.mode AND t.event_id=s.event_id")
        context, ranges = feature_context(batch_id)
        if context:
            score(frame(df.sparkSession, context)).foreachPartition(
                partial(write_signals, generation=generation)
            )
            bar_rows = bars_context(ranges)
            if bar_rows:
                bars(frame(df.sparkSession, bar_rows)).foreachPartition(
                    partial(write_bars, generation=generation)
                )
            stats.update(
                {
                    "context_rows": len(context),
                    "bar_context_rows": len(bar_rows),
                    "affected_instruments": len(ranges),
                    "compute_engine": "Spark SQL executors",
                }
            )
        publish_rejections(rejections)
        stats.update(
            {
                "processing_seconds": time.perf_counter() - start,
                "completed_at": datetime.now(UTC).isoformat(),
                "version": 2,
            }
        )
        complete_batch(batch_id, stats)
        save_report(ARTIFACTS / "latest-ingest-v2.json", stats)
    finally:
        df.unpersist()


def window_sink(df, batch_id, query_name="windows-REPLAY"):
    from psycopg.types.json import Jsonb

    rows = list(df.toLocalIterator())
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO delivery_windows_v2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(mode,instrument,window_start) DO UPDATE SET "
                "delivery_count=excluded.delivery_count,mean_value=excluded.mean_value,"
                "min_value=excluded.min_value,max_value=excluded.max_value",
                [
                    (
                        r.mode,
                        r.instrument,
                        r.window.start.replace(tzinfo=UTC),
                        r.window.end.replace(tzinfo=UTC),
                        r.delivery_count,
                        r.mean_value,
                        r.min_value,
                        r.max_value,
                    )
                    for r in rows
                ],
            )
        conn.execute(
            "INSERT INTO pipeline_runs VALUES (%s,%s,%s) ON CONFLICT(query_name,batch_id) DO UPDATE SET payload=excluded.payload",
            (
                query_name,
                batch_id,
                Jsonb(
                    {"windows_updated": len(rows), "completed_at": datetime.now(UTC).isoformat()}
                ),
            ),
        )


def run(available_now=False):
    from functools import partial

    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from observatory.producer import create_topics

    init()
    create_topics()
    spark = session()
    modes = os.getenv("SPARK_MODES", "REPLAY,SYNTHETIC,LIVE").split(",")

    def source(topics):
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BROKERS)
            .option("subscribe", ",".join(topics))
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

    queries = []

    def start_query(df, name, sink, output="append"):
        writer = (
            df.writeStream.queryName(name)
            .foreachBatch(sink)
            .outputMode(output)
            .option("checkpointLocation", f"/app/checkpoints/v2/{name}")
        )
        writer = (
            writer.trigger(availableNow=True)
            if available_now
            else writer.trigger(processingTime="5 seconds")
        )
        queries.append(writer.start())

    start_query(source([TOPICS[m] for m in modes]), "ingest-v2", raw_sink)
    valid = F.udf(valid_contract, T.BooleanType())
    for mode in modes:
        # One query and checkpoint per mode, hence no cross-mode watermark.
        accepted = (
            source([TOPICS[mode]])
            .filter(valid("payload", "topic"))
            .select(
                F.get_json_object("payload", "$.mode").alias("mode"),
                F.get_json_object("payload", "$.instrument").alias("instrument"),
                F.to_timestamp(F.get_json_object("payload", "$.event_time")).alias("event_time"),
                F.get_json_object("payload", "$.value").cast("double").alias("value"),
            )
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
        name = f"windows-{mode}"
        start_query(windows, name, partial(window_sink, query_name=name), "update")
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
