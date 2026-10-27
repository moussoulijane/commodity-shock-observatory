"""Exact bounded-window feature calculation on Spark executors (no pandas scoring)."""

from datetime import UTC, datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField(name, typ, True)
        for name, typ in [
            ("mode", T.StringType()),
            ("instrument", T.StringType()),
            ("event_id", T.StringType()),
            ("event_time", T.TimestampType()),
            ("observed_at", T.TimestampType()),
            ("ingested_at", T.TimestampType()),
            ("revision", T.StringType()),
            ("frequency", T.StringType()),
            ("unit", T.StringType()),
            ("provider", T.StringType()),
            ("kind", T.StringType()),
            ("value", T.DoubleType()),
            ("emit", T.BooleanType()),
        ]
    ]
)
JSON_OPTIONS = {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}


def frame(spark, records):
    rows = []
    for record in records:
        row = {key: record.get(key) for key in SCHEMA.fieldNames()}
        for key in ("event_time", "observed_at", "ingested_at"):
            row[key] = (
                datetime.fromisoformat(str(row[key]).replace("Z", "+00:00"))
                .astimezone(UTC)
                .replace(tzinfo=None)
            )
        row["value"] = float(row["value"])
        row["emit"] = bool(record.get("emit", True))
        rows.append(row)
    return spark.createDataFrame(rows, SCHEMA)


def median(column):
    values = F.array_sort(column)
    size = F.size(values)
    left = F.greatest(F.floor((size + 1) / 2).cast("int"), F.lit(1))
    right = F.greatest(F.floor((size + 2) / 2).cast("int"), F.lit(1))
    return F.when(size > 0, (F.element_at(values, left) + F.element_at(values, right)) / 2)


def score(df):
    order = Window.partitionBy("mode", "instrument").orderBy("event_time")
    result = (
        df.withColumn("previous", F.lag("value").over(order))
        .withColumn("delta", F.col("value") - F.col("previous"))
        .withColumn(
            "change_pct", F.when(F.abs("previous") > 1e-9, 100 * F.col("delta") / F.abs("previous"))
        )
        .withColumn("lookback", F.when(F.col("frequency") == "monthly", 36).otherwise(60))
        .withColumn("warmup", F.when(F.col("frequency") == "monthly", 12).otherwise(20))
        .withColumn("all_changes", F.collect_list("delta").over(order.rowsBetween(-60, -1)))
        .withColumn("all_prices", F.collect_list(F.abs("previous")).over(order.rowsBetween(-59, 0)))
    )
    for source, target in (("all_changes", "changes"), ("all_prices", "prices")):
        result = result.withColumn(
            target, F.expr(f"slice({source}, greatest(size({source})-lookback+1,1), lookback)")
        )
    result = (
        result.withColumn("center", median(F.col("changes")))
        .withColumn("deviations", F.expr("transform(changes, x -> abs(x-center))"))
        .withColumn("dispersion", median(F.col("deviations")))
        .withColumn("price_floor", F.greatest(median(F.col("prices")) * 0.001, F.lit(1e-8)))
        .withColumn(
            "robust_z",
            F.when(
                F.size("changes") >= F.col("warmup"),
                (F.col("delta") - F.col("center"))
                / F.greatest(1.4826 * F.col("dispersion"), F.col("price_floor")),
            ),
        )
        .withColumn("baseline_alert", F.coalesce(F.abs("change_pct") >= 5, F.lit(False)))
        .withColumn("robust_alert", F.coalesce(F.abs("robust_z") >= 4, F.lit(False)))
        .withColumn("direction", F.when(F.col("delta") >= 0, "UP").otherwise("DOWN"))
        .withColumn("detector_version", F.lit("delta-robust-v1-spark-v2"))
    )
    cols = [key for key in SCHEMA.fieldNames() if key != "emit"] + [
        "delta",
        "change_pct",
        "robust_z",
        "baseline_alert",
        "robust_alert",
        "direction",
        "detector_version",
    ]
    return result.filter("emit").select(F.to_json(F.struct(*cols), JSON_OPTIONS).alias("payload"))


def bars(df):
    result = df.withColumn(
        "bar_start",
        F.when(F.col("frequency") == "tick", F.date_trunc("minute", "event_time"))
        .when(F.col("frequency") == "daily", F.date_trunc("day", "event_time"))
        .otherwise(F.date_trunc("month", "event_time")),
    )
    key = F.struct("event_time", "event_id")
    result = result.groupBy(
        "mode", "instrument", "frequency", "unit", "provider", "kind", "bar_start"
    ).agg(
        F.min_by("value", key).alias("open"),
        F.max("value").alias("high"),
        F.min("value").alias("low"),
        F.max_by("value", key).alias("close"),
        F.count("*").alias("observation_count"),
        F.max("event_time").alias("last_observation_time"),
    )
    return result.select(F.to_json(F.struct(*result.columns), JSON_OPTIONS).alias("payload"))
