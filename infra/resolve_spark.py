from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,io.delta:delta-spark_2.12:3.3.2",
    )
    .getOrCreate()
)
spark.stop()
