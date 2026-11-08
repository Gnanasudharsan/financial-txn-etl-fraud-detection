"""SparkSession factory tuned for a laptop, CI runner, or Databricks.

On Databricks (``DATABRICKS_RUNTIME_VERSION`` set) the platform session is reused untouched:
serverless compute rejects most session-level configuration.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from fintxn.config import Settings

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(settings: Settings) -> SparkSession:
    if on_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    sc = settings.spark
    st = settings.storage
    builder = (
        SparkSession.builder.master(sc.master)
        .appName(sc.app_name)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", sc.driver_memory))
        .config("spark.sql.shuffle.partitions", str(sc.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )
    extra_packages: list[str] = []
    if st.backend == "s3":
        extra_packages.append(HADOOP_AWS)
        endpoint = st.endpoint or ""
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", str(st.path_style).lower())
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(st.ssl).lower())
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.hadoop.fs.s3a.endpoint.region", st.region)
            # Delta on S3: single-writer-per-table is guaranteed by the orchestrator.
            .config("spark.delta.logStore.s3a.impl", "io.delta.storage.S3SingleDriverLogStore")
        )
        if endpoint:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        if st.access_key and st.secret_key:
            builder = builder.config("spark.hadoop.fs.s3a.access.key", st.access_key).config(
                "spark.hadoop.fs.s3a.secret.key", st.secret_key
            )
        if st.sse != "none":
            algo = "SSE-KMS" if st.sse == "aws:kms" else "SSE-S3"
            builder = builder.config("spark.hadoop.fs.s3a.encryption.algorithm", algo)
            if st.kms_key_id:
                builder = builder.config("spark.hadoop.fs.s3a.encryption.key", st.kms_key_id)
    for k, v in sc.extra_conf.items():
        builder = builder.config(k, v)

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
