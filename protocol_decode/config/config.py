import os
import sys
import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

# Source
S3_SOURCE   = "s3a://aws-public-blockchain/v1.0/eth/logs"
S3_REGION   = "us-east-2"

# Sink — S3
S3_OUTPUT   = "s3a://money-market/decoded" 

def get_spark(app_name: str, s3_raw_bucket='money-market') -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        #  Anonymous credentials for public source bucket     
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint",         f"s3.{S3_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        #  S3 performance 
        .config("spark.hadoop.fs.s3a.connection.maximum",           "200")
        .config("spark.hadoop.fs.s3a.fast.upload",                  "true")
        .config("spark.hadoop.fs.s3a.block.size",                   "134217728")
        .config("spark.hadoop.fs.s3a.multipart.size",               "134217728")
        .config("spark.hadoop.fs.s3a.connection.timeout",           "200000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "15000")
        .config("spark.sql.parquet.enableVectorizedReader",      "true")
        .config("spark.sql.parquet.mergeSchema",                 "false")
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.skewJoin.enabled",           "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions",                  "400")
        .config("spark.sql.ansi.enabled",                        "false")
        .config("spark.driver.memory",        "4g")
        .config("spark.executor.memory",      "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.3",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # auto-detect local vs EMR
    is_local = os.getenv("EMR_RELEASE_LABEL") is None
    log.info("Environment: %s", "LOCAL" if is_local else "EMR")

    if is_local:
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key or not aws_secret_key:
            log.error(
                "AWS credentials not found. "
                "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
            )
            sys.exit(1)

        log.info("Local mode: injecting per-bucket credentials for s3a://%s", s3_raw_bucket)

        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set(
            f"fs.s3a.bucket.{s3_raw_bucket}.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        hadoop_conf.set(f"fs.s3a.bucket.{s3_raw_bucket}.access.key", aws_access_key)
        hadoop_conf.set(f"fs.s3a.bucket.{s3_raw_bucket}.secret.key", aws_secret_key)
        log.info("Per-bucket credentials injected ✓")

    log.info("SparkSession ready | version=%s", spark.version)
    return spark
