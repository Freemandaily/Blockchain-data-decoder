"""
spark_job.py

Batch Spark job that:
  1. Reads raw Ethereum event logs from the AWS public blockchain S3 bucket
  2. Filters to Aave V1/V2/V3 contracts and known topic0s
  3. Decodes each log using aave_decoder
  4. Routes each event type to its own table
  5. Writes to S3 (Parquet, date-partitioned) and/or PostgreSQL

Run
---
    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.3 \
        aave_spark_job.py \
        --start-date 2024-01-01 \
        --end-date   2024-01-07 \
        --sink       both          # s3 | postgres | both
"""

import argparse
import logging
import sys,os
from datetime import date, timedelta
from typing import Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructType, StructField, BooleanType, ArrayType

#  project modules 
from abis.aave_abis import CONTRACT_REGISTRY as AAVE_REGISTRY, DECODER_MAP as AAVE_DECODER_MAP
from abis.compound_abis import CONTRACT_REGISTRY as COMPOUND_REGISTRY, DECODER_MAP as COMPOUND_DECODER_MAP
from abis.morpho_abis import CONTRACT_REGISTRY as MORPHO_REGISTRY, DECODER_MAP as MORPHO_DECODER_MAP
from decode.decoder import decode_log

CONTRACT_REGISTRY = {**AAVE_REGISTRY, **COMPOUND_REGISTRY, **MORPHO_REGISTRY}
DECODER_MAP = {**AAVE_DECODER_MAP, **COMPOUND_DECODER_MAP, **MORPHO_DECODER_MAP}


log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


from config.config import get_spark, S3_SOURCE, S3_OUTPUT

# All contract addresses (lowercase) used for early filter
ALL_CONTRACTS = {addr.lower() for addr in CONTRACT_REGISTRY.keys()}

# All known topic0s — used for early filter
ALL_TOPIC0S = list({abi["topic0"].lower() for abi in DECODER_MAP.values()})

DECODE_RESULT_SCHEMA = StructType([
    StructField("success", BooleanType(), False),
    StructField("data", MapType(StringType(), StringType()), True),
    StructField("error", StringType(), True),
    StructField("raw_topics", ArrayType(StringType()), True),
    StructField("raw_data", StringType(), True)
])

def _decode_udf_fn(address: str, topics: list, data: str):
    """
    Thin wrapper around decode_log for the Spark UDF.
    Returns dictionary conforming to DECODE_RESULT_SCHEMA.
    """
    try:
        result = decode_log({
            "address": address,
            "topics":  topics,
            "data":    data or "0x",
        })
        if result is None:
            return {
                "success": False,
                "data": None,
                "error": "No matching ABI or decode_log returned None",
                "raw_topics": topics,
                "raw_data": data
            }
        return {
            "success": True, 
            "data": {k: str(v) for k, v in result.items()}, 
            "error": None,
            "raw_topics": None,
            "raw_data": None
        }
    except Exception as exc:  
        log.error("UDF error addr=%s exc=%s", address, exc)
        return {
            "success": False,
            "data": None,
            "error": str(exc),
            "raw_topics": topics,
            "raw_data": data
        }


DECODE_UDF = F.udf(_decode_udf_fn, DECODE_RESULT_SCHEMA)


def read_raw_logs(spark: SparkSession, start_date: date, end_date: date) -> DataFrame:
    """
    Read the public Ethereum logs Parquet dataset for a date range.

    The dataset is partitioned by date (date=YYYY-MM-DD) so Spark will
    only read the relevant partitions — no full-table scan.
    """
    date_list = []
    d = start_date
    while d <= end_date:
        date_list.append(d.isoformat())
        d += timedelta(days=1)

    log.info("Reading logs for %d day(s): %s → %s", len(date_list), start_date, end_date)

    df = (
        spark.read
        .option("basePath", S3_SOURCE)
        .parquet(S3_SOURCE)
        .filter(F.col("date").isin(date_list))
    )

    log.info("Schema: %s", df.schema.simpleString())
    return df


def filter_logs(df: DataFrame) -> DataFrame:
    """
    Keep only rows that:
      - come from a known contract address (Aave or Compound)
      - have a topics[0] we know how to decode

    Both filters use broadcast-friendly sets so Spark can evaluate
    them without shuffles.
    """
    filtered = (
        df
        .filter(F.lower(F.col("address")).isin(list(ALL_CONTRACTS)))
        .filter(F.col("topics")[0].isin(ALL_TOPIC0S))
    )

    log.info(
        "Filtered logs | contracts=%d | topic0s=%d",
        len(ALL_CONTRACTS), len(ALL_TOPIC0S),
    )
    return filtered



def decode_logs(df: DataFrame) -> DataFrame:
    """
    Apply the decode UDF. The result is a struct with success/data/error.
    """
    return (
        df
        .withColumn(
            "decode_result",
            DECODE_UDF(
                F.lower(F.col("address")),
                F.col("topics"),
                F.col("data"),
            )
        )
    )


from shapers.shapers import shape_deposit, shape_withdraw, shape_borrow, shape_repay, shape_liquidation


def write_s3(df: DataFrame, event_name: str) -> None:
    """
    Write DataFrame to S3 partitioned by date.
    """
    path = f"{S3_OUTPUT}/{event_name.lower()}"
    log.info("Writing %s → %s", event_name, path)
    (
        df.write
        .partitionBy("date")
        .mode("append")
        .option("compression", "snappy")
        .parquet(path)
    )
    log.info("Done writing %s to S3", event_name)



def run(start_date: date, end_date: date, sink: str) -> None:
    spark = get_spark(f"aave-decoder-{start_date}-{end_date}")

    raw      = read_raw_logs(spark, start_date, end_date)
    filtered = filter_logs(raw)
    decoded_raw = decode_logs(filtered).cache()

    # Good rows 
    decoded = (
        decoded_raw.filter(F.col("decode_result.success") == True)
        .withColumn("decoded", F.col("decode_result.data"))
    ).cache()
    
    # Bad rows 
    dead_letter_df = (
        decoded_raw.filter(F.col("decode_result.success") == False)
        .select(
            F.col("block_number"),
            F.col("block_timestamp"),
            F.col("transaction_hash"),
            F.col("log_index"),
            F.col("date"),
            F.col("address"),
            F.col("decode_result.error").alias("error"),
            F.col("decode_result.raw_topics").alias("raw_topics"),
            F.col("decode_result.raw_data").alias("raw_data")
        )
    )
    
    dead_letter_count = dead_letter_df.count()
    if dead_letter_count > 0:
        log.warning("Found %d dead-letter rows. Writing to dead_letter partition.", dead_letter_count)
        write_s3(dead_letter_df, "dead_letter")

    log.info("Successfully decoded row count: %d", decoded.count())
    event_shapers = {
        "Borrow":          shape_borrow,
        "Repay":           shape_repay,
        "LiquidationCall": shape_liquidation,
        "Deposit":         shape_deposit,
        "Withdraw":        shape_withdraw,

    }

    for event_name, shaper in event_shapers.items():
        log.info("Processing event: %s", event_name)
        event_df = shaper(decoded)

        row_count = event_df.count()
        log.info("  rows: %d", row_count)

        if row_count == 0:
            log.warning("  No rows for %s — skipping write", event_name)
            continue

        if sink in ("s3", "both"):
            write_s3(event_df, event_name)


    decoded.unpersist()
    decoded_raw.unpersist()
    spark.stop()
    log.info("Job complete.")

def _parse_args():
    parser = argparse.ArgumentParser(description="Aave event log decoder Spark job")
    parser.add_argument(
        "--start-date", required=True,
        type=date.fromisoformat,
        help="Start date inclusive, YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date", required=True,
        type=date.fromisoformat,
        help="End date inclusive, YYYY-MM-DD",
    )
    parser.add_argument(
        "--sink",
        choices=["s3", "postgres", "both"],
        default="s3",
        help="Where to write output (default: s3)",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    if args.start_date > args.end_date:
        log.error("start-date must be <= end-date")
        sys.exit(1)
    run(args.start_date, args.end_date, args.sink)
