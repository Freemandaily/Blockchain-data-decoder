"""
aave_spark_job.py
=================
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
from pyspark.sql.types import MapType, StringType

#  project modules 
from aave_abis import CONTRACT_REGISTRY as AAVE_REGISTRY, DECODER_MAP as AAVE_DECODER_MAP
from compound_abis import CONTRACT_REGISTRY as COMPOUND_REGISTRY, DECODER_MAP as COMPOUND_DECODER_MAP
from morpho_abis import CONTRACT_REGISTRY as MORPHO_REGISTRY, DECODER_MAP as MORPHO_DECODER_MAP
from decoder import decode_log

CONTRACT_REGISTRY = {**AAVE_REGISTRY, **COMPOUND_REGISTRY, **MORPHO_REGISTRY}
DECODER_MAP = {**AAVE_DECODER_MAP, **COMPOUND_DECODER_MAP, **MORPHO_DECODER_MAP}


log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


# Source
S3_SOURCE   = "s3a://aws-public-blockchain/v1.0/eth/logs"
S3_REGION   = "us-east-2"

# Sink — S3
S3_OUTPUT   = "s3a://money-market/decoded" 

# All contract addresses (lowercase) — used for early filter
ALL_CONTRACTS = {addr.lower() for addr in CONTRACT_REGISTRY.keys()}

# All known topic0s — used for early filter
ALL_TOPIC0S = list({abi["topic0"].lower() for abi in DECODER_MAP.values()})

def get_spark(app_name: str,s3_raw_bucket='money-market') -> SparkSession:
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

def _decode_udf_fn(address: str, topics: list, data: str):
    """
    Thin wrapper around decode_log for the Spark UDF.
    Returns MapType(String, String) — all values stringified.
    The caller casts individual fields when extracting columns.
    """
    try:
        result = decode_log({
            "address": address,
            "topics":  topics,
            "data":    data or "0x",
        })
        if result is None:
            return None
        return {k: str(v) for k, v in result.items()}
    except Exception as exc:  
        log.error("UDF error addr=%s exc=%s", address, exc)
        return None


DECODE_UDF = F.udf(_decode_udf_fn, MapType(StringType(), StringType()))


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
    Apply the decode UDF and explode the result map into a
    'decoded' struct column. Rows that fail to decode are dropped.
    """
    return (
        df
        .withColumn(
            "decoded",
            DECODE_UDF(
                F.lower(F.col("address")),
                F.col("topics"),
                F.col("data"),
            )
        )
        .filter(F.col("decoded").isNotNull())
    )


def shape_deposit(df: DataFrame) -> DataFrame:
    """
    Unified Deposit schema across Aave and Compound.
    """
    return df.filter(F.col("decoded._event").isin("Deposit", "Mint", "Supply")).select(
        F.col("block_number").cast("bigint"),
        F.col("block_timestamp").cast("timestamp"),
        F.col("transaction_hash"),
        F.col("log_index").cast("int"),
        F.coalesce(F.col("decoded._protocol"), F.lit("aave")).alias("protocol"),
        F.col("decoded._version").alias("protocol_version"),
        F.col("date"),   

        F.col("decoded._contract").alias("project_contract"),
        F.col("decoded.reserve").alias("reserve"),
        F.coalesce(
            F.col("decoded.user"),
            F.col("decoded.minter"),
            F.col("decoded.from"),
            F.col("decoded.sender")
        ).alias("user"),
        F.coalesce(
            F.col("decoded.onBehalfOf"),
            F.col("decoded.dst"),
            F.col("decoded.receiver")
        ).alias("on_behalf_of"),
        F.coalesce(
            F.col("decoded.amount"),
            F.col("decoded.mint_amount"),
            F.col("decoded.assets")
        ).cast("decimal(38,0)").alias("amount"),
        F.col("decoded.mint_tokens").cast("decimal(38,0)").alias("mint_tokens"), # Compound V2
        F.col("decoded.referral").cast("int").alias("referral"),
        F.col("decoded.referralCode").cast("int").alias("referral_code"), # v3
        F.col("decoded.timestamp").cast("bigint").alias("chain_timestamp"),  # V1
    )


def shape_withdraw(df: DataFrame) -> DataFrame:
    """
    Unified Withdraw schema across Aave and Compound.
    """
    return df.filter(F.col("decoded._event").isin("Withdraw", "Redeem")).select(
        F.col("block_number").cast("bigint"),
        F.col("block_timestamp").cast("timestamp"),
        F.col("transaction_hash"),
        F.col("log_index").cast("int"),
        F.coalesce(F.col("decoded._protocol"), F.lit("aave")).alias("protocol"),
        F.col("decoded._version").alias("protocol_version"),
        F.col("date"),   

        F.col("decoded._contract").alias("project_contract"),
        F.col("decoded.reserve").alias("reserve"),
        F.coalesce(
            F.col("decoded.user"),
            F.col("decoded.redeemer"),
            F.col("decoded.src"),
            F.col("decoded.caller")
        ).alias("user"),
        F.coalesce(
            F.col("decoded.to"),
            F.col("decoded.user"),
            F.col("decoded.redeemer"),
            F.col("decoded.src"),
            F.col("decoded.reciever")
        ).alias("to"),
        F.coalesce(
            F.col("decoded.amount"),
            F.col("decoded.redeem_amount"),
            F.col("decoded.assets")
        ).cast("decimal(38,0)").alias("amount"),
        F.col("decoded.redeem_tokens").cast("decimal(38,0)").alias("redeem_tokens"), # Compound V2
        F.col("decoded.timestamp").cast("bigint").alias("chain_timestamp"),  # V1
    )


def shape_borrow(df: DataFrame) -> DataFrame:
    """
    Unified Borrow schema across Aave and Compound.
    """
    return df.filter(F.col("decoded._event") == "Borrow").select(
        F.col("block_number").cast("bigint"),
        F.col("block_timestamp").cast("timestamp"),
        F.col("transaction_hash"),
        F.col("log_index").cast("int"),
        F.coalesce(F.col("decoded._protocol"), F.lit("aave")).alias("protocol"),
        F.col("decoded._version").alias("protocol_version"),
        F.col("date"),   

        F.col("decoded._contract").alias("project_contract"),
        F.coalesce(
            F.col("decoded.reserve"),
            F.col("decoded.asset")
        ).alias("reserve"),
        F.coalesce(
            F.col("decoded.user"),
            F.col("decoded.borrower"),
            F.col("decoded.account"),
            F.col("decoded.sender")
        ).alias("user"),
        F.coalesce(
            F.col("decoded.on_behalf_of"),
            F.col("decoded.onBehalfOf"),
            F.col("decoded.receiver")
        ).alias("on_behalf_of"),
        F.coalesce(
            F.col("decoded.amount"),
            F.col("decoded.borrow_amount"),
            F.col("decoded.eth")
        ).cast("decimal(38,0)").alias("amount"),
        F.col("decoded.borrow_rate_mode").cast("int").alias("borrow_rate_mode"),
        F.col("decoded.interest_rate_mode").cast("int").alias("interest_rate_mode"),  # V3
        F.col("decoded.borrow_rate").cast("decimal(38,0)").alias("borrow_rate"),
        F.col("decoded.origination_fee").cast("decimal(38,0)").alias("origination_fee"),            # V1
        F.col("decoded.borrow_balance_increase").cast("decimal(38,0)").alias("borrow_balance_increase"),  # V1
        F.col("decoded.startingBalance").cast("decimal(38,0)").alias("starting_balance"), # Compound V1
        F.col("decoded.borrowAmountWithFee").cast("decimal(38,0)").alias("borrow_amount_with_fee"), # Compound V1
        F.col("decoded.newBalance").cast("decimal(38,0)").alias("new_balance"), # Compound V1
        F.col("decoded.account_borrows").cast("decimal(38,0)").alias("account_borrows"), # Compound V2
        F.col("decoded.total_borrows").cast("decimal(38,0)").alias("total_borrows"), # Compound V2
        F.col("decoded.referral").cast("int").alias("referral_code"),
        F.col("decoded.timestamp").cast("bigint").alias("chain_timestamp"),  # V1
    )


def shape_repay(df: DataFrame) -> DataFrame:
    """
    Unified Repay schema across Aave and Compound.
    """
    return df.filter(F.col("decoded._event").isin("Repay", "RepayBorrow")).select(
        F.col("block_number").cast("bigint"),
        F.col("block_timestamp").cast("timestamp"),
        F.col("transaction_hash"),
        F.col("log_index").cast("int"),
        F.coalesce(F.col("decoded._protocol"), F.lit("aave")).alias("protocol"),
        F.col("decoded._version").alias("protocol_version"),
        F.col("date"),  

        F.col("decoded._contract").alias("project_contract"),
        F.coalesce(
            F.col("decoded.reserve"),
            F.col("decoded.asset")
        ).alias("reserve"),
        F.coalesce(
            F.col("decoded.user"),
            F.col("decoded.borrower"),
            F.col("decoded.account"),
            F.col("decoded.dst")
        ).alias("user"),
        F.coalesce(
            F.col("decoded.repayer"),
            F.col("decoded.payer"),
            F.col("decoded.from")
        ).alias("repayer"),
        F.coalesce(
            F.col("decoded.amount"),
            F.col("decoded.repay_amount")
        ).cast("decimal(38,0)").alias("amount"),
        F.col("decoded.amount_minus_fees").cast("decimal(38,0)").alias("amount_minus_fees"),  # V1
        F.col("decoded.fees").cast("decimal(38,0)").alias("fees"),                            # V1
        F.col("decoded.borrow_balance_increase").cast("decimal(38,0)").alias("borrow_balance_increase"),  # V1
        F.col("decoded.startingBalance").cast("decimal(38,0)").alias("starting_balance"), # Compound V1
        F.col("decoded.newBalance").cast("decimal(38,0)").alias("new_balance"), # Compound V1
        F.col("decoded.account_borrows").cast("decimal(38,0)").alias("account_borrows"), # Compound V2
        F.col("decoded.total_borrows").cast("decimal(38,0)").alias("total_borrows"), # Compound V2
        F.col("decoded.use_a_tokens").cast("boolean").alias("use_a_tokens"),  # V3
        F.col("decoded.timestamp").cast("bigint").alias("chain_timestamp"),   # V1
    )


def shape_liquidation(df: DataFrame) -> DataFrame:
    """
    Unified Liquidation schema across Aave and Compound.
    """
    return df.filter(F.col("decoded._event").isin("LiquidationCall", "LiquidateBorrow", "AbsorbCollateral", "AbsorbDebt")).select(
        F.col("block_number").cast("bigint"),
        F.col("block_timestamp").cast("timestamp"),
        F.col("transaction_hash"),
        F.col("log_index").cast("int"),
        F.coalesce(F.col("decoded._protocol"), F.lit("aave")).alias("protocol"),
        F.col("decoded._version").alias("protocol_version"),
        F.col("date"),  

        F.col("decoded._contract").alias("project_contract"),
        F.coalesce(
            F.col("decoded.collateral_asset"),
            F.col("decoded.collateral"),
            F.col("decoded.assetCollateral"),
            F.col("decoded.c_token_collateral"),
            F.col("decoded.asset")
        ).alias("collateral_asset"),
        F.coalesce(
            F.col("decoded.debt_asset"),
            F.col("decoded.reserve"),
            F.col("decoded.asset_borrow")
        ).alias("debt_asset"),
        F.coalesce(
            F.col("decoded.user"),
            F.col("decoded.target_account"),
            F.col("decoded.borrower")
        ).alias("user"),
        F.coalesce(
            F.col("decoded.debt_to_cover"),
            F.col("decoded.purchase_amount"),
            F.col("decoded.amountRepaid"),
            F.col("decoded.repay_amount"),
            F.col("decoded.base_paid_out")
        ).cast("decimal(38,0)").alias("debt_to_cover"),
        F.coalesce(
            F.col("decoded.liquidated_collateral_amount"),
            F.col("decoded.amountSeized"),
            F.col("decoded.seize_tokens"),
            F.col("decoded.collateral_absorbed")
        ).cast("decimal(38,0)").alias("liquidated_collateral_amount"),
        F.col("decoded.accrued_borrow_interest").cast("decimal(38,0)").alias("accrued_borrow_interest"),  # V1
        F.coalesce(
            F.col("decoded.liquidator"),
            F.col("decoded.absorber")
        ).alias("liquidator"),
        F.col("decoded.receive_a_token").cast("boolean").alias("receive_a_token"),
        F.col("decoded.borrowBalanceBefore").cast("decimal(38,0)").alias("borrow_balance_before"), # Compound V1
        F.col("decoded.borrowBalanceAccumulated").cast("decimal(38,0)").alias("borrow_balance_accumulated"), # Compound V1
        F.col("decoded.borrowBalanceAfter").cast("decimal(38,0)").alias("borrow_balance_after"), # Compound V1
        F.col("decoded.collateralBalanceBefore").cast("decimal(38,0)").alias("collateral_balance_before"), # Compound V1
        F.col("decoded.collateralBalanceAccumulated").cast("decimal(38,0)").alias("collateral_balance_accumulated"), # Compound V1
        F.col("decoded.collateralBalanceAfter").cast("decimal(38,0)").alias("collateral_balance_after"), # Compound V1
        F.col("decoded.usd_value").cast("decimal(38,0)").alias("usd_value"), # Compound V3
        F.col("decoded.timestamp").cast("bigint").alias("chain_timestamp"),  # V1
    )


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
    decoded  = decode_logs(filtered).cache()

    log.info("Decoded row count: %d", decoded.count())
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
        print(event_df.printSchema())

        row_count = event_df.count()
        log.info("  rows: %d", row_count)

        if row_count == 0:
            log.warning("  No rows for %s — skipping write", event_name)
            continue

        if sink in ("s3", "both"):
            write_s3(event_df, event_name)


    decoded.unpersist()
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
