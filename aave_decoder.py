
import logging
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi.exceptions import DecodingError

from aave_abis import DECODER_MAP

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# LOW-LEVEL HELPERS
# ═══════════════════════════════════════════════════════════════════

def _decode_topic_value(raw: str, solidity_type: str) -> Any:
    """
    Decode a single 32-byte topic value into a Python native.

    Topics are always 32 bytes (64 hex chars after the 0x prefix).
    - address  → strip leading 12-byte padding, return lowercase 0x-prefixed string
    - uintN    → interpret full 32 bytes as big-endian unsigned int
    - intN     → same but signed
    - bool     → last byte != 0
    - bytesN   → left-aligned; return as 0x hex string

    eth_abi.decode can handle this too, but doing it inline avoids
    the overhead of constructing a bytes object for every topic.
    """
    # Normalise: strip 0x, ensure exactly 64 chars
    raw_hex = raw.removeprefix("0x").lower().zfill(64)

    if solidity_type == "address":
        # Last 20 bytes (40 hex chars) are the address
        return "0x" + raw_hex[-40:]

    if solidity_type == "bool":
        return int(raw_hex, 16) != 0

    if solidity_type.startswith("uint"):
        return int(raw_hex, 16)

    if solidity_type.startswith("int"):
        bits = int(solidity_type[3:]) if solidity_type[3:] else 256
        value = int(raw_hex, 16)
        if value >= (1 << (bits - 1)):
            value -= 1 << bits
        return value

    if solidity_type.startswith("bytes"):
        # bytesN is left-aligned in the 32-byte slot
        n = int(solidity_type[5:]) if solidity_type[5:] else 32
        return "0x" + raw_hex[: n * 2]

    # Fallback — return raw hex
    log.warning("Unknown indexed type %s — returning raw hex", solidity_type)
    return "0x" + raw_hex


def _decode_data_field(data: str, non_indexed: list[dict]) -> dict[str, Any]:
    """
    ABI-decode the log's data field using eth_abi.

    eth_abi.decode takes ALL non-indexed types in one call and returns
    a tuple in the same order — no manual slot arithmetic needed.
    """
    if not non_indexed:
        return {}

    types = [f["type"] for f in non_indexed]
    names = [f["name"] for f in non_indexed]

    raw_bytes = bytes.fromhex(data.removeprefix("0x"))

    try:
        decoded_tuple = abi_decode(types, raw_bytes)
    except DecodingError as exc:
        raise ValueError(f"eth_abi failed to decode data field: {exc}") from exc

    result = {}
    for name, value in zip(names, decoded_tuple):
        # Normalise bytes values → 0x hex string for consistency
        if isinstance(value, bytes):
            result[name] = "0x" + value.hex()
        # eth_abi returns addresses as checksummed strings already
        elif isinstance(value, str) and value.startswith("0x"):
            result[name] = value.lower()
        else:
            result[name] = value

    return result


# ═══════════════════════════════════════════════════════════════════
# MAIN DECODER
# ═══════════════════════════════════════════════════════════════════

def decode_log(row: dict) -> dict | None:
    """
    Decode a single raw EVM log row.

    Parameters
    ----------
    row : dict
        Must contain:
            address  str        — contract address (any case, 0x-prefixed)
            topics   list[str]  — list of 0x-prefixed 32-byte hex strings
            data     str        — 0x-prefixed hex string (may be "0x" if empty)

    Returns
    -------
    dict  — decoded fields + metadata, or None if the log is not in the registry.

    Metadata fields always present on a successful decode
    -----------------------------------------------------
        _event    str   — e.g. "Borrow"
        _version  str   — e.g. "v2"
        _contract str   — lowercase contract address
    """
    address = row["address"].lower()
    topics  = row["topics"]          # list of 0x strings
    data    = row.get("data", "0x")

    if not topics:
        return None

    topic0 = topics[0].lower()
    abi    = DECODER_MAP.get((address, topic0))

    if abi is None:
        return None  # not a tracked event — caller decides whether to warn

    result: dict[str, Any] = {
        "_event":    abi["event"],
        "_version":  abi["version"],
        "_contract": address,
    }

    # ── 1. Decode indexed params from topics ──────────────────────
    for field in abi["indexed"]:
        pos = field["topic_pos"]
        if pos >= len(topics):
            log.warning(
                "Event %s %s: expected topic at pos %d but only %d topics present",
                abi["event"], abi["version"], pos, len(topics),
            )
            result[field["name"]] = None
            continue
        result[field["name"]] = _decode_topic_value(topics[pos], field["type"])

    # ── 2. Decode non-indexed params from data field ──────────────
    try:
        decoded_data = _decode_data_field(data, abi["non_indexed"])
        result.update(decoded_data)
    except ValueError as exc:
        log.error(
            "Failed to decode data for %s %s contract=%s: %s",
            abi["event"], abi["version"], address, exc,
        )
        return None

    return result


# ═══════════════════════════════════════════════════════════════════
# SPARK UDF
# ═══════════════════════════════════════════════════════════════════
# Import this block in your Spark job — do not import Spark here so
# the module stays usable in plain Python tests too.

def make_spark_udf():
    """
    Returns a Spark UDF that wraps decode_log.

    The UDF returns a MapType(StringType, StringType) — all values are
    cast to string so the schema stays generic across all event types.
    Callers then extract and cast individual fields as needed.

    Usage in a Spark job
    --------------------
        from aave_decoder import make_spark_udf
        from pyspark.sql import functions as F

        decode_udf = make_spark_udf()

        decoded_df = (
            filtered_df
            .withColumn("decoded", decode_udf(F.col("address"), F.col("topics"), F.col("data")))
            .filter(F.col("decoded").isNotNull())
        )

        # Extract individual fields
        borrow_df = (
            decoded_df
            .filter(F.col("decoded._event") == "Borrow")
            .select(
                F.col("block_number"),
                F.col("block_timestamp"),
                F.col("transaction_hash"),
                F.col("log_index"),
                F.col("decoded._version").alias("version"),
                F.col("decoded.reserve").alias("reserve"),
                F.col("decoded.user").alias("user"),
                F.col("decoded.amount").cast("decimal(38,0)").alias("amount"),
                F.col("decoded.borrow_rate").cast("decimal(38,0)").alias("borrow_rate"),
            )
        )
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import MapType, StringType

    def _udf_fn(address: str, topics: list, data: str):
        try:
            result = decode_log({"address": address, "topics": topics, "data": data or "0x"})
            if result is None:
                return None
            # Stringify all values — Spark MapType(String,String) is simplest
            # and avoids schema conflicts across event types
            return {k: str(v) for k, v in result.items()}
        except Exception as exc:   # noqa: BLE001
            log.error("UDF decode_log error: %s", exc)
            return None

    return udf(_udf_fn, MapType(StringType(), StringType()))


# ═══════════════════════════════════════════════════════════════════
# QUICK SMOKE TEST  (python aave_decoder.py)
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json

    # ── Test 1: Borrow V2 ─────────────────────────────────────────
    # Constructed from the V2 ABI shape:
    #   indexed:     reserve=USDC, onBehalfOf=someUser, referral=0
    #   non_indexed: user=someUser, amount=1000e6, borrowRateMode=2, borrowRate=5e25
    test_borrow_v2 = {
        "address": "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9",
        "topics": [
            "0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b",
            # reserve = USDC
            "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            # onBehalfOf
            "0x0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8",
            # referral = 0
            "0x0000000000000000000000000000000000000000000000000000000000000000",
        ],
        # non_indexed: user, amount=1000 USDC (6 decimals = 1_000_000_000), borrowRateMode=2, borrowRate
        "data": (
            "0x"
            + "0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8"  # user
            + "000000000000000000000000000000000000000000000000000000003b9aca00"  # amount = 1e9
            + "0000000000000000000000000000000000000000000000000000000000000002"  # borrowRateMode = 2
            + "00000000000000000000000000000000000000000002ca4a2e7ce4d5c4000000"  # borrowRate
        ),
    }

    # ── Test 2: Repay V3 ──────────────────────────────────────────
    test_repay_v3 = {
        "address": "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "topics": [
            "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
            "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # reserve
            "0x0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8",  # user
            "0x0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8",  # repayer
        ],
        "data": (
            "0x"
            + "000000000000000000000000000000000000000000000000000000003b9aca00"  # amount
            + "0000000000000000000000000000000000000000000000000000000000000000"  # useATokens = False
        ),
    }

    # ── Test 3: LiquidationCall V1 ────────────────────────────────
    test_liq_v1 = {
        "address": "0x398ec7346dcd622edc5ae82352f02be94c62d119",
        "topics": [
            "0x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237",
            "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # collateral=WETH
            "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # reserve=USDC
            "0x000000000000000000000000deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",  # user
        ],
        "data": (
            "0x"
            + "000000000000000000000000000000000000000000000000000000003b9aca00"  # purchaseAmount
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # liquidatedCollateral
            + "00000000000000000000000000000000000000000000000000038d7ea4c68000"  # accruedBorrowInterest
            + "000000000000000000000000ab5801a7d398351b8be11c439e05c5b3259aec9"  # liquidator
            + "0000000000000000000000000000000000000000000000000000000000000001"  # receiveAToken = True
            + "0000000000000000000000000000000000000000000000000000000060000000"  # timestamp
        ),
    }

    # ── Test 4: unknown event — should return None ─────────────────
    test_unknown = {
        "address": "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9",
        "topics": ["0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"],
        "data": "0x",
    }

    tests = [
        ("Borrow V2",         test_borrow_v2),
        ("Repay V3",          test_repay_v3),
        ("LiquidationCall V1",test_liq_v1),
        ("Unknown event",     test_unknown),
    ]

    for label, row in tests:
        print(f"\n{'─'*60}")
        print(f"TEST: {label}")
        result = decode_log(row)
        if result is None:
            print("  → None (not in registry or decode failed)")
        else:
            print(json.dumps(result, indent=2, default=str))
