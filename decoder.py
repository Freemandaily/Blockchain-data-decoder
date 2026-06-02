
import logging
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi.exceptions import DecodingError

from aave_abis import DECODER_MAP as AAVE_DECODER_MAP
from compound_abis import DECODER_MAP as COMPOUND_DECODER_MAP

DECODER_MAP = {**AAVE_DECODER_MAP, **COMPOUND_DECODER_MAP}


log = logging.getLogger(__name__)


def _decode_topic_value(raw: str, solidity_type: str) -> Any:
    
    raw_hex = raw.removeprefix("0x").lower().zfill(64)

    if solidity_type == "address":
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
        n = int(solidity_type[5:]) if solidity_type[5:] else 32
        return "0x" + raw_hex[: n * 2]
    return "0x" + raw_hex


def _decode_data_field(data: str, non_indexed: list[dict]) -> dict[str, Any]:

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
        if isinstance(value, bytes):
            result[name] = "0x" + value.hex()
        elif isinstance(value, str) and value.startswith("0x"):
            result[name] = value.lower()
        else:
            result[name] = value

    return result


def decode_log(row: dict) -> dict | None:

    address = row["address"].lower()
    topics  = row["topics"]          
    data    = row.get("data", "0x")

    if not topics:
        return None

    topic0 = topics[0].lower()
    abi    = DECODER_MAP.get((address, topic0))

    if abi is None:
        return None

    result: dict[str, Any] = {
        "_event":    abi["event"],
        "_version":  abi["version"],
        "_contract": address,
        "_protocol": abi.get("protocol", "aave"),
    }

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


if __name__ == "__main__":
    import json

    def print_test(label: str, raw_log: dict):
        print(f"\n{'═' * 70}")
        print(f"  TEST : {label}")
        print(f"{'─' * 70}")
        print(f"  contract : {raw_log['address']}")
        for i, t in enumerate(raw_log["topics"]):
            print(f"  topics[{i}] : {t}")
        print(f"  data     : {raw_log['data'][:66]}{'…' if len(raw_log['data']) > 66 else ''}")
        print(f"{'─' * 70}")
        result = decode_log(raw_log)
        if result is None:
            print("  → None  (not in registry or decode failed)")
        else:
            print(json.dumps(result, indent=4, default=str))

    # 1. Aave V2 Borrow
    print_test("Aave V2 Borrow", {
        "address": "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9",
        "topics": [
            "0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b",
            "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0x0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8",
            "0x0000000000000000000000000000000000000000000000000000000000000000",
        ],
        "data": (
            "0x"
            "0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8"
            "000000000000000000000000000000000000000000000000000000003b9aca00"
            "0000000000000000000000000000000000000000000000000000000000000002"
            "00000000000000000000000000000000000000000002ca4a2e7ce4d5c4000000"
        ),
    })

    # 2. Compound V2 Mint
    print_test("Compound V2 Mint", {
        "address": "0x39aa39c021dfbae8fac545936693ac917d5e7563",
        "topics": [
            "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
        ],
        "data": (
            "0x"
            "0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8"  # minter
            "000000000000000000000000000000000000000000000000000000003b9aca00"  # mintAmount
            "000000000000000000000000000000000000000000000000000000000000c350"  # mintTokens
        ),
    })

    # 3. Compound V3 Supply
    print_test("Compound V3 Supply", {
        "address": "0xc3d688b66703497daa19211eedff47f25384cdc3",
        "topics": [
            "0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e",
            "0x0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8",
            "0x0000000000000000000000008acaab8167c80cb8b3de7fa6228b889bb1130ee8",
        ],
        "data": (
            "0x"
            "000000000000000000000000000000000000000000000000000000003b9aca00"  # amount
        ),
    })




