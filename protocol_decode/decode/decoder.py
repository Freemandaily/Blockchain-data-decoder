
import logging
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi.exceptions import DecodingError

from abis.aave_abis import DECODER_MAP as AAVE_DECODER_MAP
from abis.compound_abis import DECODER_MAP as COMPOUND_DECODER_MAP
from abis.morpho_abis import DECODER_MAP as MORPHO_DECODER_MAP


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DECODER_MAP = {**AAVE_DECODER_MAP, **COMPOUND_DECODER_MAP, **MORPHO_DECODER_MAP}

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
