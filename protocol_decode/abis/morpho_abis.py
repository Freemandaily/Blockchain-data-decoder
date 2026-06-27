import logging
from web3 import Web3


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _topic0(sig: str) -> str:
    return "0x" + Web3.keccak(text=sig).hex()


BORROW_V1 = {
    "event":     "Borrow",
    "version":   "v1",
    'protocol': 'morpho',
    "signature": "Borrow(bytes32,address,address,address,uint256,uint256)",
    "topic0":    _topic0("Borrow(bytes32,address,address,address,uint256,uint256)"),
    "indexed": [
        {"name": "id",           "type": "bytes32", "topic_pos": 1},
        {"name": "onBehalf",     "type": "address", "topic_pos": 2},
        {"name": "receiver",     "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "caller",                 "type": "address"},
        {"name": "assets",                 "type": "uint256"},
        {"name": "shares",                 "type": "uint256"},
    ],
}


REPAY_V1 = {
    "event":     "Repay",
    "version":   "v1",
    "protocol": 'morpho',
    "signature": "Repay(bytes32,address,address,uint256)",
    "topic0":    _topic0("Repay(bytes32,address,address,uint256)"),
    "indexed": [
        {"name": "id",          "type": "bytes32", "topic_pos": 1},
        {"name": "caller",        "type": "address", "topic_pos": 2},
        {"name": "onBehalf",     "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "shares",        "type": "uint256"}
    ]
}



LIQUIDATION_V1 = {
    "event":     "Liquidate",
    "version":   "v1",
    "protocol": 'morpho',
    # V1 has 9 params including accruedBorrowInterest and timestamp
    "signature": "Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
    "topic0":    _topic0("Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"),
    "indexed": [
        {"name": "id",               "type": "bytes32", "topic_pos": 1},
        {"name": "caller",           "type": "address", "topic_pos": 2},
        {"name": "borrower",         "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "repaidAssets",       "type": "uint256"},
        {"name": "repaidShares",       "type": "uint256"},
        {"name": "seizedAssets",       "type": "uint256"},
        {"name": "badDebtAssets",      "type": "uint256"},
        {"name": "badDebtShares",      "type": "uint256"}
    ]   
}



DEPOSIT_V1 = {
    "event":     "Supply",
    "version":   "v1",
    "protocol": 'morpho',
    "signature": "Supply(bytes32,address,address,uint256,uint256)",
    "topic0":    _topic0("Supply(bytes32,address,address,uint256,uint256)"),
    "indexed": [
        {"name": "id",        "type": "bytes32", "topic_pos": 1},
        {"name": "caller",     "type": "address", "topic_pos": 2},
        {"name": "onBehalf",    "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "assets",      "type": "uint256"},
        {"name": "shares",      "type": "uint256"},
    ],
}


WITHDRAW_V1 = {
    "event":     "Withdraw",
    "version":   "v1",
    'protocol': 'morpho',
    "signature": "Withdraw(bytes32,address,address,uint256,uint256)",
    "topic0":    _topic0("Withdraw(bytes32,address,address,uint256,uint256)"),
    "indexed": [
        {"name": "id",        "type": "bytes32", "topic_pos": 1},
        {"name": "onBehalf",     "type": "address", "topic_pos": 2},
        {"name": "receiver",    "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "caller",      "type": "address"},
        {"name": "assets",      "type": "uint256"},
        {"name": "shares",      "type": "uint256"},
    ],
}   


CONTRACT_REGISTRY: dict[str, list[dict]] = {
    "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb": [DEPOSIT_V1,WITHDRAW_V1,BORROW_V1, REPAY_V1, LIQUIDATION_V1]
}


DECODER_MAP: dict[tuple[str, str], dict] = {
    (contract, abi["topic0"]): abi
    for contract, abis in CONTRACT_REGISTRY.items()
    for abi in abis
}


# quick sanity print when run directly   
if __name__ == "__main__":
    logger.info(f"{'Contract':<46} {'Event':<20} {'Version':<8} {'topic0':<4}")
    logger.info("-" * 130)
    for (contract, t0), abi in sorted(DECODER_MAP.items()):
        logger.info(f"{contract:<46} {abi['event']:<20} {abi['version']:<8} {t0}")
    logger.info(f"\nTotal entries: {len(DECODER_MAP)}")
