from web3 import Web3

def _topic0(sig: str) -> str:
    return "0x" + Web3.keccak(text=sig).hex()


BORROW_V1 = {
    "event":     "Borrow",
    "version":   "v1",
    'protocol': 'aave',
    "signature": "Borrow(address,address,uint256,uint256,uint256,uint256,uint256,uint16,uint256)",
    "topic0":    _topic0("Borrow(address,address,uint256,uint256,uint256,uint256,uint256,uint16,uint256)"),
    "indexed": [
        {"name": "reserve",  "type": "address", "topic_pos": 1},
        {"name": "user",     "type": "address", "topic_pos": 2},
        {"name": "referral", "type": "uint16",  "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "amount",                 "type": "uint256"},
        {"name": "borrow_rate_mode",       "type": "uint256"},
        {"name": "borrow_rate",            "type": "uint256"},
        {"name": "origination_fee",        "type": "uint256"},
        {"name": "borrow_balance_increase","type": "uint256"},
        {"name": "timestamp",              "type": "uint256"},
    ],
}

BORROW_V2 = {
    "event":     "Borrow",
    "version":   "v2",
    'protocol': 'aave',
    "signature": "Borrow(address,address,address,uint256,uint256,uint256,uint16)",
    "topic0":    _topic0("Borrow(address,address,address,uint256,uint256,uint256,uint16)"),
    "indexed": [
        {"name": "reserve",      "type": "address", "topic_pos": 1},
        {"name": "on_behalf_of", "type": "address", "topic_pos": 2},
        {"name": "referral",     "type": "uint16",  "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "user",             "type": "address"},
        {"name": "amount",           "type": "uint256"},
        {"name": "borrow_rate_mode", "type": "uint256"},
        {"name": "borrow_rate",      "type": "uint256"},
    ],
}

BORROW_V3 = {
    "event":     "Borrow",
    "version":   "v3",
    "protocol": 'aave',
    "signature": "Borrow(address,address,address,uint256,uint8,uint256,uint16)",
    "topic0":    _topic0("Borrow(address,address,address,uint256,uint8,uint256,uint16)"),
    "indexed": [
        {"name": "reserve",      "type": "address", "topic_pos": 1},
        {"name": "on_behalf_of", "type": "address", "topic_pos": 2},
        {"name": "referral",     "type": "uint16",  "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "user",               "type": "address"},
        {"name": "amount",             "type": "uint256"},
        {"name": "interest_rate_mode", "type": "uint8"},   # uint8 in V3
        {"name": "borrow_rate",        "type": "uint256"},
    ],
}


REPAY_V1 = {
    "event":     "Repay",
    "version":   "v1",
    "protocol": 'aave',
    "signature": "Repay(address,address,address,uint256,uint256,uint256,uint256)",
    "topic0":    _topic0("Repay(address,address,address,uint256,uint256,uint256,uint256)"),
    "indexed": [
        {"name": "reserve", "type": "address", "topic_pos": 1},
        {"name": "user",    "type": "address", "topic_pos": 2},
        {"name": "repayer", "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "amount_minus_fees",        "type": "uint256"},
        {"name": "fees",                     "type": "uint256"},
        {"name": "borrow_balance_increase",  "type": "uint256"},
        {"name": "timestamp",                "type": "uint256"},
    ],
}

REPAY_V2 = {
    "event":     "Repay",
    "version":   "v2",
    # V2 dropped fees, borrow_balance_increase, timestamp — only amount remains
    "protocol": 'aave',
    "signature": "Repay(address,address,address,uint256)",
    "topic0":    _topic0("Repay(address,address,address,uint256)"),
    "indexed": [
        {"name": "reserve", "type": "address", "topic_pos": 1},
        {"name": "user",    "type": "address", "topic_pos": 2},
        {"name": "repayer", "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "amount", "type": "uint256"},
    ],
}

REPAY_V3 = {
    "event":     "Repay",
    "version":   "v3",
    "protocol": 'aave',
    # V3 added bool useATokens
    "signature": "Repay(address,address,address,uint256,bool)",
    "topic0":    _topic0("Repay(address,address,address,uint256,bool)"),
    "indexed": [
        {"name": "reserve", "type": "address", "topic_pos": 1},
        {"name": "user",    "type": "address", "topic_pos": 2},
        {"name": "repayer", "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "amount",       "type": "uint256"},
        {"name": "use_a_tokens", "type": "bool"},
    ],
}


# LIQUIDATION CALL

LIQUIDATION_V1 = {
    "event":     "LiquidationCall",
    "version":   "v1",
    "protocol": 'aave',
    # V1 has 9 params including accruedBorrowInterest and timestamp
    "signature": "LiquidationCall(address,address,address,uint256,uint256,uint256,address,bool,uint256)",
    "topic0":    _topic0("LiquidationCall(address,address,address,uint256,uint256,uint256,address,bool,uint256)"),
    "indexed": [
        {"name": "collateral", "type": "address", "topic_pos": 1},
        {"name": "reserve",    "type": "address", "topic_pos": 2},
        {"name": "user",       "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "purchase_amount",               "type": "uint256"},
        {"name": "liquidated_collateral_amount",  "type": "uint256"},
        {"name": "accrued_borrow_interest",       "type": "uint256"},
        {"name": "liquidator",                    "type": "address"},
        {"name": "receive_a_token",               "type": "bool"},
        {"name": "timestamp",                     "type": "uint256"},
    ],
}

# V2 and V3 share the same signature → same topic0
# They are disambiguated by contract address in the registry
_LIQUIDATION_V2V3_SIG = "LiquidationCall(address,address,address,uint256,uint256,address,bool)"

LIQUIDATION_V2 = {
    "event":     "LiquidationCall",
    "version":   "v2",
    "protocol": 'aave',
    "signature": _LIQUIDATION_V2V3_SIG,
    "topic0":    _topic0(_LIQUIDATION_V2V3_SIG),
    "indexed": [
        {"name": "collateral_asset", "type": "address", "topic_pos": 1},
        {"name": "debt_asset",       "type": "address", "topic_pos": 2},
        {"name": "user",             "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "debt_to_cover",                "type": "uint256"},
        {"name": "liquidated_collateral_amount", "type": "uint256"},
        {"name": "liquidator",                   "type": "address"},
        {"name": "receive_a_token",              "type": "bool"},
    ],
}

LIQUIDATION_V3 = {
    "event":     "LiquidationCall",
    "version":   "v3",
    "protocol": 'aave',
    "signature": _LIQUIDATION_V2V3_SIG,   # same as V2
    "topic0":    _topic0(_LIQUIDATION_V2V3_SIG), # topic hash
    "indexed": [
        {"name": "collateral_asset", "type": "address", "topic_pos": 1},
        {"name": "debt_asset",       "type": "address", "topic_pos": 2},
        {"name": "user",             "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "debt_to_cover",                "type": "uint256"},
        {"name": "liquidated_collateral_amount", "type": "uint256"},
        {"name": "liquidator",                   "type": "address"},
        {"name": "receive_a_token",              "type": "bool"},
    ],
}


DEPOSIT_V1 = {
    "event":     "Deposit",
    "version":   "v1",
    "protocol": 'aave',
    "signature": "Deposit(address,address,uint256,uint16,uint256)",
    "topic0":    _topic0("Deposit(address,address,uint256,uint16,uint256)"),
    "indexed": [
        {"name": "reserve",     "type": "address", "topic_pos": 1},
        {"name": "user",        "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "amount",      "type": "uint256"},
        {"name": "timestamp",   "type": "uint256"},
    ],
}

DEPOSIT_V2 = {
    "event":     "Deposit",
    "version":   "v2",
    "protocol": 'aave',
    "signature": "Deposit(address,address,address,uint256,uint16)",
    "topic0":    _topic0("Deposit(address,address,address,uint256,uint16)"),
    "indexed": [
        {"name": "reserve",     "type": "address", "topic_pos": 1},
        {"name": "onBehalfOf",  "type": "address", "topic_pos": 2},
        {"name": "referral",    "type": "uint16", "topic_pos": 3}
    ],
    "non_indexed": [
        {"name": "user",      "type": "address"},
        {"name": "amount",    "type": "uint256"}
    ],
}

DEPOSIT_V3 = {
    "event":     "Deposit",
    "version":   "v3",
    'protocol': 'aave',
    "signature": "Supply(address,address,address,uint256,uint16)",
    "topic0":    _topic0("Supply(address,address,address,uint256,uint16)"),
    "indexed": [
        {"name": "reserve",     "type": "address", "topic_pos": 1},
        {"name": "onBehalfOf",  "type": "address", "topic_pos": 2},
        {"name": "referralCode","type": "uint16", "topic_pos": 3}
    ],
    "non_indexed": [
        {"name": "user",        "type": "address"},
        {"name": "amount",      "type": "uint256"}
    ],
}

WITHDRAW_V1 = {
    "event":     "Withdraw",
    "version":   "v1",
    'protocol': 'aave',
    "signature": "Withdraw(address,address,uint256,uint256)",
    "topic0":    _topic0("Withdraw(address,address,uint256,uint256)"),
    "indexed": [
        {"name": "reserve",     "type": "address", "topic_pos": 1},
        {"name": "user",        "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "amount",      "type": "uint256"},
        {"name": "timestamp",   "type": "uint256"},
    ],
}

WITHDRAW_V2 = {
    "event":     "Withdraw",
    "version":   "v2",
    "protocol": 'aave',
    "signature": "Withdraw(address,address,address,uint256)",
    "topic0":    _topic0("Withdraw(address,address,address,uint256)"),
    "indexed": [
        {"name": "reserve",     "type": "address", "topic_pos": 1},
        {"name": "user",        "type": "address", "topic_pos": 2},
        {"name": "to",          "type": "address", "topic_pos": 3}
    ],
    "non_indexed": [
        {"name": "amount",      "type": "uint256"}
    ],
}

WITHDRAW_V3 = {
    "event":     "Withdraw",
    "version":   "v3",
    "protocol": 'aave',
    "signature": "Withdraw(address,address,address,uint256)",
    "topic0":    _topic0("Withdraw(address,address,address,uint256)"),
    "indexed": [
        {"name": "reserve",     "type": "address", "topic_pos": 1},
        {"name": "user",        "type": "address", "topic_pos": 2},
        {"name": "to",          "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "amount",      "type": "uint256"}
    ],
}


CONTRACT_REGISTRY: dict[str, list[dict]] = {
    #  V1 pools (multiple deployments, same ABI) 
    "0x398ec7346dcd622edc5ae82352f02be94c62d119": [DEPOSIT_V1,WITHDRAW_V1,BORROW_V1, REPAY_V1, LIQUIDATION_V1],
    "0x2f60c3eb259d63dcca81fde7eaa216d9983d7c60": [DEPOSIT_V1,WITHDRAW_V1,BORROW_V1, REPAY_V1, LIQUIDATION_V1],
    "0x633c23fc727e8afd3546d0cf86c8644f0fff16a6": [DEPOSIT_V1,WITHDRAW_V1,BORROW_V1, REPAY_V1],
    "0x5f93ad786a1330d2ec97926f073259f70375d146": [DEPOSIT_V1],
    #  V2 pool 
    "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9": [DEPOSIT_V2,WITHDRAW_V2,BORROW_V2, REPAY_V2, LIQUIDATION_V2],

    #  V3 pool 
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": [DEPOSIT_V3,WITHDRAW_V3,BORROW_V3, REPAY_V3, LIQUIDATION_V3],

}


DECODER_MAP: dict[tuple[str, str], dict] = {
    (contract, abi["topic0"]): abi
    for contract, abis in CONTRACT_REGISTRY.items()
    for abi in abis
}


# quick sanity print when run directly   
if __name__ == "__main__":
    print(f"{'Contract':<46} {'Event':<20} {'Version':<8} {'topic0':<4}")
    print("-" * 130)
    for (contract, t0), abi in sorted(DECODER_MAP.items()):
        print(f"{contract:<46} {abi['event']:<20} {abi['version']:<8} {t0}")
    print(f"\nTotal entries: {len(DECODER_MAP)}")
