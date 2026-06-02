from web3 import Web3


def _topic0(sig: str) -> str:
    return "0x" + Web3.keccak(text=sig).hex()


# COMPOUND V2 — cToken events
# In Compound V2 every lending market is a separate cToken contract.
# The core money-market events are emitted by each cToken:
#
# Borrow(address borrower, uint256 borrowAmount, uint256 accountBorrows, uint256 totalBorrows)
# RepayBorrow(address payer, address borrower, uint256 repayAmount, uint256 accountBorrows, uint256 totalBorrows)
# LiquidateBorrow(address liquidator, address borrower, uint256 repayAmount, address cTokenCollateral)
# Mint(address minter, uint256 mintAmount, uint256 mintTokens)
# Redeem(address redeemer, uint256 redeemAmount, uint256 redeemTokens)
# None of the V2 events use indexed parameters beyond topic0.


BORROW_V1 = {
    "event":     "Borrow",
    "version":   "v1",
    "protocol":  "compound",
    "signature": "BorrowTaken(address,address,uint256,uint256,uint256,uint256)",
    "topic0":    _topic0("BorrowTaken(address,address,uint256,uint256,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "account",         "type": "address"},
        {"name": "asset",           "type": "address"},
        {"name": "amount",          "type": "uint256"},
        {"name": "startingBalance", "type": "uint256"},
        {"name": "borrowAmountWithFee", "type": "uint256"},
        {"name": "newBalance",      "type": "uint256"},
    ],
}

BORROW_V2 = {
    "event":     "Borrow",
    "version":   "v2",
    "protocol":  "compound",
    "signature": "Borrow(address,uint256,uint256,uint256)",
    "topic0":    _topic0("Borrow(address,uint256,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "borrower",        "type": "address"},
        {"name": "borrow_amount",   "type": "uint256"},
        {"name": "account_borrows", "type": "uint256"},
        {"name": "total_borrows",   "type": "uint256"},
    ],
}


REPAY_V1 = {
    "event":     "RepayBorrow",
    "version":   "v1",
    "protocol":  "compound",
    "signature": "BorrowRepaid(address,address,uint256,uint256,uint256)",
    "topic0":    _topic0("BorrowRepaid(address,address,uint256,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "account",           "type": "address"},
        {"name": "asset",             "type": "address"},
        {"name": "amount",            "type": "uint256"},
        {"name": "startingBalance",   "type": "uint256"},
        {"name": "newBalance",        "type": "uint256"},
    ],
}

REPAY_V2 = {
    "event":     "RepayBorrow",
    "version":   "v2",
    "protocol":  "compound",
    "signature": "RepayBorrow(address,address,uint256,uint256,uint256)",
    "topic0":    _topic0("RepayBorrow(address,address,uint256,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "payer",           "type": "address"},
        {"name": "borrower",       "type": "address"},
        {"name": "repay_amount",    "type": "uint256"},
        {"name": "account_borrows", "type": "uint256"},
        {"name": "total_borrows",   "type": "uint256"},
    ],
}

LIQUIDATION_V1 = {
    "event":     "LiquidateBorrow",
    "version":   "v1",
    "protocol":  "compound",
    "signature": "BorrowLiquidated(address,address,uint256,uint256,uint256,uint256,address,address,uint256,uint256,uint256,uint256)",
    "topic0":    _topic0("BorrowLiquidated(address,address,uint256,uint256,uint256,uint256,address,address,uint256,uint256,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "target_account",               "type": "address"},
        {"name": "asset_borrow",                "type": "address"},
        {"name": "borrowBalanceBefore",             "type": "uint256"},
        {"name": "borrowBalanceAccumulated",       "type": "uint256"},
        {"name": "amountRepaid",             "type": "uint256"},
        {"name": "borrowBalanceAfter",             "type": "uint256"},
        {"name": "liquidator",               "type": "address"},
        {"name": "assetCollateral",                "type": "address"},
        {"name": "collateralBalanceBefore",          "type": "uint256"},
        {"name": "collateralBalanceAccumulated",     "type": "uint256"},
        {"name": "amountSeized",                     "type": "uint256"},
        {"name": "collateralBalanceAfter",           "type": "uint256"},
    ],
}



LIQUIDATION_V2 = {
    "event":     "LiquidateBorrow",
    "version":   "v2",
    "protocol":  "compound",
    "signature": "LiquidateBorrow(address,address,uint256,address,uint256)",
    "topic0":    _topic0("LiquidateBorrow(address,address,uint256,address,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "liquidator",               "type": "address"},
        {"name": "borrower",                "type": "address"},
        {"name": "repay_amount",             "type": "uint256"},
        {"name": "c_token_collateral",       "type": "address"},
        {"name": "seize_tokens",             "type": "uint256"},
    ],
}

DEPOSIT_V2 = {
    "event":     "Mint",
    "version":   "v2",
    "protocol":  "compound",
    "signature": "Mint(address,uint256,uint256)",
    "topic0":    _topic0("Mint(address,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "minter",      "type": "address"},
        {"name": "mint_amount", "type": "uint256"},
        {"name": "mint_tokens", "type": "uint256"},
    ],
}

WITHDRAW_V2 = {
    "event":     "Redeem",
    "version":   "v2",
    "protocol":  "compound",
    "signature": "Redeem(address,uint256,uint256)",
    "topic0":    _topic0("Redeem(address,uint256,uint256)"),
    "indexed":   [],
    "non_indexed": [
        {"name": "redeemer",      "type": "address"},
        {"name": "redeem_amount", "type": "uint256"},
        {"name": "redeem_tokens", "type": "uint256"},
    ],
}


# COMPOUND V3 — Comet events
# Compound V3 (Comet) consolidates lending into a single contract per
# base asset. Key money-market events:

SUPPLY_V3 = {
    "event":     "Supply",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "Supply(address,address,uint256)",
    "topic0":    _topic0("Supply(address,address,uint256)"),
    "indexed": [
        {"name": "from", "type": "address", "topic_pos": 1},
        {"name": "dst",  "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "amount", "type": "uint256"},
    ],
}



SUPPLY_V3_B = {
    "event":     "Supply",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "Supply(address,address,uint256)",
    "topic0":    _topic0("Supply(address,address,uint256)"),
    "indexed": [
        {"name": "sender",   "type": "address", "topic_pos": 1},
        {"name": "receiver", "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "amount",   "type": "uint256"},
    ],
}

WITHDRAW_V3 = {
    "event":     "Withdraw",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "Withdraw(address,address,uint256)",
    "topic0":    _topic0("Withdraw(address,address,uint256)"),
    "indexed": [
        {"name": "src", "type": "address", "topic_pos": 1},
        {"name": "to",  "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "amount", "type": "uint256"},
    ],
}

BORROW_V3_B = {
    "event":     "Borrow",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "withdraw(address,address,uint256)",
    "topic0":    _topic0("withdraw(address,address,uint256)"),
    "indexed":   [
        {"name": "sender",        "type": "address", "topic_pos": 1},
        {"name": "receiver",      "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "eth",          "type": "uint256"},
    ],
}


REPAY_V3 = {
    "event":     "Repay",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "Supply(address,address,uint256)",
    "topic0":    _topic0("Supply(address,address,uint256)"),
    "indexed":   [
        {"name": "from",        "type": "address", "topic_pos": 1},
        {"name": "dst",      "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "amount",        "type": "uint256"}
    ],
}




ABSORB_COLLATERAL_V3 = {
    "event":     "AbsorbCollateral",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "AbsorbCollateral(address,address,address,uint256,uint256)",
    "topic0":    _topic0("AbsorbCollateral(address,address,address,uint256,uint256)"),
    "indexed": [
        {"name": "absorber", "type": "address", "topic_pos": 1},
        {"name": "borrower", "type": "address", "topic_pos": 2},
        {"name": "asset",    "type": "address", "topic_pos": 3},
    ],
    "non_indexed": [
        {"name": "collateral_absorbed", "type": "uint256"},
        {"name": "usd_value",          "type": "uint256"},
    ],
}

ABSORB_DEBT_V3 = {
    "event":     "AbsorbDebt",
    "version":   "v3",
    "protocol":  "compound",
    "signature": "AbsorbDebt(address,address,uint256,uint256)",
    "topic0":    _topic0("AbsorbDebt(address,address,uint256,uint256)"),
    "indexed": [
        {"name": "absorber", "type": "address", "topic_pos": 1},
        {"name": "borrower", "type": "address", "topic_pos": 2},
    ],
    "non_indexed": [
        {"name": "base_paid_out", "type": "uint256"},
        {"name": "usd_value",     "type": "uint256"},
    ],
}


# CONTRACT REGISTRY
# Maps each contract address → list of event ABIs that contract emits.
#
# V2: Each cToken is its own contract. Major Ethereum mainnet markets:
#   cETH:  0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5
#   cDAI:  0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
#   cUSDC: 0x39aa39c021dfbae8fac545936693ac917d5e7563
#   cUSDT: 0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9
#   cWBTC: 0xc11b1268c1a384e55c48c2391d8d480264a3a7f4
#   cBAT:  0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e
#   cLINK: 0xface851a4921ce59e912d19329929ce6da6eb0c7
#   cCOMP: 0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4
#
# V3 (Comet): One contract per base asset
#   USDC Comet:  0xc3d688b66703497daa19211eedff47f25384cdc3
#   WETH Comet:  0xa17581a9e3356d9a858b789d68b4d866e593ae94

_v1_EVENTS = [BORROW_V1,REPAY_V1,LIQUIDATION_V1]
_V2_EVENTS = [BORROW_V2, REPAY_V2, LIQUIDATION_V2,DEPOSIT_V2,WITHDRAW_V2]
_V3_EVENTS = [SUPPLY_V3,WITHDRAW_V3,SUPPLY_V3_B,BORROW_V3_B,REPAY_V3,ABSORB_COLLATERAL_V3, ABSORB_DEBT_V3]

CONTRACT_REGISTRY: dict[str, list[dict]] = {

    "0x3fda67f7583380e67ef93072294a7fac882fd7e7": _v1_EVENTS,
    "0xbd8e0def19aabbc3751b0b5ee0558cadffce759b": _v1_EVENTS,
    "0x158079ee67fce2f58472a96584a73c7ab9ac95c1": _V2_EVENTS,
    "0xf5dce57282a584d2746faf1593d3121fcac444dc": _V2_EVENTS,
    "0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407": _V2_EVENTS,
    "0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5": _V2_EVENTS,  # cETH
    "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643": _V2_EVENTS,  # cDAI
    "0x39aa39c021dfbae8fac545936693ac917d5e7563": _V2_EVENTS,  # cUSDC
    "0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9": _V2_EVENTS,  # cUSDT
    "0xc11b1268c1a384e55c48c2391d8d480264a3a7f4": _V2_EVENTS,  # cWBTC
    "0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e": _V2_EVENTS,  # cBAT
    "0xface851a4921ce59e912d19329929ce6da6eb0c7": _V2_EVENTS,  # cLINK
    "0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4": _V2_EVENTS,  # cCOMP
    "0xc3d688b66703497daa19211eedff47f25384cdc3": _V3_EVENTS,  # USDC Comet
    "0xa17581a9e3356d9a858b789d68b4d866e593ae94": _V3_EVENTS,  # WETH Comet

    "0x3afdc9bca9213a35503b077a6072f3d0d5ab0840": _V3_EVENTS,
    "0xa17581a9e3356d9a858b789d68b4d866e593ae94": _V3_EVENTS

}


# DECODER_MAP  —  (contract_address, topic0) → abi dict

# This is the single lookup used by the decoder/spark UDF to route
# a raw log to the correct shaper function.

DECODER_MAP: dict[tuple[str, str], dict] = {
    (contract, abi["topic0"]): abi
    for contract, abis in CONTRACT_REGISTRY.items()
    for abi in abis
}

if __name__ == "__main__":
    print(f"{'Contract':<46} {'Event':<22} {'Version':<8} {'topic0':<4}")
    print("-" * 130)
    for (contract, t0), abi in sorted(DECODER_MAP.items()):
        print(f"{contract:<46} {abi['event']:<22} {abi['version']:<8} {t0}")
    print(f"\nTotal entries: {len(DECODER_MAP)}")
