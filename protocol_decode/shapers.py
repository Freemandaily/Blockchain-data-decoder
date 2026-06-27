from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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
