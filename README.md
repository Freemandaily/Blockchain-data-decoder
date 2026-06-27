# EVM Blockchain Log Decoder (Multi-Protocol Money-Market Series)

A high-performance, highly extensible PySpark batch pipeline designed to read, filter, decode, and shape raw Ethereum event logs from the AWS public blockchain dataset into structured, unified DeFi event tables.

This project implements a **generalized money-market decoder** that supports multiple protocols under a single unified pipeline. Currently, it integrates **Aave (V1, V2, V3)**, **Morpho (V1)** and **Compound (V1, V2, V3)** event tracking, extracting and normalizing data for critical actions: **Deposit/Mint/Supply**, **Withdraw/Redeem**, **Borrow**, **Repay**, and **LiquidationCall/Absorb**. The architecture is explicitly decoupled to allow onboarding new EVM-based money markets (e.g., Morpho, Spark, MakerDAO) seamlessly.

---

## 📂 Core Architecture & Modules

The pipeline is split into separate, highly decoupled modules:

### 1. Registry & Schema Layers (`abis/`)
Acts as the **Protocol Registry and Schema definition layer**.
* Stores event signatures, schemas, and `topic0` hashes for their respective protocols (`aave_abis.py`, `compound_abis.py`, `morpho_abis.py`).
* Maps lowercase contract addresses to their respective event decoders to easily route log parsing.
* Dynamically hashes signatures using `web3` (or supports pre-computed values for performance).
* Identifies each event definition with a `protocol` attribute (e.g., `"aave"`, `"compound"`).

### 2. Low-Level Log Decoder (`decode/decoder.py`)
Implements **low-level EVM log decoding logic**.
* Imports registries from all registered protocols and combines their registries and `DECODER_MAP` lookups into a unified registry.
* Decodes indexed parameters from `topics` slots.
* Employs the fast `eth_abi` library to decode non-indexed parameters from the log's `data` payload.
* Dynamically injects metadata (such as `_protocol`, `_version`, and `_contract`) into decoded payloads.
* Exposes a unified `decode_log` function that functions in isolation or inside a PySpark worker environment.

### 3. PySpark ETL Engine (`spark_job.py`, `config/`, `shapers/`)
The **ETL engine and data shaper**.
* Connects to public Ethereum log parquet datasets on AWS S3 using configurations from `config.py`.
* Combines the contract addresses and topic0s of all protocols into a single, broadcast-friendly PySpark filter to perform extremely fast, early filtering without shuffles.
* Runs the decoding UDF concurrently across worker nodes, capturing failures via a robust **dead-letter pattern**.
* Transforms and aligns disparate protocol version schemas into unified event schemas (Deposit, Borrow, Repay, etc.) via dedicated modules in `shapers.py`.
* Outputs optimized Snappy-compressed, date-partitioned Parquet files back to S3.

---

## 🚀 Local Installation & Setup

This project uses [**`uv`**](https://github.com/astral-sh/uv), an extremely fast Python package and project manager, to coordinate dependencies specified in `pyproject.toml`.

### 1. Clone the Repository
```bash
git clone https://github.com/Freemandaily/Blockchain-data-decoder.git
cd Decode-Series
```

### 2. Install `uv` (if not already installed)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Synchronize Dependencies
Running `uv sync` automatically creates a virtual environment (`.venv`) and installs the exact required dependencies (including `pyspark`, `web3`, and `eth-abi`):
```bash
uv sync
```

---

## 💻 How to Run the Project

### Running Locally
To test the pipeline locally on a subset of dates:
```bash
cd protocol_decode
uv run python spark_job.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-02 \
  --sink s3
```

> [!NOTE]
> Make sure you have your AWS credentials (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) exported in your local terminal environment so Spark can write output results to your destination S3 bucket.

### Running Smoke Tests
To run isolated decoding test cases against mock Aave and Compound event payloads:
```bash
cd protocol_decode
uv run python decode/decoder.py
```

---

## ☁️ Running on AWS EMR (Cluster Environment)

To process massive date ranges at scale, submit the job to an AWS EMR cluster.

### 1. Prepare and Upload script dependencies to S3
Since the project is modularized, use the `Makefile` inside `protocol_decode/` to zip your dependencies:
```bash
cd protocol_decode
make build
```
Ensure all Python files and the newly built `deps.zip` are uploaded to your EMR-accessible script bucket:
```bash
aws s3 cp deps.zip s3://money-market/script/deps.zip
aws s3 cp spark_job.py s3://money-market/script/spark_job.py
aws s3 cp ../bootstrap.sh s3://money-market/script/bootstrap.sh
```

### 2. Submit Step to EMR Cluster
Run this AWS CLI command to add the decoding job step to your active EMR cluster (replace `j-1T10U9FY3RX2Q` with your actual Cluster ID). Note that we pass `deps.zip` to `--py-files`:

```bash
aws emr add-steps \
  --cluster-id j-1T10U9FY3RX2Q \
  --steps '[
    {
      "Name": "LendingDecoderSparkJob",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
          "spark-submit",
          "--deploy-mode", "cluster",
          "--py-files", "s3://money-market/script/deps.zip",
          "--conf", "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3",
          "s3://money-market/script/spark_job.py",
          "--env", "emr",
          "--start-date", "2026-04-27",
          "--end-date", "2026-04-27"
        ]
      }
    }
  ]'
```

---

## 🛠️ Cluster Troubleshooting & Logs
If your EMR step finishes with a `FAILED` status, use these commands on the EMR Master node to extract execution tracebacks quickly:

* **List running/completed Yarn apps:**
  ```bash
  yarn application -list -appStates FINISHED,FAILED
  ```
* **Filter and print standard Python tracebacks/errors from logs:**
  ```bash
  yarn logs -applicationId <YOUR_APPLICATION_ID> 2>/dev/null | grep -A 30 "Traceback\|Error\|ImportError\|ModuleNotFound\|SyntaxError"
  ```
