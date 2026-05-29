# EVM Blockchain Log Decoder (Aave Money-Market Series)

A high-performance PySpark batch pipeline designed to read, filter, decode, and shape raw Ethereum event logs from the AWS public blockchain dataset into structured DeFi event tables.

This project focuses on Aave V1, V2, and V3 protocols, extracting and normalizing data for critical money-market actions: **Borrow**, **Repay**, and **LiquidationCall**.

---

## 📂 Core Architecture & Modules

The pipeline is split into three highly decoupled modules:

### 1. [`aave_abis.py`]
Acts as the **Registry and Schema definition layer**.
* Stores event signatures, schemas, and `topic0` hashes for Aave V1/V2/V3 protocols.
* Maps lowercase contract addresses to their respective event decoders to easily route log parsing.
* Dynamically hashes signatures using `web3` (or supports pre-computed values for performance).

### 2. [`aave_decoder.py`]
Implements **low-level EVM log decoding logic**.
* Decodes indexed parameters from `topics` slots.
* Employs the fast `eth_abi` library to decode non-indexed parameters from the log's `data` payload.
* Exposes a unified `decode_log` function and wraps it in a broadcast-friendly PySpark UDF wrapper `make_spark_udf`.

### 3. [`aave_spark_job.py`]
The **PySpark ETL engine**.
* Connects to public Ethereum log parquet datasets on AWS S3 (`s3a://aws-public-blockchain/v1.0/eth/logs`).
* Filters raw records down to specific Aave contracts and known `topic0` signatures.
* Runs the decoding UDF concurrently across worker nodes.
* Shapes and aligns disparate protocol version schemas (V1/V2/V3) into unified event schemas.
* Outputs optimized Snappy-compressed, date-partitioned Parquet files back to S3.

---

## 🚀 Local Installation & Setup

This project uses [**`uv`**](https://github.com/astral-sh/uv), an extremely fast Python package and project manager, to coordinate dependencies specified in `pyproject.toml`.

### 1. Clone the Repository
```bash
git clone  https://Freemandaily/Blockchain-data-decoder.git
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
uv run python aave_spark_job.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-02 \
  --sink s3
```

> [!NOTE]
> Make sure you have your AWS credentials (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) exported in your local terminal environment so Spark can write output results to your destination S3 bucket.

---

## ☁️ Running on AWS EMR (Cluster Environment)

To process massive date ranges at scale, submit the job to an AWS EMR cluster.

### 1. Prepare and Upload script dependencies to S3
Ensure all Python files and dependencies are uploaded to your EMR-accessible script bucket:
```bash
aws s3 cp aave_abis.py s3://money-market/script/aave_abis.py
aws s3 cp aave_decoder.py s3://money-market/script/aave_decoder.py
aws s3 cp aave_spark_job.py s3://money-market/script/aave_spark_job.py
aws s3 cp bootstrap.sh s3://money-market/script/bootstrap.sh
```

### 2. Submit Step to EMR Cluster
Run this AWS CLI command to add the decoding job step to your active EMR cluster (replace `j-1T10U9FY3RX2Q` with your actual Cluster ID):

```bash
aws emr add-steps \
  --cluster-id j-1T10U9FY3RX2Q \
  --steps '[
    {
      "Name": "AaveDecoderSparkJob",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
          "spark-submit",
          "--deploy-mode", "cluster",
          "--py-files", "s3://money-market/script/aave_abis.py,s3://money-market/script/aave_decoder.py",
          "--conf", "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3",
          "s3://money-market/script/aave_spark_job.py",
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
