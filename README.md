# EODHD (eodhd.com) Data Fetch & Ingestion into Azure SQL

This repository contains a Python script (`script.py`) that automates:
1. **Retrieving tickers** from an Azure SQL Database (using a customizable SQL query).
2. **Fetching daily OHLC EOD** data from the [EODHD API](https://eodhd.com/).
3. **Inserting** the fetched data into an Azure SQL table, optionally executing a **pre-SQL** command and a **post-SQL** command.

All authentication to Azure SQL uses [Azure Active Directory (AAD)](https://docs.microsoft.com/azure/azure-sql/database/authentication-aad-overview). We rely on [`DefaultAzureCredential`](https://docs.microsoft.com/azure/developer/python/sdk/authentication-azure-hosted-apps) to seamlessly acquire tokens from multiple possible sources (e.g., environment variables, Managed Identities, Workload Identities, Azure CLI, etc.).

---

## Table of Contents

1. [Features](#features)
2. [Architecture Overview](#architecture-overview)
3. [Requirements](#requirements)
   1. [Azure SQL Setup](#azure-sql-setup)
   2. [Azure Workload Identity Requirements](#azure-workload-identity-requirements)
4. [Environment Variables](#environment-variables)
5. [Local Development](#local-development)
6. [Building & Running with Docker](#building--running-with-docker)
7. [Using Kubernetes (CronJob Example)](#using-kubernetes-cronjob-example)
8. [Troubleshooting](#troubleshooting)
9. [License](#license)

---

## Features

- **Parallel Fetch**: Uses a Python `ThreadPoolExecutor` to fetch EOD data in parallel.
- **Chunked Inserts**: Inserts large DataFrames in 5k-row chunks for better performance.
- **Flexible Ticker Query**: Ticker selection is driven by an environment variable so you can customize which tickers to fetch.
- **Optional Pre/Post SQL**: Allows you to run stored procedures or custom T-SQL before/after data insertion.

---

## Architecture Overview

1. **Azure AD Auth**: The script calls `DefaultAzureCredential.get_token(...)` to obtain an AAD token for SQL Database scope (`https://database.windows.net/.default`).
2. **SQLAlchemy**: A SQLAlchemy `engine` (plus `pyodbc`) is used to run queries (e.g., SELECT tickers).
3. **EODHD REST**: For each ticker, the script calls the EODHD REST API to fetch daily OHLC data.
4. **Data Insert**: Data is inserted into your Azure SQL table in parallel chunks.

---

## Requirements

1. **Python 3.9+** if running the script locally (without Docker).
2. **Azure SQL** with AAD authentication enabled.  
   - Your AAD principal (Managed Identity, Service Principal, or user) must have sufficient permissions on the target table.
3. **EODHD API Token** to fetch market data.

### Azure SQL Setup
- Make sure your Azure SQL Server is configured to allow Azure AD authentication.
- The identity used by your script must be assigned sufficient [database roles and permissions](https://learn.microsoft.com/azure/azure-sql/database/authentication-aad-configure) to execute the T-SQL statements.

### Azure Workload Identity Requirements

If you are **running on Kubernetes with Azure Workload Identity**:
- Enable [Azure AD Workload Identity](https://learn.microsoft.com/azure/aks/workload-identity-overview) in your AKS cluster.
- Configure a **federated identity credential** on your Azure AD application so pods can exchange tokens for an AAD token.
- Ensure the identity used has the appropriate roles on the Azure SQL instance (e.g., `db_datareader` / `db_datawriter` or custom roles).
- `DefaultAzureCredential` will automatically pick up the token from the Azure Workload Identity JWT if properly configured.

---

## Environment Variables

The script reads the following environment variables (with sample defaults shown):

| Variable        | Description                                                                 | Default                                                |
|-----------------|-----------------------------------------------------------------------------|--------------------------------------------------------|
| `DB_SERVER`     | FQDN of your Azure SQL server                                               | `your-sql-server.database.windows.net`                |
| `DB_NAME`       | Azure SQL database name                                                     | `xxxxxxxxxx`                                           |
| `PRE_SQL`       | Pre-import SQL (e.g., stored procedure call)                                | `EXEC etl.xxxxx1;`                                     |
| `POST_SQL`      | Post-import SQL (e.g., stored procedure call)                               | `EXEC etl.xxxxx2;`                                     |
| `EODHD_API_TOKEN` | API token to authenticate with EODHD                                      | `zzzzzzzzzz.wwwwwwwwwwww`                              |
| `TARGET_TABLE`  | SQL table name where data will be inserted                                  | `etl.xxxxxxxxxxxxxxxxxxxxxx`                           |
| `TICKER_SQL`    | SQL query used to fetch ticker symbols                                      | `SELECT TOP 10000 ...` (See script default)            |

These variables can be set:
- In your `.env` file (for local dev).
- As environment variables in your Docker or Kubernetes environment.

---

## Local Development

1. **Install dependencies** (assuming Python 3.9+):
   ```bash
   pip install -r requirements.txt
   ```
2. **Set environment variables** (example):
   ```bash
   export DB_SERVER="your-sql-server.database.windows.net"
   export DB_NAME="your-database-name"
   export EODHD_API_TOKEN="your-eodhd-token"
   export TARGET_TABLE="etl.xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
   # ... etc.
   ```
3. **Run the script**:
   ```bash
   python script.py
   ```
   - **Authentication**: If you have Azure CLI installed and signed in, `DefaultAzureCredential` can use the AzureCliCredential. Alternatively, you can use a Service Principal in environment variables or any other supported credential method.

---

## Building & Running with Docker

1. **Build the Docker image**:
   ```bash
   docker build -t my-eodhd-image:latest .
   ```
2. **Run the container**:
   ```bash
   docker run --rm \
     -e DB_SERVER="your-sql-server.database.windows.net" \
     -e DB_NAME="your-database-name" \
     -e EODHD_API_TOKEN="your-eodhd-token" \
     -e PRE_SQL="EXEC etl.xxxxxxxxxxxxx;" \
     -e POST_SQL="EXEC etl.xxxxxxxxxxxxxxxxxxxxxxxxx;" \
     -e TARGET_TABLE="etl.xxxxxxxxxxxxxxxxxxxxxxxxxx" \
     my-eodhd-image:latest
   ```
3. **Push to a container registry (optional)**:
   ```bash
   docker tag my-eodhd-image:latest myregistry.azurecr.io/my-eodhd-image:latest
   docker push myregistry.azurecr.io/my-eodhd-image:latest
   ```

---

## Using Kubernetes (CronJob Example)

Below is a sample Kubernetes CronJob that runs the container daily at 3 AM. It assumes your cluster is set up for Azure Workload Identity or another method recognized by `DefaultAzureCredential`.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: eodhd-cronjob
spec:
  schedule: "0 3 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          # If using Azure Workload Identity, you'd typically set:
          # serviceAccountName: my-workload-identity-sa
          containers:
            - name: eodhd-container
              image: myregistry.azurecr.io/my-eodhd-image:latest
              env:
                - name: DB_SERVER
                  value: "xxxxxxxxxxx.database.windows.net"
                - name: DB_NAME
                  value: "xxxxxxxxx"
                - name: EODHD_API_TOKEN
                  value: "xxxxxxx.xxxxxxxxxxx"
                - name: PRE_SQL
                  value: "EXEC etl.xxxxxxxxxxxx;"
                - name: POST_SQL
                  value: "EXEC etl.xxxxxxxxxxxx;"
                - name: TARGET_TABLE
                  value: "etl.xxxxxxxxxxxxxxx"
                - name: TICKER_SQL
                  value: |
                    SELECT TOP 10000 eodhd_ticker
                    FROM etl.xxxxxxxxxxx
```

**Apply the CronJob**:
```bash
kubectl apply -f eodhd-cronjob.yaml
```

**Check its status**:
```bash
kubectl get cronjobs
kubectl get jobs
```

**View logs**:
```bash
kubectl logs <pod-name>
```

---

## Troubleshooting

1. **Authentication Errors**:
   - Verify the container/pod can acquire an AAD token. For Azure Workload Identity, ensure the federated identity is configured properly, and the service account used by your pod matches the identity’s audience/issuer.
2. **SQL Permissions**:
   - Make sure your Azure AD principal has sufficient roles or permissions in Azure SQL to run queries and inserts.
3. **Firewall**:
   - If using Azure SQL with firewall rules or a private endpoint, confirm the container/pod has network access.
4. **EODHD API Issues**:
   - Check your EODHD token and any rate limit constraints.

---

## License

This project is provided under the Apache 2.0 License. You are free to modify, distribute, or use this code in your own projects.
