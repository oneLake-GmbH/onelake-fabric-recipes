#!/usr/bin/env python
# coding: utf-8

"""
MDSync - Full Environment Automation
=====================================
Triggers and monitors SQL Endpoint metadata sync (MDSync) across all matching
workspaces and lakehouses in a Microsoft Fabric environment.

Designed to run as a Fabric Notebook. Logs execution results to a monitoring
SQL Database for observability.

Author: oneLake (https://github.com/onelake-GmbH)
Repo:   https://github.com/onelake-GmbH/onelake-fabric-recipes
"""

import re
import json
import time
import logging
import sys
import concurrent.futures
from datetime import datetime

import sempy.fabric as fabric
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# CONFIGURATION
# Adjust these values to match your environment before running.
# ---------------------------------------------------------------------------

DRY_RUN = True  # Set to False to actually trigger MDSync operations

LOG_LEVEL = logging.DEBUG if DRY_RUN else logging.INFO

# Domains / workspace prefixes to include in the sync
# Format matches workspaces named: WS_<DOMAIN>_<ENV>
ALLOWED_DOMAINS = ["DOMAIN_A", "DOMAIN_B", "DOMAIN_C"]

# Max parallel MDSync threads
MAX_WORKERS = 8

# Seconds between polling for MDSync completion
POLL_INTERVAL = 45

# Regex pattern to match Lakehouse SQL Endpoint names
# Default pattern: LH_<something>_<something>
ENDPOINT_PATTERN = r"^LH_[^_]+_[^_]+$"

# Monitoring SQL Database - must be a SQL Database item in the same workspace
# Set to None to disable SQL logging
MONITORING_DB_PREFIX = "ProcessDB"   # Display name prefix (no underscores)
MONITORING_TABLE     = "dbo.MDSyncLog"


# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------

logger = logging.getLogger("MDSyncLogger")
logger.handlers.clear()
logger.setLevel(LOG_LEVEL)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)


# ---------------------------------------------------------------------------
# MONITORING
# ---------------------------------------------------------------------------

def log_to_sql(server_fqdn: str, db_name: str, log_entry: dict):
    """Writes a single log entry to the monitoring SQL table."""
    if not server_fqdn or not db_name:
        return
    try:
        df = spark.createDataFrame([log_entry])
        df = (df
              .withColumn("IsDryRun", col("IsDryRun").cast("boolean"))
              .withColumn("ExecutionTime", col("ExecutionTime").cast("timestamp")))

        url = f"jdbc:sqlserver://{server_fqdn};database={db_name};encrypt=true;trustServerCertificate=true;"
        df.write.mode("append").option("url", url).mssql(MONITORING_TABLE)

    except Exception as e:
        logger.error(f"Failed to write log to SQL: {e}")


# ---------------------------------------------------------------------------
# FABRIC HELPERS
# ---------------------------------------------------------------------------

client = fabric.FabricRestClient()


def get_current_context() -> tuple[str, str, str]:
    """Returns (workspace_id, workspace_name, env_suffix)."""
    curr_ws_id = fabric.get_workspace_id()
    df = fabric.list_workspaces()
    curr_ws_name = df[df["Id"] == curr_ws_id]["Name"].iloc[0]
    suffix = f"_{curr_ws_name.split('_')[-1]}" if "_" in curr_ws_name else ""
    return curr_ws_id, curr_ws_name, suffix


def get_monitoring_db(workspace_id: str) -> tuple[str, str]:
    """
    Finds the monitoring SQL Database in the workspace.
    Matches on display name starting with MONITORING_DB_PREFIX (no underscores).
    Returns (server_fqdn, database_name) or (None, None) if not found.
    """
    uri = f"/v1/workspaces/{workspace_id}/sqlDatabases"
    response = client.get(uri)

    if not response.ok:
        logger.warning(f"Could not fetch SQL Databases: {response.text}")
        return None, None

    databases = response.json().get("value", [])
    target = next(
        (db for db in databases
         if db["displayName"].startswith(MONITORING_DB_PREFIX) and "_" not in db["displayName"]),
        None
    )

    if not target:
        logger.warning(f"No SQL Database found matching prefix '{MONITORING_DB_PREFIX}' (without underscores).")
        return None, None

    props = target.get("properties", {})
    server_fqdn  = props.get("serverFqdn", "").replace(",", ":")
    database_name = props.get("databaseName")

    logger.info(f"Monitoring DB: {target['displayName']} | Server: {server_fqdn}")
    return server_fqdn, database_name


def get_targets(env_suffix: str) -> list[dict]:
    """
    Discovers all SQL Endpoints matching the naming pattern across
    workspaces that belong to the allowed domains and environment suffix.
    """
    targets = []
    domain_group = "|".join(ALLOWED_DOMAINS)
    ws_regex = f"^WS_({domain_group}){re.escape(env_suffix)}$"

    df_ws = fabric.list_workspaces()
    matched_ws = df_ws[df_ws["Name"].apply(lambda x: bool(re.match(ws_regex, x, re.IGNORECASE)))]

    logger.info(f"Found {len(matched_ws)} matching workspaces.")

    for _, row in matched_ws.iterrows():
        try:
            items = fabric.list_items(workspace=row["Id"])
            endpoints = items[
                (items["Type"] == "SQLEndpoint") &
                (items["Display Name"].str.match(ENDPOINT_PATTERN, case=False))
            ]
            for _, ep in endpoints.iterrows():
                targets.append({
                    "Workspace":    row["Name"],
                    "EndpointName": ep["Display Name"],
                    "EndpointId":   ep["Id"],
                })
        except Exception as e:
            logger.warning(f"Could not list items for workspace {row['Name']}: {e}")

    return targets


# ---------------------------------------------------------------------------
# MDSYNC EXECUTION
# ---------------------------------------------------------------------------

def run_mdsync(target: dict, server_fqdn: str, db_name: str) -> dict:
    """
    Triggers MDSync for a single SQL Endpoint and polls until completion.

    Uses the unofficial fallback API endpoint which has proven more reliable
    than the official API in cases where the official endpoint times out
    after ~2 hours despite the operation still running.

    Official API docs:
    https://learn.microsoft.com/en-us/rest/api/fabric/sqlendpoint/items/refresh-sql-endpoint-metadata

    Fallback endpoint used here:
    POST /v1.0/myorg/lhdatamarts/{endpointId}
    payload: {"commands":[{"$type":"MetadataRefreshExternalCommand"}]}
    """
    name = f"{target['Workspace']} > {target['EndpointName']}"
    log_entry = {
        "ExecutionTime": datetime.now(),
        "WorkspaceName": target["Workspace"],
        "EndpointName":  target["EndpointName"],
        "Status":        "Starting",
        "BatchId":       "N/A",
        "IsDryRun":      1 if DRY_RUN else 0,
    }

    log_to_sql(server_fqdn, db_name, log_entry)

    if DRY_RUN:
        logger.info(f"[DRY RUN] Would sync: {name}")
        log_entry["Status"] = "DRY_RUN_SUCCESS"
        log_to_sql(server_fqdn, db_name, log_entry)
        return log_entry

    try:
        # Trigger MDSync via fallback endpoint
        uri     = f"/v1.0/myorg/lhdatamarts/{target['EndpointId']}"
        payload = {"commands": [{"$type": "MetadataRefreshExternalCommand"}]}
        response = client.post(uri, json=payload)

        if response.status_code in (400, 409):
            logger.warning(f"Skipping {name}: already running or bad request.")
            log_entry["Status"] = "FAILED_ALREADY_RUNNING"
            log_to_sql(server_fqdn, db_name, log_entry)
            return log_entry

        batch_id = json.loads(response.text).get("batchId", "NO_ID")
        log_entry["BatchId"] = batch_id
        logger.info(f"Started: {name} | BatchId: {batch_id}")

        # Poll for completion
        status_uri = f"/v1.0/myorg/lhdatamarts/{target['EndpointId']}/batches/{batch_id}"
        state = "inProgress"
        while state == "inProgress":
            time.sleep(POLL_INTERVAL)
            api_res = client.get(status_uri)
            state = api_res.json().get("progressState", "unknown") if api_res.ok else "ERROR_FETCHING_STATUS"

        logger.info(f"Completed: {name} | State: {state}")
        log_entry["Status"] = state
        log_to_sql(server_fqdn, db_name, log_entry)
        return log_entry

    except Exception as e:
        logger.error(f"Error syncing {name}: {e}")
        log_entry["Status"] = f"ERROR: {str(e)[:50]}"
        log_to_sql(server_fqdn, db_name, log_entry)
        return log_entry


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    logger.info("=== MDSync Full Environment Started ===")

    # 1. Resolve current workspace context and environment suffix
    curr_ws_id, curr_ws_name, env_suffix = get_current_context()
    logger.info(f"Running from: {curr_ws_name} | Env suffix: '{env_suffix}'")

    # 2. Connect to monitoring database (optional)
    server_fqdn, internal_db_name = get_monitoring_db(curr_ws_id)

    # 3. Discover target endpoints
    targets = get_targets(env_suffix)
    logger.info(f"Targets found: {len(targets)}")

    if not targets:
        logger.info("No targets matched the naming pattern. Exiting.")
        return

    # 4. Run MDSync in parallel
    logger.info(f"Submitting {len(targets)} endpoints with {MAX_WORKERS} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(run_mdsync, t, server_fqdn, internal_db_name): t
            for t in targets
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Unexpected error in thread: {e}")

    logger.info("=== MDSync Full Environment Completed ===")


if __name__ == "__main__":
    main()
