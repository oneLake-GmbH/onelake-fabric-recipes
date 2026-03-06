"""
onelake_delta_freshness.py
──────────────────────────
Fabric Notebook recipe — checks whether delta publishing is alive
for a set of Warehouses by inspecting the youngest _delta_log file
under each Tables/** path and rolling up a per-warehouse health signal.

Requirements (all pre-installed in Fabric):
    azure-storage-file-datalake, sempy, notebookutils, pyspark
"""

# ── config ────────────────────────────────────────────────────────────────────

ALLOWED_DOMAINS          = ["FINANCE", "SALES", "PEOPLE"]   # workspace domain filter
WAREHOUSE_PATTERN        = r"^WH_[^_]+_[^_]+$"             # item display-name regex
FRESHNESS_THRESHOLD_HOURS = 24
MONITORING_TABLE         = "DeltaFreshness.FreshnessLog"
SUMMARY_TABLE            = "DeltaFreshness.WarehouseSummaryLog"

# ── imports ───────────────────────────────────────────────────────────────────

import re, sys, logging
from datetime import datetime, timezone
from azure.core.credentials import AccessToken, TokenCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, BooleanType, IntegerType,
)
import sempy.fabric as fabric
import notebookutils

# ── logger ────────────────────────────────────────────────────────────────────

log = logging.getLogger("delta_freshness")
log.handlers.clear()
log.setLevel(logging.INFO)
h = logging.StreamHandler(sys.stdout)
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
log.addHandler(h)

# ── auth: Fabric session token (DefaultAzureCredential won't work in Fabric) ──

class FabricTokenCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs) -> AccessToken:
        import time
        return AccessToken(notebookutils.credentials.getToken("storage"), int(time.time()) + 3600)

def adls_client():
    return DataLakeServiceClient(
        "https://onelake.dfs.fabric.microsoft.com",
        credential=FabricTokenCredential(),
    )

# ── discovery ─────────────────────────────────────────────────────────────────

def get_context():
    ws_id   = fabric.get_workspace_id()
    df      = fabric.list_workspaces()
    ws_name = df[df["Id"] == ws_id]["Name"].iloc[0]
    suffix  = f"_{ws_name.split('_')[-1]}" if "_" in ws_name else ""
    return ws_id, suffix

def get_targets(suffix):
    domains  = "|".join(ALLOWED_DOMAINS)
    ws_re    = rf"^WS_({domains}){re.escape(suffix)}$"
    targets  = []
    for _, ws in fabric.list_workspaces()[
        fabric.list_workspaces()["Name"].str.match(ws_re, case=False)
    ].iterrows():
        try:
            items = fabric.list_items(workspace=ws["Id"])
            for _, wh in items[
                (items["Type"] == "Warehouse") &
                items["Display Name"].str.match(WAREHOUSE_PATTERN, case=False)
            ].iterrows():
                targets.append({
                    "WorkspaceId": ws["Id"], "WorkspaceName": ws["Name"],
                    "WarehouseId": wh["Id"], "WarehouseName": wh["Display Name"],
                })
        except: pass
    log.info(f"Found {len(targets)} warehouse(s)")
    return targets

# ── onelake helpers ───────────────────────────────────────────────────────────

def list_tables(client, ws_id, wh_id):
    fs, paths = client.get_file_system_client(ws_id), []
    try:
        for schema in fs.get_paths(f"{wh_id}/Tables", recursive=False):
            if not schema.is_directory: continue
            for tbl in fs.get_paths(schema.name, recursive=False):
                if tbl.is_directory:
                    paths.append(tbl.name.replace(f"{wh_id}/", "", 1))
    except Exception as e:
        log.warning(f"list_tables({wh_id}): {e}")
    return paths

def latest_delta_log(client, ws_id, wh_id, table_path):
    """Returns (filename, tz-aware datetime) of the youngest _delta_log file."""
    fs = client.get_file_system_client(ws_id)
    latest_file, latest_ts = None, None
    try:
        for f in fs.get_paths(f"{wh_id}/{table_path}/_delta_log", recursive=False):
            if f.is_directory: continue
            mtime = f.last_modified
            if mtime.tzinfo is None: mtime = mtime.replace(tzinfo=timezone.utc)
            if latest_ts is None or mtime > latest_ts:
                latest_ts, latest_file = mtime, f.name.split("/")[-1]
    except: pass
    return latest_file, latest_ts

# ── spark / sql writer ────────────────────────────────────────────────────────

DETAIL_SCHEMA = StructType([
    StructField("RunTs",          TimestampType()), StructField("WorkspaceName", StringType()),
    StructField("WarehouseName",  StringType()),    StructField("TablePath",      StringType()),
    StructField("LatestLogFile",  StringType()),    StructField("LatestLogTs",    TimestampType()),
    StructField("AgeHours",       DoubleType()),    StructField("ThresholdHours", DoubleType()),
    StructField("IsStale",        BooleanType()),   StructField("ErrorMessage",   StringType()),
])

SUMMARY_SCHEMA = StructType([
    StructField("RunTs",              TimestampType()), StructField("WorkspaceName",     StringType()),
    StructField("WarehouseName",      StringType()),    StructField("TotalTables",        IntegerType()),
    StructField("TablesWithDeltaLog", IntegerType()),   StructField("StaleTables",        IntegerType()),
    StructField("LatestDeltaLogTs",   TimestampType()), StructField("OldestDeltaLogTs",   TimestampType()),
    StructField("MaxAgeHours",        DoubleType()),    StructField("MinAgeHours",         DoubleType()),
    StructField("ThresholdHours",     DoubleType()),    StructField("IsPublishingBroken",  BooleanType()),
])

def write_sql(row, schema, table, server, db):
    try:
        (spark.createDataFrame([row], schema=schema).write.format("jdbc").mode("append")
            .option("url",     f"jdbc:sqlserver://{server};database={db};encrypt=true;trustServerCertificate=true;")
            .option("dbtable", table)
            .option("driver",  "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .save())
    except Exception as e:
        log.error(f"write_sql({table}): {e}")

# ── ProcessDB discovery (same pattern as your existing notebooks) ─────────────

def get_processdb(ws_id):
    r = fabric.FabricRestClient().get(f"/v1/workspaces/{ws_id}/sqlDatabases")
    db = next((d for d in r.json().get("value", [])
               if d["displayName"].startswith("ProcessDB") and "_" not in d["displayName"]), None)
    if not db: raise Exception("ProcessDB not found")
    p = db["properties"]
    return p["serverFqdn"].replace(",", ":"), p["databaseName"]

# ── main ──────────────────────────────────────────────────────────────────────

ws_id, suffix     = get_context()
server, db        = get_processdb(ws_id)
targets           = get_targets(suffix)
client            = adls_client()
now_utc           = datetime.now(timezone.utc)
run_ts            = now_utc.replace(tzinfo=None)  # naive for MSSQL datetime2

for t in targets:
    wh_id, wh_name = t["WarehouseId"], t["WarehouseName"]
    log.info(f"── {t['WorkspaceName']} > {wh_name}")

    tables, results = list_tables(client, t["WorkspaceId"], wh_id), []

    for table_path in tables:
        file, ts, age, err = None, None, None, None
        try:
            file, ts = latest_delta_log(client, t["WorkspaceId"], wh_id, table_path)
            if ts:
                age = (now_utc - ts).total_seconds() / 3600
        except Exception as e:
            err = str(e)[:200]

        is_stale = bool(age and age > FRESHNESS_THRESHOLD_HOURS)
        log.info(f"  {'🔴' if is_stale else '🟢'}  {table_path:<60}  {f'{age:.1f}h' if age else err or 'no log'}")

        row = {
            "RunTs": run_ts, "WorkspaceName": t["WorkspaceName"], "WarehouseName": wh_name,
            "TablePath": table_path, "LatestLogFile": file,
            "LatestLogTs": ts.replace(tzinfo=None) if ts else None,
            "AgeHours": round(age, 2) if age else None,
            "ThresholdHours": float(FRESHNESS_THRESHOLD_HOURS),
            "IsStale": is_stale, "ErrorMessage": err,
        }
        write_sql(row, DETAIL_SCHEMA, MONITORING_TABLE, server, db)
        results.append(row)

    # ── warehouse-level rollup ────────────────────────────────────────────────
    valid       = [r for r in results if r["LatestLogTs"] is not None]
    stale_count = sum(1 for r in valid if r["IsStale"])

    summary = {
        "RunTs": run_ts, "WorkspaceName": t["WorkspaceName"], "WarehouseName": wh_name,
        "TotalTables": len(results),       "TablesWithDeltaLog": len(valid),
        "StaleTables": stale_count,
        "LatestDeltaLogTs": max((r["LatestLogTs"] for r in valid), default=None),
        "OldestDeltaLogTs": min((r["LatestLogTs"] for r in valid), default=None),
        "MaxAgeHours": round(max((r["AgeHours"] for r in valid), default=0), 2) or None,
        "MinAgeHours": round(min((r["AgeHours"] for r in valid), default=0), 2) or None,
        "ThresholdHours": float(FRESHNESS_THRESHOLD_HOURS),
        "IsPublishingBroken": (stale_count == len(valid)) if valid else None,
    }
    write_sql(summary, SUMMARY_SCHEMA, SUMMARY_TABLE, server, db)

    broken = summary["IsPublishingBroken"]
    log.info(f"  {'🔴 PUBLISHING BROKEN' if broken else '🟢 healthy'} — "
             f"{stale_count}/{len(valid)} stale  "
             f"max age {summary['MaxAgeHours']}h")
