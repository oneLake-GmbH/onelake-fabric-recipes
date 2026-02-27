# ── 0. Config ────────────────────────────────────────────────────────────────
SOURCE_CSV    = "abfss://workspace@onelake.dfs.fabric.microsoft.com/files/raw/customers.csv"
TARGET_DELTA  = "abfss://workspace@onelake.dfs.fabric.microsoft.com/tables/customers"
TEMP_DIR      = "/tmp/duckdb_spill"
MEMORY_LIMIT  = "12GB"
BATCH_SIZE    = 500_000          # rows per write batch
LOAD_STRATEGY = "merge"          # "append" | "fullload" | "merge"
MERGE_KEY     = "customer_id"    # used for incremental merge only


# ── 1. Setup DuckDB ──────────────────────────────────────────────────────────
import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
import os

os.makedirs(TEMP_DIR, exist_ok=True)

con = duckdb.connect()
con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
con.execute(f"SET temp_directory = '{TEMP_DIR}'")
con.execute("SET threads = 4")

print(f"DuckDB ready | memory_limit={MEMORY_LIMIT} | temp={TEMP_DIR}")


# ── 2. Read source CSV ───────────────────────────────────────────────────────
con.execute(f"""
    CREATE OR REPLACE VIEW source AS
    SELECT * FROM read_csv(
        '{SOURCE_CSV}',
        auto_detect = true,
        header      = true,
        nullstr     = ''
    )
""")

source_count = con.execute("SELECT COUNT(*) FROM source").fetchone()[0]
print(f"Source rows: {source_count:,}")


# ── 3. Read target via delta_scan & rowcount check ───────────────────────────
try:
    con.execute(f"""
        CREATE OR REPLACE VIEW target AS
        SELECT * FROM delta_scan('{TARGET_DELTA}')
    """)
    target_count = con.execute("SELECT COUNT(*) FROM target").fetchone()[0]
    target_exists = True
except Exception:
    target_count = 0
    target_exists = False

print(f"Target rows (before load): {target_count:,}")

# Row loss guard — warn if source is significantly smaller than target
if target_exists and LOAD_STRATEGY == "fullload":
    loss_pct = (target_count - source_count) / max(target_count, 1) * 100
    if loss_pct > 5:
        raise ValueError(
            f"⚠️  Row loss detected: source has {source_count:,} rows vs "
            f"target {target_count:,} ({loss_pct:.1f}% loss). Aborting."
        )
    print(f"Row loss check passed ({loss_pct:.1f}% delta)")


# ── 4. Load strategy → write in batches ─────────────────────────────────────
def iter_batches(con, batch_size: int):
    """Yield source data as PyArrow record batches."""
    offset = 0
    while True:
        batch = con.execute(
            f"SELECT * FROM source LIMIT {batch_size} OFFSET {offset}"
        ).fetch_arrow_table()
        if batch.num_rows == 0:
            break
        yield batch
        offset += batch_size
        print(f"  → flushed rows {offset - batch_size:,} – {offset:,}")


if LOAD_STRATEGY == "append":
    print("Strategy: APPEND")
    for i, batch in enumerate(iter_batches(con, BATCH_SIZE)):
        write_deltalake(TARGET_DELTA, batch, mode="append")


elif LOAD_STRATEGY == "fullload":
    print("Strategy: FULL LOAD (overwrite)")
    for i, batch in enumerate(iter_batches(con, BATCH_SIZE)):
        write_mode = "overwrite" if i == 0 else "append"
        write_deltalake(TARGET_DELTA, batch, mode=write_mode)


elif LOAD_STRATEGY == "merge":
    print(f"Strategy: INCREMENTAL MERGE on '{MERGE_KEY}'")
    if not target_exists:
        # No target yet — write first batch as overwrite, rest as append
        for i, batch in enumerate(iter_batches(con, BATCH_SIZE)):
            write_mode = "overwrite" if i == 0 else "append"
            write_deltalake(TARGET_DELTA, batch, mode=write_mode)
    else:
        dt = DeltaTable(TARGET_DELTA)
        for batch in iter_batches(con, BATCH_SIZE):
            (
                dt.merge(
                    source       = batch,
                    predicate    = f"s.{MERGE_KEY} = t.{MERGE_KEY}",
                    source_alias = "s",
                    target_alias = "t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

else:
    raise ValueError(f"Unknown LOAD_STRATEGY: '{LOAD_STRATEGY}'")

print("✅ Load complete")


# ── 5. Post-load rowcount validation ────────────────────────────────────────
con.execute(f"""
    CREATE OR REPLACE VIEW target_after AS
    SELECT * FROM delta_scan('{TARGET_DELTA}')
""")
target_after_count = con.execute("SELECT COUNT(*) FROM target_after").fetchone()[0]

print(f"Source rows  : {source_count:,}")
print(f"Target before: {target_count:,}")
print(f"Target after : {target_after_count:,}")

if LOAD_STRATEGY in ("fullload", "merge"):
    assert target_after_count >= source_count, \
        f"⚠️  Post-load row loss! Expected >= {source_count:,}, got {target_after_count:,}"

print("✅ Rowcount validation passed")
