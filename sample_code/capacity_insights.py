"""
capacity_insights.py
────────────────────
Fabric Notebook recipe — long-term capacity history, reservation sizing and the
real cost of each capacity-day, built from the Fabric capacity events in an
Eventhouse. It answers the questions the Fabric Capacity Metrics app cannot:

  * How much capacity must we provision and reserve, and in which scope?
  * Which reservation size pays back over 90 days, not over the last 14?
  * What does each capacity, and each item on it, really cost per day?

Why not the Capacity Metrics app?
  - It keeps about 14 days of item data and 30 days of hourly data. An Azure
    reservation is a 1-year commitment. You need months of history.
  - It shows consumption. Fabric bills PROVISIONED capacity x time. A running
    F64 bills 64 CU-hours each hour, at 5 % load and at 95 % load.
  - It shows one capacity at a time. A shared reservation gets its value from
    the diversification between capacities, which you only see on the summed series.

What this recipe builds (one Delta table per step, in SCHEMA):
  1. cap_hourly           provisioned + consumed CU-hours per capacity-hour
  2. cap_sku_history      SKU changes per capacity (SCD2)
  3. item_op_daily        CU per item / operation / local day, spread over the
                          smoothing window (24 h background, 5 min interactive)
  4. scope_percentiles    P50..P99 of the SUMMED scope series (sum first!)
  5. coverage_curve       covered / overflow / idle CU-h for each candidate reservation
  6. sku_fitness          hours above each candidate SKU ("would an F128 cope?")
  7. capacity_cost_daily  real cost per capacity-day, and item_cost_daily

Prerequisites:
  - Capacity events streamed to an Eventhouse through the Real-Time hub:
      Capacity overview events  -> tables CapacitySummary, CapacityState
      Capacity operation events -> table  CapacityOperation
  - OneLake availability switched ON for the KQL database(s).
  - A default lakehouse attached to the notebook (the target).
  - No tenant admin rights: no Admin API, no Scanner API, no activity events.

Requirements (all pre-installed in Fabric): pyspark
"""

# ── config ────────────────────────────────────────────────────────────────────

WORKSPACE_ID      = "<workspace-guid>"          # workspace that holds the Eventhouse
UTILIZATION_KQLDB = "<kql-database-guid>"       # CapacitySummary, CapacityState
OPERATIONS_KQLDB  = "<kql-database-guid>"       # CapacityOperation (can be the same DB)
SCHEMA            = "capins"
LOCAL_TZ          = "Europe/Berlin"             # the "day" your finance team reads

WINDOW_DAYS       = [30, 90]                    # analysis windows
PERCENTILES       = [50, 90, 95, 99]
COVERAGE_STEP_CU  = 64                          # step between candidate reservation sizes, in CU
COVERAGE_CEILING  = 2.0                         # candidate axis = 0 .. 2 x observed peak
PAUSE_ARTIFACT_PCT = 1000.0                     # windows above this utilisation are flagged, not deleted
MAX_CATCHUP_DAYS  = 31                          # one run reloads at most this many missed days

SKUS = [("F2", 2), ("F4", 4), ("F8", 8), ("F16", 16), ("F32", 32), ("F64", 64),
        ("F128", 128), ("F256", 256), ("F512", 512), ("F1024", 1024), ("F2048", 2048)]

# Your inventory. Replace the example rows. Prices are YOUR prices — fill them in.
#   capacity_scope: which reservation scope and region each capacity belongs to
#   reservations:   reservation orders (quantity, monthly cost, term)
#   pricing:        pay-as-you-go rate per CU-hour, slowly changing
CAPACITY_SCOPE = [
    # capacity_id (UPPER-CASE guid), reservation_scope, region
    ("00000000-0000-0000-0000-000000000001", "SCOPE_A", "westeurope"),
    ("00000000-0000-0000-0000-000000000002", "SCOPE_A", "westeurope"),
    ("00000000-0000-0000-0000-000000000003", "SCOPE_B", "eastus"),
]
RESERVATIONS = [
    # reservation_scope, reserved_cu, monthly_cost, term_start, term_end
    ("SCOPE_A", 128, 0.0, "2026-01-01", "2027-01-01"),
]
PRICING = [
    # region, payg_rate_per_cu_hour, currency, valid_from
    ("westeurope", 0.0, "EUR", "2026-01-01"),
    ("eastus",     0.0, "EUR", "2026-01-01"),
]

# ── imports ───────────────────────────────────────────────────────────────────

import calendar
from datetime import date, datetime, timedelta, timezone
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# ── helpers ───────────────────────────────────────────────────────────────────

def read_kql(db_id, table):
    """Read a KQL table through OneLake availability (Delta, parquet fallback).
    The mirror lags up to a few hours behind the KQL table: load completed days only."""
    path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{db_id}/Tables/{table}"
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return spark.read.parquet(path)

def dedupe(df, keys, order_col="time"):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def write_days(df, table, col, lo, hi, part_col=None):
    """Idempotent: replace only the days this run rebuilt."""
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"{col} >= '{lo}' AND {col} <= '{hi}'")
       .option("mergeSchema", "true").partitionBy(part_col or col)
       .saveAsTable(f"{SCHEMA}.{table}"))

def write_full(df, table, part_col=None):
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if part_col:
        w = w.partitionBy(part_col)
    w.saveAsTable(f"{SCHEMA}.{table}")

def check(label, ok, detail=""):
    """Gates are code. A failed gate stops the run before anything reads bad numbers."""
    print(f"  [{'PASS' if ok else 'FAIL'}] {label} {detail}")
    assert ok, f"{label}: {detail}"

def month_chunks(a, b):
    """Monthly chunks keep the memory bounded during a backfill."""
    out, cur = [], a
    while cur <= b:
        end = min(date(cur.year, cur.month, calendar.monthrange(cur.year, cur.month)[1]), b)
        out.append((cur.isoformat(), end.isoformat()))
        cur = end + timedelta(days=1)
    return out

def window_floor(days):
    return int((date.today() - timedelta(days=days)).strftime("%Y%m%d"))

# ── 0. seed tables ────────────────────────────────────────────────────────────

spark.createDataFrame(CAPACITY_SCOPE, "capacity_id string, reservation_scope string, region string") \
     .createOrReplaceTempView("capacity_scope")
spark.createDataFrame(RESERVATIONS, "reservation_scope string, reserved_cu int, monthly_cost double, "
                                    "term_start string, term_end string") \
     .selectExpr("*", "to_date(term_start) ts", "to_date(term_end) te").createOrReplaceTempView("reservations")
spark.createDataFrame(PRICING, "region string, payg_rate double, currency string, valid_from string") \
     .selectExpr("*", "to_date(valid_from) vf").createOrReplaceTempView("pricing")
spark.createDataFrame(SKUS, "sku string, capacity_units int").createOrReplaceTempView("skus")

# ── 1. capacity backbone: 30-second windows -> hourly provisioned + consumed ──
# CATCH-UP: start at the newest day already loaded, not at "yesterday". Then a run after a
# multi-day outage closes the gap on its own, and the partial last day is reloaded.

d1 = (datetime.now(timezone.utc) - timedelta(days=1)).date()
if spark.catalog.tableExists(f"{SCHEMA}.cap_hourly"):
    last = spark.sql(f"SELECT MAX(utc_date) FROM {SCHEMA}.cap_hourly").collect()[0][0]
    d0 = max(min(last, d1), d1 - timedelta(days=MAX_CATCHUP_DAYS - 1)) if last else d1
else:
    d0 = d1 - timedelta(days=MAX_CATCHUP_DAYS - 1)
print(f"backbone {d0}..{d1}")

for lo, hi in month_chunks(d0, d1):
    s = dedupe(read_kql(UTILIZATION_KQLDB, "CapacitySummary")
               .filter(F.col("windowStartTime").between(f"{lo}T00:00:00Z", f"{hi}T23:59:59.999Z")),
               ["capacityId", "windowStartTime", "windowEndTime"])
    win = (s.selectExpr(
            "upper(cast(capacityId as string)) capacity_id",   # normalise GUID casing once
            "cast(windowStartTime as timestamp) window_start",
            "cast(baseCapacityUnits as double) * 30 * 1000 budget_cu_ms",   # provisioned in 30 s
            "cast(capacityUnitMs as double) billable_cu_ms")                # consumed (smoothed)
          .withColumn("utilisation_pct", F.expr("100.0 * billable_cu_ms / budget_cu_ms"))
          # FLAG, DO NOT FILTER: the first windows after a resume can show absurd utilisation.
          .withColumn("is_pause_artifact", F.col("utilisation_pct") > PAUSE_ARTIFACT_PCT))

    hourly = (win.groupBy("capacity_id", F.date_trunc("hour", "window_start").alias("hour_utc"))
              .agg(F.sum("budget_cu_ms").alias("budget_cu_ms"),
                   F.sum(F.when(~F.col("is_pause_artifact"), F.col("billable_cu_ms"))
                         .otherwise(0.0)).alias("billable_cu_ms"),
                   F.sum(F.col("is_pause_artifact").cast("int")).alias("pause_artifact_windows"))
              .withColumn("provisioned_cu_h", F.col("budget_cu_ms") / 3.6e6)
              .withColumn("consumed_cu_h", F.col("billable_cu_ms") / 3.6e6)
              .withColumn("utc_date", F.to_date("hour_utc"))
              .withColumn("local_ts", F.from_utc_timestamp("hour_utc", LOCAL_TZ))
              .withColumn("date_key_local", F.date_format("local_ts", "yyyyMMdd").cast("int")))
    write_days(hourly, "cap_hourly", "utc_date", lo, hi)
    print(f"  cap_hourly {lo}..{hi}")

check("HOURLY_NO_DUPES", spark.sql(f"""SELECT COUNT(*) FROM (SELECT capacity_id, hour_utc
      FROM {SCHEMA}.cap_hourly GROUP BY 1,2 HAVING COUNT(*) > 1)""").collect()[0][0] == 0)

# ── 2. SKU history (SCD2): when did each capacity scale up or down? ─────────

src = (read_kql(UTILIZATION_KQLDB, "CapacitySummary")
       .selectExpr("upper(cast(capacityId as string)) capacity_id",
                   "cast(windowStartTime as timestamp) ts", "cast(capacitySku as string) sku",
                   "cast(baseCapacityUnits as bigint) cu")
       .filter("cu > 0"))
w = Window.partitionBy("capacity_id").orderBy("ts")
scd2 = (src.withColumn("prev_sku", F.lag("sku").over(w)).withColumn("prev_cu", F.lag("cu").over(w))
           .filter("prev_sku IS NULL OR prev_sku <> sku OR prev_cu <> cu")
           .select("capacity_id", F.col("ts").alias("valid_from"), "sku", "cu")
           .withColumn("valid_to", F.lead("valid_from").over(
               Window.partitionBy("capacity_id").orderBy("valid_from"))))
write_full(scd2, "cap_sku_history")

# ── 3. operation events -> item / operation / local day ─────────────────────
# Three traps, each one fails SILENTLY (the totals still look plausible):
#   a) A long operation sends one event per minute, each with that minute's CU. The terminal
#      event carries only a final true-up. Filter on status = 'Success' and you lose most of
#      the CU (we measured -86 % on a busy capacity). Keep EVERY status for CU, duration and
#      throttling. Only operation COUNTS use the terminal status.
#   b) Fabric charges CU across [windowStartTime, windowEndTime): 24 h for background, 5 min for
#      interactive. A refresh at 23:50 puts most of its CU into TOMORROW. Spread by overlap.
#   c) operationId is not a row key: one operation emits a row per item. Never dedupe on it.

TERMINAL = ("Success", "Failure", "Stopped")
lo, hi = d0.isoformat(), d1.isoformat()

raw = (read_kql(OPERATIONS_KQLDB, "CapacityOperation")
       # read wide: a window that ends in [lo, hi] can start up to 24 h before lo
       .filter(F.col("windowEndTime") >= F.expr(f"to_timestamp('{lo}') - interval 2 day"))
       .filter(F.col("windowStartTime") <= F.expr(f"to_timestamp('{hi} 23:59:59') + interval 2 day")))
if raw.limit(1).count() == 0:
    raise RuntimeError(f"CapacityOperation is empty for {lo}..{hi}: OneLake mirror lag or a wrong range. "
                       "An empty read must not produce an empty table that looks like a clean run.")

# Spreading is linear, so aggregate on the window boundaries FIRST: millions of events
# collapse to a few rows per item before the explode.
grp = (raw.withColumn("capacity_id", F.upper("capacityId"))
          .withColumn("workspace_id", F.upper("workspaceId"))
          .withColumn("item_id", F.upper("itemId"))
          .groupBy("capacity_id", "workspace_id", "item_id", "operationName", "utilizationType",
                   "windowStartTime", "windowEndTime")
          .agg(F.sum("capacityUnitSecond").alias("cu_s_total"),
               F.sum(F.col("durationMs") / 1000.0).alias("duration_s"),
               F.sum(F.col("throttlingDelayMs") / 60000.0).alias("throttling_min"),
               F.sum(F.col("status").isin(*TERMINAL).cast("int")).alias("operations"),
               F.sum((F.col("status") == "Failure").cast("int")).alias("failed_ops"),
               # names via max(): a rename inside the window must not split one item in two
               F.max("itemName").alias("item_name"), F.max("itemKind").alias("item_kind"),
               F.max("workspaceName").alias("workspace_name"),
               F.min("operationStartTime").alias("op_start_utc")))

spread = (grp
    .withColumn("ws", F.from_utc_timestamp("windowStartTime", LOCAL_TZ))
    .withColumn("we", F.from_utc_timestamp("windowEndTime", LOCAL_TZ))
    .withColumn("win_s", F.greatest(F.unix_timestamp("we") - F.unix_timestamp("ws"), F.lit(1)))
    .withColumn("day", F.explode(F.expr("sequence(date_trunc('DAY', ws), date_trunc('DAY', we), interval 1 day)")))
    .withColumn("ov_s", F.greatest(F.unix_timestamp(F.least("we", F.expr("day + interval 1 day")))
                                   - F.unix_timestamp(F.greatest("ws", "day")), F.lit(0)))
    .filter("ov_s > 0")
    .withColumn("cu_s", F.col("cu_s_total") * F.col("ov_s") / F.col("win_s"))
    # Duration, throttling and counts do NOT smooth: they belong to the day the work ran.
    .withColumn("start_day", F.to_date(F.from_utc_timestamp("op_start_utc", LOCAL_TZ)) == F.to_date("day")))

def on_start_day(c):
    return F.sum(F.when(F.col("start_day"), F.col(c)).otherwise(0))

daily = (spread.withColumn("date", F.to_date("day"))
         .groupBy("capacity_id", "workspace_id", "item_id", "operationName", "utilizationType", "date")
         .agg(F.sum("cu_s").alias("cu_s"), on_start_day("duration_s").alias("duration_s"),
              on_start_day("throttling_min").alias("throttling_min"),
              on_start_day("operations").alias("operations"), on_start_day("failed_ops").alias("failed_ops"),
              F.max("item_name").alias("item_name"), F.max("item_kind").alias("item_kind"),
              F.max("workspace_name").alias("workspace_name"))
         .filter(f"date BETWEEN '{lo}' AND '{hi}'")
         .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")))
write_days(daily, "item_op_daily", "date", lo, hi, part_col="date_key")

# GATE: spreading must not create or destroy CU (windows fully inside the range).
def _inside(df):
    return (df.withColumn("s", F.to_date(F.from_utc_timestamp("windowStartTime", LOCAL_TZ)))
              .withColumn("e", F.to_date(F.from_utc_timestamp("windowEndTime", LOCAL_TZ)))
              .filter(f"s >= '{lo}' AND e <= '{hi}'"))
cu_in = _inside(grp).agg(F.sum("cu_s_total")).collect()[0][0] or 0.0
cu_out = _inside(spread).agg(F.sum("cu_s")).collect()[0][0] or 0.0
check("SPREAD_CONSERVES_CU", abs(cu_in - cu_out) / max(cu_in, 1e-9) < 0.001, f"in={cu_in:.0f} out={cu_out:.0f}")

# GATE: the item grain must add back up to the billed capacity series (the backbone).
# This is what proves trap (a) is handled. Expect < 0.5 % drift on busy capacity-days.
roll = spark.sql(f"""
  WITH i AS (SELECT capacity_id, date, SUM(cu_s) cu_s FROM {SCHEMA}.item_op_daily
             WHERE date BETWEEN '{lo}' AND '{hi}' GROUP BY 1, 2),
       b AS (SELECT capacity_id, to_date(local_ts) date, SUM(billable_cu_ms) / 1000.0 cu_s
             FROM {SCHEMA}.cap_hourly WHERE to_date(local_ts) BETWEEN '{lo}' AND '{hi}' GROUP BY 1, 2)
  SELECT MAX(ABS(i.cu_s - b.cu_s) / b.cu_s) worst
  FROM i JOIN b USING (capacity_id, date) WHERE b.cu_s >= 1e6""").collect()[0][0]
print(f"  rollup identity: worst capacity-day drift {roll if roll is None else f'{roll:.2%}'}")

# ── shared: the hourly series per scope, in scope ────────────────────────────

SCOPES = [("capacity", "capacity_id"), ("reservation_scope", "reservation_scope"), ("region", "region")]

def scope_hours(days):
    return spark.sql(f"""
      SELECT h.capacity_id, s.reservation_scope, s.region, h.hour_utc,
             h.provisioned_cu_h AS provisioned_cu, h.consumed_cu_h AS consumed_cu,
             h.budget_cu_ms, h.billable_cu_ms
      FROM {SCHEMA}.cap_hourly h JOIN capacity_scope s USING (capacity_id)
      WHERE h.date_key_local >= {window_floor(days)}""")

# ── 4. percentiles: SUM the series first, THEN take the percentile ───────────
# Percentiles do not add. The P95 of a pooled scope is below the sum of member P95s, and that
# gap is exactly what a shared reservation buys you.

pct_arr = ", ".join(str(p / 100.0) for p in PERCENTILES)
parts = []
for wd in WINDOW_DAYS:
    scope_hours(wd).createOrReplaceTempView("_h")
    for stype, key in SCOPES:
        parts.append(spark.sql(f"""
          SELECT '{stype}' scope_type, scope_id, {wd} window_days,
                 posexplode(arr) AS (i, consumed_cu)
          FROM (SELECT scope_id, percentile_approx(cu, array({pct_arr}), 10000) arr
                FROM (SELECT {key} scope_id, hour_utc, SUM(consumed_cu) cu FROM _h
                      WHERE {key} IS NOT NULL GROUP BY 1, 2)   -- SUM BEFORE PERCENTILE
                GROUP BY scope_id)""")
          .withColumn("percentile", F.element_at(F.array(*[F.lit(p) for p in PERCENTILES]), F.col("i") + 1))
          .drop("i"))
pct = parts[0]
for p in parts[1:]:
    pct = pct.unionByName(p)
write_full(pct, "scope_percentiles", "window_days")

# GATE: a pooled P95 can never be above the sum of member P95s.
bad = spark.sql(f"""
  SELECT COUNT(*) FROM {SCHEMA}.scope_percentiles g
  JOIN (SELECT s.reservation_scope, c.window_days, SUM(c.consumed_cu) member_sum
        FROM {SCHEMA}.scope_percentiles c JOIN capacity_scope s ON s.capacity_id = c.scope_id
        WHERE c.scope_type = 'capacity' AND c.percentile = 95 GROUP BY 1, 2) m
    ON m.reservation_scope = g.scope_id AND m.window_days = g.window_days
  WHERE g.scope_type = 'reservation_scope' AND g.percentile = 95
    AND g.consumed_cu > m.member_sum * 1.001""").collect()[0][0]
check("PCTL_SUM_THEN_PERCENTILE", bad == 0, f"{bad} scopes above the member sum")

# ── 5. reservation coverage curve: two demand bases ──────────────────────────
#   'provisioned' = what the reservation OFFSETS. Size the reservation on this basis.
#   'consumed'    = the rightsizing floor. Downsize the SKUs on this basis.
# Size a reservation on 'consumed' while the big SKUs still run, and you move provisioned hours
# from the reserved rate to list price: the bill goes UP. Order: rightsize first, then reserve.
# Gold holds quantities only. Multiply covered_cu_h x reserved rate and overflow_cu_h x PAYG
# rate in the semantic model, so a price change needs no rebuild.

reserved_now = {r[0]: float(r[1]) for r in spark.sql(
    "SELECT reservation_scope, SUM(reserved_cu) FROM reservations GROUP BY 1").collect()}
curves = []
for wd in WINDOW_DAYS:
    scope_hours(wd).createOrReplaceTempView("_h")
    for basis in ("provisioned", "consumed"):
        for stype, key in SCOPES:
            ser = spark.sql(f"""SELECT {key} scope_id, hour_utc, SUM({basis}_cu) cu FROM _h
                                WHERE {key} IS NOT NULL GROUP BY 1, 2""").cache()
            cand = []
            for sid, peak in ser.groupBy("scope_id").agg(F.max("cu")).collect():
                # the axis must reach today's reservation, or "today vs optimum" has no row
                top = max(peak * COVERAGE_CEILING, reserved_now.get(sid, 0.0) if stype == "reservation_scope" else 0.0)
                cand += [(sid, c) for c in range(0, int(top // COVERAGE_STEP_CU + 2) * COVERAGE_STEP_CU, COVERAGE_STEP_CU)]
            if not cand:
                continue
            curves.append(ser.join(spark.createDataFrame(cand, "scope_id string, candidate_cu int"), "scope_id")
                .groupBy("scope_id", "candidate_cu").agg(
                    F.sum(F.least("cu", F.col("candidate_cu").cast("double"))).alias("covered_cu_h"),
                    F.sum(F.greatest(F.col("cu") - F.col("candidate_cu"), F.lit(0.0))).alias("overflow_cu_h"),
                    F.sum(F.greatest(F.col("candidate_cu") - F.col("cu"), F.lit(0.0))).alias("idle_reserved_cu_h"),
                    F.count("*").alias("hours_observed"))
                .withColumn("scope_type", F.lit(stype)).withColumn("window_days", F.lit(wd))
                .withColumn("demand_basis", F.lit(basis)))
cv = curves[0]
for c in curves[1:]:
    cv = cv.unionByName(c)
write_full(cv, "coverage_curve", "window_days")
check("CURVE_IDLE_NOT_NEGATIVE",
      spark.sql(f"SELECT COUNT(*) FROM {SCHEMA}.coverage_curve WHERE idle_reserved_cu_h < 0").collect()[0][0] == 0)

# ── 6. SKU fitness: would a smaller SKU have coped? ──────────────────────────
# Re-normalises the ALREADY-SMOOTHED series against each SKU. A sizing aid, not a simulation:
# on a smaller SKU, smoothing and overage burn-down behave differently. Say so in the report.

fits = []
for wd in WINDOW_DAYS:
    scope_hours(wd).createOrReplaceTempView("_h")
    for stype, key in SCOPES:
        fits.append(spark.sql(f"""
          SELECT '{stype}' scope_type, x.scope_id, k.sku candidate_sku, k.capacity_units candidate_cu,
                 {wd} window_days,
                 SUM(CASE WHEN x.cu > k.capacity_units THEN 1 ELSE 0 END) exceedance_hours,
                 COUNT(*) total_hours, MAX(x.cu) * 100.0 / k.capacity_units max_pct,
                 percentile_approx(x.cu, 0.999, 10000) * 100.0 / k.capacity_units p999_pct,
                 true is_modelled
          FROM (SELECT {key} scope_id, hour_utc, SUM(consumed_cu) cu FROM _h
                WHERE {key} IS NOT NULL GROUP BY 1, 2) x CROSS JOIN skus k
          GROUP BY x.scope_id, k.sku, k.capacity_units"""))
sf = fits[0]
for f_ in fits[1:]:
    sf = sf.unionByName(f_)
write_full(sf, "sku_fitness", "window_days")

# ── 7. the real cost of each capacity-day, then each item's share ────────────
# Fabric bills PROVISIONED CU x time. Consumption x rate reports a fraction of the invoice
# (for us: about one third). The reservation is a fixed cost per scope and day; it is shared
# by provisioned CU-hours. Provisioned hours above the reservation bill at PAYG.

max_wd = max(WINDOW_DAYS)
cost = spark.sql(f"""
WITH cap AS (
  SELECT h.capacity_id, h.date_key_local date_key, s.reservation_scope, s.region,
         SUM(h.provisioned_cu_h) provisioned_cu_h
  FROM {SCHEMA}.cap_hourly h JOIN capacity_scope s USING (capacity_id)
  WHERE h.date_key_local >= {window_floor(max_wd)}
  GROUP BY 1, 2, 3, 4),
d AS (SELECT DISTINCT date_key, to_date(cast(date_key AS string), 'yyyyMMdd') dt FROM cap),
res AS (   -- orders active on that day; monthly cost x 12 / 365 = daily equivalent
  SELECT r.reservation_scope, d.date_key, SUM(r.reserved_cu) reserved_cu,
         SUM(r.monthly_cost) * 12.0 / 365.0 reservation_cost_day
  FROM reservations r JOIN d ON d.dt >= r.ts AND d.dt < r.te GROUP BY 1, 2),
rate AS (  -- the rate in force ON THAT DAY, not the newest one
  SELECT p.region, d.date_key, p.payg_rate, p.currency,
         ROW_NUMBER() OVER (PARTITION BY p.region, d.date_key ORDER BY p.vf DESC) rn
  FROM pricing p JOIN d ON d.dt >= p.vf),
sc AS (SELECT reservation_scope, date_key, SUM(provisioned_cu_h) scope_cu_h FROM cap GROUP BY 1, 2)
SELECT cap.capacity_id, cap.date_key, cap.provisioned_cu_h,
       COALESCE(r.reservation_cost_day, 0)
         * COALESCE(cap.provisioned_cu_h / NULLIF(sc.scope_cu_h, 0), 0) reservation_cost,
       GREATEST(cap.provisioned_cu_h * (1.0 - LEAST(1.0,
         COALESCE(r.reserved_cu * 24.0 / NULLIF(sc.scope_cu_h, 0), 0))), 0) * rate.payg_rate payg_cost,
       rate.currency,
       CASE WHEN r.reserved_cu IS NULL THEN 'payg'
            WHEN r.reserved_cu * 24.0 >= sc.scope_cu_h THEN 'reserved' ELSE 'mixed' END cost_basis
FROM cap
LEFT JOIN sc ON sc.reservation_scope <=> cap.reservation_scope AND sc.date_key = cap.date_key
LEFT JOIN res r ON r.reservation_scope = cap.reservation_scope AND r.date_key = cap.date_key
JOIN rate ON rate.region = cap.region AND rate.date_key = cap.date_key AND rate.rn = 1
""").withColumn("cost", F.col("reservation_cost") + F.col("payg_cost"))
write_full(cost, "capacity_cost_daily", "date_key")

check("COST_NO_NULLS", spark.sql(f"SELECT COUNT(*) FROM {SCHEMA}.capacity_cost_daily WHERE cost IS NULL")
      .collect()[0][0] == 0, "a NULL cost reads as 'this item is free'")
check("COST_ONE_CURRENCY", spark.sql(f"SELECT COUNT(DISTINCT currency) FROM {SCHEMA}.capacity_cost_daily")
      .collect()[0][0] <= 1)

# Item cost = the capacity-day cost x the item's share of that capacity-day's CU.
# Idle capacity is part of the cost, so it is shared too: the item costs add up to the invoice.
item_cost = spark.sql(f"""
  SELECT i.date_key, i.capacity_id, i.workspace_id, i.workspace_name, i.item_id, i.item_name,
         i.item_kind, i.operationName operation_name, i.cu_s,
         c.cost * i.cu_s / NULLIF(SUM(i.cu_s) OVER (PARTITION BY i.capacity_id, i.date_key), 0) item_cost,
         c.currency
  FROM {SCHEMA}.item_op_daily i
  JOIN {SCHEMA}.capacity_cost_daily c ON c.capacity_id = i.capacity_id AND c.date_key = i.date_key""")
write_full(item_cost, "item_cost_daily", "date_key")

print("done. Point a Direct Lake semantic model at the capins.* tables; apply the rates in DAX.")
