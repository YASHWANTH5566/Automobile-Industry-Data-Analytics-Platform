import dlt
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, countDistinct,
    round as spark_round, current_date, date_sub,
    lit, when, stddev, abs as spark_abs,
    current_timestamp, date_format, to_date
)
from pyspark.sql.window import Window

# ============================================================
# GOLD 7: DAILY COMPARISON TABLE
# Grain : metric_name + category + today vs yesterday
# Purpose: Feed the LLM summariser with pre-computed
#          trend deltas and anomaly flags so the prompt
#          stays small and deterministic.
#
# Reads ONLY from existing silver tables – no changes to
# bronze/silver/other gold tables.
# ============================================================

@dlt.table(
    name="gold_daily_comparison",
    comment=(
        "Day-over-day KPI comparison with % change and z-score anomaly flags. "
        "Used by the LLM daily business summary job."
    )
)
def gold_daily_comparison():

    silver_sales     = dlt.read("silver_sales_new")
    silver_prod      = dlt.read("silver_production_new")
    silver_service   = dlt.read("silver_service_new")
    silver_warranty  = dlt.read("silver_warranty_new")
    silver_inventory = dlt.read("silver_inventory_new")

    today     = current_date()
    yesterday = date_sub(today, 1)

    # ── helper: tag each row as today / yesterday ──────────────
    def day_tag(df, date_col):
        return df.withColumn(
            "day",
            when(to_date(col(date_col)) == today,     lit("today"))
            .when(to_date(col(date_col)) == yesterday, lit("yesterday"))
            .otherwise(None)
        ).filter(col("day").isNotNull())

    # ── SALES ──────────────────────────────────────────────────
    sales_daily = (
        day_tag(silver_sales, "sale_date")
        .groupBy("day")
        .agg(
            countDistinct("vin").alias("units_sold"),
            spark_round(spark_sum("net_revenue"), 2).alias("net_revenue"),
            spark_round(spark_sum("sale_amount"), 2).alias("gross_revenue"),
            spark_round(avg("discount"), 2).alias("avg_discount"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("dealer_id").alias("active_dealers")
        )
        .withColumn("domain", lit("sales"))
    )

    # ── PRODUCTION ─────────────────────────────────────────────
    prod_daily = (
        day_tag(silver_prod, "production_date")
        .groupBy("day")
        .agg(
            count("production_id").alias("total_units"),
            spark_round(avg("production_time_minutes"), 2).alias("avg_prod_time_min"),
            spark_round(
                spark_sum("is_completed").cast("double") / count("production_id") * 100, 2
            ).alias("completion_rate_pct"),
            spark_round(
                spark_sum("is_delayed").cast("double") / count("production_id") * 100, 2
            ).alias("delay_rate_pct")
        )
        .withColumn("domain", lit("production"))
    )

    # ── SERVICE ────────────────────────────────────────────────
    svc_daily = (
        day_tag(silver_service, "service_date")
        .groupBy("day")
        .agg(
            count("service_id").alias("service_visits"),
            spark_round(spark_sum("service_cost"), 2).alias("total_service_revenue"),
            spark_round(avg("customer_feedback_rating"), 2).alias("avg_feedback"),
            spark_sum("is_warranty_claim").alias("warranty_backed_services")
        )
        .withColumn("domain", lit("service"))
    )

    # ── WARRANTY ───────────────────────────────────────────────
    wt_daily = (
        day_tag(silver_warranty, "claim_date")
        .groupBy("day")
        .agg(
            count("claim_id").alias("total_claims"),
            spark_round(spark_sum("claim_amount"), 2).alias("total_claim_value"),
            spark_round(avg("is_approved") * 100, 2).alias("approval_rate_pct")
        )
        .withColumn("domain", lit("warranty"))
    )

    # ── INVENTORY (snapshot – no date grain; use as static) ────
    # For inventory we compare stock health metrics directly;
    # no day tag needed – this is always "current snapshot".
    inv_snapshot = (
        silver_inventory
        .agg(
            spark_round(spark_sum("inventory_value"), 2).alias("total_inventory_value"),
            spark_round(avg("is_below_reorder") * 100, 2).alias("pct_below_reorder"),
            countDistinct("part_id").alias("distinct_parts")
        )
        .withColumn("day", lit("today"))
        .withColumn("domain", lit("inventory"))
    )

    # ── Union all domains into a long KPI frame ─────────────────
    # We keep wide format per domain then union a normalised long
    # table so the LLM prompt builder can iterate easily.

    from pyspark.sql.functions import struct, to_json

    def pivot_to_long(df, domain_name):
        """
        Pivot wide agg rows (today / yesterday) into one row per
        domain with today_json and yesterday_json columns.
        """
        today_row = df.filter(col("day") == "today").drop("day", "domain")
        yest_row  = df.filter(col("day") == "yesterday").drop("day", "domain")

        today_json = today_row.select(
            to_json(struct([col(c) for c in today_row.columns])).alias("today_metrics")
        ).limit(1)

        yest_json = yest_row.select(
            to_json(struct([col(c) for c in yest_row.columns])).alias("yesterday_metrics")
        ).limit(1)

        return (
            today_json.crossJoin(yest_json)
            .withColumn("domain", lit(domain_name))
        )

    sales_long = pivot_to_long(sales_daily, "sales")
    prod_long  = pivot_to_long(prod_daily,  "production")
    svc_long   = pivot_to_long(svc_daily,   "service")
    wt_long    = pivot_to_long(wt_daily,    "warranty")

    # Inventory has no yesterday row – pad it
    inv_long = (
        inv_snapshot.drop("day")
        .select(
            to_json(struct([col(c) for c in inv_snapshot.drop("day").columns]))
                .alias("today_metrics")
        )
        .limit(1)
        .withColumn("yesterday_metrics", lit(None).cast("string"))
        .withColumn("domain", lit("inventory"))
    )

    combined = (
        sales_long
        .unionByName(prod_long)
        .unionByName(svc_long)
        .unionByName(wt_long)
        .unionByName(inv_long)
        .withColumn("report_date", today)
        .withColumn("_processed_ts", current_timestamp())
    )

    return combined


# ============================================================
# GOLD 8: DAILY ANOMALY SUMMARY
# Pre-computes rolling 30-day stats per domain/dealer and
# flags today's values that are > 2 std deviations away.
# The LLM summariser joins this to enrich the prompt with
# specific anomaly details without needing to compute them
# inside the prompt itself.
# ============================================================

@dlt.table(
    name="gold_daily_anomaly_summary",
    comment=(
        "Rolling 30-day z-score anomaly flags per domain. "
        "Feeds into the LLM summariser for anomaly narration."
    )
)
def gold_daily_anomaly_summary():

    silver_sales    = dlt.read("silver_sales_new")
    silver_service  = dlt.read("silver_service_new")
    silver_warranty = dlt.read("silver_warranty_new")
    silver_prod     = dlt.read("silver_production_new")

    today      = current_date()
    window_30d = date_sub(today, 30)

    # ── Sales: daily net_revenue anomaly per dealer ─────────────
    sales_daily_dealer = (
        silver_sales
        .filter(to_date(col("sale_date")) >= window_30d)
        .groupBy("dealer_id", "dealer_name", to_date(col("sale_date")).alias("day"))
        .agg(
            spark_round(spark_sum("net_revenue"), 2).alias("daily_revenue"),
            countDistinct("vin").alias("daily_units")
        )
    )

    sales_stats = (
        sales_daily_dealer
        .groupBy("dealer_id", "dealer_name")
        .agg(
            avg("daily_revenue").alias("avg_revenue"),
            stddev("daily_revenue").alias("std_revenue"),
            avg("daily_units").alias("avg_units"),
            stddev("daily_units").alias("std_units")
        )
    )

    sales_anomaly = (
        sales_daily_dealer
        .filter(col("day") == today)
        .join(sales_stats, ["dealer_id", "dealer_name"])
        .withColumn("revenue_z_score",
            spark_round(
                (col("daily_revenue") - col("avg_revenue")) / col("std_revenue"), 2))
        .withColumn("units_z_score",
            spark_round(
                (col("daily_units") - col("avg_units")) / col("std_units"), 2))
        .withColumn("revenue_anomaly",
            when(spark_abs(col("revenue_z_score")) > 2, lit(1)).otherwise(lit(0)))
        .withColumn("units_anomaly",
            when(spark_abs(col("units_z_score")) > 2, lit(1)).otherwise(lit(0)))
        .withColumn("revenue_pct_change",
            spark_round(
                (col("daily_revenue") - col("avg_revenue")) / col("avg_revenue") * 100, 2))
        .withColumn("units_pct_change",
            spark_round(
                (col("daily_units") - col("avg_units")) / col("avg_units") * 100, 2))
        .withColumn("domain", lit("sales"))
        .select(
            "domain", "dealer_id", "dealer_name",
            "daily_revenue", "avg_revenue", "revenue_z_score",
            "revenue_anomaly", "revenue_pct_change",
            "daily_units", "avg_units", "units_z_score",
            "units_anomaly", "units_pct_change"
        )
    )

    # ── Warranty: daily claim volume anomaly per dealer ─────────
    wt_daily_dealer = (
        silver_warranty
        .filter(to_date(col("claim_date")) >= window_30d)
        .groupBy("dealer_id", "dealer_name", to_date(col("claim_date")).alias("day"))
        .agg(
            count("claim_id").alias("daily_claims"),
            spark_round(spark_sum("claim_amount"), 2).alias("daily_claim_value")
        )
    )

    wt_stats = (
        wt_daily_dealer
        .groupBy("dealer_id", "dealer_name")
        .agg(
            avg("daily_claims").alias("avg_claims"),
            stddev("daily_claims").alias("std_claims"),
            avg("daily_claim_value").alias("avg_claim_value"),
            stddev("daily_claim_value").alias("std_claim_value")
        )
    )

    wt_anomaly = (
        wt_daily_dealer
        .filter(col("day") == today)
        .join(wt_stats, ["dealer_id", "dealer_name"])
        .withColumn("claims_z_score",
            spark_round(
                (col("daily_claims") - col("avg_claims")) / col("std_claims"), 2))
        .withColumn("value_z_score",
            spark_round(
                (col("daily_claim_value") - col("avg_claim_value")) / col("std_claim_value"), 2))
        .withColumn("claims_anomaly",
            when(spark_abs(col("claims_z_score")) > 2, lit(1)).otherwise(lit(0)))
        .withColumn("claims_pct_change",
            spark_round(
                (col("daily_claims") - col("avg_claims")) / col("avg_claims") * 100, 2))
        .withColumn("domain", lit("warranty"))
        .select(
            "domain", "dealer_id", "dealer_name",
            "daily_claims", "avg_claims", "claims_z_score",
            "claims_anomaly", "claims_pct_change",
            "daily_claim_value", "avg_claim_value", "value_z_score"
        )
    )

    # ── Production: daily delay rate anomaly per plant ──────────
    prod_daily_plant = (
        silver_prod
        .filter(to_date(col("production_date")) >= window_30d)
        .groupBy("plant_id", to_date(col("production_date")).alias("day"))
        .agg(
            spark_round(
                spark_sum("is_delayed").cast("double") / count("production_id") * 100, 2
            ).alias("daily_delay_pct"),
            count("production_id").alias("daily_units")
        )
    )

    prod_stats = (
        prod_daily_plant
        .groupBy("plant_id")
        .agg(
            avg("daily_delay_pct").alias("avg_delay_pct"),
            stddev("daily_delay_pct").alias("std_delay_pct")
        )
    )

    prod_anomaly = (
        prod_daily_plant
        .filter(col("day") == today)
        .join(prod_stats, "plant_id")
        .withColumn("delay_z_score",
            spark_round(
                (col("daily_delay_pct") - col("avg_delay_pct")) / col("std_delay_pct"), 2))
        .withColumn("delay_anomaly",
            when(spark_abs(col("delay_z_score")) > 2, lit(1)).otherwise(lit(0)))
        .withColumn("delay_pct_change",
            spark_round(
                (col("daily_delay_pct") - col("avg_delay_pct")) / col("avg_delay_pct") * 100, 2))
        .withColumn("domain", lit("production"))
        .withColumn("dealer_id", lit(None).cast("string"))
        .withColumn("dealer_name", col("plant_id"))
        .select(
            "domain", "dealer_id", "dealer_name",
            col("daily_delay_pct").alias("daily_claims"),
            col("avg_delay_pct").alias("avg_claims"),
            col("delay_z_score").alias("claims_z_score"),
            col("delay_anomaly").alias("claims_anomaly"),
            col("delay_pct_change").alias("claims_pct_change"),
            lit(None).cast("double").alias("daily_claim_value"),
            lit(None).cast("double").alias("avg_claim_value"),
            lit(None).cast("double").alias("value_z_score")
        )
    )

    # ── Service: daily feedback anomaly per dealer ───────────────
    svc_daily_dealer = (
        silver_service
        .filter(to_date(col("service_date")) >= window_30d)
        .groupBy("dealer_id", to_date(col("service_date")).alias("day"))
        .agg(
            spark_round(avg("customer_feedback_rating"), 2).alias("daily_feedback"),
            count("service_id").alias("daily_visits")
        )
    )

    svc_stats = (
        svc_daily_dealer
        .groupBy("dealer_id")
        .agg(
            avg("daily_feedback").alias("avg_feedback"),
            stddev("daily_feedback").alias("std_feedback")
        )
    )

    svc_anomaly = (
        svc_daily_dealer
        .filter(col("day") == today)
        .join(svc_stats, "dealer_id")
        .withColumn("feedback_z_score",
            spark_round(
                (col("daily_feedback") - col("avg_feedback")) / col("std_feedback"), 2))
        .withColumn("feedback_anomaly",
            when(spark_abs(col("feedback_z_score")) > 2, lit(1)).otherwise(lit(0)))
        .withColumn("feedback_pct_change",
            spark_round(
                (col("daily_feedback") - col("avg_feedback")) / col("avg_feedback") * 100, 2))
        .withColumn("domain", lit("service"))
        .withColumn("dealer_name", col("dealer_id"))
        .select(
            "domain", "dealer_id", "dealer_name",
            col("daily_feedback").alias("daily_claims"),
            col("avg_feedback").alias("avg_claims"),
            col("feedback_z_score").alias("claims_z_score"),
            col("feedback_anomaly").alias("claims_anomaly"),
            col("feedback_pct_change").alias("claims_pct_change"),
            lit(None).cast("double").alias("daily_claim_value"),
            lit(None).cast("double").alias("avg_claim_value"),
            lit(None).cast("double").alias("value_z_score")
        )
    )

    return (
        sales_anomaly
        .unionByName(wt_anomaly)
        .unionByName(prod_anomaly)
        .unionByName(svc_anomaly)
        .withColumn("report_date", today)
        .withColumn("_processed_ts", current_timestamp())
    )
