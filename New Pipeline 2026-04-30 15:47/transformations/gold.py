import dlt
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    round as spark_round, countDistinct, when, lit, current_timestamp,
    to_date, date_format, month, year, quarter, datediff, concat_ws, stddev
)

# ============================================================
# GOLD LAYER – Automotive KPI Tables
# All gold tables read from silver layer.
# ============================================================


# ------------------------------------------------------------------
# GOLD 1: SALES PERFORMANCE
#   Grain: model_code + region + sale_month
#   KPIs : units sold, revenue, net revenue, avg discount,
#          avg sale amount, channel mix, fuel type mix
# ------------------------------------------------------------------
@dlt.table(
    name="gold_sales_performance",
    comment="Monthly sales KPIs by model, region and channel"
)
def gold_sales_performance():

    silver_sales = dlt.read("silver_sales_new")

    return (
        silver_sales
        .withColumn("sale_year",    year(col("sale_date")))
        .withColumn("sale_month",   month(col("sale_date")))
        .withColumn("sale_quarter", concat_ws("Q",
            year(col("sale_date")).cast("string"),
            quarter(col("sale_date")).cast("string")))
        .withColumn("sale_month_label",
            date_format(col("sale_date"), "yyyy-MM"))

        .groupBy(
            "sale_year",
            "sale_month",
            "sale_month_label",
            "sale_quarter",
            "model_code",
            "model_name",
            "fuel_type",
            "dealer_region",
            "channel",
            "payment_mode",
            "variant"
        )
        .agg(
            countDistinct("vin").alias("units_sold"),
            spark_round(spark_sum("sale_amount"), 2).alias("gross_revenue"),
            spark_round(spark_sum("net_revenue"), 2).alias("net_revenue"),
            spark_round(spark_sum("discount"), 2).alias("total_discount"),
            spark_round(spark_sum("tax_amount"), 2).alias("total_tax"),
            spark_round(avg("sale_amount"), 2).alias("avg_sale_amount"),
            spark_round(avg("discount"), 2).alias("avg_discount"),
            countDistinct("model_code").alias("models_sold"),
            countDistinct("dealer_id").alias("active_dealers"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("discount_pct",
            spark_round((col("total_discount") / col("gross_revenue")) * 100, 2))
        .withColumn("revenue_per_unit",
            spark_round(col("net_revenue") / col("units_sold"), 2))
        .withColumn("_processed_ts", current_timestamp())
    )


# ------------------------------------------------------------------
# GOLD 2: PRODUCTION EFFICIENCY
#   Grain: plant_id + assembly_line + production_month
#   KPIs : total units, completed, delayed, avg prod time,
#          completion rate, throughput by shift
# ------------------------------------------------------------------
@dlt.table(
    name="gold_production_efficiency",
    comment="Monthly plant and assembly line efficiency KPIs"
)
def gold_production_efficiency():

    silver_prod = dlt.read("silver_production_new")

    return (
        silver_prod
        .withColumn("prod_year",   year(col("production_date")))
        .withColumn("prod_month",  month(col("production_date")))
        .withColumn("prod_month_label",
            date_format(col("production_date"), "yyyy-MM"))
        .withColumn("prod_quarter",
            concat_ws("Q",
                year(col("production_date")).cast("string"),
                quarter(col("production_date")).cast("string")))

        .groupBy(
            "prod_year",
            "prod_month",
            "prod_month_label",
            "prod_quarter",
            "plant_id",
            "assembly_line",
            "shift",
            "model_code",
            "model_name",
            "fuel_type",
            "status"
        )
        .agg(
            count("production_id").alias("total_units"),
            spark_sum("is_completed").alias("completed_units"),
            spark_sum("is_delayed").alias("delayed_units"),
            spark_round(avg("production_time_minutes"), 2).alias("avg_production_time_minutes"),
            spark_round(spark_min("production_time_minutes"), 2).alias("min_production_time"),
            spark_round(spark_max("production_time_minutes"), 2).alias("max_production_time"),
            countDistinct("vin").alias("unique_vins_produced")
        )
        .withColumn("completion_rate_pct",
            spark_round((col("completed_units") / col("total_units")) * 100, 2))
        .withColumn("delay_rate_pct",
            spark_round((col("delayed_units") / col("total_units")) * 100, 2))
        .withColumn("_processed_ts", current_timestamp())
    )


# ------------------------------------------------------------------
# GOLD 3: WARRANTY CLAIMS
#   Grain: dealer_id + part_category + model_code + claim_month
#   KPIs : total claims, approved, rejected, pending,
#          total claim value, avg claim value, approval rate
# ------------------------------------------------------------------
@dlt.table(
    name="gold_warranty_claims",
    comment="Warranty claim analytics by dealer, part category and model"
)
def gold_warranty_claims():

    silver_warranty = dlt.read("silver_warranty_new")
    silver_service  = dlt.read("silver_service_new")

    # ── Aggregate service satisfaction per dealer ──
    svc_summary = (
        silver_service
        .groupBy("dealer_id")
        .agg(
            spark_round(avg("customer_feedback_rating"), 2)
                .alias("avg_service_feedback"),
            count("service_id").alias("total_service_visits"),
            spark_round(avg("service_cost"), 2).alias("avg_service_cost")
        )
    )

    return (
        silver_warranty
        .withColumn("claim_year",  year(col("claim_date")))
        .withColumn("claim_month", month(col("claim_date")))
        .withColumn("claim_month_label",
            date_format(col("claim_date"), "yyyy-MM"))
        .withColumn("claim_quarter",
            concat_ws("Q",
                year(col("claim_date")).cast("string"),
                quarter(col("claim_date")).cast("string")))

        .groupBy(
            "claim_year",
            "claim_month",
            "claim_month_label",
            "claim_quarter",
            "dealer_id",
            "dealer_name",
            "dealer_region",
            "model_code",
            "model_name",
            "fuel_type",
            "part_id",
            "part_name",
            "part_category",
            "claim_status"
        )
        .agg(
            count("claim_id").alias("total_claims"),
            spark_sum("is_approved").alias("approved_claims"),
            spark_sum("is_rejected").alias("rejected_claims"),
            spark_round(spark_sum("claim_amount"), 2).alias("total_claim_amount"),
            spark_round(avg("claim_amount"), 2).alias("avg_claim_amount"),
            spark_round(spark_max("claim_amount"), 2).alias("max_claim_amount"),
            countDistinct("vin").alias("unique_vehicles_claimed")
        )
        .withColumn("approval_rate_pct",
            spark_round((col("approved_claims") / col("total_claims")) * 100, 2))
        .withColumn("rejection_rate_pct",
            spark_round((col("rejected_claims") / col("total_claims")) * 100, 2))
        .withColumn("pending_claims",
            col("total_claims") - col("approved_claims") - col("rejected_claims"))

        # ── Join service feedback ──
        .join(svc_summary, "dealer_id", "left")

        .withColumn("_processed_ts", current_timestamp())
    )


# ------------------------------------------------------------------
# GOLD 4: DEALER SCORECARD
#   Grain: dealer_id + month
#   KPIs : sales volume, revenue, service visits, avg rating,
#          warranty claims filed, inventory health, composite score
# ------------------------------------------------------------------
@dlt.table(
    name="gold_dealer_scorecard",
    comment="Comprehensive monthly dealer performance scorecard"
)
def gold_dealer_scorecard():

    silver_sales    = dlt.read("silver_sales_new")
    silver_service  = dlt.read("silver_service_new")
    silver_warranty = dlt.read("silver_warranty_new")
    silver_inventory = dlt.read("silver_inventory_new")

    # ── Sales aggregation per dealer per month ──
    sales_agg = (
        silver_sales
        .withColumn("sale_month_label",
            date_format(col("sale_date"), "yyyy-MM"))
        .withColumn("sale_year",  year(col("sale_date")))
        .withColumn("sale_month", month(col("sale_date")))
        .groupBy("dealer_id", "dealer_name", "dealer_region",
                 "dealer_type", "dealer_rating",
                 "sale_year", "sale_month", "sale_month_label")
        .agg(
            countDistinct("vin").alias("units_sold"),
            spark_round(spark_sum("net_revenue"), 2).alias("total_revenue"),
            spark_round(avg("sale_amount"), 2).alias("avg_sale_amount"),
            spark_round(avg("discount"), 2).alias("avg_discount"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("model_code").alias("models_sold")
        )
    )

    # ── Service aggregation per dealer per month ──
    service_agg = (
        silver_service
        .withColumn("svc_month_label",
            date_format(col("service_date"), "yyyy-MM"))
        .groupBy("dealer_id", "svc_month_label")
        .agg(
            count("service_id").alias("service_visits"),
            spark_round(avg("service_cost"), 2).alias("avg_service_cost"),
            spark_round(avg("customer_feedback_rating"), 2).alias("avg_feedback_score"),
            spark_sum("is_warranty_claim").alias("warranty_backed_services")
        )
        .withColumnRenamed("svc_month_label", "sale_month_label")
    )

    # ── Warranty aggregation per dealer per month ──
    warranty_agg = (
        silver_warranty
        .withColumn("wt_month_label",
            date_format(col("claim_date"), "yyyy-MM"))
        .groupBy("dealer_id", "wt_month_label")
        .agg(
            count("claim_id").alias("warranty_claims_filed"),
            spark_round(spark_sum("claim_amount"), 2).alias("total_warranty_cost"),
            spark_round(avg("is_approved") * 100, 2).alias("warranty_approval_rate_pct")
        )
        .withColumnRenamed("wt_month_label", "sale_month_label")
    )

    # ── Inventory health per dealer ──
    inventory_health = (
        silver_inventory
        .groupBy("dealer_id")
        .agg(
            spark_sum("dealer_available_stock").alias("total_parts_stock"),
            spark_round(avg("is_below_reorder") * 100, 2).alias("pct_parts_below_reorder"),
            spark_round(spark_sum("inventory_value"), 2).alias("total_inventory_value")
        )
    )

    return (
        sales_agg
        # ── Join service ──
        .join(service_agg, ["dealer_id", "sale_month_label"], "left")
        # ── Join warranty ──
        .join(warranty_agg, ["dealer_id", "sale_month_label"], "left")
        # ── Join inventory (static, no month grain) ──
        .join(inventory_health, "dealer_id", "left")

        # ── Composite Dealer Score (0–100) ──
        # Weighted: Revenue 40%, Service Feedback 30%, Warranty Approval 20%, Inventory Health 10%
        .withColumn("composite_score",
            spark_round(
                (
                    # revenue score: normalize units_sold contribution (cap at 50 = 40pts)
                    when(col("units_sold") >= 50, lit(40.0))
                    .otherwise((col("units_sold") / lit(50.0)) * 40.0)
                ) + (
                    # service feedback: out of 5 → out of 30
                    when(col("avg_feedback_score").isNotNull(),
                         (col("avg_feedback_score") / lit(5.0)) * 30.0)
                    .otherwise(lit(0.0))
                ) + (
                    # warranty approval rate: pct → out of 20
                    when(col("warranty_approval_rate_pct").isNotNull(),
                         col("warranty_approval_rate_pct") / lit(5.0))
                    .otherwise(lit(0.0))
                ) + (
                    # inventory health: inverse of pct_below_reorder → out of 10
                    when(col("pct_parts_below_reorder").isNotNull(),
                         ((lit(100.0) - col("pct_parts_below_reorder")) / lit(100.0)) * 10.0)
                    .otherwise(lit(10.0))
                ), 2))

        # ── Score tier ──
        .withColumn("performance_tier",
            when(col("composite_score") >= 80, lit("PLATINUM"))
            .when(col("composite_score") >= 60, lit("GOLD"))
            .when(col("composite_score") >= 40, lit("SILVER"))
            .otherwise(lit("BRONZE")))

        .withColumn("_processed_ts", current_timestamp())
    )



# ============================================================
# NEW GOLD 5: WARRANTY ANOMALY (BEHAVIOR-BASED) 
# ============================================================
@dlt.table(
    name="gold_warranty_anomaly_behavioral",
    comment="Detect anomalies using dealer historical behavior (z-score)"
)
def gold_warranty_anomaly_behavioral():

    sales = dlt.read("silver_sales_new").select("vin","dealer_id")
    warranty = dlt.read("silver_warranty_new")

    df = warranty.join(sales, ["vin","dealer_id"], "left")

    dealer_monthly = (
        df.withColumn("month", date_format(col("claim_date"), "yyyy-MM"))
        .groupBy("dealer_id","month")
        .agg(
            count("claim_id").alias("total_claims"),
            countDistinct("vin").alias("vehicles_claimed")
        )
        .withColumn("claim_rate",
            col("total_claims") / col("vehicles_claimed"))
    )

    stats = dealer_monthly.groupBy("dealer_id").agg(
        avg("claim_rate").alias("avg_claim_rate"),
        stddev("claim_rate").alias("stddev_claim_rate")
    )

    return (
        dealer_monthly.join(stats, "dealer_id")
        .withColumn("z_score",
            (col("claim_rate") - col("avg_claim_rate")) / col("stddev_claim_rate"))
        .withColumn("anomaly_flag",
            when(col("z_score") > 2, 1).otherwise(0))
        .withColumn("_processed_ts", current_timestamp())
    )


# ============================================================
# NEW GOLD 6: DISCOUNT ANOMALY (BEHAVIOR-BASED) 
# ============================================================
@dlt.table(
    name="gold_discount_anomaly_behavioral",
    comment="Detect discount leakage using dealer historical behavior"
)
def gold_discount_anomaly_behavioral():

    sales = dlt.read("silver_sales_new")

    dealer_monthly = (
        sales
        .withColumn("month", date_format(col("sale_date"), "yyyy-MM"))
        .groupBy("dealer_id","month")
        .agg(
            spark_sum("discount").alias("total_discount"),
            spark_sum("sale_amount").alias("gross_sales")
        )
        .withColumn("discount_pct",
            col("total_discount") / col("gross_sales"))
    )

    stats = dealer_monthly.groupBy("dealer_id").agg(
        avg("discount_pct").alias("avg_discount_pct"),
        stddev("discount_pct").alias("stddev_discount_pct")
    )

    return (
        dealer_monthly.join(stats, "dealer_id")
        .withColumn("z_score",
            (col("discount_pct") - col("avg_discount_pct")) / col("stddev_discount_pct"))
        .withColumn("anomaly_flag",
            when(col("z_score") > 2, 1).otherwise(0))
        .withColumn("_processed_ts", current_timestamp())
    )