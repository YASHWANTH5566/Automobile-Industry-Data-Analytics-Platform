import dlt
from pyspark.sql.functions import (
    col, upper, trim, when, regexp_replace, current_timestamp,
    to_date, to_timestamp, round as spark_round, length, lit,
    coalesce, initcap, lower, regexp_extract, sha2, concat_ws
)

# SILVER LAYER – Automobile Analytics Platform
# Transformations:
#   - Deduplication
#   - Null filtering on critical keys
#   - PII masking (email, contact_number)
#   - Standardization (upper/initcap, trim, date casting)
#   - Enrichment via joins
#   - _processed_ts audit column


# HELPER: VIN validator – must be 17 chars, alphanumeric, no I/O/Q
def is_valid_vin(vin_col):
    return (
        length(vin_col) == 17
    ) & (
        regexp_extract(vin_col, r"^[A-HJ-NPR-Z0-9]{17}$", 0) != ""
    )


# 1. SILVER SALES
#    Source: bronze_sales_new + bronze_customer_new + bronze_dealer_new + bronze_vehicle_master_new
@dlt.table(
    name="silver_sales_new",
    comment="Cleansed, enriched and deduplicated sales transactions"
)
@dlt.expect("valid_sale_amount", "sale_amount > 0")
@dlt.expect("vin_not_null", "vin IS NOT NULL")
def silver_sales_new():

    sales   = dlt.read.table("bronze_sales_new")
    customer = dlt.read.table("bronze_customer_new")
    dealer  = dlt.read.table("bronze_dealer_new")
    vehicle = dlt.read.table("bronze_vehicle_master_new")

    # PII mask on customer before join
    customer_masked = customer.select(
        "customer_id",
        initcap(trim(col("customer_name"))).alias("customer_name"),
        upper(trim(col("gender"))).alias("gender"),
        col("age"),
        initcap(trim(col("city"))).alias("customer_city"),
        upper(trim(col("state"))).alias("customer_state"),
        upper(trim(col("income_range"))).alias("income_range"),
        # Mask email: keep domain, hash local part
        sha2(col("email"), 256).alias("email_hash"),
        # Mask contact: show last 4 digits only
        regexp_replace(col("contact_number"), r"\d(?=\d{4})", "*").alias("contact_number_masked")
    )

    dealer_clean = dealer.select(
        "dealer_id",
        initcap(trim(col("dealer_name"))).alias("dealer_name"),
        upper(trim(col("dealer_type"))).alias("dealer_type"),
        upper(trim(col("region"))).alias("dealer_region"),
        initcap(trim(col("city"))).alias("dealer_city"),
        upper(trim(col("state"))).alias("dealer_state"),
        col("rating").alias("dealer_rating")
    )

    vehicle_clean = vehicle.select(
        "vin",
        "model_code",
        initcap(trim(col("model_name"))).alias("model_name"),
        upper(trim(col("fuel_type"))).alias("fuel_type"),
        upper(trim(col("transmission"))).alias("transmission"),
        upper(trim(col("variant"))).alias("variant"),
        col("ex_showroom_price")
    )

    return (
        sales
        # Quality filters
        .filter(col("vin").isNotNull())
        .filter(col("sale_amount") > 0)
        .filter(is_valid_vin(col("vin")))
        .dropDuplicates(["vin", "sale_date"])

        # Standardize sales columns
        .withColumn("sale_date",    to_date(col("sale_date")))
        .withColumn("region",       upper(trim(col("region"))))
        .withColumn("channel",      upper(trim(col("channel"))))
        .withColumn("payment_mode", upper(trim(col("payment_mode"))))
        .withColumn("city",         initcap(trim(col("city"))))
        .withColumn("sale_amount",  spark_round(col("sale_amount"), 2))
        .withColumn("discount",     spark_round(coalesce(col("discount"), lit(0.0)), 2))
        .withColumn("tax_amount",   spark_round(coalesce(col("tax_amount"), lit(0.0)), 2))
        .withColumn("net_revenue",  spark_round(
            col("sale_amount") - col("discount") + col("tax_amount"), 2))

        # Enrich with customer
        .join(customer_masked, "customer_id", "left")

        # Enrich with dealer
        .join(dealer_clean, "dealer_id", "left")

        # Enrich with vehicle
        .join(vehicle_clean, "vin", "left")

        # Audit
        .withColumn("_processed_ts", current_timestamp())

        # Drop raw PII fields that were replaced
        .drop("email", "contact_number", "_ingest_ts")
    )


# 2. SILVER PRODUCTION
#    Source: bronze_production_new + bronze_vehicle_master_new
@dlt.table(
    name="silver_production_new",
    comment="Cleansed production records enriched with vehicle master"
)
@dlt.expect("vin_not_null", "vin IS NOT NULL")
@dlt.expect("positive_production_time", "production_time_minutes > 0")
def silver_production_new():

    production = dlt.read("bronze_production_new")
    vehicle    = dlt.read("bronze_vehicle_master_new")

    vehicle_clean = vehicle.select(
        "vin", "model_code",
        initcap(trim(col("model_name"))).alias("model_name"),
        upper(trim(col("fuel_type"))).alias("fuel_type"),
        upper(trim(col("variant"))).alias("variant"),
        "plant_id"
    )

    return (
        production
        .filter(col("vin").isNotNull())
        .filter(col("production_time_minutes") > 0)
        .dropDuplicates(["production_id"])

        # Standardize
        .withColumn("production_date", to_date(col("production_date")))
        .withColumn("status",          upper(trim(col("status"))))
        .withColumn("shift",           upper(trim(col("shift"))))
        .withColumn("assembly_line",   upper(trim(col("assembly_line"))))
        .withColumn("plant_id",        upper(trim(col("plant_id"))))

        # Derived: efficiency flag
        .withColumn("is_completed",
            when(col("status") == "COMPLETED", 1).otherwise(0))
        .withColumn("is_delayed",
            when(col("status") == "DELAYED", 1).otherwise(0))

        # Enrich with vehicle 
        .join(vehicle_clean.drop("plant_id"), "vin", "left")

        .withColumn("_processed_ts", current_timestamp())
        .drop("_ingest_ts")
    )


# 3. SILVER SERVICE
#    Source: bronze_service_new + bronze_warranty_new + bronze_dealer_new + bronze_vehicle_master_new
@dlt.table(
    name="silver_service_new",
    comment="Cleansed service records enriched with warranty and dealer info"
)
@dlt.expect("vin_not_null", "vin IS NOT NULL")
@dlt.expect("valid_service_cost", "service_cost >= 0")
def silver_service_new():

    service = dlt.read("bronze_service_new")
    warranty = dlt.read("bronze_warranty_new")
    dealer   = dlt.read("bronze_dealer_new")
    vehicle  = dlt.read("bronze_vehicle_master_new")

    warranty_clean = warranty.select(
        "vin",
        col("claim_id"),
        to_date(col("claim_date")).alias("claim_date"),
        upper(trim(col("claim_status"))).alias("claim_status"),
        spark_round(col("claim_amount"), 2).alias("claim_amount"),
        "part_id"
    )

    dealer_clean = dealer.select(
        "dealer_id",
        initcap(trim(col("dealer_name"))).alias("dealer_name"),
        upper(trim(col("region"))).alias("dealer_region"),
        upper(trim(col("dealer_type"))).alias("dealer_type")
    )

    vehicle_clean = vehicle.select(
        "vin",
        "model_code",
        initcap(trim(col("model_name"))).alias("model_name"),
        upper(trim(col("fuel_type"))).alias("fuel_type")
    )

    return (
        service
        .filter(col("vin").isNotNull())
        .filter(col("service_cost") >= 0)
        .dropDuplicates(["service_id"])

        # Standardize
        .withColumn("service_date",  to_date(col("service_date")))
        .withColumn("service_type",  upper(trim(col("service_type"))))
        .withColumn("service_cost",  spark_round(col("service_cost"), 2))
        .withColumn("mileage",       coalesce(col("mileage"), lit(0)))
        .withColumn("customer_feedback_rating",
            when(col("customer_feedback_rating").between(1, 5),
                 col("customer_feedback_rating"))
            .otherwise(lit(None)))

        # Warranty enrichment (left join on vin)
        .join(warranty_clean, "vin", "left")

        # Dealer enrichment
        .join(dealer_clean, "dealer_id", "left")

        # Vehicle enrichment ──
        .join(vehicle_clean, "vin", "left")

        # Derived: is warranty covered
        .withColumn("is_warranty_claim",
            when(col("claim_id").isNotNull(), 1).otherwise(0))

        .withColumn("_processed_ts", current_timestamp())
        .drop("_ingest_ts")
    )


# 4. SILVER INVENTORY
#    Source: bronze_inventory_new + bronze_parts_new + bronze_dealer_parts_new
@dlt.table(
    name="silver_inventory_new",
    comment="Cleansed inventory enriched with parts master and dealer stock"
)
@dlt.expect("part_id_not_null", "part_id IS NOT NULL")
def silver_inventory_new():

    inventory    = dlt.read("bronze_inventory_new")
    parts        = dlt.read("bronze_parts_new")
    dealer_parts = dlt.read("bronze_dealer_parts_new")

    parts_clean = parts.select(
        "part_id",
        initcap(trim(col("part_name"))).alias("part_name"),
        upper(trim(col("category"))).alias("category"),
        "supplier_id",
        spark_round(col("unit_cost"), 2).alias("unit_cost"),
        to_date(col("manufacture_date")).alias("manufacture_date"),
        to_date(col("expiry_date")).alias("expiry_date"),
        col("quality_rating")
    )

    dealer_parts_clean = dealer_parts.select(
        "part_id",
        "dealer_id",
        coalesce(col("available_stock"), lit(0)).alias("dealer_available_stock"),
        to_date(col("last_restock_date")).alias("last_restock_date")
    )

    return (
        inventory
        .filter(col("part_id").isNotNull())
        .dropDuplicates(["inventory_id"])

        # Standardize
        .withColumn("stock_quantity",
            coalesce(col("stock_quantity"), lit(0)))
        .withColumn("reorder_level",
            coalesce(col("reorder_level"), lit(0)))
        .withColumn("warehouse_id",  upper(trim(col("warehouse_id"))))

        # Derived: below reorder flag
        .withColumn("is_below_reorder",
            when(col("stock_quantity") < col("reorder_level"), 1).otherwise(0))

        # Parts enrichment
        .join(parts_clean, "part_id", "left")

        # Dealer parts enrichment
        .join(dealer_parts_clean, "part_id", "left")

        # Derived: inventory value
        .withColumn("inventory_value",
            spark_round(col("stock_quantity") * col("unit_cost"), 2))

        .withColumn("_processed_ts", current_timestamp())
        .drop("_ingest_ts")
    )


# 5. SILVER WARRANTY  (standalone – consumed by gold warranty_claims)
#    Source: bronze_warranty_new + bronze_parts_new + bronze_vehicle_master_new + bronze_dealer_new
@dlt.table(
    name="silver_warranty_new",
    comment="Cleansed warranty claims with parts, vehicle and dealer enrichment"
)
@dlt.expect("vin_not_null", "vin IS NOT NULL")
@dlt.expect("valid_claim_amount", "claim_amount >= 0")
def silver_warranty_new():

    warranty = dlt.read("bronze_warranty_new")
    parts    = dlt.read("bronze_parts_new")
    vehicle  = dlt.read("bronze_vehicle_master_new")
    dealer   = dlt.read("bronze_dealer_new")

    parts_clean = parts.select(
        "part_id",
        initcap(trim(col("part_name"))).alias("part_name"),
        upper(trim(col("category"))).alias("part_category"),
        col("quality_rating")
    )

    vehicle_clean = vehicle.select(
        "vin",
        "model_code",
        initcap(trim(col("model_name"))).alias("model_name"),
        upper(trim(col("fuel_type"))).alias("fuel_type")
    )

    dealer_clean = dealer.select(
        "dealer_id",
        initcap(trim(col("dealer_name"))).alias("dealer_name"),
        upper(trim(col("region"))).alias("dealer_region")
    )

    return (
        warranty
        .filter(col("vin").isNotNull())
        .filter(col("claim_amount") >= 0)
        .dropDuplicates(["claim_id"])

        .withColumn("claim_date",   to_date(col("claim_date")))
        .withColumn("claim_status", upper(trim(col("claim_status"))))
        .withColumn("claim_amount", spark_round(col("claim_amount"), 2))

        # Derived flags
        .withColumn("is_approved",
            when(col("claim_status") == "APPROVED", 1).otherwise(0))
        .withColumn("is_rejected",
            when(col("claim_status") == "REJECTED", 1).otherwise(0))

        .join(parts_clean, "part_id", "left")
        .join(vehicle_clean, "vin", "left")
        .join(dealer_clean, "dealer_id", "left")

        .withColumn("_processed_ts", current_timestamp())
        .drop("_ingest_ts")
    )
