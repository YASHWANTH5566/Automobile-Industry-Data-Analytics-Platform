import dlt
from pyspark.sql.functions import *

# 1.CUSTOMER
@dlt.table(name="bronze_customer_new")
def bronze_customer_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/customer/")
        .load("s3://automobile-pipeline/Raw/customer_new/")
        .filter(col("_metadata.file_path").contains("customer"))
    )
#2. DEALER_PARTS
@dlt.table(name="bronze_dealer_parts_new")
def bronze_dealer_parts_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/dealer_parts/")
        .load("s3://automobile-pipeline/Raw/dealer_parts_new/")
    )

# 3.DEALER
@dlt.table(name="bronze_dealer_new")
def bronze_dealer_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/dealer/")
        .load("s3://automobile-pipeline/Raw/dealer_new/")
    )

# 4.INVENTORY
@dlt.table(name="bronze_inventory_new")
def bronze_inventory_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/inventory/")
        .load("s3://automobile-pipeline/Raw/inventory_new/")
    )

#5. PARTS
@dlt.table(name="bronze_parts_new")
def bronze_parts_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/parts/")
        .load("s3://automobile-pipeline/Raw/parts_new/")
    )

#6. PRODUCTION
@dlt.table(name="bronze_production_new")
def bronze_production_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/production/")
        .load("s3://automobile-pipeline/Raw/production_new/")
    )

# 7.SALES
@dlt.table(name="bronze_sales_new")
def bronze_sales_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/sales/")
        .load("s3://automobile-pipeline/Raw/sales_new/")
    )

#8.SERVICE
@dlt.table(name="bronze_service_new")
def bronze_service_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/service/")
        .load("s3://automobile-pipeline/Raw/service_new/")
    )
# 9.VEHICLE_MASTER
@dlt.table(name="bronze_vehicle_master_new")
def bronze_vehicle_master_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/vehicle_master/")
        .load("s3://automobile-pipeline/Raw/vehicle_master_new/")
    )
#10.Warranty
@dlt.table(name="bronze_warranty_new")
def bronze_warranty_new():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", "s3://automobile-pipeline/checkpoints/warranty/")
        .load("s3://automobile-pipeline/Raw/warranty_new/")
    )


