# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC Load the raw datasets into the Bronze layer while preserving the original data._

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Load the ingested customer support datasets into the Bronze layer while preserving the original data and adding ingestion metadata for tracking.
# MAGIC
# MAGIC Input datasets:
# MAGIC - agent_profiles
# MAGIC - day1_tickets
# MAGIC - day2_tickets
# MAGIC
# MAGIC Output:
# MAGIC - bronze_profiles
# MAGIC - bronze_day1
# MAGIC - bronze_day2

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

CATALOG = "workspace"
SCHEMA = "hrm6131_landing"

# Raw Tables
TBL_PROFILES = f"{CATALOG}.{SCHEMA}.agent_profiles"
TBL_DAY1 = f"{CATALOG}.{SCHEMA}.day1_tickets"
TBL_DAY2 = f"{CATALOG}.{SCHEMA}.day2_tickets"

# Bronze Tables
BRONZE_PROFILES = f"{CATALOG}.{SCHEMA}.bronze_profiles"
BRONZE_DAY1 = f"{CATALOG}.{SCHEMA}.bronze_day1"
BRONZE_DAY2 = f"{CATALOG}.{SCHEMA}.bronze_day2"

print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Tables

# COMMAND ----------

raw_profiles = spark.table(TBL_PROFILES)
raw_day1 = spark.table(TBL_DAY1)
raw_day2 = spark.table(TBL_DAY2)

# COMMAND ----------

display(raw_profiles)
display(raw_day1)
display(raw_day2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Ingestion Metadata

# COMMAND ----------

bronze_profiles = (
    raw_profiles
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit("agent_profiles.csv"))
)

# COMMAND ----------

bronze_day1 = (
    raw_day1
    .withColumnRenamed("category", "ticket_category")
    .withColumn("day", F.lit(1))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit("day1_tickets.csv"))
)

# COMMAND ----------

bronze_day2 = (
    raw_day2
    .withColumn("day", F.lit(2))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit("day2_tickets.csv"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Bronze Tables

# COMMAND ----------

(
    bronze_profiles.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_PROFILES)
)

(
    bronze_day1.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_DAY1)
)

(
    bronze_day2.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_DAY2)
)

print("Bronze tables created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Layer

# COMMAND ----------

print(f"Bronze Profiles : {spark.table(BRONZE_PROFILES).count()} rows")
print(f"Bronze Day 1    : {spark.table(BRONZE_DAY1).count()} rows")
print(f"Bronze Day 2    : {spark.table(BRONZE_DAY2).count()} rows")

# COMMAND ----------

display(spark.table(BRONZE_PROFILES))
display(spark.table(BRONZE_DAY1))
display(spark.table(BRONZE_DAY2))