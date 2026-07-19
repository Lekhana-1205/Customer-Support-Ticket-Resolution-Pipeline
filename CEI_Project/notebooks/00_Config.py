# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Support Ticket Resolution Pipeline
# MAGIC
# MAGIC **Pipeline Design:** Medallion Architecture (Bronze → Silver → Gold)
# MAGIC
# MAGIC ## Configuration
# MAGIC
# MAGIC This notebook initializes the project environment for the Customer Support Ticket Resolution Pipeline.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Import required libraries
# MAGIC - Configure the project workspace
# MAGIC - Create the project schema
# MAGIC - Define table references
# MAGIC - Verify the project configuration
# MAGIC
# MAGIC **Author:** Lekhana Donthula  
# MAGIC **Internship:** Celebal Excellence Internship – Data Engineering

# COMMAND ----------

# Import Required Libraries

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Project
# MAGIC
# MAGIC Create widgets to define the catalog and schema. Using widgets makes the notebook reusable across different environments without changing the code.

# COMMAND ----------

# Remove existing widgets (if any)
dbutils.widgets.removeAll()

# Create project widgets
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "hrm6131_landing")

# Read widget values
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Project Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Table References
# MAGIC
# MAGIC Create fully qualified table names that will be used throughout the pipeline.

# COMMAND ----------

# Raw Tables
TBL_PROFILES = f"{CATALOG}.{SCHEMA}.agent_profiles"
TBL_DAY1 = f"{CATALOG}.{SCHEMA}.day1_tickets"
TBL_DAY2 = f"{CATALOG}.{SCHEMA}.day2_tickets"

# Bronze Tables
BRONZE_DAY1 = f"{CATALOG}.{SCHEMA}.bronze_day1"
BRONZE_DAY2 = f"{CATALOG}.{SCHEMA}.bronze_day2"
BRONZE_PROFILES = f"{CATALOG}.{SCHEMA}.bronze_profiles"

# Silver Tables
SILVER_TICKETS = f"{CATALOG}.{SCHEMA}.silver_tickets"

# Gold Tables
GOLD_TEAM_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_team_performance"
GOLD_AGENT_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_agent_performance"
GOLD_QUALITY_COMPLIANCE = f"{CATALOG}.{SCHEMA}.gold_quality_compliance"
GOLD_DAY2_CARRYOVER = f"{CATALOG}.{SCHEMA}.gold_day2_carryover"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Configuration

# COMMAND ----------

print("Project configuration loaded successfully.")
print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")

# COMMAND ----------

# Project Name

PROJECT_NAME = "Customer Support Ticket Resolution Pipeline"