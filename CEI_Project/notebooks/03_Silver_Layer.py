# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC Transform the Bronze data into a clean and standardized dataset by applying all business rules.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import re

CATALOG = "workspace"
SCHEMA = "hrm6131_landing"

# Bronze Tables
BRONZE_PROFILES = f"{CATALOG}.{SCHEMA}.bronze_profiles"
BRONZE_DAY1 = f"{CATALOG}.{SCHEMA}.bronze_day1"
BRONZE_DAY2 = f"{CATALOG}.{SCHEMA}.bronze_day2"

# Silver Table
SILVER_TICKETS = f"{CATALOG}.{SCHEMA}.silver_tickets"

print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Tables

# COMMAND ----------

bronze_profiles = spark.table(BRONZE_PROFILES)
bronze_day1 = spark.table(BRONZE_DAY1)
bronze_day2 = spark.table(BRONZE_DAY2)

# COMMAND ----------

print("Profiles :", bronze_profiles.count())
print("Day 1 :", bronze_day1.count())
print("Day 2 :", bronze_day2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge Day 1 and Day 2 Tickets

# COMMAND ----------

print("Bronze Day1 Columns")
print(bronze_day1.columns)

print("\nBronze Day2 Columns")
print(bronze_day2.columns)

# COMMAND ----------

bronze_day1.printSchema()
bronze_day2.printSchema()

# COMMAND ----------

tickets = bronze_day1.unionByName(bronze_day2)

print("Total Tickets :", tickets.count())

display(tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Duplicate Records

# COMMAND ----------

tickets_clean = tickets.dropDuplicates(["ticket_id"])

print("Tickets before removing duplicates :", tickets.count())
print("Tickets after removing duplicates  :", tickets_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Agent Profiles

# COMMAND ----------

silver_tickets = tickets_clean.join(
    bronze_profiles.select(
        "agent_id",
        "agent_name",
        "role",
        "team_lead_id"
    ),
    on="agent_id",
    how="left"
)

display(silver_tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter Team Scope

# COMMAND ----------

silver_tickets = silver_tickets.filter(
    F.col("team_lead_id").isin(
        "TL01",
        "TL02",
        "TL03",
        "TL04",
        "TL05",
        "TL06",
        "TL07",
        "TL08"
    )
)

print("Rows after team filter :", silver_tickets.count())

display(silver_tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Invalid Resolution Times

# COMMAND ----------

silver_tickets = silver_tickets.filter(
    F.col("resolution_time").rlike(r"^\d+h \d+m \d+s$")
)

print("Rows after removing invalid time records:")
print(silver_tickets.count())

# COMMAND ----------

silver_tickets.select("resolution_time").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Resolution Time

# COMMAND ----------

silver_tickets = (
    silver_tickets
    .withColumn(
        "hours",
        F.expr("try_cast(regexp_extract(resolution_time, '(\\\\d+)h', 1) as int)")
    )
    .withColumn(
        "minutes",
        F.expr("try_cast(regexp_extract(resolution_time, '(\\\\d+)m', 1) as int)")
    )
    .withColumn(
        "seconds",
        F.expr("try_cast(regexp_extract(resolution_time, '(\\\\d+)s', 1) as int)")
    )
)

# COMMAND ----------

silver_tickets = silver_tickets.withColumn(
    "resolution_time_minutes",
    (
        F.regexp_extract("resolution_time", r"(\d+)h", 1).cast("int") * 60
        + F.regexp_extract("resolution_time", r"(\d+)m", 1).cast("int")
        + F.when(
            F.regexp_extract("resolution_time", r"(\d+)s", 1).cast("int") >= 30,
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

#Resolution time conversion
display(
    silver_tickets.select(
        "resolution_time",
        "resolution_time_minutes"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter Resolved Tickets

# COMMAND ----------

silver_tickets = silver_tickets.filter(
    F.col("status") == "Resolved"
)

print("Resolved Tickets:", silver_tickets.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Quality Threshold

# COMMAND ----------

silver_tickets = silver_tickets.filter(
    F.col("resolution_time_minutes") > 15
)

print("Tickets after quality threshold:", silver_tickets.count())

display(silver_tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Day 2 Carry-over Rule

# COMMAND ----------

# Successful Day 1 agents
day1_success_agents = (
    silver_tickets
    .filter(F.col("day") == 1)
    .select("agent_id")
    .distinct()
)
# Remove Day 2 records of successful Day 1 agents
silver_tickets = (
    silver_tickets
    .filter(
        (F.col("day") == 1) |
        (
            (F.col("day") == 2) &
            (~F.col("agent_id").isin(
                [row.agent_id for row in day1_success_agents.collect()]
            ))
        )
    )
)

print(
    "After Day2 carry-over rule:",
    silver_tickets.count()
)

display(silver_tickets)

# COMMAND ----------

# Validation
print("Null validation")

silver_tickets.select(
    [
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in silver_tickets.columns
    ]
).show()

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.hrm6131_landing").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Silver Table

# COMMAND ----------

(
    silver_tickets.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TICKETS)
)

print("Silver table created successfully.")

# COMMAND ----------

silver_tickets.printSchema()

# COMMAND ----------

display(
    silver_tickets.select(
        "ticket_id",
        "agent_id",
        "agent_name",
        "team_lead_id",
        "resolution_time_minutes"
    )
)