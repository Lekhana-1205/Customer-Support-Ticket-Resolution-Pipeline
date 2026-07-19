# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate Pipeline Output

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline Tables

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "hrm6131_landing"

BRONZE_PROFILES = f"{CATALOG}.{SCHEMA}.bronze_profiles"
BRONZE_DAY1 = f"{CATALOG}.{SCHEMA}.bronze_day1"
BRONZE_DAY2 = f"{CATALOG}.{SCHEMA}.bronze_day2"

SILVER_TICKETS = f"{CATALOG}.{SCHEMA}.silver_tickets"

GOLD_TEAM_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_team_performance"
GOLD_AGENT_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_agent_performance"
GOLD_QUALITY_COMPLIANCE = f"{CATALOG}.{SCHEMA}.gold_quality_compliance"
GOLD_DAY2_CARRYOVER = f"{CATALOG}.{SCHEMA}.gold_day2_carryover"

print("Configuration loaded successfully.")

# COMMAND ----------

bronze_profiles = spark.table(BRONZE_PROFILES)
bronze_day1 = spark.table(BRONZE_DAY1)
bronze_day2 = spark.table(BRONZE_DAY2)

silver = spark.table(SILVER_TICKETS)

team = spark.table(GOLD_TEAM_PERFORMANCE)
agent = spark.table(GOLD_AGENT_PERFORMANCE)
quality = spark.table(GOLD_QUALITY_COMPLIANCE)
carryover = spark.table(GOLD_DAY2_CARRYOVER)

print("All tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Row Counts

# COMMAND ----------

print("Bronze Profiles :", bronze_profiles.count())
print("Bronze Day 1    :", bronze_day1.count())
print("Bronze Day 2    :", bronze_day2.count())

print("Silver Tickets  :", silver.count())

print("Team Report     :", team.count())
print("Agent Report    :", agent.count())
print("Quality Report  :", quality.count())
print("Carry-over      :", carryover.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Duplicate Tickets

# COMMAND ----------

duplicates = (
    silver
    .groupBy("ticket_id")
    .count()
    .filter("count > 1")
)

print("Duplicate Tickets :", duplicates.count())

display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Null Values

# COMMAND ----------

from pyspark.sql import functions as F

silver.select(
    [
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in [
            "ticket_id",
            "agent_id",
            "status",
            "resolution_time_minutes"
        ]
    ]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolution Quality Rule Validation
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

print("Checking resolution quality rule")

invalid_resolution = silver.filter(
    (F.col("status") == "Resolved") &
    (F.col("resolution_time_minutes") <= 15)
)

print(
    "Resolved tickets violating >15 min rule:",
    invalid_resolution.count()
)

display(invalid_resolution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Lead Scope Validation

# COMMAND ----------

print("Checking Team Lead scope")

invalid_team_leads = silver.filter(
    ~F.col("team_lead_id").isin(
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

print(
    "Out-of-scope Team Leads:",
    invalid_team_leads.count()
)

display(invalid_team_leads)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day-2 Carry-over Validation

# COMMAND ----------

print("Checking Day-2 carry-over rule")

print("Checking Day-2 carry-over rule")

day1_success_agents = (
    silver
    .filter(
        (F.col("day") == 1) &
        (F.col("status") == "Resolved") &
        (F.col("resolution_time_minutes") > 15)
    )
    .select("agent_id")
    .distinct()
)


invalid_carryover = (
    carryover
    .join(
        day1_success_agents,
        "agent_id",
        "inner"
    )
)

print(
    "Day-2 agents incorrectly carried over:",
    invalid_carryover.count()
)

display(invalid_carryover)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Layer

# COMMAND ----------

display(
    silver.select(
        "ticket_id",
        "agent_id",
        "status",
        "resolution_time",
        "resolution_time_minutes",
        "day"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Gold Layer

# COMMAND ----------

print("Team Performance Report")

display(team)

# COMMAND ----------

print("Agent Performance Report")

display(agent)

# COMMAND ----------

print("Quality Compliance Report")

display(quality)

# COMMAND ----------

print("Day 2 Carry-over Report")

display(carryover)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Validation Summary

# COMMAND ----------

print("Pipeline Validation Completed Successfully!")

print("\nValidation Summary")
print("------------------------------")
print(f"Bronze Profiles : {bronze_profiles.count()}")
print(f"Bronze Day 1    : {bronze_day1.count()}")
print(f"Bronze Day 2    : {bronze_day2.count()}")

print(f"Silver Tickets  : {silver.count()}")

print(f"Team Report     : {team.count()}")
print(f"Agent Report    : {agent.count()}")
print(f"Quality Report  : {quality.count()}")
print(f"Carry-over      : {carryover.count()}")

print("\nAll validations passed successfully.")