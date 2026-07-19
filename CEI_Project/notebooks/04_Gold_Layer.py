# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC
# MAGIC This notebook creates business-ready reports from the cleaned Silver dataset.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "hrm6131_landing"

SILVER_TICKETS = f"{CATALOG}.{SCHEMA}.silver_tickets"

GOLD_TEAM_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_team_performance"
GOLD_AGENT_PERFORMANCE = f"{CATALOG}.{SCHEMA}.gold_agent_performance"
GOLD_QUALITY_COMPLIANCE = f"{CATALOG}.{SCHEMA}.gold_quality_compliance"
GOLD_DAY2_CARRYOVER = f"{CATALOG}.{SCHEMA}.gold_day2_carryover"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

silver = spark.table(SILVER_TICKETS)

print("Silver Records:", silver.count())

display(silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Performance Report

# COMMAND ----------

team_performance = (
    silver
    .groupBy(
        "day",
        "team_lead_id"
    )
    .agg(
        F.count("ticket_id").alias("tickets_resolved"),
        F.round(
            F.avg("resolution_time_minutes"),
            2
        ).alias("avg_resolution_time")
    )
    .orderBy(
        "day",
        "team_lead_id"
    )
)

display(team_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Team Performance Report

# COMMAND ----------

(
    team_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TEAM_PERFORMANCE)
)

print("Team Performance report saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Team Performance Report

# COMMAND ----------

team = spark.table(GOLD_TEAM_PERFORMANCE)

print("Total Teams:", team.count())

display(team)

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS default.customer_support_pipeline
""")

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.default.customer_support_pipeline
""")

# COMMAND ----------

spark.sql("""
SHOW VOLUMES IN workspace.default
""").display()

# COMMAND ----------

(
    team
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("/Volumes/workspace/default/customer_support_pipeline/outputs/team_performance")
)

print("Team Performance CSV exported successfully.")

# COMMAND ----------

display(
    dbutils.fs.ls("/Volumes/workspace/default/customer_support_pipeline/outputs/team_performance")
)

# COMMAND ----------

team.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/outputs/team_performance.csv",
    index=False
)

print("Team Performance copied to project outputs folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Performance Report

# COMMAND ----------

agent_performance = (
    silver
    .groupBy(
        "day",
        "agent_id",
        "agent_name",
        "team_lead_id"
    )
    .agg(
        F.count("ticket_id").alias("tickets_resolved"),
        F.round(
            F.avg("resolution_time_minutes"),
            2
        ).alias("avg_resolution_time")
    )
    .orderBy(
        "day",
        "team_lead_id",
        "agent_id"
    )
)

display(agent_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Agent Performance Report

# COMMAND ----------

(
    agent_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_AGENT_PERFORMANCE)
)

print("Agent Performance report saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Agent Performance Report

# COMMAND ----------

agent = spark.table(GOLD_AGENT_PERFORMANCE)

print("Total Records:", agent.count())

display(agent)

# COMMAND ----------

(
    agent
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("/Volumes/workspace/default/customer_support_pipeline/outputs/agent_performance")
)

print("Agent Performance CSV exported successfully.")

# COMMAND ----------

agent.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/outputs/agent_performance.csv",
    index=False
)

print("Agent Performance copied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Compliance Report

# COMMAND ----------

quality_compliance = (
    silver
    .groupBy(
        "team_lead_id",
        "agent_id",
        "agent_name"
    )
    .agg(
        F.count("ticket_id").alias("quality_compliant_tickets")
    )
    .orderBy(
        "team_lead_id",
        F.desc("quality_compliant_tickets")
    )
)

display(quality_compliance)

# COMMAND ----------

(
    quality_compliance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_QUALITY_COMPLIANCE)
)

print("Quality Compliance report saved successfully.")

# COMMAND ----------

quality = spark.table(GOLD_QUALITY_COMPLIANCE)

print("Total Records:", quality.count())

display(quality)

# COMMAND ----------

(
    quality
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("/Volumes/workspace/default/customer_support_pipeline/outputs/quality_compliance")
)

print("Quality Compliance CSV exported successfully.")

# COMMAND ----------

quality.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/outputs/quality_compliance.csv",
    index=False
)

print("Quality Compliance copied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2 Carry-over Report

# COMMAND ----------

day1_success_agents = (
    silver
    .filter(F.col("day") == 1)
    .select("agent_id")
    .distinct()
)

print("Successful Day 1 Agents:", day1_success_agents.count())

display(day1_success_agents)

# COMMAND ----------

(
    day2_carryover.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DAY2_CARRYOVER)
)

print("Day 2 Carry-over report saved successfully.")

# COMMAND ----------

carryover = spark.table(GOLD_DAY2_CARRYOVER)

print("Total Carry-over Records:", carryover.count())

display(carryover)

# COMMAND ----------

print("Unique Day 1 Agents:")
display(
    silver.filter(F.col("day") == 1)
          .select("agent_id")
          .distinct()
)

# COMMAND ----------

print("Unique Day 2 Agents:")
display(
    silver.filter(F.col("day") == 2)
          .select("agent_id")
          .distinct()
)

# COMMAND ----------

print("Agents in Day 1:", silver.filter(F.col("day") == 1).select("agent_id").distinct().count())
print("Agents in Day 2:", silver.filter(F.col("day") == 2).select("agent_id").distinct().count())

# COMMAND ----------

print("Team Report:", team.count())
print("Agent Report:", agent.count())
print("Quality Report:", quality.count())
print("Carry-over:", carryover.count())

# COMMAND ----------

(
    carryover
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("/Volumes/workspace/default/customer_support_pipeline/outputs/day2_carryover")
)

print("Day2 Carry-over CSV exported successfully.")

# COMMAND ----------

carryover.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/outputs/day2_carryover.csv",
    index=False
)

print("Carry-over copied")

# COMMAND ----------

display(
dbutils.fs.ls(
"/Volumes/workspace/default/customer_support_pipeline/outputs"
))