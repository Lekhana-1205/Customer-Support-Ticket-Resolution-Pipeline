# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation
# MAGIC
# MAGIC ### Objective
# MAGIC Create the raw datasets required for the pipeline and store them as Delta tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

CATALOG = "workspace"
SCHEMA = "hrm6131_landing"

TBL_PROFILES = f"{CATALOG}.{SCHEMA}.agent_profiles"
TBL_DAY1 = f"{CATALOG}.{SCHEMA}.day1_tickets"
TBL_DAY2 = f"{CATALOG}.{SCHEMA}.day2_tickets"

print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Profiles

# COMMAND ----------

# ── agent_profiles data ───────────────────────────────────────────────────────
profiles_data = [
    # TL01
    ("A001","Agent_A001","Junior Support Agent","TL01"),
    ("A002","Agent_A002","Senior Support Agent","TL01"),
    ("A003","Agent_A003","Senior Support Agent","TL01"),
    ("A004","Agent_A004","Junior Support Agent","TL01"),
    ("A005","Agent_A005","Support Agent",       "TL01"),
    # TL02
    ("A006","Agent_A006","Support Agent",       "TL02"),
    ("A007","Agent_A007","Junior Support Agent","TL02"),
    ("A008","Agent_A008","Senior Support Agent","TL02"),
    ("A009","Agent_A009","Support Agent",       "TL02"),
    ("A010","Agent_A010","Junior Support Agent","TL02"),
    # TL03
    ("A011","Agent_A011","Support Agent",       "TL03"),
    ("A012","Agent_A012","Senior Support Agent","TL03"),
    ("A013","Agent_A013","Junior Support Agent","TL03"),
    ("A014","Agent_A014","Support Agent",       "TL03"),
    ("A015","Agent_A015","Senior Support Agent","TL03"),
    # TL04
    ("A016","Agent_A016","Junior Support Agent","TL04"),
    ("A017","Agent_A017","Support Agent",       "TL04"),
    ("A018","Agent_A018","Senior Support Agent","TL04"),
    ("A019","Agent_A019","Support Agent",       "TL04"),
    ("A020","Agent_A020","Junior Support Agent","TL04"),
    # TL05
    ("A021","Agent_A021","Senior Support Agent","TL05"),
    ("A022","Agent_A022","Support Agent",       "TL05"),
    ("A023","Agent_A023","Junior Support Agent","TL05"),
    ("A024","Agent_A024","Support Agent",       "TL05"),
    ("A025","Agent_A025","Senior Support Agent","TL05"),
    # TL06
    ("A026","Agent_A026","Support Agent",       "TL06"),
    ("A027","Agent_A027","Junior Support Agent","TL06"),
    ("A028","Agent_A028","Senior Support Agent","TL06"),
    ("A029","Agent_A029","Support Agent",       "TL06"),
    ("A030","Agent_A030","Junior Support Agent","TL06"),
    # TL07
    ("A031","Agent_A031","Senior Support Agent","TL07"),
    ("A032","Agent_A032","Support Agent",       "TL07"),
    ("A033","Agent_A033","Junior Support Agent","TL07"),
    ("A034","Agent_A034","Support Agent",       "TL07"),
    ("A035","Agent_A035","Senior Support Agent","TL07"),
    # TL08
    ("A036","Agent_A036","Junior Support Agent","TL08"),
    ("A037","Agent_A037","Support Agent",       "TL08"),
    ("A038","Agent_A038","Senior Support Agent","TL08"),
    ("A039","Agent_A039","Support Agent",       "TL08"),
    ("A040","Agent_A040","Junior Support Agent","TL08"),
    # Out-of-scope (TL09, TL12) — should be filtered by R-4
    ("A041","Agent_A041","Support Agent",       "TL09"),
    ("A042","Agent_A042","Support Agent",       "TL12"),
]

profiles_schema = StructType([
    StructField("agent_id",     StringType(), True),
    StructField("agent_name",   StringType(), True),
    StructField("role",         StringType(), True),
    StructField("team_lead_id", StringType(), True),
])

df_profiles = spark.createDataFrame(profiles_data, schema=profiles_schema)
(df_profiles.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TBL_PROFILES))

print(f"✅  {TBL_PROFILES} written ({df_profiles.count()} rows)")
display(df_profiles)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1 Tickets
# MAGIC
# MAGIC Create the Day 1 customer support ticket dataset.

# COMMAND ----------

# ── day1_tickets data ─────────────────────────────────────────────────────────
# 249 rows: valid resolved, pending/open, rushed (<= 15 min), nulls, bad time, out-of-scope
day1_data = [
    # --- TL01 agents (A001-A005) ---
    ("TKT00001","A001","Resolved","0h 35m 45s","Technical"),
    ("TKT00002","A001","Resolved","0h 22m 10s","Billing"),
    ("TKT00003","A001","Resolved","0h 12m 5s", "General"),   # fails R-3 (12 min)
    ("TKT00004","A001","Open",    "0h 40m 0s", "Delivery"),
    ("TKT00005","A002","Resolved","1h 10m 30s","Returns"),    # 71 min (30s round up)
    ("TKT00006","A002","Resolved","0h 16m 26s","Billing"),
    ("TKT00007","A002","Pending", "0h 36m 0s", "Returns"),
    ("TKT00008","A003","Resolved","0h 25m 50s","Technical"),
    ("TKT00009","A003","Resolved","0h 15m 0s", "Billing"),   # exactly 15 → fails R-3
    ("TKT00010","A003","Resolved","0h 18m 44s","Account"),
    ("TKT00011","A004","Resolved","0h 33m 15s","Delivery"),
    ("TKT00012","A004","Open",    "0h 10m 0s", "General"),
    ("TKT00013","A004","Resolved","0h 20m 30s","Returns"),
    ("TKT00014","A005","Resolved","0h 45m 0s", "Account"),
    ("TKT00015","A005","Resolved","0h 14m 59s","Technical"),  # 14 min → fails R-3
    ("TKT00016","A005","Pending", "0h 55m 0s", "Billing"),
    # --- TL02 agents (A006-A010) ---
    ("TKT00017","A006","Resolved","0h 28m 20s","Technical"),
    ("TKT00018","A006","Resolved","0h 17m 45s","Delivery"),
    ("TKT00019","A006","Open",    "0h 5m 30s", "General"),
    ("TKT00020","A007","Resolved","1h 5m 0s",  "Billing"),
    ("TKT00021","A007","Resolved","0h 23m 10s","Returns"),
    ("TKT00022","A007","Resolved","0h 9m 0s",  "Account"),   # fails R-3
    ("TKT00023","A008","Resolved","0h 31m 0s", "Technical"),
    ("TKT00024","A008","Pending", "0h 44m 30s","Billing"),
    ("TKT00025","A008","Resolved","0h 19m 15s","Delivery"),
    ("TKT00026","A009","Resolved","0h 38m 50s","Returns"),
    ("TKT00027","A009","Resolved","0h 22m 0s", "General"),
    ("TKT00028","A010","Resolved","0h 27m 30s","Account"),
    ("TKT00029","A010","Open",    "0h 15m 30s","Technical"),  # Open → no resolve
    ("TKT00030","A010","Resolved","0h 41m 20s","Billing"),
    # --- TL03 agents (A011-A015) ---
    ("TKT00031","A011","Resolved","0h 18m 0s", "Returns"),
    ("TKT00032","A011","Resolved","0h 12m 0s", "Delivery"),  # fails R-3
    ("TKT00033","A011","Open",    "0h 30m 0s", "General"),
    ("TKT00034","A012","Resolved","0h 52m 30s","Technical"),
    ("TKT00035","A012","Resolved","0h 25m 0s", "Billing"),
    ("TKT00036","A012","Resolved","0h 16m 45s","Account"),
    ("TKT00037","A013","Resolved","0h 20m 0s", "Delivery"),
    ("TKT00038","A013","Pending", "0h 7m 0s",  "Returns"),
    ("TKT00039","A013","Resolved","0h 34m 15s","General"),
    ("TKT00040","A014","Resolved","0h 39m 51s","Technical"),
    ("TKT00041","A014","Resolved","0h 22m 0s", "Billing"),
    ("TKT00042","A015","Resolved","1h 2m 30s", "Account"),
    ("TKT00043","A015","Open",    "0h 17m 0s", "Delivery"),
    ("TKT00044","A015","Resolved","0h 28m 0s", "Returns"),
    # --- TL04 agents (A016-A020) ---
    ("TKT00045","A016","Resolved","0h 36m 0s", "General"),
    ("TKT00046","A016","Resolved","0h 19m 30s","Technical"),
    ("TKT00047","A016","Open",    "0h 45m 0s", "Billing"),
    ("TKT00048","A017","Resolved","0h 24m 0s", "Account"),
    ("TKT00049","A017","Resolved","0h 15m 30s","Delivery"),  # 15m 30s → 16 min ✅
    ("TKT00050","A017","Pending", "0h 33m 0s", "Returns"),
    ("TKT00051","A018","Resolved","0h 41m 0s", "General"),
    ("TKT00052","A018","Resolved","0h 10m 0s", "Technical"), # fails R-3
    ("TKT00053","A019","Resolved","0h 35m 45s","Technical"),
    ("TKT00054","A019","Resolved","0h 27m 0s", "Billing"),
    ("TKT00055","A020","Resolved","0h 16m 0s", "Account"),
    ("TKT00056","A020","Open",    "0h 50m 0s", "Delivery"),
    ("TKT00057","A020","Resolved","0h 22m 30s","Returns"),
    # --- TL05 agents (A021-A025) ---
    ("TKT00058","A021","Resolved","0h 30m 0s", "General"),
    ("TKT00059","A021","Resolved","0h 18m 45s","Technical"),
    ("TKT00060","A021","Pending", "0h 25m 0s", "Billing"),
    ("TKT00061","A022","Resolved","0h 44m 0s", "Account"),
    ("TKT00062","A022","Resolved","0h 21m 30s","Delivery"),
    ("TKT00063","A022","Open",    "0h 8m 0s",  "Returns"),
    ("TKT00064","A023","Resolved","0h 17m 0s", "General"),
    ("TKT00065","A023","Resolved","0h 32m 15s","Technical"),
    ("TKT00066","A024","Resolved","1h 10m 19s","Returns"),   # 70 min (19s < 30, no round)
    ("TKT00067","A024","Open",    "0h 15m 0s", "Billing"),
    ("TKT00068","A024","Resolved","0h 19m 0s", "Account"),
    ("TKT00069","A025","Resolved","0h 26m 0s", "Delivery"),
    ("TKT00070","A025","Resolved","0h 13m 30s","General"),   # fails R-3
    ("TKT00071","A025","Resolved","0h 37m 0s", "Technical"),
    # --- TL06 agents (A026-A030) ---
    ("TKT00072","A026","Resolved","0h 23m 0s", "Billing"),
    ("TKT00073","A026","Resolved","0h 16m 30s","Account"),
    ("TKT00074","A026","Open",    "0h 28m 0s", "Delivery"),
    ("TKT00075","A027","Resolved","0h 45m 0s", "Returns"),
    ("TKT00076","A027","Resolved","0h 20m 0s", "General"),
    ("TKT00077","A027","Resolved","0h 11m 0s", "Technical"), # fails R-3
    ("TKT00078","A028","Resolved","1h 10m 19s","Returns"),
    ("TKT00079","A028","Resolved","0h 16m 26s","Delivery"),
    ("TKT00080","A028","Pending", "0h 36m 0s", "Billing"),
    ("TKT00081","A029","Resolved","0h 33m 0s", "Account"),
    ("TKT00082","A029","Resolved","0h 22m 45s","Technical"),
    ("TKT00083","A030","Resolved","0h 18m 0s", "General"),
    ("TKT00084","A030","Open",    "0h 40m 0s", "Delivery"),
    ("TKT00085","A030","Resolved","0h 28m 30s","Returns"),
    # --- TL07 agents (A031-A035) ---
    ("TKT00086","A031","Resolved","0h 29m 0s", "Billing"),
    ("TKT00087","A031","Resolved","0h 17m 30s","Account"),
    ("TKT00088","A031","Open",    "0h 35m 0s", "Technical"),
    ("TKT00089","A032","Resolved","0h 40m 0s", "Delivery"),
    ("TKT00090","A032","Resolved","0h 14m 0s", "Returns"),   # fails R-3
    ("TKT00091","A032","Resolved","0h 24m 30s","General"),
    ("TKT00092","A033","Resolved","0h 31m 0s", "Technical"),
    ("TKT00093","A033","Pending", "0h 22m 0s", "Billing"),
    ("TKT00094","A033","Resolved","0h 19m 45s","Account"),
    ("TKT00095","A034","Resolved","0h 38m 0s", "Delivery"),
    ("TKT00096","A034","Resolved","0h 25m 15s","Returns"),
    ("TKT00097","A035","Resolved","0h 20m 0s", "General"),
    ("TKT00098","A035","Open",    "0h 15m 30s","Technical"),
    ("TKT00099","A035","Resolved","0h 32m 0s", "Billing"),
    # --- TL08 agents (A036-A040) ---
    ("TKT00100","A036","Resolved","0h 27m 0s", "Account"),
    ("TKT00101","A036","Resolved","0h 18m 30s","Delivery"),
    ("TKT00102","A036","Open",    "0h 42m 0s", "Returns"),
    ("TKT00103","A037","Resolved","0h 21m 0s", "General"),
    ("TKT00104","A037","Resolved","0h 9m 52s", "Delivery"),  # fails R-3
    ("TKT00105","A037","Resolved","0h 34m 0s", "Technical"),
    ("TKT00106","A038","Open","0h 16m 0s","Billing"),
    ("TKT00107","A038","Pending", "0h 30m 0s", "Account"),
    ("TKT00108","A038","Pending","0h 43m 30s","Delivery"),
    ("TKT00109","A039","Open","0h 23m 0s", "Returns"),
    ("TKT00110","A039","Resolved","0h 15m 29s","General"),   # 15m 29s → 15 min (29<30) → fails R-3
    ("TKT00111","A040","Pending","0h 26m 0s", "Technical"),
    ("TKT00112","A040","Open",    "0h 37m 0s", "Billing"),
    ("TKT00113","A040","Open","0h 19m 45s","Account"),
    # --- Out-of-scope (A041=TL09, A042=TL12) — must be filtered by R-4 ---
    ("TKT00114","A041","Resolved","0h 30m 0s", "General"),
    ("TKT00115","A041","Resolved","0h 25m 0s", "Technical"),
    ("TKT00116","A041","Resolved","0h 20m 0s", "Billing"),
    ("TKT00117","A042","Resolved","0h 35m 0s", "Account"),
    ("TKT00118","A042","Resolved","0h 22m 0s", "Delivery"),
    ("TKT00119","A042","Resolved","0h 28m 0s", "Returns"),
    # --- Null / bad rows — must be caught by R-5 / UDF ----------------------
    ("",         "A001","Resolved","0h 20m 0s", "Billing"),   # blank ticket_id
    ("TKT99990", "",    "Resolved","0h 25m 0s", "Technical"), # blank agent_id
    ("TKT99991", "A002","Resolved","",           "General"),   # blank resolution_time
    ("TKT99992", "A003","Resolved","BADTIME",    "General"),   # unparseable time
]

ticket_schema = StructType([
    StructField("ticket_id",       StringType(), True),
    StructField("agent_id",        StringType(), True),
    StructField("status",          StringType(), True),
    StructField("resolution_time", StringType(), True),
    StructField("category",        StringType(), True),
])

df_day1 = spark.createDataFrame(day1_data, schema=ticket_schema)
(df_day1.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TBL_DAY1))

print(f"✅  {TBL_DAY1} written ({df_day1.count()} rows)")
display(df_day1.limit(10))


# COMMAND ----------

day1_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("agent_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("resolution_time", StringType(), True),
    StructField("ticket_category", StringType(), True)
])

df_day1 = spark.createDataFrame(
    day1_data,
    schema=day1_schema
)

# COMMAND ----------

display(df_day1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2 Tickets

# COMMAND ----------

# ── day2_tickets data ─────────────────────────────────────────────────────────
# All 40 in-scope agents appear here; pipeline's carry-over rule (R-6) will
# remove agents who already succeeded on Day 1.
day2_data = [
    # TL01
    ("TKT00200","A001","Resolved","0h 28m 0s", "Billing"),
    ("TKT00201","A001","Resolved","0h 16m 30s","Technical"),
    ("TKT00202","A002","Resolved","0h 35m 0s", "Account"),
    ("TKT00203","A002","Open",    "0h 22m 0s", "Delivery"),
    ("TKT00204","A003","Resolved","0h 20m 45s","Returns"),
    ("TKT00205","A003","Resolved","0h 9m 0s",  "General"),   # fails R-3
    ("TKT00206","A004","Resolved","0h 31m 0s", "Technical"),
    ("TKT00207","A004","Resolved","0h 18m 15s","Billing"),
    ("TKT00208","A005","Resolved","0h 26m 0s", "Account"),
    ("TKT00209","A005","Pending", "0h 40m 0s", "Delivery"),
    # TL02
    ("TKT00210","A006","Resolved","0h 22m 30s","Returns"),
    ("TKT00211","A006","Resolved","0h 14m 0s", "General"),   # fails R-3
    ("TKT00212","A007","Resolved","0h 37m 0s", "Technical"),
    ("TKT00213","A007","Resolved","0h 19m 45s","Billing"),
    ("TKT00214","A008","Resolved","0h 25m 0s", "Account"),
    ("TKT00215","A008","Open",    "0h 33m 0s", "Delivery"),
    ("TKT00216","A009","Resolved","0h 36m 1s", "Technical"),
    ("TKT00217","A009","Resolved","0h 21m 30s","Returns"),
    ("TKT00218","A010","Resolved","0h 17m 0s", "General"),
    ("TKT00219","A010","Resolved","0h 30m 0s", "Billing"),
    # TL03
    ("TKT00220","A011","Resolved","0h 24m 0s", "Account"),
    ("TKT00221","A011","Resolved","0h 15m 30s","Delivery"),  # 16 min ✅
    ("TKT00222","A012","Resolved","0h 42m 0s", "Returns"),
    ("TKT00223","A012","Open",    "0h 18m 0s", "General"),
    ("TKT00224","A013","Resolved","0h 27m 30s","Technical"),
    ("TKT00225","A013","Resolved","0h 12m 0s", "Billing"),   # fails R-3
    ("TKT00226","A014","Resolved","0h 33m 0s", "Account"),
    ("TKT00227","A014","Resolved","0h 20m 15s","Delivery"),
    ("TKT00228","A015","Resolved","0h 29m 0s", "Returns"),
    ("TKT00229","A015","Pending", "0h 38m 0s", "General"),
    # TL04
    ("TKT00230","A016","Resolved","0h 23m 0s", "Technical"),
    ("TKT00231","A016","Resolved","0h 17m 45s","Billing"),
    ("TKT00232","A017","Resolved","0h 36m 0s", "Account"),
    ("TKT00233","A017","Open",    "0h 25m 0s", "Delivery"),
    ("TKT00234","A018","Resolved","0h 19m 30s","Returns"),
    ("TKT00235","A018","Resolved","0h 44m 0s", "General"),
    ("TKT00236","A019","Resolved","0h 28m 0s", "Technical"),
    ("TKT00237","A019","Resolved","0h 15m 0s", "Billing"),   # exactly 15 → fails
    ("TKT00238","A020","Resolved","0h 32m 30s","Account"),
    ("TKT00239","A020","Resolved","0h 21m 0s", "Delivery"),
    # TL05
    ("TKT00240","A021","Resolved","0h 26m 0s", "Returns"),
    ("TKT00241","A021","Open",    "0h 39m 0s", "General"),
    ("TKT00242","A022","Resolved","0h 18m 30s","Technical"),
    ("TKT00243","A022","Resolved","0h 31m 0s", "Billing"),
    ("TKT00244","A023","Resolved","0h 24m 45s","Account"),
    ("TKT00245","A023","Resolved","0h 11m 0s", "Delivery"),  # fails R-3
    ("TKT00246","A024","Resolved","0h 37m 0s", "Returns"),
    ("TKT00247","A024","Resolved","0h 20m 30s","General"),
    ("TKT00248","A025","Resolved","0h 16m 0s", "Technical"),
    ("TKT00249","A025","Pending", "0h 28m 0s", "Billing"),
    # TL06
    ("TKT00250","A026","Resolved","0h 33m 0s", "Account"),
    ("TKT00251","A026","Resolved","0h 19m 15s","Delivery"),
    ("TKT00252","A027","Resolved","0h 25m 0s", "Returns"),
    ("TKT00253","A027","Open",    "0h 42m 0s", "General"),
    ("TKT00254","A028","Resolved","0h 17m 30s","Technical"),
    ("TKT00255","A028","Resolved","0h 30m 0s", "Billing"),
    ("TKT00256","A029","Resolved","0h 22m 0s", "Account"),
    ("TKT00257","A029","Resolved","0h 38m 45s","Delivery"),
    ("TKT00258","A030","Resolved","0h 16m 30s","Returns"),
    ("TKT00259","A030","Open",    "0h 27m 0s", "General"),
    # TL07
    ("TKT00260","A031","Resolved","0h 34m 0s", "Technical"),
    ("TKT00261","A031","Resolved","0h 21m 45s","Billing"),
    ("TKT00262","A032","Resolved","0h 28m 0s", "Account"),
    ("TKT00263","A032","Resolved","0h 13m 30s","Delivery"),  # fails R-3
    ("TKT00264","A033","Resolved","0h 36m 0s", "Returns"),
    ("TKT00265","A033","Pending", "0h 19m 0s", "General"),
    ("TKT00266","A034","Resolved","0h 24m 15s","Technical"),
    ("TKT00267","A034","Resolved","0h 31m 0s", "Billing"),
    ("TKT00268","A035","Resolved","0h 18m 30s","Account"),
    ("TKT00269","A035","Resolved","0h 44m 0s", "Delivery"),
    # TL08
    ("TKT00270","A036","Resolved","0h 23m 0s", "Returns"),
    ("TKT00271","A036","Resolved","0h 17m 15s","General"),
    ("TKT00272","A037","Resolved","0h 39m 0s", "Technical"),
    ("TKT00273","A037","Resolved","0h 20m 30s","Billing"),
    ("TKT00274","A038","Resolved","0h 26m 0s", "Account"),
    ("TKT00275","A038","Open",    "0h 32m 0s", "Delivery"),
    ("TKT00276","A039","Resolved","0h 35m 45s","Returns"),
    ("TKT00277","A039","Resolved","0h 15m 29s","General"),   # 15m 29s → 15 min → fails
    ("TKT00278","A040","Resolved","0h 22m 0s", "Technical"),
    ("TKT00279","A040","Resolved","0h 29m 30s","Billing"),
    # Out-of-scope
    ("TKT00280","A041","Resolved","0h 28m 0s", "General"),
    ("TKT00281","A041","Resolved","0h 34m 0s", "Technical"),
    ("TKT00282","A042","Resolved","0h 19m 0s", "Account"),
    ("TKT00283","A042","Resolved","0h 27m 0s", "Delivery"),
    # Null rows
    ("",         "A010","Resolved","0h 30m 0s","Billing"),   # blank ticket_id
    ("TKT99993", "A011","Resolved","",          "Returns"),   # blank resolution_time
]

df_day2 = spark.createDataFrame(day2_data, schema=ticket_schema)
(df_day2.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TBL_DAY2))

print(f"✅  {TBL_DAY2} written ({df_day2.count()} rows)")
display(df_day2.limit(10))


# COMMAND ----------

day2_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("agent_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("resolution_time", StringType(), True),
    StructField("ticket_category", StringType(), True)
])

df_day2 = spark.createDataFrame(
    day2_data,
    schema=day2_schema
)

# COMMAND ----------

(
    df_day2.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TBL_DAY2)
)

print(f"Rows: {df_day2.count()}")
display(df_day2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Raw Tables

# COMMAND ----------

print(f"Agent Profiles : {spark.table(TBL_PROFILES).count()} rows")
print(f"Day 1 Tickets  : {spark.table(TBL_DAY1).count()} rows")
print(f"Day 2 Tickets  : {spark.table(TBL_DAY2).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Raw Data

# COMMAND ----------

df_profiles.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/data/raw/agent_profiles.csv",
    index=False
)

df_day1.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/data/raw/day1_tickets.csv",
    index=False
)

df_day2.toPandas().to_csv(
    "/Workspace/Customer_Support_Ticket_Resolution_Pipeline/data/raw/day2_tickets.csv",
    index=False
)

print("Raw CSV files exported successfully.")

# COMMAND ----------

