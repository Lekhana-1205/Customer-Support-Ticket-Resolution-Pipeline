# 🚀 Customer Support Ticket Resolution Pipeline

An Azure Data Engineering project implementing the **Medallion Architecture (Bronze, Silver, and Gold)** using **Azure Databricks** and **PySpark**. The pipeline processes customer support ticket data, performs validation and transformation, applies business rules, and generates business-ready datasets for performance analysis.

---

## 📌 Project Overview

Customer support ticket data collected over two days is processed through an end-to-end data engineering pipeline. The project demonstrates how raw operational data can be transformed into clean, validated, and analytics-ready datasets by implementing the Medallion Architecture.

The pipeline focuses on data quality, business rule implementation, and KPI generation for customer support operations.

---

## 🛠️ Technology Stack

- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Data Factory
- Azure Databricks
- PySpark
- Delta Lake
- Git & GitHub

---

## 📂 Repository Structure

```
CEI_Project/
│
├── data/
│   └── raw/
│       ├── agent_profiles.csv
│       ├── day1_tickets.csv
│       └── day2_tickets.csv
│
├── notebooks/
│   ├── 00_Config.py
│   ├── 01_Data_Generation.py
│   ├── 02_Bronze_Layer.py
│   ├── 03_Silver_Layer.py
│   ├── 04_Gold_Layer.py
│   └── 05_Validation.py
│
├── outputs/
│   ├── team_performance.csv
│   ├── agent_performance.csv
│   ├── quality_compliance.csv
│   └── day2_carryover.csv
│
├── docs/
│   ├── architecture.jpeg
│   ├── business_rules.md
│   ├── pipeline_flow.md
│   └── project_summary.md
│
├── Screenshots/
│
└── README.md
```

---

## 🏗️ Architecture

```
CSV Files
    │
    ▼
Azure Data Lake Storage Gen2
    │
    ▼
Azure Data Factory
    │
    ▼
Azure Databricks
    │
    ▼
Bronze Layer
(Raw Data + Metadata)
    │
    ▼
Silver Layer
(Data Cleaning, Validation &
Business Rule Processing)
    │
    ▼
Gold Layer
(Business KPIs)
```

---

## 📋 Business Rules Implemented

- Only **Resolved** tickets are considered.
- Resolution time must be greater than **15 minutes**.
- Resolution time stored in **Xh Xm Xs** format is converted into total minutes.
- Seconds greater than or equal to **30** are rounded up.
- Only Team Leads **TL01–TL08** are included.
- Agents who successfully qualified on **Day 1** are excluded from **Day 2** analysis (Carry-over Rule).

---

## 📊 Gold Layer Outputs

The pipeline generates four business-ready datasets:

- **Team Lead Performance**
- **Agent Day-wise Performance**
- **Resolution Quality Compliance**
- **Day-2 Carry-over Analysis**

---

## ✅ Data Validation

The validation notebook verifies:

- Row counts
- Duplicate records
- Null values
- Resolution quality rule
- Team Lead scope
- Day-2 carry-over rule

---

## ▶️ Execution Order

1. 00_Config.py
2. 01_Data_Generation.py
3. 02_Bronze_Layer.py
4. 03_Silver_Layer.py
5. 04_Gold_Layer.py
6. 05_Validation.py

---

## 📁 Project Outputs

The pipeline produces analytics-ready datasets:

- `team_performance.csv`
- `agent_performance.csv`
- `quality_compliance.csv`
- `day2_carryover.csv`

---

## 📸 Project Screenshots

The repository includes screenshots demonstrating:

- Azure Storage configuration
- Azure Data Factory pipeline
- Bronze layer processing
- Silver layer transformations
- Gold layer outputs
- Validation results

---

## ⚠️ Implementation Note

The solution was designed following an Azure-based architecture using **Azure Data Lake Storage Gen2**, **Azure Data Factory**, and **Azure Databricks**.

During implementation, storage authentication policies in the learning environment limited direct runtime access from Databricks to Azure Data Lake Storage. To ensure the complete Medallion Architecture and business logic could be implemented and validated successfully, the downstream Bronze, Silver, and Gold transformations were executed using Delta tables within Azure Databricks while preserving the intended pipeline design.

---

## 🎯 Project Outcome

The project demonstrates an end-to-end Azure Data Engineering workflow that transforms raw customer support ticket data into validated, business-ready datasets for analyzing:

- Team Lead performance
- Agent performance
- Resolution quality compliance
- Day-2 carry-over analysis

---

## 👩‍💻 Author

**Lekhana Donthula**

B.Tech – Computer Science and Engineering (Data Science)

CVR College of Engineering
