# Project Summary

This project implements an Azure Data Engineering pipeline for customer support ticket data using the Medallion Architecture.

The pipeline processes raw ticket datasets through Bronze, Silver, and Gold layers.

The Silver layer performs data cleaning, validation, enrichment, and business rule implementation.

The Gold layer generates analytics-ready datasets that answer key business questions related to:

- Team Lead performance
- Agent performance
- Resolution quality compliance
- Day-2 carry-over analysis

The project demonstrates data ingestion, transformation, validation, and KPI generation using Azure Databricks, PySpark, and Delta Lake.