## Project Structure
Project Details:
This project demonstrates an end-to-end analytics workflow: ingesting daily source files, transforming and modeling data in PostgreSQL (staging → warehouse), and delivering KPI-driven Power BI dashboards. It showcases Python ETL development, SQL/data modeling, and operational reporting design.

## Key KPIs Delivered
- Total Revenue
- Total Session
- Total Units
- Conversion Rate
- revenue per session
- Daily trend monitoring (sales/traffic movement)

## data/raw/input/amazon  
Contains raw Amazon Seller Central reports as received (no transformations).

scripts/raw_loaders  
Python scripts to ingest raw Seller Central reports into the database.

scripts/staging_transform  
Data cleaning, standardization, and business-rule transformations.

warehouse/amazon/dimensions  
Dimension tables (ASIN, SKU, date).

warehouse/amazon/facts  
Fact tables for sales, traffic, and advertising metrics.

## Execution Overview (Conceptual)

This repository demonstrates the structure and logic of an end-to-end
analytics pipeline. Due to data confidentiality, raw source files and
credentials are not included.

The ETL process follows these steps:
1. Ingest structured daily source reports into staging tables
2. Apply data standardization and business-rule transformations
3. Populate fact and dimension tables in PostgreSQL
4. Consume curated tables in Power BI for KPI reporting

## Architecture Overview
Structured Source Reports  
→ Python ETL (ingestion & transformation)  
→ PostgreSQL (staging → curated tables)  
→ Power BI dashboards
