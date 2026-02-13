Sales Data Warehouse (Medallion Architecture)

🚀 Project Overview

This project implements a modern data warehouse architecture using a Medallion (Bronze–Silver–Gold) approach to transform raw transactional sales data into business-ready analytical datasets.

The goal of this project is to simulate a real-world data engineering pipeline that:

> Ingests raw operational data

> Cleans and standardizes it

> Transforms it into structured analytics tables

> Enables KPI reporting and business insights

This repository demonstrates data modeling, ETL structuring, and warehouse layer separation aligned with industry practices.

🏗️ Architecture Overview

The warehouse follows a 3-layer architecture:

🥉 Bronze Layer – Raw Data

> Stores data exactly as received from source systems

> No transformations applied

> All fields stored as VARCHAR to prevent load failures

> Acts as immutable historical storage

Tables:

- bronze.orders_raw

- bronze.customers_raw

- bronze.order_items

- bronze.payments_raw

- bronze.shipments_raw

Purpose:

Ensure reliable ingestion and full traceability of source data.

🥈 Silver Layer – Cleaned & Standardized Data

> Data type conversions (VARCHAR → DATE, NUMERIC, etc.)

> Null handling & data validation

> Deduplication

> Business rule standardization

> Relationship alignment

Purpose:

Create structured, reliable datasets ready for analytics modeling.

🥇 Gold Layer – Business Analytics Layer

> Star schema modeling

> Fact & Dimension tables

> KPI-ready datasets

> Aggregated business views

Example Tables:

- gold.fact_orders

- gold.dim_customers

- gold.dim_products

- gold.fact_payments

Purpose:

Enable fast reporting, dashboarding, and executive-level insights.
