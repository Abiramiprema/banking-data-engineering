# Banking Data Engineering Project

## Overview

This project implements a banking data engineering pipeline that transforms data from a normalized MySQL OLTP database into a Databricks OLAP data warehouse.

The pipeline uses PySpark and Databricks to perform data ingestion, data quality checks, cleansing, transformations, dimensional modeling, incremental processing, SCD Type 2, reconciliation, and analytics.

## Architecture

MySQL OLTP → PySpark → Bronze → Silver → Gold → Analytics → Databricks Dashboard

## Technologies Used

- MySQL
- PySpark
- Databricks
- Delta Lake
- SQL
- GitHub

## Data Pipeline

### Bronze Layer
Stores raw data extracted from the source tables with minimal transformation.

### Silver Layer
Performs:
- Duplicate removal
- Null validation
- Invalid record filtering
- Data cleansing
- Standardization

### Gold Layer

The Gold layer follows a Star Schema consisting of:

#### Dimensions
- dim_customer
- dim_account
- dim_branch
- dim_date
- dim_loan
- dim_card

#### Fact Tables
- fact_transactions
- fact_loan_payments
- fact_card_transactions
- fact_account_balance

## Data Quality

A data quality report is generated to identify:

- Total records
- Valid records
- Invalid records
- Duplicate records
- Null ID records

## Incremental Processing

Incremental processing is implemented using a watermark/control table and timestamp-based filtering.

## SCD Type 2

SCD Type 2 is implemented for the customer dimension to maintain historical versions of customer records.

## Reconciliation

Source and target data are reconciled using:

- Record counts
- Transaction amounts
- Count differences
- Amount differences

## Analytics

The project provides analytics for:

- Active customers
- Transaction volume
- Credit and debit totals
- Daily transaction trends
- Branch transaction volume
- Loan outstanding amounts
- Card transactions
- Monthly transaction trends

## Dashboard

A Databricks dashboard is created to visualize key banking KPIs.

## Project Structure

```text
banking-data-engineering/
│
├── notebooks/
├── dashboard/
├── mysql/
├── screenshots/
├── architecture/
└── README.md


Author

Computer Science & Engineering Student
