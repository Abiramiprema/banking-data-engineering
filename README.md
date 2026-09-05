# Banking Data Engineering Project

## Overview

This project implements an end-to-end banking data engineering pipeline
using MySQL, PySpark, and Databricks.

The pipeline transforms normalized banking OLTP data into a
business-ready Gold layer and analytical dashboard.

## Architecture

MySQL OLTP
→ PySpark / JDBC
→ Bronze Layer
→ Silver Layer
→ Data Quality
→ Gold Layer
→ Star Schema
→ Analytics
→ Databricks Dashboard

## Technologies

- MySQL
- PySpark
- Databricks
- Delta Lake
- SQL
- GitHub

## Data Sources

The project contains 10 banking tables:

- Customers
- Accounts
- Branches
- Transactions
- Loans
- Loan Payments
- Cards
- Card Transactions
- Beneficiaries
- Account Payments

## Medallion Architecture

### Bronze Layer
Raw banking data stored as Delta tables.

### Silver Layer
Data cleaning, duplicate removal, null validation,
and data quality checks.

### Gold Layer
Business-ready star schema containing dimensions and facts.

## Star Schema

### Dimensions

- dim_customer
- dim_account
- dim_branch
- dim_date
- dim_loan
- dim_card

### Facts

- fact_transactions
- fact_loan_payments
- fact_card_transactions
- fact_account_balance

## Data Engineering Features

- Data quality validation
- Duplicate detection
- Null validation
- Incremental processing
- Watermark/control table
- SCD Type 2
- Source-to-target reconciliation
- Business transformations
- Analytical SQL queries

## Analytics

The dashboard provides:

- Active customers
- Total transaction amount
- Credit vs debit
- Daily transaction trends
- Branch transaction volume
- Loan outstanding amounts

## Project Structure

```text
banking-data-engineering/
│
├── data/
├── notebooks/
├── dashboard/
├── screenshots/
└── README.md
