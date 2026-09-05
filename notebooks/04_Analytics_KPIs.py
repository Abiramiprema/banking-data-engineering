# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT customer_id) AS active_customers
# MAGIC FROM Workspace.default.fact_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     SUM(amount) AS total_transaction_amount
# MAGIC FROM Workspace.default.fact_transactions
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY total_transaction_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     YEAR(date) AS year,
# MAGIC     MONTH(date) AS month,
# MAGIC     SUM(amount) AS monthly_transaction_amount
# MAGIC FROM Workspace.default.fact_transactions
# MAGIC GROUP BY YEAR(date), MONTH(date)
# MAGIC ORDER BY year, month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_type,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     SUM(amount) AS total_amount
# MAGIC FROM Workspace.default.fact_transactions
# MAGIC GROUP BY transaction_type
# MAGIC ORDER BY transaction_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(credit_amount) AS total_credit,
# MAGIC     SUM(debit_amount) AS total_debit,
# MAGIC     SUM(net_transaction_amount) AS net_transaction_amount
# MAGIC FROM Workspace.default.fact_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     branch_id,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     SUM(amount) AS transaction_amount
# MAGIC FROM Workspace.default.fact_transactions
# MAGIC GROUP BY branch_id
# MAGIC ORDER BY transaction_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     AVG(balance) AS average_account_balance
# MAGIC FROM Workspace.default.fact_account_balance;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     l.loan_id,
# MAGIC     l.customer_id,
# MAGIC     l.loan_amount,
# MAGIC     COALESCE(SUM(p.payment_amount), 0) AS total_repaid,
# MAGIC     l.loan_amount - COALESCE(SUM(p.payment_amount), 0)
# MAGIC         AS outstanding_amount
# MAGIC FROM Workspace.default.dim_loan l
# MAGIC LEFT JOIN Workspace.default.fact_loan_payments p
# MAGIC     ON l.loan_id = p.loan_id
# MAGIC GROUP BY
# MAGIC     l.loan_id,
# MAGIC     l.customer_id,
# MAGIC     l.loan_amount
# MAGIC ORDER BY outstanding_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_type,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     SUM(amount) AS total_amount
# MAGIC FROM Workspace.default.fact_card_transactions
# MAGIC GROUP BY transaction_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     YEAR(date) AS year,
# MAGIC     MONTH(date) AS month,
# MAGIC     SUM(amount) AS total_card_amount
# MAGIC FROM Workspace.default.fact_card_transactions
# MAGIC GROUP BY YEAR(date), MONTH(date)
# MAGIC ORDER BY year, month;