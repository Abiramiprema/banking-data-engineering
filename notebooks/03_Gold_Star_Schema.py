# Databricks notebook source
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, dayofweek

customers = spark.table("Workspace.default.silver_customers")
accounts = spark.table("Workspace.default.silver_accounts")
branches = spark.table("Workspace.default.silver_branches")
transactions = spark.table("Workspace.default.silver_transactions")
loans = spark.table("Workspace.default.silver_loans")
loan_payments = spark.table("Workspace.default.silver_loan_payments")
cards = spark.table("Workspace.default.silver_cards")
card_transactions = spark.table("Workspace.default.silver_card_transactions")


# COMMAND ----------

dim_customer = customers.select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("phone"),
    col("date_of_birth"),
    col("address"),
    col("city"),
    col("state")
)

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.dim_customer")

# COMMAND ----------

dim_branch = branches.select(
    col("branch_id"),
    col("branch_name"),
    col("city"),
    col("state")
)

dim_branch.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.dim_branch")

# COMMAND ----------

dim_account = accounts.select(
    col("account_id"),
    col("customer_id"),
    col("branch_id"),
    col("account_number"),
    col("account_type"),
    col("account_status")
)

dim_account.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.dim_account")

# COMMAND ----------

dim_loan = loans.select(
    col("loan_id"),
    col("customer_id"),
    col("branch_id"),
    col("loan_type"),
    col("loan_amount"),
    col("interest_rate"),
    col("loan_status"),
    col("start_date"),
    col("end_date")
)

dim_loan.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.dim_loan")

# COMMAND ----------

dim_card = cards.select(
    col("card_id"),
    col("customer_id"),
    col("account_id"),
    col("card_number"),
    col("card_type"),
    col("card_status"),
    col("issue_date"),
    col("expiry_date")
)

dim_card.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.dim_card")

# COMMAND ----------

from pyspark.sql.functions import sequence, explode, lit

date_df = transactions.select(
    to_date("transaction_date").alias("date")
).distinct()

dim_date = date_df.select(
    col("date"),
    year("date").alias("year"),
    month("date").alias("month"),
    dayofmonth("date").alias("day"),
    dayofweek("date").alias("day_of_week")
)

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.dim_date")

# COMMAND ----------

display(spark.table("Workspace.default.dim_customer"))

# COMMAND ----------

display(spark.table("Workspace.default.dim_account"))

# COMMAND ----------

display(spark.table("Workspace.default.dim_date"))

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, date_format, when, lit, current_date
)

accounts = spark.table("Workspace.default.silver_accounts")
transactions = spark.table("Workspace.default.silver_transactions")
loans = spark.table("Workspace.default.silver_loans")
loan_payments = spark.table("Workspace.default.silver_loan_payments")
cards = spark.table("Workspace.default.silver_cards")
card_transactions = spark.table("Workspace.default.silver_card_transactions")

# COMMAND ----------

fact_transactions = transactions.join(
    accounts.select("account_id", "customer_id", "branch_id"),
    on="account_id",
    how="left"
).select(
    col("transaction_id"),
    col("account_id"),
    col("customer_id"),
    col("branch_id"),
    to_date("transaction_date").alias("date"),
    date_format(
        to_date("transaction_date"), "yyyyMMdd"
    ).cast("int").alias("date_key"),
    col("transaction_type"),
    col("amount"),
    when(
        col("transaction_type").isin("DEPOSIT", "CREDIT"),
        col("amount")
    ).otherwise(0).alias("credit_amount"),
    when(
        col("transaction_type").isin("WITHDRAWAL", "DEBIT"),
        col("amount")
    ).otherwise(0).alias("debit_amount")
)

fact_transactions = fact_transactions.withColumn(
    "net_transaction_amount",
    col("credit_amount") - col("debit_amount")
)

display(fact_transactions)

# COMMAND ----------

fact_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.fact_transactions")

# COMMAND ----------

fact_loan_payments = loan_payments.join(
    loans.select(
        "loan_id",
        "customer_id",
        "branch_id"
    ),
    on="loan_id",
    how="left"
).select(
    col("payment_id"),
    col("loan_id"),
    col("customer_id"),
    col("branch_id"),
    col("payment_date").alias("date"),
    date_format(
        col("payment_date"), "yyyyMMdd"
    ).cast("int").alias("date_key"),
    col("payment_amount"),
    col("payment_status")
)

display(fact_loan_payments)

# COMMAND ----------

fact_loan_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.fact_loan_payments")

# COMMAND ----------

fact_card_transactions = card_transactions.join(
    cards.select(
        "card_id",
        "customer_id",
        "account_id"
    ),
    on="card_id",
    how="left"
).select(
    col("card_transaction_id"),
    col("card_id"),
    col("customer_id"),
    col("account_id"),
    to_date("transaction_date").alias("date"),
    date_format(
        to_date("transaction_date"), "yyyyMMdd"
    ).cast("int").alias("date_key"),
    col("transaction_type"),
    col("amount"),
    col("merchant_name")
)

display(fact_card_transactions)

# COMMAND ----------

fact_card_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.fact_card_transactions")

# COMMAND ----------

fact_account_balance = accounts.select(
    col("account_id"),
    col("customer_id"),
    col("branch_id"),
    current_date().alias("snapshot_date"),
    col("balance")
)

fact_account_balance = fact_account_balance.withColumn(
    "date_key",
    date_format(
        col("snapshot_date"), "yyyyMMdd"
    ).cast("int")
)

display(fact_account_balance)

# COMMAND ----------

fact_account_balance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.fact_account_balance")

# COMMAND ----------

spark.sql("SHOW TABLES IN Workspace.default").show(truncate=False)


# COMMAND ----------

for table in [
    "fact_transactions",
    "fact_loan_payments",
    "fact_card_transactions",
    "fact_account_balance"
]:
    count = spark.table(
        f"Workspace.default.{table}"
    ).count()

    print(table, "=", count)

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp
)

customer_scd2 = customers.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "address",
    "city",
    "state"
).withColumn(
    "effective_date", current_timestamp()
).withColumn(
    "end_date", lit(None).cast("timestamp")
).withColumn(
    "is_current", lit(True)
)

display(customer_scd2)

# COMMAND ----------

customer_scd2.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Workspace.default.dim_customer")

# COMMAND ----------

spark.sql("""
SELECT *
FROM Workspace.default.dim_customer
""").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import lit

watermark_data = [
    ("transactions", "2026-01-01 00:00:00"),
    ("customers", "2026-01-01 00:00:00"),
    ("loans", "2026-01-01 00:00:00"),
    ("loan_payments", "2026-01-01 00:00:00"),
    ("card_transactions", "2026-01-01 00:00:00"),
    ("account_payments", "2026-01-01 00:00:00")
]

watermark_df = spark.createDataFrame(
    watermark_data,
    ["table_name", "last_watermark"]
)

display(watermark_df)

# COMMAND ----------

watermark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.pipeline_watermark")

# COMMAND ----------

spark.sql("""
SELECT *
FROM Workspace.default.pipeline_watermark
""").show()

# COMMAND ----------

last_watermark = "2026-01-01 00:00:00"

incremental_transactions = transactions.filter(
    col("updated_at") > last_watermark
)

display(incremental_transactions)

# COMMAND ----------

source_count = spark.table(
    "Workspace.default.silver_transactions"
).count()

target_count = spark.table(
    "Workspace.default.fact_transactions"
).count()

print("Source count :", source_count)
print("Target count :", target_count)
print("Difference   :", source_count - target_count)

# COMMAND ----------

source_amount = spark.sql("""
SELECT SUM(amount) AS total_amount
FROM Workspace.default.silver_transactions
""").collect()[0]["total_amount"]

target_amount = spark.sql("""
SELECT SUM(amount) AS total_amount
FROM Workspace.default.fact_transactions
""").collect()[0]["total_amount"]

print("Source amount :", source_amount)
print("Target amount :", target_amount)
print("Difference    :", source_amount - target_amount)

# COMMAND ----------

reconciliation_data = [
    (
        "transactions",
        source_count,
        target_count,
        source_count - target_count,
        source_amount,
        target_amount,
        source_amount - target_amount
    )
]

reconciliation_report = spark.createDataFrame(
    reconciliation_data,
    [
        "table_name",
        "source_count",
        "target_count",
        "count_difference",
        "source_amount",
        "target_amount",
        "amount_difference"
    ]
)

display(reconciliation_report)

# COMMAND ----------

reconciliation_report.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "Workspace.default.reconciliation_report"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Workspace.default.dim_customer
# MAGIC ORDER BY customer_id;

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

customers = spark.table("Workspace.default.dim_customer")

# Current record
old_record = customers.filter(col("customer_id") == 1) \
    .withColumn("end_date", current_timestamp()) \
    .withColumn("is_current", lit(False))

# New record
new_record = customers.filter(col("customer_id") == 1) \
    .withColumn("city", lit("Chennai")) \
    .withColumn("effective_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# Keep other customers unchanged
other_records = customers.filter(col("customer_id") != 1)

# Combine
updated_customers = other_records.unionByName(old_record) \
    .unionByName(new_record)

updated_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Workspace.default.dim_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     city,
# MAGIC     effective_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC FROM Workspace.default.dim_customer
# MAGIC WHERE customer_id = 1;