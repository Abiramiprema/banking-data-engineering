# Databricks notebook source
customers = spark.table("Workspace.default.bronze_customers")
accounts = spark.table("Workspace.default.bronze_accounts")
transactions = spark.table("Workspace.default.bronze_transactions")
branches = spark.table("Workspace.default.bronze_branches")
loans = spark.table("Workspace.default.bronze_loans")
loan_payments = spark.table("Workspace.default.bronze_loan_payments")
cards = spark.table("Workspace.default.bronze_cards")
card_transactions = spark.table("Workspace.default.bronze_card_transactions")
beneficiaries = spark.table("Workspace.default.bronze_beneficiaries")
account_payments = spark.table("Workspace.default.bronze_account_payments")

# COMMAND ----------

from pyspark.sql.functions import col, count

print("Total customers:", customers.count())

print("Duplicate customer IDs:")
customers.groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

print("Customers with missing IDs:")
customers.filter(col("customer_id").isNull()).show()

# COMMAND ----------

print("Total transactions:", transactions.count())

print("Duplicate transaction IDs:")
transactions.groupBy("transaction_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

print("Transactions with null amount:")
transactions.filter(col("amount").isNull()).show()

print("Transactions with invalid amount:")
transactions.filter(col("amount") <= 0).show()

# COMMAND ----------

silver_customers = customers \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull())

# COMMAND ----------

silver_transactions = transactions \
    .dropDuplicates(["transaction_id"]) \
    .filter(
        col("transaction_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

# COMMAND ----------

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.silver_customers")

silver_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.silver_transactions")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, to_date

silver_branches = branches.dropDuplicates(["branch_id"]) \
    .filter(col("branch_id").isNotNull())

silver_customers = customers.dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull())

silver_accounts = accounts.dropDuplicates(["account_id"]) \
    .filter(
        col("account_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("branch_id").isNotNull()
    )

silver_transactions = transactions.dropDuplicates(["transaction_id"]) \
    .filter(
        col("transaction_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

silver_loans = loans.dropDuplicates(["loan_id"]) \
    .filter(
        col("loan_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("branch_id").isNotNull() &
        (col("loan_amount") > 0)
    )

silver_loan_payments = loan_payments.dropDuplicates(["payment_id"]) \
    .filter(
        col("payment_id").isNotNull() &
        col("loan_id").isNotNull() &
        col("payment_amount").isNotNull() &
        (col("payment_amount") > 0)
    )

silver_cards = cards.dropDuplicates(["card_id"]) \
    .filter(
        col("card_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("account_id").isNotNull()
    )

silver_card_transactions = card_transactions.dropDuplicates(["card_transaction_id"]) \
    .filter(
        col("card_transaction_id").isNotNull() &
        col("card_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

silver_beneficiaries = beneficiaries.dropDuplicates(["beneficiary_id"]) \
    .filter(
        col("beneficiary_id").isNotNull() &
        col("customer_id").isNotNull()
    )

silver_account_payments = account_payments.dropDuplicates(["payment_id"]) \
    .filter(
        col("payment_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

# COMMAND ----------

silver_tables = {
    "branches": silver_branches,
    "customers": silver_customers,
    "accounts": silver_accounts,
    "transactions": silver_transactions,
    "loans": silver_loans,
    "loan_payments": silver_loan_payments,
    "cards": silver_cards,
    "card_transactions": silver_card_transactions,
    "beneficiaries": silver_beneficiaries,
    "account_payments": silver_account_payments
}

for name, df in silver_tables.items():
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"Workspace.default.silver_{name}")

    print(f"Created silver_{name}")

# COMMAND ----------

from pyspark.sql.functions import col

silver_branches = branches.dropDuplicates(["branch_id"]) \
    .filter(col("branch_id").isNotNull())

silver_customers = customers.dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull())

silver_accounts = accounts.dropDuplicates(["account_id"]) \
    .filter(
        col("account_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("branch_id").isNotNull()
    )

silver_transactions = transactions.dropDuplicates(["transaction_id"]) \
    .filter(
        col("transaction_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

silver_loans = loans.dropDuplicates(["loan_id"]) \
    .filter(
        col("loan_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("branch_id").isNotNull() &
        (col("loan_amount") > 0)
    )

silver_loan_payments = loan_payments.dropDuplicates(["payment_id"]) \
    .filter(
        col("payment_id").isNotNull() &
        col("loan_id").isNotNull() &
        col("payment_amount").isNotNull() &
        (col("payment_amount") > 0)
    )

# CARD TABLE
silver_cards = cards.dropDuplicates(["card_id"]) \
    .filter(
        col("card_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("account_id").isNotNull()
    )

# CARD TRANSACTIONS TABLE
silver_card_transactions = card_transactions \
    .dropDuplicates(["card_transaction_id"]) \
    .filter(
        col("card_transaction_id").isNotNull() &
        col("card_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

silver_beneficiaries = beneficiaries \
    .dropDuplicates(["beneficiary_id"]) \
    .filter(
        col("beneficiary_id").isNotNull() &
        col("customer_id").isNotNull()
    )

silver_account_payments = account_payments \
    .dropDuplicates(["payment_id"]) \
    .filter(
        col("payment_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

# COMMAND ----------

silver_tables = {
    "branches": silver_branches,
    "customers": silver_customers,
    "accounts": silver_accounts,
    "transactions": silver_transactions,
    "loans": silver_loans,
    "loan_payments": silver_loan_payments,
    "cards": silver_cards,
    "card_transactions": silver_card_transactions,
    "beneficiaries": silver_beneficiaries,
    "account_payments": silver_account_payments
}

for name, df in silver_tables.items():
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"Workspace.default.silver_{name}")

    print(f"Created silver_{name}")

# COMMAND ----------

cards = spark.table("Workspace.default.bronze_cards")

card_transactions = spark.table(
    "Workspace.default.bronze_card_transactions"
)

print("CARDS:")
cards.printSchema()

print("CARD TRANSACTIONS:")
card_transactions.printSchema()

# COMMAND ----------

silver_cards = cards \
    .dropDuplicates(["card_id"]) \
    .filter(
        col("card_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("account_id").isNotNull()
    )

silver_card_transactions = card_transactions \
    .dropDuplicates(["card_transaction_id"]) \
    .filter(
        col("card_transaction_id").isNotNull() &
        col("card_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )
    

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, when, lit

dq_results = []

def check_table(name, df, id_column):
    total = df.count()
    null_ids = df.filter(col(id_column).isNull()).count()
    duplicates = total - df.dropDuplicates([id_column]).count()
    valid = total - null_ids - duplicates
    invalid = null_ids

    dq_results.append((
        name,
        total,
        valid,
        invalid,
        duplicates,
        null_ids
    ))

check_table("customers", customers, "customer_id")
check_table("accounts", accounts, "account_id")
check_table("transactions", transactions, "transaction_id")
check_table("loans", loans, "loan_id")
check_table("loan_payments", loan_payments, "payment_id")
check_table("cards", cards, "card_id")
check_table(
    "card_transactions",
    card_transactions,
    "card_transaction_id"
)
check_table("beneficiaries", beneficiaries, "beneficiary_id")
check_table("account_payments", account_payments, "payment_id")

# COMMAND ----------

card_transactions = spark.table(
    "Workspace.default.bronze_card_transactions"
)

print("Columns:")
print(card_transactions.columns)

# COMMAND ----------

check_table(
    "card_transactions",
    card_transactions,
    "card_transaction_id"
)

# COMMAND ----------

spark.sql("SHOW TABLES IN Workspace.default").show(100, False)

# COMMAND ----------

spark.table("Workspace.default.bronze_card_transactions").printSchema()

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/Workspace/default/banking_files/"))

# COMMAND ----------

card_tx = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/Workspace/default/banking_files/card_transactions.csv")

print(card_tx.columns)
display(card_tx)

# COMMAND ----------

card_transactions = spark.createDataFrame([
    (1, 1, "2026-08-02 12:30:00", "PURCHASE", 1500.00, "Amazon"),
    (2, 1, "2026-08-06 18:20:00", "PURCHASE", 800.00, "Reliance"),
    (3, 2, "2026-08-08 11:15:00", "PURCHASE", 2500.00, "Flipkart"),
    (4, 2, "2026-08-11 20:00:00", "PURCHASE", 1200.00, "Swiggy"),
    (5, 3, "2026-08-13 14:10:00", "PURCHASE", 5000.00, "Apple Store"),
    (6, 4, "2026-08-16 16:30:00", "PURCHASE", 1800.00, "Amazon"),
    (7, 5, "2026-08-19 19:45:00", "PURCHASE", 3500.00, "Myntra")
], [
    "card_transaction_id",
    "card_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "merchant_name"
])

display(card_transactions)

# COMMAND ----------

card_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.bronze_card_transactions")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS Workspace.default.bronze_card_transactions")

# COMMAND ----------

card_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Workspace.default.bronze_card_transactions")

# COMMAND ----------

spark.table(
    "Workspace.default.bronze_card_transactions"
).show()

# COMMAND ----------

spark.table(
    "Workspace.default.bronze_card_transactions"
).printSchema()

# COMMAND ----------

card_transactions = spark.table(
    "Workspace.default.bronze_card_transactions"
)

# COMMAND ----------

print("Total card transactions:", card_transactions.count())

print("Duplicate IDs:")
card_transactions.groupBy("card_transaction_id") \
    .count() \
    .filter("count > 1") \
    .show()

print("Null IDs:")
card_transactions \
    .filter("card_transaction_id IS NULL") \
    .show()

print("Invalid amounts:")
card_transactions \
    .filter("amount IS NULL OR amount <= 0") \
    .show()

# COMMAND ----------

silver_card_transactions = card_transactions \
    .dropDuplicates(["card_transaction_id"]) \
    .filter(
        col("card_transaction_id").isNotNull() &
        col("card_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

# COMMAND ----------

silver_card_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Workspace.default.silver_card_transactions")

# COMMAND ----------

print(
    "Silver card transactions:",
    silver_card_transactions.count()
)

# COMMAND ----------

from pyspark.sql.functions import col

silver_branches = branches.dropDuplicates(["branch_id"]) \
    .filter(col("branch_id").isNotNull())

silver_customers = customers.dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull())

silver_accounts = accounts.dropDuplicates(["account_id"]) \
    .filter(
        col("account_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("branch_id").isNotNull()
    )

silver_transactions = transactions.dropDuplicates(["transaction_id"]) \
    .filter(
        col("transaction_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

silver_loans = loans.dropDuplicates(["loan_id"]) \
    .filter(
        col("loan_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("branch_id").isNotNull() &
        (col("loan_amount") > 0)
    )

silver_loan_payments = loan_payments.dropDuplicates(["payment_id"]) \
    .filter(
        col("payment_id").isNotNull() &
        col("loan_id").isNotNull() &
        col("payment_amount").isNotNull() &
        (col("payment_amount") > 0)
    )

silver_cards = cards.dropDuplicates(["card_id"]) \
    .filter(
        col("card_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("account_id").isNotNull()
    )

silver_card_transactions = card_transactions \
    .dropDuplicates(["card_transaction_id"]) \
    .filter(
        col("card_transaction_id").isNotNull() &
        col("card_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

silver_beneficiaries = beneficiaries \
    .dropDuplicates(["beneficiary_id"]) \
    .filter(
        col("beneficiary_id").isNotNull() &
        col("customer_id").isNotNull()
    )

silver_account_payments = account_payments \
    .dropDuplicates(["payment_id"]) \
    .filter(
        col("payment_id").isNotNull() &
        col("account_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

# COMMAND ----------

silver_tables = {
    "branches": silver_branches,
    "customers": silver_customers,
    "accounts": silver_accounts,
    "transactions": silver_transactions,
    "loans": silver_loans,
    "loan_payments": silver_loan_payments,
    "cards": silver_cards,
    "card_transactions": silver_card_transactions,
    "beneficiaries": silver_beneficiaries,
    "account_payments": silver_account_payments
}

for name, df in silver_tables.items():
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"Workspace.default.silver_{name}")

    print(f"✓ silver_{name}")

# COMMAND ----------

for name, df in silver_tables.items():
    print(f"{name}: {df.count()} records")