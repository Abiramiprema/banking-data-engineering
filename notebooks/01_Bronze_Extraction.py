# Databricks notebook source
base_path = "/Volumes/Workspace/default/banking_files/"

display(dbutils.fs.ls(base_path))

# COMMAND ----------

customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(base_path + "customers.csv")

display(customers)


# COMMAND ----------

tables = [
    "branches",
    "customers",
    "accounts",
    "transactions",
    "loans",
    "loan_payments",
    "cards",
    "card_transactions",
    "beneficiaries",
    "account_payments"
]

for table in tables:

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(base_path + table + ".csv")

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"Workspace.default.bronze_{table}")

    print(f"Created: bronze_{table}")