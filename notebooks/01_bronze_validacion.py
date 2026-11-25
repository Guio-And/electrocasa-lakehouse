# Databricks notebook source
from pyspark.sql import functions as F

catalog = "electrocasa_dev"
bronze_table = f"{catalog}.bronze.tracking_envios"

df_bronze = spark.table(bronze_table)

total_rows = df_bronze.count()
distinct_pks = df_bronze.select("evento_id").distinct().count()

print(f"Tabla: {bronze_table}")
print(f"Total de registros: {total_rows}")
print(f"Llaves primarias únicas (evento_id): {distinct_pks}")

assert total_rows > 0, "Error: La tabla Bronze está vacía."
assert total_rows == distinct_pks, f"Error: Existen {total_rows - distinct_pks} registros duplicados por evento_id."

null_counts = df_bronze.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_bronze.columns if not c.startswith("_")
])
display(null_counts)
