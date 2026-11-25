# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "electrocasa_dev"
bronze_table = f"{catalog}.bronze.tracking_envios"
silver_table = f"{catalog}.silver.tracking_envios_clean"

# Asegurar que exista el esquema silver
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

df_bronze = spark.table(bronze_table)

# 1. Deduplicación preventiva conservando la última modificación por evento_id
window_spec = Window.partitionBy("evento_id").orderBy(F.col("fecha_modificacion").desc())
df_dedup = (
    df_bronze
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 2. Estandarización de textos, tipado y tratamiento de los 285 nulos en fecha_actualizacion
df_silver = (
    df_dedup
    .select(
        F.col("evento_id").cast("bigint").alias("evento_id"),
        F.trim(F.col("tracking_id")).alias("tracking_id"),
        F.upper(F.trim(F.col("venta_id"))).alias("venta_id"),
        F.upper(F.trim(F.col("courier"))).alias("courier"),
        F.upper(F.trim(F.col("estado_entrega"))).alias("estado_entrega"),
        F.upper(F.trim(F.col("sucursal_origen"))).alias("sucursal_origen"),
        F.coalesce(F.col("fecha_actualizacion"), F.col("fecha_modificacion")).cast("timestamp").alias("fecha_actualizacion"),
        F.col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        F.col("fecha_actualizacion").isNull().alias("flag_fecha_imputada"),
        F.current_timestamp().alias("silver_processed_at")
    )
    .filter(F.col("evento_id").isNotNull() & F.col("tracking_id").isNotNull())
)

# 3. Escritura en Delta Lake (Capa Silver)
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

total_silver = spark.table(silver_table).count()
print(f"Tabla Silver creada: {silver_table}")
print(f"Total de registros limpios en Silver: {total_silver}")
display(spark.table(silver_table).limit(10))
