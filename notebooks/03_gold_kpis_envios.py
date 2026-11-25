# Databricks notebook source
from pyspark.sql import functions as F

catalog = "electrocasa_dev"
silver_table = f"{catalog}.silver.tracking_envios_clean"
gold_sucursal_table = f"{catalog}.gold.kpi_entregas_sucursal"
gold_courier_table = f"{catalog}.gold.kpi_desempeno_courier"

# Asegurar que exista el esquema gold
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

df_silver = spark.table(silver_table)

# Calcular tiempo de gestion en horas entre modificación y ultima actualizacion
df_enriched = df_silver.withColumn(
    "horas_gestion",
    F.round(
        F.abs(F.unix_timestamp("fecha_modificacion") - F.unix_timestamp("fecha_actualizacion")) / 3600.0,
        2
    )
)

# 1. Tabla Gold por sucursal y courier
df_gold_sucursal = (
    df_enriched
    .groupBy("sucursal_origen", "courier")
    .agg(
        F.count("evento_id").alias("total_eventos"),
        F.countDistinct("tracking_id").alias("total_envios_unicos"),
        F.countDistinct("venta_id").alias("total_ventas_asociadas"),
        F.sum(F.when(F.col("estado_entrega") == "ENTREGADO", 1).otherwise(0)).alias("envios_entregados"),
        F.sum(F.when(F.col("estado_entrega") == "PENDIENTE", 1).otherwise(0)).alias("envios_pendientes"),
        F.sum(F.when(F.col("estado_entrega").isin("INCIDENCIA", "DEVUELTO", "RETRASADO"), 1).otherwise(0)).alias("envios_con_novedad"),
        F.round(F.avg("horas_gestion"), 2).alias("promedio_horas_gestion"),
        F.current_timestamp().alias("gold_updated_at")
    )
    .withColumn(
        "tasa_entrega_pct",
        F.round((F.col("envios_entregados") / F.col("total_eventos")) * 100, 2)
    )
    .orderBy(F.col("total_eventos").desc())
)

(
    df_gold_sucursal.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_sucursal_table)
)

# 2. Tabla Gold resumen ejecutivo por courier
df_gold_courier = (
    df_enriched
    .groupBy("courier")
    .agg(
        F.count("evento_id").alias("total_eventos"),
        F.countDistinct("tracking_id").alias("total_envios_unicos"),
        F.sum(F.when(F.col("estado_entrega") == "ENTREGADO", 1).otherwise(0)).alias("envios_entregados"),
        F.sum(F.when(F.col("estado_entrega") == "PENDIENTE", 1).otherwise(0)).alias("envios_pendientes"),
        F.round(F.avg("horas_gestion"), 2).alias("promedio_horas_gestion"),
        F.current_timestamp().alias("gold_updated_at")
    )
    .withColumn(
        "tasa_entrega_pct",
        F.round((F.col("envios_entregados") / F.col("total_eventos")) * 100, 2)
    )
    .orderBy(F.col("total_eventos").desc())
)

(
    df_gold_courier.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_courier_table)
)

print(f"Tablas Gold creadas:\n - {gold_sucursal_table}\n - {gold_courier_table}")
display(spark.table(gold_courier_table))
display(spark.table(gold_sucursal_table).limit(15))
