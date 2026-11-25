# Electrocasa Lakehouse — Arquitectura Medallon en Databricks

Pipeline de ingeniería de datos end-to-end para el monitoreo logístico de envíos (`TrackingEnvios`) desde **Azure SQL Database** hacia **Unity Catalog** (`electrocasa_dev`) en Databricks, aplicando arquitectura Medallion (Bronze, Silver, Gold), contratos de datos y orquestación automatizada.

## Arquitectura del Flujo

```text
Azure SQL (electrocasadb.dbo.TrackingEnvios)
   │  [Lakeflow Connect - Ingesta Incremental por fecha_modificacion]
   ▼
Bronze: electrocasa_dev.bronze.tracking_envios (15,139 registros)
   │  [01_bronze_validacion.py - Auditoría de PK, nulos e historial Delta]
   ▼
Silver: electrocasa_dev.silver.tracking_envios_clean
   │  [02_silver_limpieza_tracking.py - Deduplicación, estandarización e imputación de 285 fechas nulas]
   ▼
Gold:
   ├── electrocasa_dev.gold.kpi_entregas_sucursal
   └── electrocasa_dev.gold.kpi_desempeno_courier
