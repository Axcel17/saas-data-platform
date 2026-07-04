# Databricks notebook source
# MAGIC %md
# MAGIC # SAAS pipeline en Databricks (Unity Catalog)
# MAGIC
# MAGIC Corre el paquete `saas_pipeline` desde este Git folder y escribe tablas
# MAGIC Unity Catalog `<catalog>.<layer>_<tenant>.<table>`.
# MAGIC
# MAGIC **Prerrequisitos** (ver `databricks/setup.sql` y `docs/databricks.md`):
# MAGIC 1. Catálogo creado (por defecto `saas_dev`).
# MAGIC 2. Volume `saas_dev.raw.files` con los dos CSV subidos.
# MAGIC 3. Este repo agregado como Git folder.

# COMMAND ----------
# La única dependencia que el runtime no trae es OmegaConf. PySpark y Delta ya vienen.
# MAGIC %pip install omegaconf==2.3.0

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# Parámetros del run (editables como widgets, sin nada hardcodeado).
dbutils.widgets.text("catalog", "saas_dev", "Unity Catalog")
dbutils.widgets.text("env", "dev", "Ambiente")
dbutils.widgets.text("tenant", "all", "Tenant (código o 'all')")
dbutils.widgets.text("volume", "/Volumes/saas_dev/raw/files", "Volume con los CSV")

# COMMAND ----------
import os
import sys

# Ruta del repo derivada de la ubicación de este notebook, sin usuarios hardcodeados.
# El notebook vive en <repo>/databricks/run_pipeline, así que <repo> = dos niveles arriba.
notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))
sys.path.insert(0, f"{repo_root}/src")
print("repo_root:", repo_root)

# COMMAND ----------
from saas_pipeline.config import load_config
from saas_pipeline.pipeline import run

catalog = dbutils.widgets.get("catalog")
volume = dbutils.widgets.get("volume")

cfg = load_config(
    env=dbutils.widgets.get("env"),
    overrides={
        "storage.catalog": catalog,
        "execution.tenant": dbutils.widgets.get("tenant"),
        "paths.raw.deliveries": f"{volume}/global_mobility_data_entrega_productos.csv",
        "paths.raw.materials": f"{volume}/materials_catalog.csv",
    },
)
# Reuse the notebook's serverless session; the pipeline must not create or stop one.
report = run(cfg, spark=spark)
print(report)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(spark.table(f"{catalog}.gold_sv.daily_metrics_by_delivery_type").limit(20))
display(
    spark.table(f"{catalog}.shared.quality_logs").select(
        "tenant_id", "layer", "check_name", "check_severity", "check_passed"
    )
)
