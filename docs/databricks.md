# Ejecución en Databricks (Unity Catalog)

El mismo código corre local (paths, por defecto) o sobre Databricks escribiendo
tablas Unity Catalog. Es un cambio de configuración: al setear `storage.catalog`
(o `--catalog`), cada tabla se materializa como `<catalog>.<layer>_<tenant>.<table>`
en vez de un path local. Esto implementa el mapeo que la arquitectura anticipa
en la sección 5.2, con los mismos nombres lógicos.

Es un modo opcional. El artefacto principal y probado es el local (ver README).
Este modo lo valido en Databricks Free Edition, que ya incluye Unity Catalog.

## Prerrequisitos

1. Un catálogo creado en Unity Catalog, por ejemplo `saas_dev`:
   ```sql
   CREATE CATALOG IF NOT EXISTS saas_dev;
   ```
   Los schemas por tenant (`bronze_sv`, `silver_sv`, `gold_sv`, ...) y el schema
   `shared` los crea el pipeline solo.

2. Un Volume con los CSVs de entrada. En serverless no hay filesystem local, así
   que los inputs van a un UC Volume:
   ```sql
   CREATE SCHEMA IF NOT EXISTS saas_dev.raw;
   CREATE VOLUME IF NOT EXISTS saas_dev.raw.files;
   ```
   Subir `global_mobility_data_entrega_productos.csv` y `materials_catalog.csv` a
   `/Volumes/saas_dev/raw/files/`.

3. El repo disponible en el workspace como Git folder (Repos), para importar el
   paquete `saas_pipeline`.

## Ejecutar

Desde un notebook, apuntando `sys.path` al `src/` del repo y pasando los overrides:

```python
import sys
sys.path.append("/Workspace/Repos/<tu-usuario>/saas-data-platform/src")

from saas_pipeline.config import load_config
from saas_pipeline.pipeline import run

cfg = load_config(env="dev", overrides={
    "storage.catalog": "saas_dev",
    "execution.tenant": "all",
    "paths.raw.deliveries": "/Volumes/saas_dev/raw/files/global_mobility_data_entrega_productos.csv",
    "paths.raw.materials":  "/Volumes/saas_dev/raw/files/materials_catalog.csv",
})
run(cfg)
```

## Verificar

```sql
SHOW SCHEMAS IN saas_dev;                       -- bronze_sv, silver_sv, gold_sv, shared, ...
SELECT * FROM saas_dev.gold_sv.daily_metrics_by_delivery_type LIMIT 20;
SELECT tenant_id, check_name, check_passed FROM saas_dev.shared.quality_logs;
```

## Notas

- En Databricks se omite `configure_spark_with_delta_pip` (el runtime ya trae
  Delta) y se reutiliza la sesión existente. Ver `spark_session.py`.
- La idempotencia es la misma que en local: MERGE por clave de negocio y
  overwrite dinámico por partición, ahora sobre tablas UC.
