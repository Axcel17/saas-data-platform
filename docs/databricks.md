# Ejecución en Databricks (Unity Catalog)

El mismo código corre local (paths, por defecto) o en Databricks escribiendo
tablas Unity Catalog. Es un cambio de configuración: al setear `storage.catalog`
(o `--catalog`), cada tabla se materializa como `<catalog>.<layer>_<tenant>.<table>`,
con los mismos nombres lógicos que describe la arquitectura en la sección 5.2.

Este modo corre todo el Spark **dentro** de Databricks (no Spark Connect), así
que el MERGE de `DeltaTable` funciona igual que en local. Se valida en Databricks
Free Edition, que ya incluye Unity Catalog serverless.

## 1. Autenticación (opcional, con el CLI)

Correr en el workspace por la UI no requiere el CLI. Si lo querés para
scripting, es el CLI nuevo (binario, **no** el paquete pip viejo):

```bash
brew tap databricks/tap && brew install databricks
databricks auth login --host https://<tu-workspace>.cloud.databricks.com
```

## 2. Setup de Unity Catalog

Ejecutar [`databricks/setup.sql`](../databricks/setup.sql) en un notebook SQL.
Crea el catálogo `saas_dev`, el schema `raw` y el volume `raw.files`. Después,
subir a `/Volumes/saas_dev/raw/files/` (Catalog Explorer > Volume > Upload):

- `global_mobility_data_entrega_productos.csv`
- `materials_catalog.csv`

Los schemas por tenant (`bronze_sv`, `silver_sv`, `gold_sv`, ...) y `shared` los
crea el pipeline solo.

## 3. Agregar el repo como Git folder

En el workspace: **Workspace > Repos > Add Repo** y pegar la URL del repo
público. Eso deja el paquete `saas_pipeline` disponible sin instalar un wheel
(la config se lee desde el árbol de fuentes).

## 4. Correr el pipeline

Abrir [`databricks/run_pipeline.py`](../databricks/run_pipeline.py) como notebook
y ejecutarlo. El notebook:

- Instala `omegaconf` (única dependencia que el runtime no trae).
- Deriva la ruta del repo desde su propia ubicación (sin usuarios hardcodeados).
- Toma catálogo, ambiente, tenant y volume desde widgets.
- Corre `saas_pipeline.pipeline.run` contra Unity Catalog.

Equivalente en una celda suelta:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<tu-usuario>/saas-data-platform/src")

from saas_pipeline.config import load_config
from saas_pipeline.pipeline import run

run(load_config(env="dev", overrides={
    "storage.catalog": "saas_dev",
    "execution.tenant": "all",
    "paths.raw.deliveries": "/Volumes/saas_dev/raw/files/global_mobility_data_entrega_productos.csv",
    "paths.raw.materials":  "/Volumes/saas_dev/raw/files/materials_catalog.csv",
}))
```

## 5. Verificar

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
