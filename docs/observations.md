# Observaciones a la arquitectura

Notas sobre decisiones, ambigüedades y mejoras a futuro, para conversar en la
sustentación. No cambié la arquitectura provista. Donde no coincido, lo dejo
acá y la implementación se ciñe a lo que dice la sección 5.

## 1. La dimensión `dim_materials` se duplica en cada tenant

La arquitectura aísla por schema (`silver_<tenant>.dim_materials`), así que hay
que escribir una copia del catálogo por cada tenant. Pero el catálogo es
corporativo: no cambia entre tenants.

El costo de eso es concreto. El mismo dato queda replicado N veces (N = cantidad
de tenants), la SCD Type 2 hay que mantenerla en N tablas, y corregir un precio
del catálogo obliga a N MERGE. Además de gastar almacenamiento, se abre la
puerta a que un tenant quede desincronizado de otro.

Yo lo resolvería con una sola `dim_materials` en un schema compartido
(`saas_<env>.shared.dim_materials`), igual que la arquitectura ya centraliza
`shared.quality_logs`. Cada tenant la consume por vista o join directo. Una sola
fuente de verdad.

El trade-off es que el aislamiento por schema deja de ser total para esa tabla y
el join cross-schema pide un grant de lectura. Para una dimensión chica y de
propiedad corporativa, tener una sola copia lo vale. Igual, en esta entrega
respeté la arquitectura y la escribo por tenant; cambiar a compartida es mover
una ruta en la config.

## 2. Qué hacer con `fecha_proceso` nula o inválida en Bronze

Bronze tiene que particionar por `fecha_proceso` y correr sobre un rango
`start_date`–`end_date`. El problema: alrededor del 1.5% de las filas trae la
fecha vacía o inválida (`00000000`, `20251332`). Esas filas no entran en una
partición de fecha ni se pueden comparar contra el rango, y la sección 5.6 pide
mandarlas a cuarentena, no perderlas.

Lo resolví dejando que el filtro de rango en Bronze conserve una fila cuando su
fecha es válida y cae en el rango, o cuando su fecha es inválida o nula. Con eso
las anomalías de fecha siempre entran a Bronze y llegan a Silver, donde se
cuarentenan con `_quarantine_reason = invalid_fecha_proceso`. Si las filtrara,
desaparecerían sin dejar rastro, que es justo lo que la política quiere evitar.

Pensé en detectar la fecha inválida ya en Bronze y mandarla a
`bronze_quarantine`, pero preferí que Bronze sea copia fiel del origen (esquema
más columnas técnicas) y concentrar toda la política de anomalías en Silver,
como plantea el documento.

## 3. La clave de negocio del hecho no es única en el origen

La sección 5.5 define la clave del MERGE como `(tenant_id, fecha_proceso,
transporte, ruta, material, tipo_entrega)`. El dataset no trae un id de línea, y
es perfectamente posible que un mismo transporte y ruta entregue el mismo
material y tipo el mismo día en más de una línea, con precio o cantidad
distintos. Cuando pasa eso, `MERGE INTO` falla: varias filas de origen matchean
la misma fila destino.

Lo manejo deduplicando exactos primero (política 5.6) y, antes del MERGE,
forzando unicidad sobre la clave: me quedo con la fila ingestada más reciente vía
`row_number` por `_ingestion_timestamp`. No descarto nada en silencio; los
duplicados exactos ya se colapsaron y los cuasi-duplicados quedan resueltos por
una regla explícita.

La solución de fondo es que el origen entregue un id de línea, o construir un
`_line_hash` estable como clave. La dedup por clave es una defensa mientras eso
no exista.

Aparte, agregué un motivo de cuarentena que la 5.6 no contempla:
`no_material_version_for_date`. Es el caso de un material que sí está en el
catálogo pero sin versión SCD2 vigente a la fecha de la transacción. Si no lo
atajo, el join temporal lo deja con atributos nulos y ensucia las métricas. Lo
trato como violación de integridad temporal y lo mando a cuarentena.

## 4. A futuro: framework declarativo de calidad

Las validaciones de hoy son checks propios, bien estructurados y persistidos en
`quality_logs`. Alcanzan para este scope. En una próxima iteración movería la
calidad a un framework declarativo: expectations de Delta Live Tables (nativo en
Databricks) o Great Expectations. Ganás expectativas versionadas y
autodocumentadas, cuarentena declarativa (`EXPECT ... ON VIOLATION DROP ROW`),
métricas atadas al lineage de Unity Catalog y menos código imperativo que
mantener. El esquema `quality_logs` actual serviría como histórico unificado.

## 5. A futuro: ingesta incremental, orquestación y despliegue

Hoy Bronze es batch CSV a Delta con overwrite dinámico por partición, que da
idempotencia demostrable. El paso siguiente es Auto Loader (`cloudFiles`) para
ingesta incremental desde ADLS, con detección de esquema y checkpointing,
tratando cada archivo nuevo como un micro-batch. La clave de negocio y el MERGE
de Silver ya soportan ese modo sin tocar nada.

El CLI alcanza para correr y demostrar el pipeline, pero en producción las
dependencias entre capas y entre tenants las orquestaría con Databricks Workflows
o Airflow: reintentos, alertas, SLAs y paralelismo por tenant, con
`fail_fast`/`fail_on_critical` mapeados a la política de tareas del job. Eso
reemplaza la iteración secuencial de tenants que hoy hace `pipeline.run`.

Sobre despliegue: la prueba pide CI (quality gate) y un snippet de IaC, no un
deploy. El cierre natural del ciclo es empaquetar el pipeline con Databricks
Asset Bundles y desplegar jobs por ambiente desde el mismo repo, apoyándose en el
CI que ya existe.

## Nota de implementación: modo local y Unity Catalog

El enunciado dice que Unity Catalog "no aplica" y pide implementar la estructura
de paths, y así lo hice. Pero dejé el código desacoplado del backend de
almacenamiento (`storage.py` y `paths.Location`): por defecto escribe Delta en
paths locales, reproducible y sin cuenta, y seteando `storage.catalog`
(`--catalog`) escribe tablas Unity Catalog `<catalog>.<layer>_<tenant>.<table>`.
Así el mapeo path→tabla que la arquitectura describe en teoría queda ejecutable,
sin perder la reproducibilidad local que la sustentación necesita. El modo local
es el que está probado por defecto; el modo UC lo valido en Databricks Free
Edition. Ver [databricks.md](databricks.md).

## Resumen de decisiones ante ambigüedad

| Tema | Decisión | Sección |
|------|----------|---------|
| Fecha inválida o nula | Se ingesta siempre y se cuarentena en Silver | 2 |
| Clave de negocio no única | Dedup determinista por `_ingestion_timestamp` antes del MERGE | 3 |
| Material sin versión a la fecha | Nuevo motivo de cuarentena `no_material_version_for_date` | 3 |
| `is_current` | No se usa en el join; se usa el rango `valid_from`/`valid_to` | 5.7 |
