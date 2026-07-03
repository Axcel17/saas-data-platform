# Code review — `bad_code.py`

Revisión de la entrega del junior. Para cada punto: qué está mal, por qué
importa y cómo se corrige. El refactor está en [`good_code.py`](good_code.py).

## 1. pandas e iteración fila por fila

El código lee con `pd.read_csv`, filtra con pandas y recorre con
`for i, row in df.iterrows()`.

Eso trae todo el archivo a memoria del driver y procesa fila por fila en Python.
No escala más allá de un archivo chico, no usa el clúster y rompe el modelo
distribuido de Spark. Acá los volúmenes son multi-tenant y van a crecer.

Se corrige leyendo con `spark.read.csv` y expresando la lógica como
transformaciones de columnas (`when`/`otherwise`, `withColumn`), sin `iterrows`.
Ver `normalize_deliveries` en el refactor.

## 2. Reglas de negocio y constantes hardcoded

El factor `20` (CS a ST), los tipos `"ZPRE"`/`"ZVE1"`, el país `"GT"`, la ruta
`"/tmp/output/"` y `"data.csv"` están escritos a mano en el código.

Cambiar una regla obliga a editar código. No hay multi-tenant de verdad: el país
es un argumento suelto, no configuración. Y las rutas fijas impiden correr en
otro ambiente. El enunciado penaliza explícitamente los pipelines con datos o
rutas hardcodeadas.

Se corrige parametrizando por configuración o inyección (`DeliveryJobConfig`),
dando nombre a las constantes de negocio (`UNITS_PER_CASE`,
`ROUTINE_DELIVERY_TYPES`) y derivando las rutas por ambiente y tenant.

## 3. Escritura no idempotente y en el formato equivocado

`sdf.write.mode("overwrite").parquet("/tmp/output/" + country)`.

Ese `overwrite` borra y reescribe todo el destino en cada corrida: no hay
overwrite por partición ni MERGE. Usa Parquet, que no da transacciones ACID ni
time travel. Y depende de `/tmp`, que es efímero. Un reproceso parcial puede
dejar los datos inconsistentes.

Se corrige escribiendo en Delta con overwrite por partición (`replaceWhere`) o
`MERGE INTO` sobre una clave de negocio, particionando por una columna estable.

## 4. Sin validación, tipado ni manejo de errores

No valida columnas ni tipos, asume que `precio` y `cantidad` son numéricos, no
maneja nulos y tampoco el caso de `result` vacío (con una lista vacía,
`spark.createDataFrame` falla). No hay tests.

Cualquier anomalía del origen (un nulo, una cantidad negativa, un SKU que no
existe) termina en un resultado incorrecto silencioso o en una excepción opaca.
En una plataforma gobernada, los datos malos tienen que ser visibles y
auditables, no desaparecer.

Se corrige con esquema explícito al leer, casteo y validación de tipos, ruteo de
anomalías a cuarentena en vez de descarte silencioso, y tests sobre la
transformación. En la plataforma eso vive en `transforms.py` y `quality.py`.

## 5. Naming inconsistente y efectos colaterales

Mezcla de idiomas y convenciones (`qty`, `total`, `pais`, `fecha`), una
`SparkSession` global a nivel de módulo, y un `print("done")` como feedback.

El naming mezclado complica el mantenimiento. La sesión global acopla el módulo y
hace difícil testearlo. Y los `print` no sirven como observabilidad.

Se corrige con naming consistente en inglés para el código, inyectando la
`SparkSession` como dependencia y usando `logging` en vez de `print`.
