-- Setup de Unity Catalog para correr el pipeline en Databricks.
-- Ejecutar una vez, en un notebook SQL o en el editor de SQL del workspace.
-- Ajustar el nombre del catálogo si usás otro.

CREATE CATALOG IF NOT EXISTS saas_dev;

-- Volume para los CSV de entrada (serverless no tiene filesystem local).
CREATE SCHEMA IF NOT EXISTS saas_dev.raw;
CREATE VOLUME IF NOT EXISTS saas_dev.raw.files;

-- Después de crear el volume, subir estos dos archivos a
-- /Volumes/saas_dev/raw/files/ (Catalog Explorer > Volume > Upload):
--   global_mobility_data_entrega_productos.csv
--   materials_catalog.csv
