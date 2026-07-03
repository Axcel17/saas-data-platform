# Infraestructura — Terraform para onboarding de tenants

Descripción de lo que Terraform provisionaría para incorporar un tenant nuevo a
la plataforma SAAS sobre Databricks + ADLS Gen2 + Unity Catalog. En esta prueba
el almacenamiento se simula con paths locales; el snippet es ilustrativo y no
está pensado para ejecutarse contra una cuenta real (`terraform plan` funcional
es un bonus, sección 7.2).

## Qué se provisiona por tenant

Dar de alta un tenant es crear su aislamiento lógico y sus permisos. Terraform
manejaría cuatro cosas:

1. **Schemas en Unity Catalog.** Uno por capa dentro del catálogo del ambiente:
   `saas_<env>.bronze_<tenant>`, `silver_<tenant>`, `gold_<tenant>`. El catálogo
   y el schema `shared` (con `quality_logs` y, si se adopta la observación 1, la
   `dim_materials` compartida) ya existen y no se recrean por tenant.
2. **Ubicación de almacenamiento (ADLS Gen2).** Un `external_location`,
   contenedor o prefijo por tenant (`abfss://<tenant>@<account>.dfs.core.windows.net/`),
   con su `storage_credential`, para el layout `data/<layer>/<tenant>/...`.
3. **Grants.** Lectura/escritura del service principal del pipeline sobre los
   schemas del tenant, y lectura para los consumidores analíticos sobre
   `gold_<tenant>`. El gobierno queda centralizado a nivel de catálogo.
4. **Secretos.** Credenciales de las fuentes operacionales del tenant (MongoDB,
   Couchbase) en un `databricks_secret_scope`.

El catálogo y las políticas quedan centralizados. Agregar un tenant es instanciar
el módulo con `tenant = "xx"`, sin tocar los recursos compartidos.

## Snippet ilustrativo: módulo `tenant_onboarding`

```hcl
# modules/tenant_onboarding/variables.tf
variable "env"                { type = string }  # dev | qa | main
variable "tenant"             { type = string }  # sv, hn, ...
variable "catalog"            { type = string }  # saas_dev
variable "storage_account"    { type = string }
variable "storage_credential" { type = string }
variable "pipeline_principal" { type = string }  # service principal del pipeline

# modules/tenant_onboarding/main.tf
locals {
  layers = ["bronze", "silver", "gold"]
}

# Un schema por capa: bronze_<tenant>, silver_<tenant>, gold_<tenant>
resource "databricks_schema" "tenant_layer" {
  for_each     = toset(local.layers)
  catalog_name = var.catalog
  name         = "${each.value}_${var.tenant}"
  comment      = "SAAS ${each.value} layer for tenant ${var.tenant}"
  properties   = { tenant = var.tenant, env = var.env }
}

# Ubicación externa en ADLS Gen2 para el layout data/<layer>/<tenant>/...
resource "databricks_external_location" "tenant_root" {
  name            = "saas-${var.env}-${var.tenant}"
  url             = "abfss://${var.tenant}@${var.storage_account}.dfs.core.windows.net/"
  credential_name = var.storage_credential
  comment         = "Storage root for tenant ${var.tenant}"
}

# El pipeline puede leer y escribir en los schemas del tenant.
resource "databricks_grants" "pipeline_rw" {
  for_each = databricks_schema.tenant_layer
  schema   = "${each.value.catalog_name}.${each.value.name}"
  grant {
    principal  = var.pipeline_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
}

# Secreto para la fuente operacional del tenant.
resource "databricks_secret_scope" "tenant" {
  name = "saas-${var.env}-${var.tenant}"
}
```

```hcl
# Uso: onboarding de un tenant nuevo = una invocación del módulo.
module "onboard_cr" {
  source             = "./modules/tenant_onboarding"
  env                = "dev"
  tenant             = "cr"           # Costa Rica
  catalog            = "saas_dev"
  storage_account    = "saasdatalake"
  storage_credential = "saas-adls-cred"
  pipeline_principal = "sp-saas-pipeline"
}
```

Con esto, el trabajo restante para habilitar el tenant es de configuración de la
aplicación (ver [onboarding-tenant.md](onboarding-tenant.md)), no de código.
