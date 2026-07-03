# Onboarding de un tenant nuevo

El pipeline es data-driven: incorporar un tenant es **configuración, no código**.
Ningún módulo de transformación cambia. Ejemplo: alta de Costa Rica (`cr`).

## Pasos (aplicación)

1. **Registrar el tenant** en `config/base.yaml`, lista `tenants:`. Solo así lo
   toma `--tenant all`.

   ```yaml
   tenants:
     - sv
     - hn
     - ec
     - jm
     - gt
     - pe
     - cr   # nuevo
   ```

2. **Crear su archivo de perfil** `config/tenants/cr.yaml`:

   ```yaml
   tenant_profile:
     id: cr
     display_name: Costa Rica
     timezone: America/Costa_Rica
   ```

   Aquí también se ponen overrides específicos del tenant si los necesita (por
   ejemplo, activar `quality.fail_on_critical` solo para ese tenant).

3. **Asegurar el origen.** El CSV transaccional debe traer filas con `pais = CR`.
   El código normaliza `CR` → `cr` al ingresar a Bronze.

4. **Ejecutar** para el tenant nuevo:

   ```bash
   uv run saas-pipeline --env dev --tenant cr
   ```

   Se crean automáticamente los paths `data/dev/{bronze,silver,gold}/cr/...` y las
   escrituras a `shared/quality_logs` quedan etiquetadas con `tenant_id = cr`.

## Pasos (infraestructura, en Databricks)

En un despliegue real, antes del paso 4 se provisiona el aislamiento del tenant
con Terraform (schemas Unity, ubicación ADLS, grants, secretos). Ver
[infra.md](infra.md): es una invocación del módulo `tenant_onboarding` con
`tenant = "cr"`.

## Verificación

```bash
# El tenant nuevo aparece en los logs de calidad, aislado del resto.
uv run saas-pipeline --env dev --tenant cr --layer silver
```

Revisar que existan `data/dev/silver/cr/fact_deliveries` y
`data/dev/silver/cr/dim_materials`, y que `data/dev/shared/quality_logs` tenga
filas con `tenant_id = cr`.

## Checklist

- [ ] `cr` agregado a `tenants:` en `config/base.yaml`.
- [ ] `config/tenants/cr.yaml` creado.
- [ ] Origen entrega filas con `pais = CR`.
- [ ] (Prod) módulo Terraform aplicado para `cr`.
- [ ] Corrida de prueba `--tenant cr` exitosa y verificada en `quality_logs`.
