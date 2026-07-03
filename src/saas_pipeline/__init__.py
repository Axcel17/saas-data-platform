"""Multi-tenant Medallion data pipeline (Bronze / Silver / Gold).

The package implements the SAAS reference architecture: raw CSV deliveries are
ingested to Bronze (Delta), cleaned and enriched with an SCD Type 2 material
dimension in Silver, and aggregated into business metrics in Gold. Tenant
isolation is expressed through the storage path layout, mirroring the
per-tenant schema layout of Unity Catalog.
"""

__version__ = "0.1.0"
