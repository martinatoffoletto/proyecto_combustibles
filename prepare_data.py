"""
PUNTO 1: GRUPO 2
MARTINA TOFFOLETTO.
CONSIGNA: Realizar una integración con otra fuente de datos y procesar los mismos en una base de datos utilizando lenguaje SQL, utilizando alguna herramienta de ELT como dbt (o equivalente). Por integración se entiende que deberá existir al menos una adaptación de los datos, un join entre diferentes orígenes y una tabla final o de stg, que deberá ser generada de manera orquestada
"""
# 1. Instalar dependencias necesarias para el proyecto
import pandas as pd
import duckdb
import os

# 2. Cargar datasets desde archivos CSV

# Dataset GRUPO 2: http://datos.energia.gob.ar/dataset/1c181390-5045-475e-94dc-410429be4b17/archivo/80ac25de-a44a-4445-9215-090cf55cfda5
df_comb = pd.read_csv("data/precios-en-surtidor-resolucin-3142016.csv")

# Dataset nuevo: https://datos.gob.ar/series/api/series/?chartType=column&ids=145.3_INGNACUAL_DICI_M_38
df_ipc = pd.read_csv("data/indice-precios-al-consumidor-nivel-general-base-diciembre-2016-mensual.csv")


# 3. LIMPIEZA Y TRANSFORMACIÓN de df_comb (combustibles)

# Normalizar columna region a mayúsculas para evitar errores
df_comb["region"] = df_comb["region"].str.upper()

# mapeo regiones para unificar "CENTRO" a "PAMPEANA"  y pasar todo a minúsculas
region_map = {
    "PAMPEANA": "pampeana",
    "NEA": "nea",
    "NOA": "noa",
    "CUYO": "cuyo",
    "PATAGONIA": "patagonia",
    "CENTRO": "pampeana",
}

df_comb["region_normalizada"] = df_comb["region"].map(region_map)

# Elimino filas que no tengan una region
df_comb = df_comb.dropna(subset=["region_normalizada"])

# Elimino filas con datos nulos
df_comb = df_comb.dropna(subset=["precio", "producto", "indice_tiempo"])

# Converti precios a float y elimino filas donde no pudo convertir
df_comb["precio"] = pd.to_numeric(df_comb["precio"], errors="coerce")
df_comb = df_comb.dropna(subset=["precio"])

# Agrupar para obtener precio promedio mensual por región y producto
df_comb_grouped = (
    df_comb.groupby(["indice_tiempo", "region_normalizada", "producto"])["precio"]
    .mean()
    .reset_index()
)

# Renombrar la columna para simplificar
df_comb_grouped.rename(columns={"region_normalizada": "region"}, inplace=True)


# 4. LIMPIEZA Y TRANSFORMACIÓN de df_ipc (ipc)

# indice_tiempo a formato "YYYY-MM" para que coincidan
df_ipc["indice_tiempo"] = df_ipc["indice_tiempo"].astype(str).str.slice(0,7)

# Normalizo columnas a minúsculas
df_ipc.columns = [col.lower() for col in df_ipc.columns]

# Eliminar filas con datos nulos
df_ipc = df_ipc.dropna()


# 5. Guardar CSVs limpios en carpeta /seeds para usar con dbt seed
os.makedirs("seeds", exist_ok=True)
df_comb_grouped.to_csv("seeds/stg_combustibles.csv", index=False)
df_ipc.to_csv("seeds/stg_ipc.csv", index=False)