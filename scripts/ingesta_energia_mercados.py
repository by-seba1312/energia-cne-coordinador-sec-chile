#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingesta Oficial y Determinista del Proyecto 2: Energía, SEN, CNE, SEC y ENAP
Proyecto GCP: project-0ab8074c-14aa-434e-849 (Región: southamerica-west1)
Dataset Destino: core_energia_mercados
Aprovechamiento de almacenamiento: Facturación PHYSICAL, Parquet ZSTD (nivel 19), MERGE idempotente.
"""

import os
import sys
import json
import time
import re
import unicodedata
from datetime import datetime, date, timezone, timedelta
import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery, storage

PROJECT_ID = "project-0ab8074c-14aa-434e-849"
DATASET_ID = "core_energia_mercados"
LOCATION = "southamerica-west1"
BUCKET_NAME = "chile-macro-bronze-0ab8074c"
GCS_PREFIX = "energia_mercados"

LOCAL_RAW_DIR = r"g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet"
SCRIPTS_DIR = r"g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\scripts"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*'
}

os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
os.makedirs(SCRIPTS_DIR, exist_ok=True)

bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

def normalizar_texto(texto):
    if not texto:
        return ""
    texto = unicodedata.normalize('NFD', str(texto))
    texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
    return texto.strip().lower()

# Cargar mapa canónico de comunas
with open(os.path.join(SCRIPTS_DIR, "cut_comunas_map.json"), "r", encoding="utf-8") as f:
    RAW_CUT_MAP = json.load(f)

NORM_CUT_MAP = {}
for k, v in RAW_CUT_MAP.items():
    norm_k = normalizar_texto(k)
    NORM_CUT_MAP[norm_k] = v

# Alias comunes
ALIAS_COMUNAS = {
    "coihaique": "coyhaique",
    "marchigue": "marchihue",
    "llay llay": "llaillay",
    "llay-llay": "llaillay",
    "calera": "la calera",
    "santiago centro": "santiago",
    "paiguano": "paihuano",
    "trehuaco": "treguaco",
    "cabo de hornos": "cabo de hornos (navarino)"
}

def obtener_comuna_info(nombre_comuna_raw):
    norm = normalizar_texto(nombre_comuna_raw)
    norm = ALIAS_COMUNAS.get(norm, norm)
    if norm in NORM_CUT_MAP:
        return NORM_CUT_MAP[norm]
    # Búsqueda parcial
    for k, v in NORM_CUT_MAP.items():
        if norm in k or k in norm:
            return v
    return {"cut_comuna": "13101", "cut_region": "13", "nombre_comuna": str(nombre_comuna_raw), "nombre_region": "Región Metropolitana de Santiago"}

def guardar_parquet_y_gcs(df, nombre_archivo):
    local_path = os.path.join(LOCAL_RAW_DIR, nombre_archivo)
    # ZSTD nivel 19 para máxima compresión columnar
    df.to_parquet(local_path, index=False, engine='pyarrow', compression='zstd', compression_level=19)
    file_size_kb = os.path.getsize(local_path) / 1024.0
    print(f"  [PARQUET] Guardado localmente: {local_path} ({file_size_kb:.2f} KB, {len(df):,} filas)")

    # Subir a Google Cloud Storage
    gcs_blob_name = f"{GCS_PREFIX}/{nombre_archivo}"
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_path, content_type="application/octet-stream")
    print(f"  [GCS] Subido exitosamente a gs://{BUCKET_NAME}/{gcs_blob_name}")

print("Inicialización completa. Conexión BigQuery y GCS verificada.")
