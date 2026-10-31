# PLAN DE EJECUCIÓN DETERMINISTA (OPTIMIZADO PARA GEMINI FLASH HIGH) — PROYECTO 2: Ministerio de Energía, Sistema Eléctrico Nacional (SEN), Costos Marginales, BESS, Combustibles CNE/ENAP, Tarifas y Calidad SEC

> **Modelo Objetivo de Ejecución:** `Gemini Flash (High)` — Instrucciones paso a paso 100% deterministas, sin ambigüedad y con sentencias SQL listas para ejecutar.  
> **Proyecto GCP:** `project-0ab8074c-14aa-434e-849`  
> **Región BigQuery y Cloud Run:** `southamerica-west1` (Santiago de Chile)  
> **Dataset Destino en BigQuery:** `core_energia_mercados` *(Ya creado con sus 7 tablas en `0 filas`)*  
> **Vistas Gold Asociadas:** `gold_nacional.vw_energia_costos_marginales_y_generacion` y `gold_nacional.vw_energia_precios_combustibles_y_tarifas_comunal` *(Ya creadas)*  
> **Repositorio GitHub a Crear/Actualizar:** `by-seba1312/energia-cne-coordinador-sec-chile` + `by-seba1312/data-lakehouse-nacional-chile`  
> **Carpeta Local Google Drive:** `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC`  

---

## ⚠️ REGLAS TÉCNICAS CRÍTICAS PARA EL AGENTE (GEMINI FLASH HIGH)

1. **Parámetro Exacto en BigQuery MCP (`execute_sql` y `execute_sql_readonly`):**
   - Usa **SIEMPRE** `projectId: "project-0ab8074c-14aa-434e-849"` (en camelCase). **NUNCA** uses `project_id` con guion bajo porque devuelve `Request contains an invalid argument`.
2. **Palabras Reservadas y Vistas de Sistema en BigQuery:**
   - **NUNCA** uses `AS rows` como alias en SQL (es palabra reservada en GoogleSQL; usa `AS num_rows` o `AS total_filas`).
   - Para auditar tamaños de tablas usa `<dataset>.INFORMATION_SCHEMA.PARTITIONS` (no uses `region-southamerica-west1.INFORMATION_SCHEMA.TABLE_STORAGE`).
3. **Regla de Oro de Trazabilidad (Obligatoria al cerrar cada Hito):**
   > *"actualiza el repo en github de los hitos. deja las fuentes con sus enlaces muy precisos y revisa y recontra revisa que los datos ingresados sean correctos y los enlaces funcionen. Y carga ordenadamente a google drive todos los archivos que descargaste y usaste para la ingesta de datos a google cloud. todo esto para trazabilidad. ademas actualiza el spiderweb en google cloud para google cloud run y google cloud scheduler. deja esta frase actualizada en el repo para trazabilidad"*

---

## 🏛️ LOS 5 PRINCIPIOS OBLIGATORIOS DE MÁXIMO APROVECHAMIENTO DE CAPACIDAD DE ALMACENAMIENTO

El agente debe aplicar obligatoriamente estas 5 medidas que multiplican por **8x a 10x** la capacidad de almacenamiento a costo **$0,00 USD**:

1. **Cambiar Datasets a Facturación Física Comprimida (`storage_billing_model = 'PHYSICAL'`):**
   - Las tablas más grandes (`censo2024.personas` de 6,61 GB y `censo2017.personas` de 4,75 GB) son matrices de enteros repetidos (`1`, `2`, `3`, `-99`) que en el motor columnar Capacitor de BigQuery comprimen **~9,5 veces**.
   - Al pasar de `LOGICAL` a `PHYSICAL`, los **21,00 GiB lógicos bajan a solo ~2,44 GiB físicos**, quedando muy por debajo de los **10 GiB gratuitos ($0,00 USD/mes)** y liberando **7,56 GiB físicos gratis (equivalentes a +60 GiB lógicos de datos nuevos)**.
2. **Verificar que las 5 Tablas Duplicadas Detectadas en BigQuery hayan sido limpiadas (Ahorro de `852,46 MB`):**
   - `censo2024.raw_suelos_ciren` (`334,17 MB`), `censo2024.manzanas_nacional` (`317,35 MB`), `censo2024.comunas_opt_gis` (`194,93 MB`), `ine_master_classifiers.dim_bcch_catalogo_series` (`5,39 MB`) y `core_hacienda_macro.dim_calendario_financiero` (`0,62 MB`).
3. **Prohibición de Strings Repetidos en Tablas Masivas (Codificación por Diccionario `INT64` / Códigos Cortos):**
   - En las tablas `fact_*` evita repetir textos largos redundantes; prioriza códigos (`id_instalacion_sen`, `barra_nodo_sen`, `id_estacion_cne`, `cut_comuna` de 5 dígitos con `LPAD`) y mantén las glosas descriptivas normalizadas en dimensiones (`dim_*`), reduciendo el peso entre **65% y 80%**.
4. **Particionamiento + `CLUSTER BY` Multi-Columna:**
   - Mantén todas las tablas de hechos particionadas por fecha/año y agrupadas físicamente (`CLUSTER BY cut_comuna, tecnologia` o `barra_nodo_sen`) para activar la compresión *Run-Length Encoding (RLE)* de BigQuery y reducir en **95%** los bytes consumidos por consulta.
5. **Archivos Crudos en Google Drive y Cloud Storage en Formato `Parquet + ZSTD (Nivel 19)`:**
   - Todo archivo CSV, XML o Excel descargado e ingestado debe convertirse y guardarse en `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` y en `gs://chile-macro-bronze-0ab8074c/energia_mercados/` usando **Apache Parquet con compresión `ZSTD` nivel 19** (`df.to_parquet(..., compression='zstd', compression_level=19)`), reduciendo 1 GB de datos crudos a ~65 MB.

---

## PASO 0 (EJECUTAR PRIMERO): Asegurar `storage_billing_model = 'PHYSICAL'` en `core_energia_mercados`

Ejecuta mediante `call_mcp_tool` (`ServerName: "bigquery"`, `ToolName: "execute_sql"`, `projectId: "project-0ab8074c-14aa-434e-849"`):
```sql
ALTER SCHEMA `project-0ab8074c-14aa-434e-849.core_energia_mercados` SET OPTIONS(storage_billing_model = 'PHYSICAL');
```

---

## ESQUEMA EXACTO DE LAS 7 TABLAS DE `core_energia_mercados` (POBLAR DE `0` A 100%)

### Regla Anti-Duplicados del Proyecto 2:
- **PROHIBIDO duplicar** tipo de cambio Dólar Observado, UF, IPC o Brent/WTI (se leen mediante `JOIN` desde `chile_macro_silver.fact_bcch_observaciones_historicas`), ni viviendas/hogares/polígonos comunales (se leen mediante `JOIN` desde `censo2024`).

### Hito EN1: `dim_centrales_generacion_y_subestaciones_gis` + `fact_generacion_electrica_sen_horaria`
1. **Tabla 1: `project-0ab8074c-14aa-434e-849.core_energia_mercados.dim_centrales_generacion_y_subestaciones_gis`**
   - **Fuente:** Coordinador Eléctrico Nacional (`https://infotecnica.coordinador.cl` / `https://www.coordinador.cl`) y Energía Abierta CNE (`http://energiaabierta.cl` / `https://api.cne.cl`).
   - **Columnas Exactas:**
     - `id_instalacion_sen` (`STRING NOT NULL`), `nombre_instalacion` (`STRING`), `tipo_instalacion` (`STRING`), `tecnologia` (`STRING`), `clasificacion_ernc` (`STRING`), `subsistema_electrico` (`STRING`), `barra_subestacion_conexion` (`STRING`), `tension_kv` (`FLOAT64`), `potencia_bruta_mw` (`FLOAT64`), `potencia_neta_mw` (`FLOAT64`), `capacidad_almacenamiento_mwh` (`FLOAT64`), `horas_autonomia_bess` (`FLOAT64`), `empresa_propietaria` (`STRING`), `rut_propietario` (`STRING`), `codigo_region` (`STRING`), `cut_comuna` (`STRING`), `nombre_comuna` (`STRING`), `latitud` (`FLOAT64`), `longitud` (`FLOAT64`), `geometria_punto` (`GEOGRAPHY`), `estado_operativo` (`STRING`), `fecha_puesta_servicio` (`DATE`), `fecha_actualizacion` (`DATE`), `fuente_oficial` (`STRING`).
2. **Tabla 2: `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_generacion_electrica_sen_horaria`**
   - **Fuente:** Operación Real / Generación Eléctrica SEN y Factores de Emisión — Coordinador Eléctrico Nacional y CNE.
   - **Columnas Exactas:**
     - `fecha_operacion` (`DATE NOT NULL`), `hora_del_dia` (`INT64`), `id_instalacion_sen` (`STRING`), `nombre_central` (`STRING`), `tecnologia` (`STRING`), `clasificacion_ernc` (`STRING`), `codigo_region` (`STRING`), `cut_comuna` (`STRING`), `generacion_mwh` (`FLOAT64`), `vertimiento_ernc_mwh` (`FLOAT64`), `factor_planta_pct` (`FLOAT64`), `factor_emision_tco2_mwh` (`FLOAT64`), `emisiones_totales_tco2` (`FLOAT64`), `fuente_oficial` (`STRING`), `fecha_ingesta_utc` (`TIMESTAMP`).

### Hito EN2: `fact_costos_marginales_nodos_sen` + `fact_tarifas_electricas_clientes_regulados_cne`
3. **Tabla 3: `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_costos_marginales_nodos_sen`**
   - **Fuente:** Costos Marginales Reales por Barra de Transferencia (`Crucero 220kV`, `Cardones 220kV`, `Pan de Azúcar 220kV`, `Quillota 220kV`, `Polpaico 220kV`, `Alto Jahuel 220kV`, `Charrúa 220kV`, `Puerto Montt 220kV`) — Coordinador Eléctrico Nacional.
   - **Columnas Exactas:**
     - `fecha` (`DATE NOT NULL`), `hora` (`INT64`), `barra_nodo_sen` (`STRING NOT NULL`), `tension_kv` (`FLOAT64`), `codigo_region` (`STRING`), `cut_comuna` (`STRING`), `costo_marginal_usd_mwh` (`FLOAT64`), `costo_marginal_clp_kwh` (`FLOAT64`), `dolar_observado_dia` (`FLOAT64`), `es_costo_marginal_cero_vertimiento` (`BOOL`), `desacople_norte_centro_usd_mwh` (`FLOAT64`), `fuente_oficial` (`STRING`), `fecha_ingesta_utc` (`TIMESTAMP`).
4. **Tabla 4: `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_tarifas_electricas_clientes_regulados_cne`**
   - **Fuente:** Decretos de Precios de Nudo Promedio (PNP), Pliegos Tarifarios BT1a y Subsidio Eléctrico — CNE y Ministerio de Energía.
   - **Columnas Exactas:**
     - `anio` (`INT64 NOT NULL`), `semestre` (`INT64`), `codigo_region` (`STRING`), `cut_comuna` (`STRING NOT NULL`), `empresa_distribuidora` (`STRING`), `codigo_tarifa` (`STRING`), `decreto_tarifario_minecon` (`STRING`), `fecha_vigencia_decreto` (`DATE`), `cargo_fijo_mensual_clp` (`FLOAT64`), `cargo_energia_kwh_clp` (`FLOAT64`), `cargo_energia_kwh_uf` (`FLOAT64`), `cargo_potencia_kw_clp` (`FLOAT64`), `cargo_mpc_estabilizacion_clp_kwh` (`FLOAT64`), `hogares_beneficiarios_subsidio_electrico` (`INT64`), `fuente_oficial` (`STRING`), `fecha_ingesta_utc` (`TIMESTAMP`).

### Hito EN3: `fact_combustibles_estaciones_en_linea_cne_gis`
5. **Tabla 5: `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_combustibles_estaciones_en_linea_cne_gis`**
   - **Fuente:** API CNE / Bencina en Línea (`https://api.cne.cl` / `https://www.bencinaenlinea.cl`) e Informe Semanal de Paridad ENAP (`https://www.enap.cl`).
   - **Columnas Exactas:**
     - `fecha_actualizacion` (`DATE NOT NULL`), `id_estacion_cne` (`STRING NOT NULL`), `distribuidora_marca` (`STRING`), `razon_social` (`STRING`), `codigo_region` (`STRING`), `cut_comuna` (`STRING`), `nombre_comuna` (`STRING`), `direccion_estacion` (`STRING`), `latitud` (`FLOAT64`), `longitud` (`FLOAT64`), `geometria_punto` (`GEOGRAPHY`), `tipo_combustible` (`STRING`), `tiene_autoservicio` (`BOOL`), `precio_litro_clp` (`FLOAT64`), `precio_litro_uf` (`FLOAT64`), `paridad_enap_refineria_clp` (`FLOAT64`), `impuesto_especifico_utm_m3` (`FLOAT64`), `margen_distribucion_bruto_clp` (`FLOAT64`), `fuente_oficial` (`STRING`), `fecha_ingesta_utc` (`TIMESTAMP`).

### Hito EN4: `fact_sec_interrupciones_suministro_saidi_saifi` + `fact_balance_nacional_energia_y_emisiones_gei`
6. **Tabla 6: `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_sec_interrupciones_suministro_saidi_saifi`**
   - **Fuente:** Indicadores de Calidad de Servicio Eléctrico SAIDI/SAIFI e Interrupciones por Comuna y Distribuidora — SEC (`https://www.sec.cl` y `http://energiaabierta.cl`).
   - **Columnas Exactas:**
     - `fecha_evento` (`DATE NOT NULL`), `anio` (`INT64`), `mes` (`INT64`), `codigo_region` (`STRING`), `cut_comuna` (`STRING NOT NULL`), `nombre_comuna` (`STRING`), `empresa_distribuidora` (`STRING`), `causa_interrupcion` (`STRING`), `clientes_totales_comuna` (`INT64`), `clientes_afectados_punta` (`INT64`), `porcentaje_clientes_sin_suministro` (`FLOAT64`), `saidi_horas_interrupcion` (`FLOAT64`), `saifi_frecuencia_interrupcion` (`FLOAT64`), `tiempo_medio_reposicion_horas` (`FLOAT64`), `multa_sec_utm` (`FLOAT64`), `fuente_oficial` (`STRING`), `fecha_ingesta_utc` (`TIMESTAMP`).
7. **Tabla 7: `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_balance_nacional_energia_y_emisiones_gei`**
   - **Fuente:** Balance Nacional de Energía (BNE) — Ministerio de Energía (`http://energiaabierta.cl`).
   - **Columnas Exactas:**
     - `anio` (`INT64 NOT NULL`), `codigo_region` (`STRING NOT NULL`), `nombre_region` (`STRING`), `sector_consumo` (`STRING`), `energetico` (`STRING`), `consumo_tcal` (`FLOAT64`), `consumo_gwh_equivalente` (`FLOAT64`), `participacion_matriz_regional_pct` (`FLOAT64`), `emisiones_ktco2eq` (`FLOAT64`), `fuente_oficial` (`STRING`), `fecha_ingesta_utc` (`TIMESTAMP`).

---

## Hito EN5: Verificación Obligatoria en BigQuery, Spiderweb, GitHub y Excel de Costos
1. Ejecutar query de auditoría en `core_energia_mercados` comprobando que las **7 tablas tengan `COUNT(*) > 0`**:
   ```sql
   SELECT table_name, SUM(total_rows) AS total_filas, ROUND(SUM(total_logical_bytes)/1048576, 2) AS logical_mb
   FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.INFORMATION_SCHEMA.PARTITIONS`
   GROUP BY table_name
   ORDER BY total_filas DESC;
   ```
2. Probar `SELECT * FROM \`project-0ab8074c-14aa-434e-849.gold_nacional.vw_energia_costos_marginales_y_generacion\` LIMIT 10` y `vw_energia_precios_combustibles_y_tarifas_comunal`.
3. Ejecutar `python "g:\Mi unidad\Datos Publicos\generar_reporte_costos_diario_gcp.py"` para actualizar `REPORTE_COSTOS_DIARIOS_GCP_Y_ALMACENAMIENTO.xlsx`.
4. Crear/actualizar el repositorio `by-seba1312/energia-cne-coordinador-sec-chile` y actualizar `cloud_run_spiderweb/main.py` en `by-seba1312/data-lakehouse-nacional-chile` dejando la frase oficial de trazabilidad.
