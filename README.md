# ⚡ Proyecto 2: Ministerio de Energía, Sistema Eléctrico Nacional (SEN), CNE, SEC y ENAP

> **Proyecto Google Cloud Platform:** `project-0ab8074c-14aa-434e-849`  
> **Región Soberana de Datos:** `southamerica-west1` (Santiago de Chile)  
> **Dataset Destino BigQuery:** `core_energia_mercados`  
> **Vistas Gold Asociadas:** `gold_nacional.vw_energia_costos_marginales_y_generacion`, `gold_nacional.vw_energia_precios_combustibles_y_tarifas_comunal`, `gold_nacional.v_matriz_energetica_ernc_e_inversion_comunal`, `gold_nacional.v_pobreza_energetica_combustibles_y_resiliencia_sec`  
> **Modo de Facturación de Almacenamiento:** `storage_billing_model = 'PHYSICAL'`  
> **Directorio Local Google Drive:** `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\`  
> **Bucket Cloud Storage (Bronze Parquet ZSTD):** `gs://chile-macro-bronze-0ab8074c/energia_mercados/`  

---

## 🏛️ Directiva Permanente de Trazabilidad Oficial

> *"actualiza el repo en github de los hitos. deja las fuentes con sus enlaces muy precisos y revisa y recontra revisa que los datos ingresados sean correctos y los enlaces funcionen. Y carga ordenadamente a google drive todos los archivos que descargaste y usaste para la ingesta de datos a google cloud. todo esto para trazabilidad. ademas actualiza el spiderweb en google cloud para google cloud run y google cloud scheduler. deja esta frase actualizada en el repo para trazabilidad"*

---

## 1. Resumen Ejecutivo y Resultados de Ingesta

El **Proyecto 2 (Energía, Electricidad y Combustibles)** ha completado al 100% sus hitos **EN1, EN2, EN3, EN4 y EN5**, poblando de forma idempotente (`MERGE INTO`), sin duplicación con otros datasets ministeriales y bajo modelo de compresión física Capacitor las 7 tablas del dataset `core_energia_mercados`:

| Tabla BigQuery (`core_energia_mercados`) | Total Filas | MB Lógicos | Estado Hito | Fuente Oficial Primaria Verificada |
| :--- | :---: | :---: | :---: | :--- |
| `fact_combustibles_estaciones_en_linea_cne_gis` | **6.851** | **1,96 MB** | ✅ EN3 | [CNE Bencina en Línea API](https://api.bencinaenlinea.cl/api/busqueda_estacion_filtro) e [Informe ENAP](https://www.enap.cl/) |
| `fact_balance_nacional_energia_y_emisiones_gei` | **3.840** | **0,63 MB** | ✅ EN4 | [Balance Nacional de Energía (BNE)](https://energiaabierta.cl/categorias-estadistica/balance-energetico/) y Ministerio de Energía |
| `fact_generacion_electrica_sen_horaria` | **3.600** | **0,71 MB** | ✅ EN1 | [Coordinador Eléctrico Nacional](https://infotecnica.coordinador.cl/) y [CNE Energía Abierta](https://energiaabierta.cl/) |
| `fact_costos_marginales_nodos_sen` | **2.688** | **0,41 MB** | ✅ EN2 | [Coordinador Eléctrico Nacional (Costos Marginales Horarios)](https://infotecnica.coordinador.cl/) |
| `dim_centrales_generacion_y_subestaciones_gis` | **2.498** | **0,89 MB** | ✅ EN1 | [API Infotécnica Coordinador Eléctrico Nacional](https://api-infotecnica.coordinador.cl/v1/centrales) |
| `fact_sec_interrupciones_suministro_saidi_saifi` | **1.038** | **0,25 MB** | ✅ EN4 | [Superintendencia de Electricidad y Combustibles (SEC)](https://www.sec.cl/) |
| `fact_tarifas_electricas_clientes_regulados_cne` | **346** | **0,08 MB** | ✅ EN2 | [Comisión Nacional de Energía (CNE Pliegos BT1a / Subsidio Eléctrico)](https://www.cne.cl/) |
| **TOTAL DATASET `core_energia_mercados`** | **20.861** | **4,93 MB** | **100% POBLADO** | **7 tablas particionadas y clusterizadas en modo PHYSICAL** |

---

## 2. Fuentes Oficiales y Enlaces Verificados

Todos los enlaces y endpoints han sido testeados con código HTTP 200 en vivo:

1. **Coordinador Eléctrico Nacional (CEN):**
   - Catálogo de Centrales Eléctricas: `https://api-infotecnica.coordinador.cl/v1/centrales`
   - Catálogo de Subestaciones de Transmisión: `https://api-infotecnica.coordinador.cl/v1/subestaciones`
   - Barras de Transferencia y Niveles de Tensión: `https://api-infotecnica.coordinador.cl/v1/barras`
   - Unidades Generadoras y Tecnologías: `https://api-infotecnica.coordinador.cl/v1/unidades-generadoras`
   - Portal Infotécnica Web: [https://infotecnica.coordinador.cl/](https://infotecnica.coordinador.cl/)
   - Portal Institucional CEN: [https://www.coordinador.cl/](https://www.coordinador.cl/)
2. **Comisión Nacional de Energía (CNE):**
   - API Bencina en Línea (Estaciones de Combustible y Precios): `https://api.bencinaenlinea.cl/api/busqueda_estacion_filtro`
   - API Bencina en Línea (Marcas de Distribución): `https://api.bencinaenlinea.cl/api/marca`
   - Portal Bencina en Línea: [https://www.bencinaenlinea.cl/](https://www.bencinaenlinea.cl/)
   - Portal Energía Abierta CNE: [https://energiaabierta.cl/](https://energiaabierta.cl/)
   - Portal Institucional CNE: [https://www.cne.cl/](https://www.cne.cl/)
3. **Superintendencia de Electricidad y Combustibles (SEC):**
   - Portal de Calidad de Suministro e Interrupciones: [https://www.sec.cl/](https://www.sec.cl/)
4. **Empresa Nacional del Petróleo (ENAP):**
   - Informes Semanales de Precios Mayoristas y MEPCO: [https://www.enap.cl/](https://www.enap.cl/)
5. **Ministerio de Energía:**
   - Balance Nacional de Energía (BNE): [https://energia.gob.cl/balance-energetico-nacional](https://energia.gob.cl/balance-energetico-nacional)

---

## 3. Arquitectura y Principios de Almacenamiento FinOps ($0 USD)

1. **Facturación Física Activada (`storage_billing_model = 'PHYSICAL'`):**
   - Ejecutado en BigQuery:
     ```sql
     ALTER SCHEMA `project-0ab8074c-14aa-434e-849.core_energia_mercados` SET OPTIONS(storage_billing_model = 'PHYSICAL');
     ```
   - Las 7 tablas aprovechan la compresión columnar Capacitor de BigQuery (~7,5x a 9,5x), manteniendo el almacenamiento total del dataset bajo los 0,65 MB físicos facturables, muy dentro del nivel gratuito permanente (10 GiB/mes = $0,00 USD).
2. **Cero Duplicación Inter-Dataset (Zero-Copy):**
   - El Dólar Observado, la UF y los precios internacionales de Petróleo WTI se leen por `JOIN` desde `chile_macro_silver.fact_bcch_observaciones_historicas` y `core_hacienda_macro.dim_calendario_financiero`.
   - Las poblaciones, viviendas y polígonos comunales se leen por `JOIN` desde `censo2024` e `ine_gold_core`.
3. **Respaldos Comprimidos en Parquet ZSTD Nivel 19:**
   - Todos los datos procesados e ingestados se respaldan en Google Drive (`g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\`) y en Cloud Storage (`gs://chile-macro-bronze-0ab8074c/energia_mercados/`).

---

## 4. Vistas Gold Verificadas en BigQuery (`gold_nacional`)

1. `gold_nacional.vw_energia_costos_marginales_y_generacion`:
   - Cruza la serie horaria de Costos Marginales del SEN (8 nodos troncales: Crucero, Cardones, Pan de Azúcar, Quillota, Polpaico, Alto Jahuel, Charrúa y Puerto Montt) con la Generación Horaria por Tecnología (Solar, Eólica, BESS, Hidro, Térmica) y el Vertimiento ERNC.
2. `gold_nacional.vw_energia_precios_combustibles_y_tarifas_comunal`:
   - Integra a nivel comunal (345 comunas) los precios promedio de Gasolina 93/95/97, Diésel y Parafina con las Tarifas Eléctricas BT1a, beneficiarios del Subsidio Eléctrico (Ley 21.667) y la calidad de servicio SEC (SAIDI/SAIFI).
3. `gold_nacional.v_matriz_energetica_ernc_e_inversion_comunal`:
   - Cruce multidimensional con proyectos de inversión SEIA, derechos de agua DGA y tejido empresarial de energía del SII.
4. `gold_nacional.v_pobreza_energetica_combustibles_y_resiliencia_sec`:
   - Clasificación de vulnerabilidad socio-energética territorial combinando ingresos ESI, horas de corte de luz SAIDI y conflictividad SERNAC.

---

## 5. Orquestación Periódica Spiderweb (`spiderweb-daily-sync`)

El servicio Cloud Run `spiderweb-daily-sync` en `southamerica-west1` y los jobs de Cloud Scheduler sincronizan periódicamente las tablas mediante el endpoint `/sync/energia`:
- **Diario (07:30 AM):** Actualización de Costos Marginales horarios del CEN y precios de combustibles en línea CNE.
- **Semanal (Lunes 03:00 AM):** Actualización de informes de paridad ENAP y pliegos tarifarios CNE.
