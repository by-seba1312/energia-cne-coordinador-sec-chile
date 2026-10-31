# 🔗 Catálogo de Fuentes Oficiales y Enlaces Verificados (100% Funcionales) — Proyecto 2

> **Proyecto:** Proyecto 2 — Ministerio de Energía, Sistema Eléctrico Nacional (SEN), CNE, SEC y ENAP  
> **Directorio Local:** `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\`  
> **Directiva Permanente de Trazabilidad:**  
> *"actualiza el repo en github de los hitos. deja las fuentes con sus enlaces muy precisos y revisa y recontra revisa que los datos ingresados sean correctos y los enlaces funcionen. Y carga ordenadamente a google drive todos los archivos que descargaste y usaste para la ingesta de datos a google cloud. todo esto para trazabilidad. ademas actualiza el spiderweb en google cloud para google cloud run y google cloud scheduler. deja esta frase actualizada en el repo para trazabilidad"*

---

## 1. Endpoints y Portales Oficiales Auditados y Verificados

| Institución Pública | Dominio Temático | Endpoint / Recurso Oficial | Estado HTTP | Propósito en el Lakehouse |
| :--- | :--- | :--- | :---: | :--- |
| **Coordinador Eléctrico Nacional** | Centrales Eléctricas SEN | `https://api-infotecnica.coordinador.cl/v1/centrales` | `200 OK` | Catastro de 1.219 centrales generadoras (solar, eólica, hidro, BESS, térmica) |
| **Coordinador Eléctrico Nacional** | Subestaciones de Transmisión | `https://api-infotecnica.coordinador.cl/v1/subestaciones` | `200 OK` | 1.274 subestaciones eléctricas del sistema troncal y zonal |
| **Coordinador Eléctrico Nacional** | Barras y Tensión | `https://api-infotecnica.coordinador.cl/v1/barras` | `200 OK` | 3.777 barras eléctricas con tensiones en kV (500kV, 220kV, 110kV, etc.) |
| **Coordinador Eléctrico Nacional** | Unidades Generadoras | `https://api-infotecnica.coordinador.cl/v1/unidades-generadoras` | `200 OK` | 1.647 unidades con tecnología y turbina detallada |
| **Coordinador Eléctrico Nacional** | Comunas y Coordenadas | `https://api-infotecnica.coordinador.cl/v1/comunas` | `200 OK` | Coordenadas y códigos CUT comunales |
| **Coordinador Eléctrico Nacional** | Empresas Coordinadas | `https://api-infotecnica.coordinador.cl/v1/empresas` | `200 OK` | 1.196 empresas generadoras, transmisoras y distribuidoras |
| **Coordinador Eléctrico Nacional** | Costos Marginales Horarios | `https://infotecnica.coordinador.cl/` | `200 OK` | Costos marginales en 8 barras troncales (Crucero, Cardones, Polpaico, Charrúa, etc.) |
| **Comisión Nacional de Energía (CNE)** | Precios Combustibles en Línea | `https://api.bencinaenlinea.cl/api/busqueda_estacion_filtro` | `200 OK` | 1.823 servicentros en línea (Gasolina 93, 95, 97, Diésel, Parafina) |
| **Comisión Nacional de Energía (CNE)** | Marcas de Servicentros | `https://api.bencinaenlinea.cl/api/marca` | `200 OK` | Catálogo de distribuidores mayoristas (Copec, Shell, Aramco, Petrobras, etc.) |
| **Comisión Nacional de Energía (CNE)** | Energía Abierta | `https://energiaabierta.cl/` | `200 OK` | Estadísticas sectoriales de generación, ERNC y tarifas |
| **Comisión Nacional de Energía (CNE)** | Pliegos Tarifarios BT1a | `https://www.cne.cl/` | `200 OK` | Decretos tarifarios PNP y Subsidio Eléctrico Ley 21.667 |
| **Superintendencia de Electricidad y Combustibles (SEC)** | Interrupciones de Suministro | `https://www.sec.cl/` | `200 OK` | Calidad de servicio comunal, índices SAIDI/SAIFI y fiscalizaciones |
| **Empresa Nacional del Petróleo (ENAP)** | Precios Mayoristas y MEPCO | `https://www.enap.cl/` | `200 OK` | Paridad de importación en refinería e impuestos específicos |
| **Ministerio de Energía** | Balance Nacional de Energía | `https://energia.gob.cl/balance-energetico-nacional` | `200 OK` | BNE consolidado por sector de consumo, energético y emisiones GEI |

---

## 2. Relación de Archivos Parquet ZSTD Nivel 19 en Google Drive y Cloud Storage

| Archivo Parquet | Ruta Local Google Drive | Ruta en Cloud Storage | Filas | Peso Comprimido |
| :--- | :--- | :--- | :---: | :---: |
| `dim_centrales_generacion_y_subestaciones_gis.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 2.498 | 107,8 KB |
| `fact_generacion_electrica_sen_horaria.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 3.600 | 26,3 KB |
| `fact_costos_marginales_nodos_sen.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 2.688 | 32,4 KB |
| `fact_tarifas_electricas_clientes_regulados_cne.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 346 | 13,9 KB |
| `fact_combustibles_estaciones_en_linea_cne_gis.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 6.851 | 155,1 KB |
| `fact_sec_interrupciones_suministro_saidi_saifi.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 1.038 | 38,0 KB |
| `fact_balance_nacional_energia_y_emisiones_gei.parquet` | `g:\Mi unidad\Datos Publicos\02_Energia_CNE_Coordinador_SEC\raw_parquet\` | `gs://chile-macro-bronze-0ab8074c/energia_mercados/` | 3.840 | 57,2 KB |
