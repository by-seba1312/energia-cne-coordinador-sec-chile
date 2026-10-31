-- ==============================================================================
-- PROYECTO 2: DATA LAKEHOUSE SECTORIAL DE ENERGÍA DE CHILE (MINISTERIO, CEN, CNE, SEC, ENAP)
-- Dataset: project-0ab8074c-14aa-434e-849.core_energia_mercados (southamerica-west1)
-- Modo de almacenamiento: PHYSICAL ($0,00 USD / mes bajo cuota gratuita de 10 GiB)
-- ==============================================================================

-- 0. Configurar facturación física comprimida
ALTER SCHEMA `project-0ab8074c-14aa-434e-849.core_energia_mercados` 
SET OPTIONS(storage_billing_model = 'PHYSICAL');

-- 1. Dimensión de Centrales de Generación, Subestaciones y BESS
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.dim_centrales_generacion_y_subestaciones_gis` (
  id_instalacion_sen STRING NOT NULL,
  nombre_instalacion STRING,
  tipo_instalacion STRING,
  tecnologia STRING,
  clasificacion_ernc STRING,
  subsistema_electrico STRING,
  barra_subestacion_conexion STRING,
  tension_kv FLOAT64,
  potencia_bruta_mw FLOAT64,
  potencia_neta_mw FLOAT64,
  capacidad_almacenamiento_mwh FLOAT64,
  horas_autonomia_bess FLOAT64,
  empresa_propietaria STRING,
  rut_propietario STRING,
  codigo_region STRING,
  cut_comuna STRING,
  nombre_comuna STRING,
  latitud FLOAT64,
  longitud FLOAT64,
  geometria_punto GEOGRAPHY,
  estado_operativo STRING,
  fecha_puesta_servicio DATE,
  fecha_actualizacion DATE,
  fuente_oficial STRING
)
CLUSTER BY cut_comuna, tecnologia, estado_operativo;

-- 2. Hechos de Generación Eléctrica SEN Horaria y Emisiones
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_generacion_electrica_sen_horaria` (
  fecha_operacion DATE NOT NULL,
  hora_del_dia INT64,
  id_instalacion_sen STRING,
  nombre_central STRING,
  tecnologia STRING,
  clasificacion_ernc STRING,
  codigo_region STRING,
  cut_comuna STRING,
  generacion_mwh FLOAT64,
  vertimiento_ernc_mwh FLOAT64,
  factor_planta_pct FLOAT64,
  factor_emision_tco2_mwh FLOAT64,
  emisiones_totales_tco2 FLOAT64,
  fuente_oficial STRING,
  fecha_ingesta_utc TIMESTAMP
)
PARTITION BY fecha_operacion
CLUSTER BY cut_comuna, tecnologia, id_instalacion_sen;

-- 3. Hechos de Costos Marginales Horarios en Nodos SEN
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_costos_marginales_nodos_sen` (
  fecha DATE NOT NULL,
  hora INT64,
  barra_nodo_sen STRING NOT NULL,
  tension_kv FLOAT64,
  codigo_region STRING,
  cut_comuna STRING,
  costo_marginal_usd_mwh FLOAT64,
  costo_marginal_clp_kwh FLOAT64,
  dolar_observado_dia FLOAT64,
  es_costo_marginal_cero_vertimiento BOOL,
  desacople_norte_centro_usd_mwh FLOAT64,
  fuente_oficial STRING,
  fecha_ingesta_utc TIMESTAMP
)
PARTITION BY fecha
CLUSTER BY barra_nodo_sen, cut_comuna;

-- 4. Hechos de Tarifas Eléctricas Reguladas BT1a y Subsidio CNE
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_tarifas_electricas_clientes_regulados_cne` (
  anio INT64 NOT NULL,
  semestre INT64,
  codigo_region STRING,
  cut_comuna STRING NOT NULL,
  empresa_distribuidora STRING,
  codigo_tarifa STRING,
  decreto_tarifario_minecon STRING,
  fecha_vigencia_decreto DATE,
  cargo_fijo_mensual_clp FLOAT64,
  cargo_energia_kwh_clp FLOAT64,
  cargo_energia_kwh_uf FLOAT64,
  cargo_potencia_kw_clp FLOAT64,
  cargo_mpc_estabilizacion_clp_kwh FLOAT64,
  hogares_beneficiarios_subsidio_electrico INT64,
  fuente_oficial STRING,
  fecha_ingesta_utc TIMESTAMP
)
CLUSTER BY cut_comuna, empresa_distribuidora;

-- 5. Hechos de Estaciones de Combustibles Líquidos en Línea CNE / ENAP GIS
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_combustibles_estaciones_en_linea_cne_gis` (
  fecha_actualizacion DATE NOT NULL,
  id_estacion_cne STRING NOT NULL,
  distribuidora_marca STRING,
  razon_social STRING,
  codigo_region STRING,
  cut_comuna STRING,
  nombre_comuna STRING,
  direccion_estacion STRING,
  latitud FLOAT64,
  longitud FLOAT64,
  geometria_punto GEOGRAPHY,
  tipo_combustible STRING,
  tiene_autoservicio BOOL,
  precio_litro_clp FLOAT64,
  precio_litro_uf FLOAT64,
  paridad_enap_refineria_clp FLOAT64,
  impuesto_especifico_utm_m3 FLOAT64,
  margen_distribucion_bruto_clp FLOAT64,
  fuente_oficial STRING,
  fecha_ingesta_utc TIMESTAMP
)
PARTITION BY fecha_actualizacion
CLUSTER BY cut_comuna, distribuidora_marca, tipo_combustible;

-- 6. Hechos de Calidad de Suministro Eléctrico SAIDI/SAIFI SEC
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_sec_interrupciones_suministro_saidi_saifi` (
  fecha_evento DATE NOT NULL,
  anio INT64,
  mes INT64,
  codigo_region STRING,
  cut_comuna STRING NOT NULL,
  nombre_comuna STRING,
  empresa_distribuidora STRING,
  causa_interrupcion STRING,
  clientes_totales_comuna INT64,
  clientes_afectados_punta INT64,
  porcentaje_clientes_sin_suministro FLOAT64,
  saidi_horas_interrupcion FLOAT64,
  saifi_frecuencia_interrupcion FLOAT64,
  tiempo_medio_reposicion_horas FLOAT64,
  multa_sec_utm FLOAT64,
  fuente_oficial STRING,
  fecha_ingesta_utc TIMESTAMP
)
PARTITION BY fecha_evento
CLUSTER BY cut_comuna, empresa_distribuidora;

-- 7. Hechos de Balance Nacional de Energía (BNE) y Emisiones GEI
CREATE TABLE IF NOT EXISTS `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_balance_nacional_energia_y_emisiones_gei` (
  anio INT64 NOT NULL,
  codigo_region STRING NOT NULL,
  nombre_region STRING,
  sector_consumo STRING,
  energetico STRING,
  consumo_tcal FLOAT64,
  consumo_gwh_equivalente FLOAT64,
  participacion_matriz_regional_pct FLOAT64,
  emisiones_ktco2eq FLOAT64,
  fuente_oficial STRING,
  fecha_ingesta_utc TIMESTAMP
)
CLUSTER BY anio, codigo_region, sector_consumo;
