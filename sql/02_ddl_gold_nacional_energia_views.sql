-- ==============================================================================
-- PROYECTO 2: VISTAS ANALÍTICAS GOLD DE ENERGÍA DE CHILE (MINISTERIO, CEN, CNE, SEC)
-- Dataset: project-0ab8074c-14aa-434e-849.gold_nacional (southamerica-west1)
-- ==============================================================================

-- 1. Vista Operacional: Costos Marginales Nodales y Generación Eléctrica SEN Horaria
CREATE OR REPLACE VIEW `project-0ab8074c-14aa-434e-849.gold_nacional.vw_energia_costos_marginales_y_generacion` AS
WITH gen_resumen AS (
  SELECT
    fecha_operacion,
    hora_del_dia,
    codigo_region,
    cut_comuna,
    COUNT(DISTINCT id_instalacion_sen) AS total_centrales_operando,
    ROUND(SUM(generacion_mwh), 2) AS generacion_total_mwh,
    ROUND(SUM(IF(clasificacion_ernc = 'ERNC', generacion_mwh, 0)), 2) AS generacion_ernc_mwh,
    ROUND(SUM(vertimiento_ernc_mwh), 2) AS vertimiento_ernc_total_mwh,
    ROUND(SUM(emisiones_totales_tco2), 2) AS emisiones_totales_tco2
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_generacion_electrica_sen_horaria`
  GROUP BY fecha_operacion, hora_del_dia, codigo_region, cut_comuna
),
costos_resumen AS (
  SELECT
    fecha,
    hora,
    codigo_region,
    cut_comuna,
    barra_nodo_sen,
    costo_marginal_usd_mwh,
    costo_marginal_clp_kwh,
    dolar_observado_dia,
    es_costo_marginal_cero_vertimiento,
    desacople_norte_centro_usd_mwh
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_costos_marginales_nodos_sen`
)
SELECT
  COALESCE(c.fecha, g.fecha_operacion) AS fecha,
  COALESCE(c.hora, g.hora_del_dia) AS hora,
  COALESCE(c.codigo_region, g.codigo_region) AS codigo_region,
  COALESCE(c.cut_comuna, g.cut_comuna) AS cut_comuna,
  COALESCE(c.barra_nodo_sen, 'NODO_REGIONAL_SEN') AS barra_nodo_sen,
  c.costo_marginal_usd_mwh,
  c.costo_marginal_clp_kwh,
  c.dolar_observado_dia,
  COALESCE(c.es_costo_marginal_cero_vertimiento, FALSE) AS es_costo_marginal_cero_vertimiento,
  COALESCE(c.desacople_norte_centro_usd_mwh, 0.0) AS desacople_norte_centro_usd_mwh,
  COALESCE(g.total_centrales_operando, 0) AS total_centrales_operando,
  COALESCE(g.generacion_total_mwh, 0.0) AS generacion_total_mwh,
  COALESCE(g.generacion_ernc_mwh, 0.0) AS generacion_ernc_mwh,
  COALESCE(g.vertimiento_ernc_total_mwh, 0.0) AS vertimiento_ernc_total_mwh,
  COALESCE(g.emisiones_totales_tco2, 0.0) AS emisiones_totales_tco2
FROM costos_resumen c
FULL OUTER JOIN gen_resumen g
  ON c.fecha = g.fecha_operacion 
 AND c.hora = g.hora_del_dia 
 AND c.codigo_region = g.codigo_region 
 AND c.cut_comuna = g.cut_comuna;

-- 2. Vista Territorial: Precios de Combustibles Líquidos y Tarifas Reguladas por Comuna
CREATE OR REPLACE VIEW `project-0ab8074c-14aa-434e-849.gold_nacional.vw_energia_precios_combustibles_y_tarifas_comunal` AS
WITH comb_comunal AS (
  SELECT
    cut_comuna,
    COUNT(DISTINCT id_estacion_cne) AS total_estaciones_cne,
    ROUND(AVG(IF(tipo_combustible = 'GASOLINA_93', precio_litro_clp, NULL)), 1) AS precio_promedio_gasolina93_clp,
    ROUND(AVG(IF(tipo_combustible = 'GASOLINA_95', precio_litro_clp, NULL)), 1) AS precio_promedio_gasolina95_clp,
    ROUND(AVG(IF(tipo_combustible = 'GASOLINA_97', precio_litro_clp, NULL)), 1) AS precio_promedio_gasolina97_clp,
    ROUND(AVG(IF(tipo_combustible = 'PETROLEO_DIESEL', precio_litro_clp, NULL)), 1) AS precio_promedio_diesel_clp,
    ROUND(AVG(IF(tipo_combustible = 'KEROSENE_PARAFINA', precio_litro_clp, NULL)), 1) AS precio_promedio_parafina_clp
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_combustibles_estaciones_en_linea_cne_gis`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
tarifas_comunal AS (
  SELECT
    cut_comuna,
    MAX(empresa_distribuidora) AS distribuidora_principal,
    ROUND(AVG(cargo_fijo_mensual_clp), 1) AS cargo_fijo_mensual_clp,
    ROUND(AVG(cargo_energia_kwh_clp), 2) AS tarifa_residencial_bt1_clp_kwh,
    ROUND(AVG(cargo_energia_kwh_uf), 6) AS tarifa_residencial_bt1_uf_kwh,
    ROUND(AVG(cargo_mpc_estabilizacion_clp_kwh), 2) AS cargo_mpc_estabilizacion_clp_kwh,
    SUM(hogares_beneficiarios_subsidio_electrico) AS hogares_beneficiarios_subsidio_electrico
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_tarifas_electricas_clientes_regulados_cne`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
calidad_sec AS (
  SELECT
    cut_comuna,
    ROUND(AVG(saidi_horas_interrupcion), 2) AS saidi_horas_promedio_sec,
    ROUND(AVG(saifi_frecuencia_interrupcion), 2) AS saifi_frecuencia_promedio_sec,
    SUM(clientes_afectados_punta) AS clientes_afectados_punta_acumulados
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_sec_interrupciones_suministro_saidi_saifi`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
demografia AS (
  SELECT
    cut_comuna,
    nombre_comuna,
    nombre_region,
    poblacion_2024,
    viviendas_2024
  FROM `project-0ab8074c-14aa-434e-849.ine_gold_core.fct_cruce_multidimensional_comunal`
)
SELECT
  d.cut_comuna,
  d.nombre_comuna,
  d.nombre_region,
  d.poblacion_2024,
  d.viviendas_2024,
  COALESCE(t.distribuidora_principal, 'POR_ASIGNAR_CNE') AS distribuidora_principal,
  COALESCE(t.cargo_fijo_mensual_clp, 0.0) AS cargo_fijo_mensual_clp,
  COALESCE(t.tarifa_residencial_bt1_clp_kwh, 0.0) AS tarifa_residencial_bt1_clp_kwh,
  COALESCE(t.tarifa_residencial_bt1_uf_kwh, 0.0) AS tarifa_residencial_bt1_uf_kwh,
  COALESCE(t.cargo_mpc_estabilizacion_clp_kwh, 0.0) AS cargo_mpc_estabilizacion_clp_kwh,
  COALESCE(t.hogares_beneficiarios_subsidio_electrico, 0) AS hogares_beneficiarios_subsidio_electrico,
  COALESCE(q.saidi_horas_promedio_sec, 0.0) AS saidi_horas_promedio_sec,
  COALESCE(q.saifi_frecuencia_promedio_sec, 0.0) AS saifi_frecuencia_promedio_sec,
  COALESCE(q.clientes_afectados_punta_acumulados, 0) AS clientes_afectados_punta_acumulados,
  COALESCE(c.total_estaciones_cne, 0) AS total_estaciones_cne,
  c.precio_promedio_gasolina93_clp,
  c.precio_promedio_gasolina95_clp,
  c.precio_promedio_gasolina97_clp,
  c.precio_promedio_diesel_clp,
  c.precio_promedio_parafina_clp,
  CASE
    WHEN COALESCE(q.saidi_horas_promedio_sec, 0) > 20.0 AND COALESCE(t.tarifa_residencial_bt1_clp_kwh, 0) > 180 THEN 'ALTA ELECTRO-VULNERABILIDAD Y TARIFA CRÍTICA'
    WHEN COALESCE(c.total_estaciones_cne, 0) = 0 THEN 'DESIERTO DE COMBUSTIBLES LÍQUIDOS'
    WHEN COALESCE(t.hogares_beneficiarios_subsidio_electrico, 0) > 5000 THEN 'ZONA DE ALTO SUBSIDIO ELÉCTRICO RESIDENCIAL'
    ELSE 'CONDICIONES ENERGÉTICAS ESTÁNDAR'
  END AS clasificacion_perfil_energetico_comunal
FROM demografia d
LEFT JOIN comb_comunal c ON d.cut_comuna = c.cut_comuna
LEFT JOIN tarifas_comunal t ON d.cut_comuna = t.cut_comuna
LEFT JOIN calidad_sec q ON d.cut_comuna = q.cut_comuna;

-- 3. Súper-Vista Gold Transversal: Matriz Energética ERNC e Inversión Comunal
CREATE OR REPLACE VIEW `project-0ab8074c-14aa-434e-849.gold_nacional.v_matriz_energetica_ernc_e_inversion_comunal` AS
WITH demografia AS (
  SELECT
    cut_comuna,
    nombre_comuna,
    nombre_region,
    poblacion_2024,
    viviendas_2024
  FROM `project-0ab8074c-14aa-434e-849.ine_gold_core.fct_cruce_multidimensional_comunal`
),
centrales_sen AS (
  SELECT
    cut_comuna,
    COUNT(*) AS total_centrales_sen,
    ROUND(SUM(COALESCE(potencia_neta_mw, 0)), 2) AS potencia_instalada_total_mw,
    ROUND(SUM(IF(clasificacion_ernc = 'ERNC', COALESCE(potencia_neta_mw, 0), 0)), 2) AS potencia_ernc_mw,
    ROUND(SUM(COALESCE(capacidad_almacenamiento_mwh, 0)), 2) AS capacidad_bess_mwh
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.dim_centrales_generacion_y_subestaciones_gis`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
costos_marginales AS (
  SELECT
    cut_comuna,
    ROUND(AVG(costo_marginal_usd_mwh), 2) AS costo_marginal_promedio_usd_mwh,
    COUNTIF(es_costo_marginal_cero_vertimiento = TRUE) AS horas_costo_marginal_cero
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_costos_marginales_nodos_sen`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
seia_energia AS (
  SELECT
    cut_comuna,
    COUNT(*) AS proyectos_seia_energia,
    ROUND(SUM(COALESCE(monto_inversion_musd, 0)), 2) AS inversion_seia_energia_musd
  FROM `project-0ab8074c-14aa-434e-849.core_geologia_mineria.fact_seia_proyectos_inversion_gis`
  WHERE sector_productivo IN ('ENERGIA', 'ENERGIA_RENOVABLE', 'Energía', 'Energía y Minería') AND cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
dga_agua AS (
  SELECT
    cut_comuna,
    COUNT(*) AS derechos_agua_totales,
    ROUND(SUM(COALESCE(caudal_concedido_lps, 0)), 2) AS caudal_concedido_lps
  FROM `project-0ab8074c-14aa-434e-849.core_geologia_mineria.fact_dga_derechos_agua_gis`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
sii_energia AS (
  SELECT
    cut_comuna,
    SUM(COALESCE(numero_empresas, 0)) AS empresas_energia_sii_seccion_d,
    ROUND(SUM(COALESCE(ventas_anuales_uf, 0)), 2) AS ventas_energia_sii_uf,
    SUM(COALESCE(trabajadores_dependientes, 0)) AS trabajadores_energia_sii
  FROM `project-0ab8074c-14aa-434e-849.core_economia_empresas.sii_tejido_empresarial_comunal`
  WHERE seccion_ciiu4 = 'D' AND anio_comercial = 2023
  GROUP BY cut_comuna
),
oc_energia AS (
  SELECT
    cut_comuna_comprador AS cut_comuna,
    COUNT(DISTINCT codigo_oc) AS ordenes_compra_energia_combustibles,
    ROUND(SUM(COALESCE(monto_neto_item_uf, 0)), 2) AS gasto_publico_energia_combustibles_uf
  FROM `project-0ab8074c-14aa-434e-849.core_hacienda_macro.fact_compras_publicas_oc`
  WHERE rubro_onu_nivel1 IN (
    'Artículos eléctricos y de iluminación',
    'Combustibles, lubricantes y anticorrosivos',
    'Maquinaria para generación y distribución de energía'
  ) AND cut_comuna_comprador IS NOT NULL
  GROUP BY cut_comuna_comprador
),
estaciones_clima AS (
  SELECT
    LPAD(CAST(cut_comuna AS STRING), 5, '0') AS cut_comuna,
    COUNT(*) AS estaciones_agromet_radiacion
  FROM `project-0ab8074c-14aa-434e-849.agro_chile.estaciones_agromet`
  WHERE cut_comuna IS NOT NULL
  GROUP BY 1
)
SELECT
  d.cut_comuna,
  d.nombre_comuna,
  d.nombre_region,
  d.poblacion_2024,
  d.viviendas_2024,
  COALESCE(c.total_centrales_sen, 0) AS total_centrales_sen,
  COALESCE(c.potencia_instalada_total_mw, 0.0) AS potencia_instalada_total_mw,
  COALESCE(c.potencia_ernc_mw, 0.0) AS potencia_ernc_mw,
  COALESCE(c.capacidad_bess_mwh, 0.0) AS capacidad_bess_mwh,
  COALESCE(cm.costo_marginal_promedio_usd_mwh, 0.0) AS costo_marginal_promedio_usd_mwh,
  COALESCE(cm.horas_costo_marginal_cero, 0) AS horas_costo_marginal_cero,
  COALESCE(se.proyectos_seia_energia, 0) AS proyectos_seia_energia,
  COALESCE(se.inversion_seia_energia_musd, 0.0) AS inversion_seia_energia_musd,
  COALESCE(w.derechos_agua_totales, 0) AS derechos_agua_totales,
  COALESCE(w.caudal_concedido_lps, 0.0) AS caudal_concedido_lps,
  COALESCE(s.empresas_energia_sii_seccion_d, 0) AS empresas_energia_sii_seccion_d,
  COALESCE(s.ventas_energia_sii_uf, 0.0) AS ventas_energia_sii_uf,
  COALESCE(s.trabajadores_energia_sii, 0) AS trabajadores_energia_sii,
  COALESCE(oc.ordenes_compra_energia_combustibles, 0) AS ordenes_compra_energia_combustibles,
  COALESCE(oc.gasto_publico_energia_combustibles_uf, 0.0) AS gasto_publico_energia_combustibles_uf,
  COALESCE(ec.estaciones_agromet_radiacion, 0) AS estaciones_agromet_radiacion,
  CASE
    WHEN COALESCE(c.potencia_ernc_mw, 0) > 100 OR COALESCE(se.inversion_seia_energia_musd, 0) > 100 THEN 'POLO ESTRATÉGICO DE GENERACIÓN E INVERSIÓN ERNC'
    WHEN COALESCE(s.ventas_energia_sii_uf, 0) > 500000 THEN 'HUB ENERGÉTICO Y DE DISTRIBUCIÓN ELÉCTRICA'
    WHEN COALESCE(s.empresas_energia_sii_seccion_d, 0) > 0 OR COALESCE(oc.gasto_publico_energia_combustibles_uf, 0) > 10000 THEN 'ACTIVIDAD ENERGÉTICA COMUNAL ACTIVA'
    ELSE 'DEMANDA RESIDENCIAL / POTENCIAL DISTRIBUIDO PMGD'
  END AS vocacion_energetica_comunal
FROM demografia d
LEFT JOIN centrales_sen c ON d.cut_comuna = c.cut_comuna
LEFT JOIN costos_marginales cm ON d.cut_comuna = cm.cut_comuna
LEFT JOIN seia_energia se ON d.cut_comuna = se.cut_comuna
LEFT JOIN dga_agua w ON d.cut_comuna = w.cut_comuna
LEFT JOIN sii_energia s ON d.cut_comuna = s.cut_comuna
LEFT JOIN oc_energia oc ON d.cut_comuna = oc.cut_comuna
LEFT JOIN estaciones_clima ec ON d.cut_comuna = ec.cut_comuna;

-- 4. Súper-Vista Gold Transversal: Pobreza Energética, Combustibles y Resiliencia SEC
CREATE OR REPLACE VIEW `project-0ab8074c-14aa-434e-849.gold_nacional.v_pobreza_energetica_combustibles_y_resiliencia_sec` AS
WITH demografia AS (
  SELECT
    cut_comuna,
    nombre_comuna,
    nombre_region,
    poblacion_2024,
    viviendas_2024,
    tasa_hacinamiento_2024_pct,
    ingreso_medio_persona_esi
  FROM `project-0ab8074c-14aa-434e-849.ine_gold_core.fct_cruce_multidimensional_comunal`
),
sec_calidad AS (
  SELECT
    cut_comuna,
    MAX(empresa_distribuidora) AS distribuidora_principal,
    ROUND(AVG(saidi_horas_interrupcion), 2) AS saidi_horas_sin_luz_anual,
    ROUND(AVG(saifi_frecuencia_interrupcion), 2) AS saifi_cortes_promedio,
    SUM(clientes_afectados_punta) AS clientes_afectados_acumulados
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_sec_interrupciones_suministro_saidi_saifi`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
tarifas AS (
  SELECT
    cut_comuna,
    ROUND(AVG(cargo_energia_kwh_clp), 2) AS tarifa_residencial_bt1_clp_kwh,
    SUM(hogares_beneficiarios_subsidio_electrico) AS hogares_subsidio_electrico
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_tarifas_electricas_clientes_regulados_cne`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
combustibles AS (
  SELECT
    cut_comuna,
    COUNT(DISTINCT id_estacion_cne) AS estaciones_bencina_cne,
    ROUND(AVG(IF(tipo_combustible = 'GASOLINA_93', precio_litro_clp, NULL)), 1) AS precio_promedio_gasolina93_clp,
    ROUND(AVG(IF(tipo_combustible = 'PETROLEO_DIESEL', precio_litro_clp, NULL)), 1) AS precio_promedio_diesel_clp,
    ROUND(AVG(IF(tipo_combustible = 'KEROSENE_PARAFINA', precio_litro_clp, NULL)), 1) AS precio_promedio_parafina_clp
  FROM `project-0ab8074c-14aa-434e-849.core_energia_mercados.fact_combustibles_estaciones_en_linea_cne_gis`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
reclamos_servicios AS (
  SELECT
    UPPER(TRIM(comuna_consumidor)) AS comuna_upper,
    COUNT(*) AS reclamos_sernac_servicios_basicos_y_combustible
  FROM `project-0ab8074c-14aa-434e-849.core_economia_empresas.sernac_reclamos_mercado`
  WHERE nombre_mercado IN ('SERVICIOS BASICOS', 'COMBUSTIBLE') AND comuna_consumidor IS NOT NULL
  GROUP BY 1
),
amenazas_climaticas AS (
  SELECT
    cut_comuna,
    COUNT(*) AS alertas_senapred_historicas
  FROM `project-0ab8074c-14aa-434e-849.core_riesgos_emergencias.fact_alertas_senapred`
  WHERE cut_comuna IS NOT NULL
  GROUP BY cut_comuna
),
macro_energia AS (
  SELECT
    valor_uf AS uf_vigente,
    dolar_observado AS dolar_observado_vigente,
    COALESCE(petroleo_wti_usd, 70.0) AS petroleo_wti_usd_vigente
  FROM `project-0ab8074c-14aa-434e-849.core_hacienda_macro.dim_calendario_financiero`
  WHERE valor_uf IS NOT NULL
  ORDER BY fecha DESC
  LIMIT 1
)
SELECT
  d.cut_comuna,
  d.nombre_comuna,
  d.nombre_region,
  d.poblacion_2024,
  d.viviendas_2024,
  ROUND(d.ingreso_medio_persona_esi, 0) AS ingreso_medio_persona_esi_clp,
  COALESCE(s.distribuidora_principal, 'POR_ASIGNAR_SEC') AS distribuidora_principal,
  COALESCE(s.saidi_horas_sin_luz_anual, 0.0) AS saidi_horas_sin_luz_anual,
  COALESCE(s.saifi_cortes_promedio, 0.0) AS saifi_cortes_promedio,
  COALESCE(t.tarifa_residencial_bt1_clp_kwh, 0.0) AS tarifa_residencial_bt1_clp_kwh,
  COALESCE(t.hogares_subsidio_electrico, 0) AS hogares_subsidio_electrico,
  COALESCE(cb.estaciones_bencina_cne, 0) AS estaciones_bencina_cne,
  cb.precio_promedio_gasolina93_clp,
  cb.precio_promedio_diesel_clp,
  cb.precio_promedio_parafina_clp,
  COALESCE(rs.reclamos_sernac_servicios_basicos_y_combustible, 0) AS reclamos_sernac_servicios_basicos_y_combustible,
  COALESCE(ac.alertas_senapred_historicas, 0) AS alertas_senapred_historicas,
  m.uf_vigente,
  m.dolar_observado_vigente,
  m.petroleo_wti_usd_vigente,
  CASE
    WHEN d.ingreso_medio_persona_esi < 450000 AND COALESCE(s.saidi_horas_sin_luz_anual, 0) > 20 THEN 'POBREZA ENERGÉTICA Y ELECTRO-VULNERABILIDAD CRÍTICA'
    WHEN COALESCE(rs.reclamos_sernac_servicios_basicos_y_combustible, 0) > 250 THEN 'ALTA CONFLICTIVIDAD TARIFARIA Y DE SUMINISTRO'
    WHEN d.ingreso_medio_persona_esi < 480000 THEN 'ZONA PRIORITARIA SUBSIDIO ELÉCTRICO Y CALEFACCIÓN LIMPIA'
    ELSE 'RESILIENCIA ENERGÉTICA ESTÁNDAR'
  END AS clasificacion_vulnerabilidad_energetica
FROM demografia d
LEFT JOIN sec_calidad s ON d.cut_comuna = s.cut_comuna
LEFT JOIN tarifas t ON d.cut_comuna = t.cut_comuna
LEFT JOIN combustibles cb ON d.cut_comuna = cb.cut_comuna
LEFT JOIN reclamos_servicios rs ON UPPER(TRIM(d.nombre_comuna)) = rs.comuna_upper
LEFT JOIN amenazas_climaticas ac ON d.cut_comuna = ac.cut_comuna
CROSS JOIN macro_energia m;
