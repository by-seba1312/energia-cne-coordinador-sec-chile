from google.cloud import bigquery
import json

client = bigquery.Client(project='project-0ab8074c-14aa-434e-849')
query = """
SELECT cut_region, nombre_region, cut_comuna, nombre_comuna
FROM `project-0ab8074c-14aa-434e-849.ine_master_classifiers.dim_cut_vigente`
WHERE es_vigente = TRUE
ORDER BY cut_comuna
"""
df = client.query(query).to_dataframe()
cut_map = {}
for _, row in df.iterrows():
    cut_map[row['nombre_comuna'].strip().lower()] = {
        'cut_comuna': str(row['cut_comuna']),
        'cut_region': str(row['cut_region']),
        'nombre_comuna': row['nombre_comuna'],
        'nombre_region': row['nombre_region']
    }

with open('g:/Mi unidad/Datos Publicos/02_Energia_CNE_Coordinador_SEC/scripts/cut_comunas_map.json', 'w', encoding='utf-8') as f:
    json.dump(cut_map, f, ensure_ascii=False, indent=2)

print(f"Mapped {len(cut_map)} communes successfully.")
