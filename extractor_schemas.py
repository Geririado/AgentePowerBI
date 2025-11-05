import os
import sys
import clr
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ============================================
# CONFIGURACIÓN
# ============================================
load_dotenv()

WORKSPACE_URL = "powerbi://api.powerbi.com/v1.0/myorg/Femxa_Aciturri_BI"
EXCEL_PATH = "LISTADO_CM_INICIALES_ASISTENTE_18_09_25.xlsx"
OUTPUT_JSON = "schemas_powerbi.json"

# Cargar ADOMD.NET
dll_versions = ["160", "190", "200"]
found = False
for ver in dll_versions:
    dll_path = Path(fr"C:\Program Files\Microsoft.NET\ADOMD.NET\{ver}")
    dll_file = dll_path / "Microsoft.AnalysisServices.AdomdClient.dll"
    if dll_file.exists():
        os.environ["PATH"] += os.pathsep + str(dll_path)
        sys.path.append(str(dll_path))
        clr.AddReference(str(dll_file))
        found = True
        print(f"✅ ADOMD.NET v{ver} cargado correctamente.\n")
        break

if not found:
    raise ImportError("❌ No se encontró ADOMD.NET")

from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand


# ============================================
# FUNCIONES DE EXTRACCIÓN
# ============================================

def extraer_schema_completo(dataset_name: str) -> dict:
    """
    Extrae toda la información del modelo de Power BI:
    - Tablas y columnas con tipos
    - Relaciones
    - Medidas
    - 5 valores de ejemplo por columna
    """
    print(f"\n{'='*60}")
    print(f"📊 EXTRAYENDO: {dataset_name}")
    print('='*60)
    
    connection_string = (
        f"Provider=MSOLAP;"
        f"Data Source={WORKSPACE_URL};"
        f"Initial Catalog={dataset_name};"
        f"Integrated Security=ClaimsToken;"
    )
    
    try:
        conn = AdomdConnection(connection_string)
        conn.Open()
        print("✅ Conexión establecida\n")
        
        # === 1️⃣ MAPEO DE TABLAS ===
        print("📋 Obteniendo tablas...")
        q_tables = "SELECT [ID], [Name] FROM $SYSTEM.TMSCHEMA_TABLES"
        reader = AdomdCommand(q_tables, conn).ExecuteReader()
        tables_map = {}
        tables_info = []
        while reader.Read():
            table_id = str(reader.GetValue(0))
            table_name = reader.GetValue(1)
            tables_map[table_id] = table_name
            tables_info.append({
                "id": table_id,
                "nombre": table_name
            })
        reader.Close()
        print(f"   ✅ {len(tables_info)} tablas encontradas")
        
        # === 2️⃣ COLUMNAS CON TIPOS ===
        print("📋 Obteniendo columnas...")
        # Usar método alternativo: extraer columnas directamente de las tablas
        columns_by_table = {}
        
        for table_name in tables_map.values():
            try:
                # Query simple para obtener estructura
                query = f"EVALUATE TOPN(1, '{table_name}')"
                cmd = AdomdCommand(query, conn)
                reader = cmd.ExecuteReader()
                
                # Obtener nombres de columnas y tipos del resultado
                if table_name not in columns_by_table:
                    columns_by_table[table_name] = []
                
                for i in range(reader.FieldCount):
                    col_name = reader.GetName(i)
                    col_type_obj = reader.GetFieldType(i)
                    col_type = str(col_type_obj.Name).lower() if col_type_obj else "unknown"
                    
                    columns_by_table[table_name].append({
                        "nombre": col_name,
                        "tipo": col_type
                    })
                
                reader.Close()
                
            except Exception as e:
                print(f"   ⚠️ Error obteniendo columnas de {table_name}: {str(e)[:50]}")
                continue
        
        print(f"   ✅ Columnas extraídas de {len(columns_by_table)} tablas")
        
        # === 3️⃣ RELACIONES ===
        print("🔗 Obteniendo relaciones...")
        
        try:
            q_rels = """
            SELECT [FromTableID], [FromColumnID], [ToTableID], [ToColumnID], [IsActive]
            FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS
            """
            reader = AdomdCommand(q_rels, conn).ExecuteReader()
            
            # Primero construir mapeo de columnas con método alternativo
            columns_map = {}
            for table_name, cols_list in columns_by_table.items():
                for col_info in cols_list:
                    # Crear key aproximado (no tenemos IDs, usamos nombres)
                    key = f"{table_name}_{col_info['nombre']}"
                    columns_map[key] = {
                        "table": table_name,
                        "column": col_info["nombre"]
                    }
            
            relaciones = []
            while reader.Read():
                from_table_id = str(reader.GetValue(0))
                from_col_id = str(reader.GetValue(1))
                to_table_id = str(reader.GetValue(2))
                to_col_id = str(reader.GetValue(3))
                is_active = reader.GetValue(4)
                
                # Intentar resolver nombres
                from_table = tables_map.get(from_table_id, "Unknown")
                to_table = tables_map.get(to_table_id, "Unknown")
                
                relaciones.append({
                    "desde_tabla": from_table,
                    "desde_columna": "columna_desde",  # Placeholder
                    "hacia_tabla": to_table,
                    "hacia_columna": "columna_hacia",  # Placeholder
                    "activa": is_active,
                    "from_col_id": from_col_id,
                    "to_col_id": to_col_id
                })
            reader.Close()
            print(f"   ✅ {len(relaciones)} relaciones encontradas")
            
        except Exception as e:
            print(f"   ⚠️ No se pudieron extraer relaciones: {str(e)[:50]}")
            relaciones = []
        
        # === 4️⃣ MEDIDAS ===
        print("📐 Obteniendo medidas...")
        try:
            q_measures = "SELECT [TableID], [Name], [Expression] FROM $SYSTEM.TMSCHEMA_MEASURES"
            reader = AdomdCommand(q_measures, conn).ExecuteReader()
            medidas = []
            while reader.Read():
                table_id = str(reader.GetValue(0))
                medidas.append({
                    "tabla": tables_map.get(table_id, "Unknown"),
                    "nombre": reader.GetValue(1),
                    "expresion": reader.GetValue(2)
                })
            reader.Close()
            print(f"   ✅ {len(medidas)} medidas encontradas")
        except Exception as e:
            print(f"   ⚠️ No se pudieron extraer medidas: {str(e)[:50]}")
            medidas = []
        
        # === 5️⃣ DATOS DE EJEMPLO (5 filas por tabla) ===
        print("📊 Extrayendo datos de ejemplo...")
        datos_ejemplo = {}
        
        for table_name in list(tables_map.values())[:20]:  # Limitar a 20 tablas para no tardar mucho
            try:
                query = f"EVALUATE TOPN(5, '{table_name}')"
                cmd = AdomdCommand(query, conn)
                reader = cmd.ExecuteReader()
                
                cols = [reader.GetName(i) for i in range(reader.FieldCount)]
                rows = []
                while reader.Read():
                    row = {}
                    for i in range(reader.FieldCount):
                        val = reader.GetValue(i)
                        # Convertir tipos especiales a string
                        if val is not None:
                            if hasattr(val, '__class__') and 'DateTime' in val.__class__.__name__:
                                val = str(val)
                            elif isinstance(val, (int, float, str, bool)):
                                pass
                            else:
                                val = str(val)
                        row[cols[i]] = val
                    rows.append(row)
                reader.Close()
                
                datos_ejemplo[table_name] = rows
                print(f"   ✅ {table_name}: {len(rows)} filas")
                
            except Exception as e:
                print(f"   ⚠️ {table_name}: Error - {str(e)[:50]}")
                continue
        
        conn.Close()
        print("\n✅ Conexión cerrada\n")
        
        # === 6️⃣ CONSTRUIR SCHEMA COMPLETO ===
        schema = {
            "dataset": dataset_name,
            "tablas": [],
            "relaciones": relaciones,
            "medidas": medidas
        }
        
        for table_name in columns_by_table.keys():
            tabla_info = {
                "nombre": table_name,
                "columnas": columns_by_table[table_name],
                "datos_ejemplo": datos_ejemplo.get(table_name, [])
            }
            schema["tablas"].append(tabla_info)
        
        return schema
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        return None


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    print("\n" + "="*60)
    print("🚀 EXTRACTOR DE SCHEMAS DE POWER BI")
    print("="*60 + "\n")
    
    # Leer el Excel con los nombres de los CMs
    print(f"📖 Leyendo Excel: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH, header=1)
    
    # Buscar columna de nombres de CM
    col_nombre = None
    for col in df.columns:
        if "nombre" in str(col).lower() and "cm" in str(col).lower():
            col_nombre = col
            break
    
    if not col_nombre:
        print("❌ No se encontró columna con nombres de CM")
        return
    
    # Obtener lista de CMs únicos
    cms = df[col_nombre].dropna().unique().tolist()
    print(f"✅ {len(cms)} Cuadros de Mando encontrados\n")
    
    # Extraer schemas de todos los CMs
    schemas_completos = {}
    errores = []
    
    for i, cm_name in enumerate(cms, 1):
        print(f"\n[{i}/{len(cms)}] Procesando: {cm_name}")
        schema = extraer_schema_completo(cm_name)
        
        if schema:
            schemas_completos[cm_name] = schema
            print(f"✅ Schema extraído correctamente")
        else:
            errores.append(cm_name)
            print(f"❌ Error extrayendo schema")
    
    # Guardar resultado en JSON
    print(f"\n{'='*60}")
    print("💾 GUARDANDO RESULTADOS")
    print('='*60)
    
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(schemas_completos, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Schemas guardados en: {OUTPUT_JSON}")
    print(f"📊 CMs procesados exitosamente: {len(schemas_completos)}")
    print(f"❌ CMs con errores: {len(errores)}")
    
    if errores:
        print(f"\nCMs con errores:")
        for cm in errores:
            print(f"  - {cm}")
    
    # Mostrar resumen en pantalla
    print(f"\n{'='*60}")
    print("📋 RESUMEN DE SCHEMAS EXTRAÍDOS")
    print('='*60 + "\n")
    
    for cm_name, schema in schemas_completos.items():
        print(f"\n📊 {cm_name}")
        print(f"   Tablas: {len(schema['tablas'])}")
        print(f"   Relaciones: {len(schema['relaciones'])}")
        print(f"   Medidas: {len(schema['medidas'])}")
        
        # Mostrar primeras 3 tablas con sus columnas
        print(f"\n   Primeras tablas:")
        for tabla in schema['tablas'][:3]:
            print(f"      • {tabla['nombre']} ({len(tabla['columnas'])} columnas)")
            for col in tabla['columnas'][:5]:
                print(f"        - {col['nombre']} ({col['tipo']})")
            if len(tabla['columnas']) > 5:
                print(f"        ... y {len(tabla['columnas']) - 5} columnas más")
            
            # Mostrar datos de ejemplo
            if tabla['datos_ejemplo']:
                print(f"        Ejemplo (primeras filas):")
                for row in tabla['datos_ejemplo'][:2]:
                    print(f"          {list(row.keys())[:3]}: {list(row.values())[:3]}")
    
    print(f"\n{'='*60}")
    print("✅ EXTRACCIÓN COMPLETADA")
    print(f"{'='*60}\n")
    print(f"Archivo generado: {OUTPUT_JSON}")
    print(f"Ahora puedes usar este JSON para mejorar el analyzer.py\n")


if __name__ == "__main__":
    main()