import json
from pathlib import Path

# ============================================
# PROCESADOR DE SCHEMAS AUTOMÁTICO
# ============================================

def procesar_columna(col_nombre: str, col_tipo: str) -> dict:
    """Clasifica una columna y extrae información útil"""
    col_lower = col_nombre.lower()
    
    # Eliminar prefijo de tabla si existe (ej: "Tabla[Columna]" -> "Columna")
    if "[" in col_nombre and "]" in col_nombre:
        col_limpia = col_nombre.split("[")[1].rstrip("]")
    else:
        col_limpia = col_nombre
    
    info = {
        "nombre": col_limpia,
        "nombre_completo": col_nombre,
        "tipo": col_tipo,
        "es_numerica": col_tipo in ["int64", "double", "decimal", "float"],
        "es_fecha": col_tipo in ["datetime", "date"],
        "es_id": any(k in col_lower for k in ["id", "key", "codigo", "code"]),
        "es_nombre_persona": False,
        "es_metrica": False,
        "prioridad": 0
    }
    
    # Detectar nombres de personas
    if any(k in col_lower for k in ["nombre", "name", "empleado", "apellido", "usuario", "user", "persona"]):
        if not info["es_id"]:
            info["es_nombre_persona"] = True
            info["prioridad"] = 10
    
    # Detectar métricas
    if info["es_numerica"] and not info["es_id"]:
        keywords_metrica = ["saldo", "total", "importe", "cantidad", "horas", "dias", "valor", "precio", "coste"]
        if any(k in col_lower for k in keywords_metrica):
            info["es_metrica"] = True
            info["prioridad"] = 8
    
    # Detectar columnas de fecha importantes
    if info["es_fecha"]:
        info["prioridad"] = 5
    
    return info


def clasificar_tabla(nombre_tabla: str, columnas: list) -> dict:
    """Clasifica el tipo de tabla y extrae información clave"""
    nombre_lower = nombre_tabla.lower()
    
    clasificacion = {
        "nombre": nombre_tabla,
        "tipo": "other",
        "columnas_procesadas": [],
        "columnas_nombres_persona": [],
        "columnas_metricas": [],
        "columnas_fecha": [],
        "mejor_columna_nombre": None,
        "mejor_metrica": None
    }
    
    # Clasificar tipo de tabla
    if "fact" in nombre_lower or "fact_" in nombre_lower:
        clasificacion["tipo"] = "fact"
    elif any(k in nombre_lower for k in ["dim", "dimension", "stg"]):
        if any(k in nombre_lower for k in ["personal", "user", "empleado", "rrhh"]):
            clasificacion["tipo"] = "dim_personas"
        else:
            clasificacion["tipo"] = "dim"
    elif any(k in nombre_lower for k in ["fecha", "date", "calendar", "calendario"]):
        if "local" not in nombre_lower:  # Ignorar LocalDateTable
            clasificacion["tipo"] = "fecha"
    elif "local" in nombre_lower or "_medidas" in nombre_lower:
        clasificacion["tipo"] = "ignorar"
    
    # Procesar columnas
    for col in columnas:
        col_info = procesar_columna(col["nombre"], col["tipo"])
        clasificacion["columnas_procesadas"].append(col_info)
        
        if col_info["es_nombre_persona"]:
            clasificacion["columnas_nombres_persona"].append(col_info)
        
        if col_info["es_metrica"]:
            clasificacion["columnas_metricas"].append(col_info)
        
        if col_info["es_fecha"]:
            clasificacion["columnas_fecha"].append(col_info)
    
    # Seleccionar mejor columna de nombre
    if clasificacion["columnas_nombres_persona"]:
        mejor = max(clasificacion["columnas_nombres_persona"], key=lambda x: x["prioridad"])
        clasificacion["mejor_columna_nombre"] = mejor["nombre"]
    
    # Seleccionar mejor métrica
    if clasificacion["columnas_metricas"]:
        mejor = max(clasificacion["columnas_metricas"], key=lambda x: x["prioridad"])
        clasificacion["mejor_metrica"] = mejor["nombre"]
    
    return clasificacion


def procesar_cm(cm_name: str, cm_data: dict) -> dict:
    """Procesa un cuadro de mando completo"""
    print(f"\n📊 Procesando: {cm_name}")
    
    resultado = {
        "nombre": cm_name,
        "tablas_fact": [],
        "tablas_dim_personas": [],
        "tablas_fecha": [],
        "medidas": cm_data.get("medidas", []),
        "tabla_principal": None,
        "tabla_personas": None,
        "mapeo_columnas": {}
    }
    
    # Clasificar todas las tablas
    for tabla in cm_data.get("tablas", []):
        clasificacion = clasificar_tabla(tabla["nombre"], tabla["columnas"])
        
        if clasificacion["tipo"] == "ignorar":
            continue
        
        # Guardar según tipo
        if clasificacion["tipo"] == "fact":
            resultado["tablas_fact"].append(clasificacion)
        elif clasificacion["tipo"] == "dim_personas":
            resultado["tablas_dim_personas"].append(clasificacion)
        elif clasificacion["tipo"] == "fecha":
            resultado["tablas_fecha"].append(clasificacion)
        
        # Guardar mapeo de columnas importantes
        resultado["mapeo_columnas"][tabla["nombre"]] = {
            "mejor_nombre": clasificacion["mejor_columna_nombre"],
            "mejor_metrica": clasificacion["mejor_metrica"],
            "columnas_numericas": [c["nombre"] for c in clasificacion["columnas_metricas"]],
            "columnas_nombres": [c["nombre"] for c in clasificacion["columnas_nombres_persona"]]
        }
    
    # Seleccionar tabla principal (primera FACT)
    if resultado["tablas_fact"]:
        resultado["tabla_principal"] = resultado["tablas_fact"][0]["nombre"]
    
    # Seleccionar tabla de personas (primera DIM de personas)
    if resultado["tablas_dim_personas"]:
        resultado["tabla_personas"] = resultado["tablas_dim_personas"][0]["nombre"]
    
    print(f"   ✅ FACT: {len(resultado['tablas_fact'])}")
    print(f"   ✅ DIM Personas: {len(resultado['tablas_dim_personas'])}")
    print(f"   ✅ Medidas: {len(resultado['medidas'])}")
    
    return resultado


def generar_schemas_py(schemas_procesados: dict, output_file: str = "schemas.py"):
    """Genera el archivo schemas.py con los metadatos"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('"""\n')
        f.write('Metadatos de Cuadros de Mando de Power BI\n')
        f.write('Generado automáticamente desde schemas_powerbi.json\n')
        f.write('"""\n\n')
        
        f.write('SCHEMAS = {\n')
        
        for cm_name, schema in schemas_procesados.items():
            f.write(f'    "{cm_name}": {{\n')
            f.write(f'        "tabla_principal": "{schema["tabla_principal"]}",\n')
            f.write(f'        "tabla_personas": "{schema["tabla_personas"]}",\n')
            
            # Tablas FACT
            f.write('        "tablas_fact": [\n')
            for fact in schema["tablas_fact"]:
                f.write(f'            "{fact["nombre"]}",\n')
            f.write('        ],\n')
            
            # Mapeo de columnas
            f.write('        "mapeo_columnas": {\n')
            for tabla_nombre, mapeo in schema["mapeo_columnas"].items():
                if mapeo["mejor_nombre"] or mapeo["mejor_metrica"]:
                    f.write(f'            "{tabla_nombre}": {{\n')
                    if mapeo["mejor_nombre"]:
                        f.write(f'                "columna_nombre_persona": "{mapeo["mejor_nombre"]}",\n')
                    if mapeo["mejor_metrica"]:
                        f.write(f'                "metrica_principal": "{mapeo["mejor_metrica"]}",\n')
                    if mapeo["columnas_numericas"]:
                        f.write(f'                "columnas_numericas": {mapeo["columnas_numericas"][:3]},\n')
                    f.write('            },\n')
            f.write('        },\n')
            
            # Medidas principales (top 5)
            f.write('        "medidas_principales": [\n')
            for medida in schema["medidas"][:5]:
                f.write(f'            "{medida["nombre"]}",\n')
            f.write('        ],\n')
            
            f.write('    },\n')
        
        f.write('}\n\n')
        
        # Función helper
        f.write('''
def get_schema(cm_name: str) -> dict:
    """Obtiene el schema de un CM específico"""
    return SCHEMAS.get(cm_name, {})


def get_columna_nombre_persona(cm_name: str) -> tuple[str, str] | None:
    """Obtiene (tabla, columna) para nombres de persona en un CM"""
    schema = get_schema(cm_name)
    tabla_personas = schema.get("tabla_personas")
    
    if tabla_personas:
        mapeo = schema.get("mapeo_columnas", {}).get(tabla_personas, {})
        col_nombre = mapeo.get("columna_nombre_persona")
        if col_nombre:
            return (tabla_personas, col_nombre)
    
    return None


def get_metrica_principal(cm_name: str) -> tuple[str, str] | None:
    """Obtiene (tabla, columna) para la métrica principal de un CM"""
    schema = get_schema(cm_name)
    tabla_principal = schema.get("tabla_principal")
    
    if tabla_principal:
        mapeo = schema.get("mapeo_columnas", {}).get(tabla_principal, {})
        metrica = mapeo.get("metrica_principal")
        if metrica:
            return (tabla_principal, metrica)
    
    return None
''')
    
    print(f"\n✅ Archivo generado: {output_file}")


def main():
    print("\n" + "="*60)
    print("🔧 PROCESADOR DE SCHEMAS DE POWER BI")
    print("="*60)
    
    # Leer JSON
    json_file = "schemas_powerbi.json"
    print(f"\n📖 Leyendo: {json_file}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        schemas_raw = json.load(f)
    
    print(f"✅ {len(schemas_raw)} CMs encontrados")
    
    # Procesar cada CM
    schemas_procesados = {}
    for cm_name, cm_data in schemas_raw.items():
        try:
            schema_procesado = procesar_cm(cm_name, cm_data)
            schemas_procesados[cm_name] = schema_procesado
        except Exception as e:
            print(f"❌ Error procesando {cm_name}: {e}")
            continue
    
    # Generar schemas.py
    print(f"\n{'='*60}")
    print("💾 GENERANDO ARCHIVO SCHEMAS.PY")
    print('='*60)
    
    generar_schemas_py(schemas_procesados)
    
    # Resumen
    print(f"\n{'='*60}")
    print("📊 RESUMEN")
    print('='*60)
    print(f"CMs procesados: {len(schemas_procesados)}")
    
    total_fact = sum(len(s["tablas_fact"]) for s in schemas_procesados.values())
    total_personas = sum(len(s["tablas_dim_personas"]) for s in schemas_procesados.values())
    
    print(f"Total tablas FACT: {total_fact}")
    print(f"Total tablas DIM Personas: {total_personas}")
    
    print(f"\n✅ COMPLETADO")
    print(f"Usa 'schemas.py' en tu analyzer para acceder a metadatos optimizados\n")


if __name__ == "__main__":
    main()