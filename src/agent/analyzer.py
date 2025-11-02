import os
import sys
import clr
import openai
import json
import pandas as pd
import re
import datetime
from pathlib import Path
from dotenv import load_dotenv
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

# ============================================
# CONFIGURACIÓN INICIAL
# ============================================
load_dotenv()

print("🔍 Intentando cargar schemas...")

# Importar schemas generados
try:
    import sys
    from pathlib import Path
    # Añadir el directorio actual al path si no está
    current_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    print(f"📁 Buscando schemas en: {current_dir}")
    
    from schemas import get_schema, get_columna_nombre_persona, get_metrica_principal, SCHEMAS
    SCHEMAS_DISPONIBLES = True
    print(f"✅ Schemas cargados correctamente: {len(SCHEMAS)} CMs disponibles")
except ImportError as e:
    print(f"⚠️ schemas.py no encontrado: {e}")
    print(f"   Buscando en: {sys.path}")
    SCHEMAS_DISPONIBLES = False
except Exception as e:
    print(f"⚠️ Error cargando schemas: {e}")
    import traceback
    traceback.print_exc()
    SCHEMAS_DISPONIBLES = False

openai.api_type = "azure"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai.api_version = "2024-06-01"
DEPLOYMENT_GPT4O = os.getenv("AZURE_OPENAI_DEPLOYMENT_GPT4O", "gpt-4o")

WORKSPACE_URL = "powerbi://api.powerbi.com/v1.0/myorg/Femxa_Aciturri_BI"

# ADOMD.NET
dll_path = Path(r"C:\Program Files\Microsoft.NET\ADOMD.NET\160")
os.environ["PATH"] += os.pathsep + str(dll_path)
sys.path.append(str(dll_path))
dll_file = dll_path / "Microsoft.AnalysisServices.AdomdClient.dll"
clr.AddReference(str(dll_file))

# ============================================
# CONSTANTES DE CONTEXTO FEMXA
# ============================================
FEMXA_CONTEXT = {
    "empresa": "FEMXA",
    "sector": "Formación profesional y capacitación",
    "actividades": [
        "Cursos de formación profesional",
        "Programas de capacitación",
        "Gestión de alumnos e instructores",
        "Seguimiento de asistencias y calificaciones",
        "Gestión de recursos humanos",
        "Control de vacaciones y horas lectivas"
    ],
    "terminologia": {
        "alumnos": ["estudiantes", "participantes", "matriculados"],
        "cursos": ["programas formativos", "acciones formativas", "módulos"],
        "instructores": ["docentes", "formadores", "profesorado"],
        "personal": ["empleados", "equipo", "plantilla"]
    }
}


# ============================================
# PATRONES DAX PRE-CONSTRUIDOS
# ============================================
DAX_PATTERNS = {
    "agregacion_simple": {
        "template": """EVALUATE
SUMMARIZECOLUMNS(
    {dimension_columns}{filters},
    "{metric_name}", {metric_expression}
)""",
        "keywords": ["total", "suma", "cuántos", "cantidad", "por", "cada"],
        "required": ["dimension_columns", "metric_expression"]
    },
    
    "filtro_temporal": {
        "template": """EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        {dimension_columns},
        "{metric_name}", {metric_expression}
    ){filters}
)""",
        "keywords": ["año", "mes", "2024", "2025", "este año", "en"],
        "required": ["metric_expression"]
    },
    
    "top_n": {
        "template": """EVALUATE
TOPN(
    {n},
    SUMMARIZECOLUMNS(
        {dimension_columns},
        "{metric_name}", {metric_expression}
    ),
    [{metric_name}], DESC
)""",
        "keywords": ["top", "primeros", "mayores", "principales", "mejores"],
        "required": ["dimension_columns", "metric_expression", "n"]
    },
    
    "conteo_simple": {
        "template": """EVALUATE
ROW(
    "Total", COUNTROWS('{table}')
)""",
        "keywords": ["cuántos hay", "total de registros", "número total"],
        "required": ["table"]
    },
    
    "conteo_con_filtro": {
        "template": """EVALUATE
ROW(
    "Total", CALCULATE(
        COUNTROWS('{table}'){filters}
    )
)""",
        "keywords": ["cuántos", "número de", "total"],
        "required": ["table"]
    },
    
    "valor_unico": {
        "template": """EVALUATE
ROW(
    "{metric_name}", {metric_expression}
)""",
        "keywords": ["cuál es", "dame el", "total general"],
        "required": ["metric_expression"]
    }
}


# ============================================
# UTILIDADES DE CONEXIÓN
# ============================================
def _to_json_safe(obj):
    """Convierte objetos Python a tipos serializables en JSON"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    try:
        import numpy as np
        if isinstance(obj, (np.int64, np.float64)):
            return obj.item()
    except ImportError:
        pass
    return str(obj)


def _open_conn(dataset_name: str) -> AdomdConnection:
    """Abre conexión a Power BI con ADOMD.NET"""

    cs = (
        f"Provider=MSOLAP;"
        f"Data Source={WORKSPACE_URL};"
        f"Initial Catalog={dataset_name};"
        f"Integrated Security=ClaimsToken;"
    )
    conn = AdomdConnection(cs)
    conn.Open()
    return conn


def _dmv_tables(conn) -> dict:
    """Obtiene mapeo de IDs a nombres de tablas"""
    q = "SELECT [ID],[Name] FROM $SYSTEM.TMSCHEMA_TABLES"
    r = AdomdCommand(q, conn).ExecuteReader()
    mapping = {}
    while r.Read():
        mapping[str(r.GetValue(0))] = r.GetValue(1)
    r.Close()
    return mapping


def _dmv_columns(conn, tables_map) -> list[dict]:
    """Obtiene todas las columnas del modelo con sus tipos"""
    q = "SELECT [TableID],[Name],[DataType] FROM $SYSTEM.TMSCHEMA_COLUMNS"
    r = AdomdCommand(q, conn).ExecuteReader()
    cols = []
    while r.Read():
        table_id = str(r.GetValue(0))
        cols.append({
            "tabla": tables_map.get(table_id, ""),
            "columna": r.GetValue(1),
            "tipo": str(r.GetValue(2)).lower()
        })
    r.Close()
    return cols


def _dmv_measures(conn, tables_map) -> list[dict]:
    """Obtiene todas las medidas del modelo"""
    q = "SELECT [TableID],[Name],[Expression] FROM $SYSTEM.TMSCHEMA_MEASURES"
    r = AdomdCommand(q, conn).ExecuteReader()
    measures = []
    while r.Read():
        measures.append({
            "tabla": tables_map.get(str(r.GetValue(0)), ""),
            "measure": r.GetValue(1),
            "expr": r.GetValue(2)
        })
    r.Close()
    return measures


# ============================================
# SELECCIÓN DE PATRÓN DAX
# ============================================
def _select_pattern(prompt: str) -> tuple[str, dict]:
    """Selecciona el patrón DAX más adecuado según la pregunta"""
    prompt_lower = prompt.lower()
    
    scores = {}
    for pattern_name, pattern_info in DAX_PATTERNS.items():
        score = sum(1 for kw in pattern_info["keywords"] if kw in prompt_lower)
        scores[pattern_name] = score
    
    # Si hay un claro ganador
    best_pattern = max(scores, key=scores.get)
    if scores[best_pattern] > 0:
        return best_pattern, DAX_PATTERNS[best_pattern]
    
    # Fallback: heurística simple
    if any(w in prompt_lower for w in ["cuántos hay", "número total"]):
        return "conteo_simple", DAX_PATTERNS["conteo_simple"]
    
    return "agregacion_simple", DAX_PATTERNS["agregacion_simple"]


# ============================================
# FILTRADO DE TABLAS IRRELEVANTES
# ============================================
def _filtrar_tablas_irrelevantes(contexto: list[dict]) -> list[dict]:
    """
    Filtra tablas generadas automáticamente por Power BI que no aportan valor al análisis
    """
    tablas_filtradas = []
    for t in contexto:
        nombre = t.get("nombre", "")
        # Excluir tablas autogeneradas por Power BI
        if nombre.startswith("LocalDateTable_") or \
           nombre.startswith("DateTableTemplate_") or \
           nombre == "_Medidas" or \
           nombre == "_medidas":
            continue
        tablas_filtradas.append(t)

    print(f"📊 Tablas filtradas: {len(contexto)} → {len(tablas_filtradas)} (eliminadas {len(contexto) - len(tablas_filtradas)} tablas irrelevantes)")
    return tablas_filtradas


# ============================================
# EXTRACCIÓN DE PARÁMETROS CON GPT
# ============================================
def _extract_query_parameters(prompt: str, ctx: dict, pattern_name: str) -> dict:
    """
    GPT identifica QUÉ componentes usar (tablas, columnas, medidas)
    pero NO genera código DAX directamente
    """

    dataset_name = ctx.get("cm_seleccionado", "")

    # 🆕 FILTRAR TABLAS IRRELEVANTES DEL CONTEXTO
    ctx_filtrado = ctx.copy()
    ctx_filtrado["contexto"] = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))

    # 🆕 USAR SCHEMAS SI ESTÁN DISPONIBLES
    usar_schema = SCHEMAS_DISPONIBLES and dataset_name
    dimension_sugerida = None
    metrica_sugerida = None

    if usar_schema:
        schema = get_schema(dataset_name)
        if schema:
            # Obtener columna de nombre de persona del schema
            persona_info = get_columna_nombre_persona(dataset_name)
            if persona_info:
                tabla_persona, col_persona = persona_info
                dimension_sugerida = f"'{tabla_persona}'[{col_persona}]"
                print(f"📌 Schema sugiere dimensión: {dimension_sugerida}")

            # Obtener métrica principal del schema
            metrica_info = get_metrica_principal(dataset_name)
            if metrica_info:
                tabla_metrica, col_metrica = metrica_info
                metrica_sugerida = f"SUM('{tabla_metrica}'[{col_metrica}])"
                print(f"📌 Schema sugiere métrica: {metrica_sugerida}")

    # Preparar contexto simplificado CON NOMBRES REALES DE COLUMNAS
    tablas_disponibles = []
    for t in ctx_filtrado["contexto"]:
        if not t.get("muestra") or len(t["muestra"]) == 0:
            continue

        # Obtener nombres reales de columnas y un ejemplo de valor
        cols_con_ejemplo = []
        for col_name, col_value in t["muestra"][0].items():
            ejemplo = str(col_value)[:50] if col_value is not None else "null"
            cols_con_ejemplo.append({
                "nombre": col_name,
                "ejemplo": ejemplo,
                "tipo": type(col_value).__name__
            })

        tablas_disponibles.append({
            "nombre": t["nombre"],
            "columnas": cols_con_ejemplo[:15],  # Limitar a 15 columnas
            "tipo": "fact" if "fact" in t["nombre"].lower() else "dimension"
        })
    
    # Obtener medidas del modelo (capturar errores silenciosamente)
    medidas_disponibles = []
    try:
        conn = _open_conn(ctx["cm_seleccionado"])
        try:
            tmap = _dmv_tables(conn)
            measures = _dmv_measures(conn, tmap)
            medidas_disponibles = [
                {"tabla": m['tabla'], "medida": m['measure']}
                for m in measures
            ]
        finally:
            conn.Close()
    except Exception as e:
        # Error 401 u otros problemas de autenticación no deben interrumpir el flujo
        error_msg = str(e)
        if "401" in error_msg or "No autorizado" in error_msg:
            print(f"⚠️ No se pudo acceder a medidas (problema de autenticación). Continuando sin medidas del modelo.")
        else:
            print(f"⚠️ No se pudieron obtener medidas: {e}")
    
    # 🆕 AÑADIR SUGERENCIAS DEL SCHEMA AL PROMPT
    sugerencias_schema = ""
    if usar_schema and (dimension_sugerida or metrica_sugerida):
        sugerencias_schema = f"""

SUGERENCIAS BASADAS EN EL SCHEMA DEL CM:
"""
        if dimension_sugerida:
            sugerencias_schema += f"- Para agrupar por persona, usa: {dimension_sugerida}\n"
        if metrica_sugerida:
            sugerencias_schema += f"- Para la métrica principal, usa: {metrica_sugerida}\n"
        sugerencias_schema += """
Estas son las columnas más adecuadas detectadas automáticamente. Úsalas a menos que la pregunta requiera explícitamente otras columnas.
"""
    
    prompt_gpt = f"""Eres un experto en modelos de datos de Power BI.

PREGUNTA DEL USUARIO:
"{prompt}"

TABLAS DISPONIBLES:
{json.dumps(tablas_disponibles, indent=2, ensure_ascii=False)}

MEDIDAS DISPONIBLES:
{json.dumps(medidas_disponibles, indent=2, ensure_ascii=False)}
{sugerencias_schema}
PATRÓN DE QUERY SELECCIONADO: {pattern_name}

Tu tarea es SOLO identificar los componentes necesarios. NO generes código DAX.

Tu tarea es SOLO identificar los componentes necesarios. NO generes código DAX.

Responde en JSON con este formato exacto:
{{
    "tabla_principal": "nombre de la tabla fact o dimension principal",
    "dimension_columns": ["'NombreTabla'[NombreColumna]"],
    "metric_expression": "SUM('NombreTabla'[NombreColumna])" o "[NombreMedida]",
    "metric_name": "Nombre descriptivo para el resultado",
    "filters": [
        {{"column": "'Tabla'[Columna]", "operator": "=", "value": 2024}}
    ],
    "n": 10
}}

REGLAS CRÍTICAS DE SINTAXIS:
1. USA EXACTAMENTE los nombres de columna que aparecen en el JSON de tablas disponibles. NO inventes nombres.
2. SINTAXIS CORRECTA: SUM('Tabla'[Columna]) ← comillas simples solo alrededor del nombre de tabla
3. SINTAXIS INCORRECTA: SUM('Tabla[Columna]') ← NO hagas esto
4. Las medidas no llevan comillas: [NombreMedida]
5. Para dimension_columns: "'Tabla'[Columna]" ← nota las comillas
6. Si preguntan por "empleado", "persona" o "nombre": 
   - NUNCA uses columnas con "Id", "ID", "Key" en el nombre
   - Busca columnas que contengan "nombre", "name", "user", "usuario", "apellido"
   - Prioriza columnas descriptivas sobre identificadores numéricos
7. Si mencionan año/mes/fecha, añade filtros apropiados
8. Para TOP N, identifica la columna de agrupación y el top number
9. VERIFICA que la columna existe en el JSON antes de usarla
10. Para agregaciones por persona/empleado:
    - Primero busca en tablas "DIM" o tablas que tengan "User", "Empleado", "RRHH"
    - Usa columnas descriptivas (nombre completo, apellidos) NO IDs

Ejemplos de metric_expression correctos:
- [Total Vacaciones]  (si existe la medida)
- SUM('FactVacaciones'[Dias])
- COUNT('FactHoras'[IdRegistro])
- AVERAGE('FactSalarios'[Importe])

Analiza y responde SOLO el JSON:"""
    
    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0,
            max_tokens=1000,
            messages=[
                {"role": "system", "content": "Eres un experto en esquemas de datos. Respondes SOLO JSON válido sin explicaciones."},
                {"role": "user", "content": prompt_gpt}
            ]
        )
        
        text = response["choices"][0]["message"]["content"].strip()
        
        # Extraer JSON del texto (puede venir con markdown)
        text = re.sub(r'```json\n?|```\n?', '', text)
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            params = json.loads(match.group(0))
        else:
            params = json.loads(text)
        
        # Validaciones básicas
        params.setdefault("dimension_columns", [])
        params.setdefault("metric_name", "Resultado")
        params.setdefault("filters", [])
        params.setdefault("n", 10)
        
        # ✅ VALIDAR QUE LAS COLUMNAS EXISTEN EN EL CONTEXTO
        params = _validate_columns(params, ctx)
        
        print(f"✅ Parámetros extraídos: {json.dumps(params, indent=2, ensure_ascii=False)}")
        return params
        
    except Exception as e:
        print(f"❌ Error extrayendo parámetros con GPT: {e}")
        return _fallback_parameters(prompt, ctx)


def _corregir_sintaxis_referencia(ref: str) -> str:
    """
    Corrige sintaxis de referencia DAX.

    Formatos incorrectos:
    - 'Tabla[Columna]'  → 'Tabla'[Columna]
    - Tabla[Columna]    → 'Tabla'[Columna]
    - 'Tabla'[Tabla[Columna]] → 'Tabla'[Columna]
    """
    # 1. Corregir: 'Tabla[Columna]' → 'Tabla'[Columna]
    match_incorrecto = re.match(r"'([^']+)\[([^\]]+)\]'", ref)
    if match_incorrecto:
        tabla, columna = match_incorrecto.groups()
        # Limpiar columna si tiene duplicación
        if "[" in columna:
            columna = columna.split("[")[-1].rstrip("]")
        return f"'{tabla}'[{columna}]"

    # 2. Corregir: Tabla[Columna] → 'Tabla'[Columna] (sin comillas)
    match_sin_comillas = re.match(r"^([A-Za-z_][A-Za-z0-9_\s]*)\[([^\]]+)\]$", ref)
    if match_sin_comillas and "'" not in ref:
        tabla, columna = match_sin_comillas.groups()
        tabla = tabla.strip()
        if "[" in columna:
            columna = columna.split("[")[-1].rstrip("]")
        return f"'{tabla}'[{columna}]"

    # 3. Ya tiene formato correcto: 'Tabla'[Columna]
    match_correcto = re.match(r"'([^']+)'\[([^\]]+)\]", ref)
    if match_correcto:
        tabla, columna = match_correcto.groups()
        # Limpiar si hay duplicación de tabla en columna
        if "[" in columna:
            columna = columna.split("[")[-1].rstrip("]")
            return f"'{tabla}'[{columna}]"
        return ref

    # 4. No pudo parsear, devolver original
    return ref


def _validate_columns(params: dict, ctx: dict) -> dict:
    """
    Valida que las columnas especificadas en los parámetros existan en el contexto.
    Si no existen, intenta encontrar columnas similares.
    """
    # Construir un mapa de todas las columnas disponibles (usar contexto filtrado)
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))
    columnas_disponibles = {}
    for t in ctx_filtrado:
        if t.get("muestra") and len(t["muestra"]) > 0:
            tabla_nombre = t["nombre"]
            cols = list(t["muestra"][0].keys())
            columnas_disponibles[tabla_nombre] = cols

    # Validar dimension_columns
    dimension_cols_validadas = []
    for col_ref in params.get("dimension_columns", []):
        # 🔧 CORRECCIÓN ROBUSTA DE SINTAXIS
        col_ref_corregido = _corregir_sintaxis_referencia(col_ref)
        if col_ref_corregido != col_ref:
            print(f"🔧 Corrigiendo sintaxis: {col_ref} → {col_ref_corregido}")
            col_ref = col_ref_corregido

        # Extraer tabla y columna
        match = re.match(r"'([^']+)'\[([^\]]+)\]", col_ref)
        if match:
            tabla, columna = match.groups()

            if tabla in columnas_disponibles:
                if columna in columnas_disponibles[tabla]:
                    dimension_cols_validadas.append(col_ref)
                else:
                    # Buscar columna similar
                    col_similar = _find_similar_column(columna, columnas_disponibles[tabla])
                    if col_similar:
                        print(f"⚠️ Corrigiendo columna: {columna} → {col_similar}")
                        dimension_cols_validadas.append(f"'{tabla}'[{col_similar}]")
                    else:
                        print(f"❌ Columna no encontrada: {col_ref}")
            else:
                print(f"❌ Tabla no encontrada: {tabla}")

    params["dimension_columns"] = dimension_cols_validadas
    
    # Validar metric_expression (si es una columna, no una medida)
    metric = params.get("metric_expression", "")
    
    # 🔧 CORRECCIÓN: Arreglar sintaxis incorrecta de GPT
    # Cambiar 'Tabla[Col]' a 'Tabla'[Col]
    if "(" in metric:  # Es una función como SUM, COUNT
        # Buscar patrón: SUM('Tabla[Columna]')
        pattern_incorrecto = r"(SUM|COUNT|AVERAGE)\('([^']+)\[([^\]]+)\]'\)"
        match = re.search(pattern_incorrecto, metric)
        if match:
            func, tabla, columna = match.groups()
            metric_correcto = f"{func}('{tabla}'[{columna}])"
            print(f"🔧 Corrigiendo sintaxis: {metric} → {metric_correcto}")
            params["metric_expression"] = metric_correcto
            metric = metric_correcto
    
    if "SUM(" in metric or "COUNT(" in metric or "AVERAGE(" in metric:
        match = re.search(r"'([^']+)'\[([^\]]+)\]", metric)
        if match:
            tabla, columna = match.groups()
            if tabla in columnas_disponibles:
                if columna not in columnas_disponibles[tabla]:
                    col_similar = _find_similar_column(columna, columnas_disponibles[tabla])
                    if col_similar:
                        print(f"⚠️ Corrigiendo columna en métrica: {columna} → {col_similar}")
                        # ✅ CORRECCIÓN: Solo reemplazar el nombre de columna dentro de los corchetes
                        func_match = re.match(r"(SUM|COUNT|AVERAGE)\('([^']+)'\[([^\]]+)\]\)", metric)
                        if func_match:
                            func, tabla_orig, _ = func_match.groups()
                            # Asegurarse de que col_similar no tiene prefijo de tabla
                            col_similar_limpia = col_similar.split("[")[-1].rstrip("]") if "[" in col_similar else col_similar
                            params["metric_expression"] = f"{func}('{tabla_orig}'[{col_similar_limpia}])"
                            print(f"✅ Métrica corregida: {params['metric_expression']}")
                else:
                    # ✅ VALIDAR TIPO DE DATO: No hacer SUM de fechas o strings
                    tabla_ctx = next((t for t in ctx["contexto"] if t["nombre"] == tabla), None)
                    if tabla_ctx and tabla_ctx.get("muestra"):
                        tipo_valor = type(tabla_ctx["muestra"][0].get(columna)).__name__
                        
                        # Si es fecha o string, cambiar a COUNT
                        if tipo_valor in ["str", "DateTime"] or "date" in columna.lower() or "fecha" in columna.lower():
                            print(f"⚠️ No se puede hacer SUM de {columna} (tipo: {tipo_valor}). Buscando columna numérica.")
                            
                            # Buscar una columna numérica en la misma tabla
                            col_numerica = None
                            for col, val in tabla_ctx["muestra"][0].items():
                                col_lower = col.lower()
                                # Buscar columnas numéricas relevantes (saldo, dias, horas, importe)
                                if isinstance(val, (int, float)) and not any(k in col_lower for k in ["id", "key", "codigo"]):
                                    # Priorizar columnas con keywords relevantes
                                    if any(kw in col_lower for kw in ["saldo", "dias", "horas", "importe", "total", "cantidad"]):
                                        col_numerica = col
                                        break
                            
                            if not col_numerica:
                                # Si no encontró con keywords, usar cualquier numérica
                                for col, val in tabla_ctx["muestra"][0].items():
                                    if isinstance(val, (int, float)) and "id" not in col.lower():
                                        col_numerica = col
                                        break
                            
                            if col_numerica:
                                params["metric_expression"] = f"SUM('{tabla}'[{col_numerica}])"
                                print(f"✅ Usando columna numérica: {col_numerica}")
                            else:
                                params["metric_expression"] = f"COUNTROWS('{tabla}')"
                                print(f"✅ Usando COUNTROWS en su lugar")
    
    return params


def _find_similar_column(target: str, available_cols: list) -> str | None:
    """
    Busca una columna similar usando heurísticas simples.
    Por ejemplo, si busca "Nombre" puede encontrar "US_nombre" o "nombreCompleto"
    
    IMPORTANTE: Retorna SOLO el nombre de columna, sin prefijos de tabla
    """
    target_lower = target.lower()
    
    # Limpiar nombres de columnas que puedan tener prefijo de tabla
    def limpiar_nombre(col):
        """Extrae solo el nombre de columna, sin prefijo de tabla"""
        if "[" in col and "]" in col:
            return col.split("[")[-1].rstrip("]")
        return col
    
    cols_limpias = [limpiar_nombre(col) for col in available_cols]
    
    # 1. Coincidencia exacta (case-insensitive)
    for col in cols_limpias:
        if col.lower() == target_lower:
            return col
    
    # 2. Contiene la palabra completa
    for col in cols_limpias:
        if target_lower in col.lower():
            return col
    
    # 3. La columna contiene la palabra buscada
    for col in cols_limpias:
        if target_lower in col.lower().split("_"):
            return col
    
    # 4. Heurísticas especiales para nombres comunes
    if target_lower in ["nombre", "name", "empleado", "persona", "usuario", "user"]:
        # Priorizar columnas descriptivas sobre IDs
        priority_keywords = ["nombre", "name", "apellido", "completo", "full"]
        for kw in priority_keywords:
            for col in cols_limpias:
                col_lower = col.lower()
                # Evitar IDs explícitamente
                if kw in col_lower and not any(x in col_lower for x in ["id", "key", "codigo", "code"]):
                    return col
        
        # Si no encontró con keywords priority, buscar cualquiera sin ID
        for col in cols_limpias:
            col_lower = col.lower()
            if any(k in col_lower for k in ["nombre", "name", "usuario", "user", "persona"]):
                if not any(x in col_lower for x in ["id", "key"]):
                    return col
    
    return None


def _fallback_parameters(prompt: str, ctx: dict) -> dict:
    """Extracción de parámetros con heurísticas si GPT falla"""
    print("⚠️ Usando extracción heurística de parámetros")

    dataset_name = ctx.get("cm_seleccionado", "")

    # Usar contexto filtrado
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))

    # 🆕 INTENTAR USAR SCHEMA PRIMERO
    if SCHEMAS_DISPONIBLES and dataset_name:
        schema = get_schema(dataset_name)
        if schema:
            print("📌 Usando información del schema")

            # Obtener tabla principal
            tabla_principal = schema.get("tabla_principal", "None")
            if tabla_principal == "None" and schema.get("tablas_fact"):
                tabla_principal = schema["tablas_fact"][0]

            # Obtener dimensión de persona
            dimension_col = None
            persona_info = get_columna_nombre_persona(dataset_name)
            if persona_info:
                tabla_persona, col_persona = persona_info
                dimension_col = f"'{tabla_persona}'[{col_persona}]"
                print(f"✅ Dimensión desde schema: {dimension_col}")

            # Obtener métrica
            metric_col = None
            metrica_info = get_metrica_principal(dataset_name)
            if metrica_info:
                tabla_metrica, col_metrica = metrica_info
                metric_col = f"SUM('{tabla_metrica}'[{col_metrica}])"
                print(f"✅ Métrica desde schema: {metric_col}")

            if dimension_col or metric_col:
                return {
                    "tabla_principal": tabla_principal,
                    "dimension_columns": [dimension_col] if dimension_col else [],
                    "metric_expression": metric_col or f"COUNTROWS('{tabla_principal}')",
                    "metric_name": "Total",
                    "filters": [],
                    "n": 10
                }

    # Fallback original si no hay schema
    # Buscar tabla principal (primera fact o primera tabla)
    tabla_principal = next(
        (t["nombre"] for t in ctx_filtrado if "fact" in t["nombre"].lower()),
        ctx_filtrado[0]["nombre"] if ctx_filtrado else ""
    )
    
    # Buscar columna de dimensión (nombre, usuario, empleado)
    # Priorizar tablas DIM y stg con información de usuarios
    dimension_col = None
    prioridad_tablas = ["stg RRHH_Users", "DIM_Responsable", "stg GEN_DptoRRHH"]

    # Primero buscar en tablas prioritarias
    for tabla_name in prioridad_tablas:
        t = next((tab for tab in ctx_filtrado if tab["nombre"] == tabla_name), None)
        if t and t.get("muestra"):
            cols = list(t["muestra"][0].keys())
            # Buscar columnas descriptivas, evitando IDs
            for col in cols:
                col_lower = col.lower()
                if any(k in col_lower for k in ["nombre", "name", "apellido", "usuario", "user"]):
                    if not any(x in col_lower for x in ["id", "key", "codigo"]):
                        dimension_col = f"'{t['nombre']}'[{col}]"
                        break
            if dimension_col:
                break

    # Si no encontró en tablas prioritarias, buscar en todas
    if not dimension_col:
        for t in ctx_filtrado:
            if t.get("muestra"):
                cols = list(t["muestra"][0].keys())
                for col in cols:
                    col_lower = col.lower()
                    if any(k in col_lower for k in ["nombre", "name", "usuario", "empleado", "persona"]):
                        if not any(x in col_lower for x in ["id", "key"]):
                            dimension_col = f"'{t['nombre']}'[{col}]"
                            break
                if dimension_col:
                    break

    # Buscar columna numérica (priorizar columnas con keywords relevantes)
    metric_col = None
    keywords_metricas = ["saldo", "dias", "horas", "importe", "total", "cantidad", "valor"]

    for t in ctx_filtrado:
        if t.get("muestra") and t["muestra"]:
            # Primero buscar con keywords
            for col, val in t["muestra"][0].items():
                col_lower = col.lower()
                if isinstance(val, (int, float)) and any(kw in col_lower for kw in keywords_metricas):
                    if not any(k in col_lower for k in ["id", "key", "codigo"]):
                        metric_col = f"SUM('{t['nombre']}'[{col}])"
                        break
            if metric_col:
                break

    # Si no encontró con keywords, usar cualquier numérica
    if not metric_col:
        for t in ctx_filtrado:
            if t.get("muestra") and t["muestra"]:
                for col, val in t["muestra"][0].items():
                    if isinstance(val, (int, float)) and not any(k in col.lower() for k in ["id", "key", "codigo"]):
                        metric_col = f"SUM('{t['nombre']}'[{col}])"
                        break
                if metric_col:
                    break
    
    return {
        "tabla_principal": tabla_principal,
        "dimension_columns": [dimension_col] if dimension_col else [],
        "metric_expression": metric_col or f"COUNTROWS('{tabla_principal}')",
        "metric_name": "Total",
        "filters": [],
        "n": 10
    }


# ============================================
# CONSTRUCCIÓN DE DAX DESDE PATRÓN
# ============================================
def _format_filter_value(val, col_tipo=None):
    """
    Formatea un valor de filtro según su tipo para DAX.

    Args:
        val: Valor a formatear
        col_tipo: Tipo de la columna (opcional) para detectar fechas

    Returns:
        str: Valor formateado para DAX
    """
    # Detectar fechas
    if isinstance(val, datetime.datetime):
        return f"DATE({val.year}, {val.month}, {val.day})"
    elif isinstance(val, datetime.date):
        return f"DATE({val.year}, {val.month}, {val.day})"
    elif isinstance(val, str):
        # 1. Intentar convertir a número si es string numérico
        # Esto evita errores de comparación Integer vs Text
        if val.strip().replace('.', '', 1).replace('-', '', 1).isdigit():
            # Es un número (puede tener punto decimal o signo negativo)
            try:
                # Intentar como float primero
                num_val = float(val)
                # Si es entero, devolver sin decimales
                if num_val == int(num_val):
                    print(f"🔢 Número detectado: \"{val}\" → {int(num_val)}")
                    return str(int(num_val))
                else:
                    print(f"🔢 Número detectado: \"{val}\" → {num_val}")
                    return str(num_val)
            except ValueError:
                pass

        # 2. Intentar parsear como fecha si parece una fecha
        if "/" in val or "-" in val:
            # Eliminar hora si existe
            fecha_parte = val.split()[0] if " " in val else val

            # Lista de formatos a intentar
            formatos_fecha = [
                "%d/%m/%Y",      # 01/10/2023
                "%Y-%m-%d",      # 2023-10-01
                "%d-%m-%Y",      # 01-10-2023
                "%Y/%m/%d",      # 2023/10/01
                "%m/%d/%Y",      # 10/01/2023 (formato US)
            ]

            for fmt in formatos_fecha:
                try:
                    dt = datetime.datetime.strptime(fecha_parte, fmt)
                    print(f"📅 Fecha parseada: {val} → DATE({dt.year}, {dt.month}, {dt.day})")
                    return f"DATE({dt.year}, {dt.month}, {dt.day})"
                except ValueError:
                    continue

        # 3. Si no es número ni fecha, escapar como string
        if not val.startswith('"'):
            return f'"{val}"'
        return val
    elif isinstance(val, (int, float)):
        return str(val)
    else:
        return str(val)


def _build_dax_from_pattern(pattern: dict, params: dict) -> str:
    """
    Construye query DAX aplicando el patrón con los parámetros extraídos.

    IMPORTANTE: Cuando hay filtros con SUMMARIZECOLUMNS, usa CALCULATETABLE para evitar errores.
    """

    template = pattern["template"]

    # Construir componentes
    dimension_cols = ", ".join(params.get("dimension_columns", [])) if params.get("dimension_columns") else ""
    metric_expr = params.get("metric_expression", "1")
    metric_name = params.get("metric_name", "Resultado")
    tiene_filtros = bool(params.get("filters"))

    # ✅ Si no hay dimension_columns, usar ROW() para valor único
    if not dimension_cols and "SUMMARIZECOLUMNS" in template:
        if tiene_filtros:
            filter_parts = []
            for f in params["filters"]:
                col = _corregir_sintaxis_referencia(f["column"])
                op = f.get("operator", "=")
                val = _format_filter_value(f["value"])
                filter_parts.append(f"{col} {op} {val}")

            filters_str = ",\n        " + ",\n        ".join(filter_parts)

            dax = f"""EVALUATE
ROW(
    "{metric_name}", CALCULATE(
        {metric_expr}{filters_str}
    )
)"""
        else:
            dax = f"""EVALUATE
ROW(
    "{metric_name}", {metric_expr}
)"""
        return dax.strip()

    # ✅ CORRECCIÓN PRINCIPAL: Cuando hay filtros Y dimension_columns, usar CALCULATETABLE
    if tiene_filtros and dimension_cols:
        # Construir filtros correctamente formateados
        filter_parts = []
        for f in params["filters"]:
            col = _corregir_sintaxis_referencia(f["column"])
            op = f.get("operator", "=")
            val = _format_filter_value(f["value"])
            filter_parts.append(f"{col} {op} {val}")

        filters_str = ",\n    " + ",\n    ".join(filter_parts)

        # Usar CALCULATETABLE en lugar de poner filtros directamente en SUMMARIZECOLUMNS
        dax = f"""EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        {dimension_cols},
        "{metric_name}", {metric_expr}
    ){filters_str}
)"""
        return dax.strip()

    # Sin filtros, usar SUMMARIZECOLUMNS normal
    if dimension_cols:
        dax = f"""EVALUATE
SUMMARIZECOLUMNS(
    {dimension_cols},
    "{metric_name}", {metric_expr}
)"""
        return dax.strip()

    # Fallback: usar template original (para patterns especiales como top_n)
    try:
        # Construir filtros para template (si existen)
        filters_str = ""
        if tiene_filtros:
            filter_parts = []
            for f in params["filters"]:
                col = _corregir_sintaxis_referencia(f["column"])
                op = f.get("operator", "=")
                val = _format_filter_value(f["value"])
                filter_parts.append(f"{col} {op} {val}")

            if filter_parts:
                filters_str = ",\n    " + ",\n    ".join(filter_parts)

        dax = template.format(
            dimension_columns=dimension_cols,
            filters=filters_str,
            metric_name=metric_name,
            metric_expression=metric_expr,
            table=params.get("tabla_principal", ""),
            n=params.get("n", 10)
        )
        return dax.strip()
    except KeyError as e:
        print(f"❌ Error formateando template: {e}")
        # Fallback a pattern más simple
        return f"EVALUATE\nROW(\"{metric_name}\", {metric_expr})"


# ============================================
# EJECUCIÓN DE DAX
# ============================================
def _ejecutar_dax(dax_query: str, dataset_name: str) -> pd.DataFrame:
    """Ejecuta query DAX y devuelve DataFrame"""
    
    # ✅ Validación básica antes de ejecutar
    if not dax_query.strip().upper().startswith("EVALUATE"):
        raise ValueError("La query DAX debe comenzar con EVALUATE")
    
    connection_string = (
        f"Provider=MSOLAP;"
        f"Data Source={WORKSPACE_URL};"
        f"Initial Catalog={dataset_name};"
        f"Integrated Security=ClaimsToken;"
    )
    
    print(f"\n🔧 Ejecutando query DAX:\n{dax_query}\n")
    
    try:
        conn = AdomdConnection(connection_string)
        conn.Open()

        cmd = AdomdCommand(dax_query, conn)
        reader = cmd.ExecuteReader()
        
        cols = [reader.GetName(i) for i in range(reader.FieldCount)]
        rows = []
        while reader.Read():
            rows.append([reader.GetValue(i) for i in range(reader.FieldCount)])
        
        conn.Close()

        df = pd.DataFrame(rows, columns=cols)
        print(f"✅ Query ejecutada: {len(df)} filas devueltas")
        return df
        
    except Exception as e:
        error_msg = str(e)
        
        # Detectar errores comunes y sugerir soluciones
        if "No se encuentra la tabla" in error_msg:
            print(f"❌ Error: Tabla no encontrada en el modelo")
            print(f"💡 Sugerencia: Verifica que el nombre de la tabla existe y usa comillas simples: 'NombreTabla'")
        elif "No se encuentra la columna" in error_msg:
            print(f"❌ Error: Columna no encontrada")
            print(f"💡 Sugerencia: Verifica el nombre exacto de la columna en el modelo")
        elif "no puede utilizarse en esta expresión" in error_msg:
            print(f"❌ Error: Tipo de dato incompatible")
            print(f"💡 Sugerencia: No se puede hacer SUM() de fechas o textos")
        
        raise e


# ============================================
# RESUMEN DE RESULTADOS (REFACTORIZADO CON CONTEXTO FEMXA)
# ============================================
def _resumir_resultado_multiple(df_principal: pd.DataFrame, todos_resultados: dict, user_prompt: str) -> str:
    """
    GPT genera respuesta considerando múltiples queries ejecutadas.
    Responde como analista de FEMXA con conocimiento del sector formativo.
    """
    
    if df_principal.empty:
        return "La consulta principal no devolvió resultados."
    
    # Preparar contexto con todos los resultados
    contexto_completo = {
        "query_principal": {
            "datos": df_principal.head(10).to_dict(orient="records"),
            "filas_totales": len(df_principal)
        }
    }
    
    # Añadir queries complementarias
    for key, resultado in todos_resultados.items():
        if key != "principal":
            contexto_completo[key] = {
                "proposito": resultado.get("proposito", ""),
                "descripcion": resultado.get("descripcion", ""),
                "datos": resultado.get("preview", [])
            }
    
    # 🆕 PROMPT REFACTORIZADO CON CONTEXTO FEMXA
    prompt = f"""Eres un analista de datos de FEMXA, empresa líder en formación profesional y capacitación.

CONTEXTO EMPRESARIAL:
- FEMXA se dedica a la gestión de cursos, programas formativos y capacitación profesional
- Trabajas con datos de: alumnos/estudiantes, instructores/formadores, cursos, asistencias, calificaciones, 
  vacaciones del personal docente, horas lectivas, matriculaciones, finalizaciones
- Tu rol es interpretar datos del sistema de gestión y proporcionar insights claros para la toma de decisiones

PREGUNTA DEL USUARIO:
"{user_prompt}"

RESULTADOS DE MÚLTIPLES QUERIES EN POWER BI:
{json.dumps(contexto_completo, ensure_ascii=False, indent=2, default=_to_json_safe)}

INSTRUCCIONES:
1. Analiza TODOS los resultados proporcionados
2. Si la pregunta pide porcentajes o ratios, CALCULA el valor exacto usando los datos disponibles
3. Responde directamente con cifras específicas y contexto relevante
4. Usa terminología del sector formativo cuando sea apropiado:
   - "alumnos" o "estudiantes" en lugar de "usuarios" o "registros"
   - "cursos" o "programas formativos" en lugar de "items"
   - "instructores" o "formadores" en lugar de "personal"
   - "finalizaciones" en lugar de "completados"
5. Sé preciso con números, porcentajes y métricas
6. Usa lenguaje profesional pero cercano
7. Sé conciso (máximo 4-5 líneas)

EJEMPLOS DE BUENAS RESPUESTAS:
- "El 75% de los alumnos han finalizado exitosamente el curso. De 400 estudiantes matriculados, 300 completaron el programa formativo."
- "En el área de Recursos Humanos hay 8 empleados con saldo de vacaciones pendientes, acumulando un total de 125 días."
- "Los 5 formadores con más horas lectivas acumulan entre 180 y 220 horas cada uno durante este trimestre."

Genera tu respuesta como analista de FEMXA:"""
    
    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0.3,
            max_tokens=500,
            messages=[
                {
                    "role": "system", 
                    "content": """Eres un analista de datos de FEMXA, empresa de formación profesional. 
                    Interpretas datos de gestión académica, RRHH y operaciones formativas. 
                    Comunicas resultados con precisión usando terminología del sector educativo.
                    Eres profesional, claro y orientado a insights accionables."""
                },
                {"role": "user", "content": prompt}
            ]
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"⚠️ Error generando resumen multi-query: {e}")
        # Fallback: descripción simple
        total_principal = len(df_principal)
        return f"Análisis completado. Query principal: {total_principal} registros. Queries adicionales ejecutadas: {len(todos_resultados) - 1}"


def _resumir_resultado(df: pd.DataFrame, user_prompt: str) -> str:
    """
    GPT genera respuesta en lenguaje natural basándose en los datos.
    Responde como analista de FEMXA con conocimiento del sector formativo.
    """
    
    if df.empty:
        return "La consulta no devolvió resultados."
    
    preview = df.head(20).to_dict(orient="records")
    
    # 🆕 PROMPT REFACTORIZADO CON CONTEXTO FEMXA
    prompt = f"""Eres un analista de datos de FEMXA, empresa líder en formación profesional y capacitación.

CONTEXTO EMPRESARIAL:
- FEMXA gestiona programas formativos, cursos profesionales y capacitación
- Datos típicos: alumnos, instructores, cursos, asistencias, calificaciones, vacaciones, horas lectivas
- Tu audiencia son gestores, coordinadores académicos y responsables de RRHH

PREGUNTA DEL USUARIO:
"{user_prompt}"

RESULTADOS DE POWER BI:
{json.dumps(preview, ensure_ascii=False, indent=2, default=_to_json_safe)}

INSTRUCCIONES:
1. Responde directamente a la pregunta usando los datos
2. Menciona las cifras y datos más relevantes
3. Si hay muchos registros, resume los principales hallazgos
4. Usa terminología del sector formativo:
   - "alumnos/estudiantes" (no "usuarios" o "clientes")
   - "cursos/programas formativos" (no "productos")
   - "instructores/formadores" (no "recursos")
   - "finalizaciones/completados" (no "cerrados")
5. Lenguaje profesional, claro y orientado a acción
6. Sé conciso (máximo 4-5 líneas)

EJEMPLOS:
- "Los 10 alumnos con mejor rendimiento han obtenido calificaciones entre 9.2 y 9.8 en el programa de capacitación técnica."
- "El personal docente del área de Idiomas tiene un saldo promedio de 18 días de vacaciones pendientes."
- "Se han identificado 45 estudiantes que no han completado el 75% de asistencias requeridas en sus cursos actuales."

Respuesta como analista de FEMXA:"""
    
    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0.3,
            max_tokens=400,
            messages=[
                {
                    "role": "system", 
                    "content": """Eres un analista de datos de FEMXA, empresa de formación profesional.
                    Interpretas datos académicos, de RRHH y operativos.
                    Comunicas resultados de forma clara, profesional y usando terminología educativa.
                    Das respuestas directas con datos específicos y contexto relevante."""
                },
                {"role": "user", "content": prompt}
            ]
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"⚠️ Error generando resumen: {e}")
        # Fallback: descripción simple con terminología FEMXA
        if len(df) == 1 and len(df.columns) == 1:
            return f"El resultado es: {df.iloc[0, 0]}"
        else:
            return f"Se encontraron {len(df)} registros en el sistema de gestión. Primeros resultados: {df.head(3).to_dict(orient='records')}"


# ============================================
# FALLBACK: GPT GENERA DAX COMPLETO
# ============================================
# NOTA: La función _fix_table_quotes fue reemplazada por _fix_table_quotes_v2 (más abajo)
# que maneja más casos de corrección de sintaxis


def _fix_table_quotes_v2(dax: str) -> str:
    """
    Versión mejorada: Corrige referencias a tablas y columnas en DAX.

    Problemas comunes que corrige:
    1. Tabla[Columna] → 'Tabla'[Columna]
    2. 'Tabla[Columna]' → 'Tabla'[Columna]
    3. 'stg 'RRHH_Users'[Col]' → 'stg RRHH_Users'[Col] (comillas anidadas)
    4. 'Table-'guid'[Col]' → 'Table-guid'[Col] (comillas cortadas en GUIDs)
    """

    # 1. CRÍTICO: Corregir nombres de tabla con GUIDs cortados
    # Pattern: 'TableName-'restOfGuid'[Column]
    # Ejemplo: 'DateTableTemplate_cb2f60f2-17a4-4889-b229-'e99fada39121'[NroMes]
    # → 'DateTableTemplate_cb2f60f2-17a4-4889-b229-e99fada39121'[NroMes]
    dax = re.sub(r"'([^']+)-'([^']+)'\[", r"'\1-\2'[", dax)

    # 2. Corregir comillas anidadas mal puestas: 'stg 'RRHH_Users'[Col]' → 'stg RRHH_Users'[Col]
    # Hacerlo múltiples veces por si hay anidación profunda
    for _ in range(3):
        dax = re.sub(r"'([^']+)\s+'([^']+)'\[", r"'\1 \2'[", dax)

    # 3. Corregir: 'Tabla[Columna]' → 'Tabla'[Columna]
    dax = re.sub(r"'([^']+)\[([^\]]+)\]'", r"'\1'[\2]", dax)

    # 4. Corregir: Tabla[Columna] → 'Tabla'[Columna] (solo si no es función DAX)
    # NOTA: Este paso va ANTES de la corrección de condiciones para no romper las referencias
    funciones_dax = ["SUM", "COUNT", "AVERAGE", "MIN", "MAX", "COUNTROWS", "CALCULATE",
                     "SUMX", "FILTER", "ALL", "VALUES", "DISTINCT", "RELATED", "DATE",
                     "YEAR", "MONTH", "DAY", "TODAY", "NOW", "ROW", "SUMMARIZECOLUMNS",
                     "CALCULATETABLE", "TOPN", "VAR", "RETURN", "IF", "AND", "OR"]

    pattern = r"(?<!')(\b[A-Za-z_][A-Za-z0-9_\s\-]*)\[([^\]]+)\]"

    def replacer(match):
        tabla = match.group(1).strip()
        columna = match.group(2)
        # No corregir funciones DAX
        if tabla.upper() in funciones_dax:
            return match.group(0)
        return f"'{tabla}'[{columna}]"

    dax = re.sub(pattern, replacer, dax)

    return dax


def _fallback_gpt_pure(prompt: str, ctx: dict) -> dict:
    """
    Último recurso: GPT genera DAX completo desde cero.
    Incluye validación y corrección automática del código generado.
    """
    print("⚠️ Usando fallback: GPT genera DAX completo")

    # Usar contexto filtrado (sin LocalDateTable)
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))

    schema_simple = []
    for t in ctx_filtrado[:8]:  # Limitar a 8 tablas relevantes
        cols = list(t["muestra"][0].keys()) if t.get("muestra") else []
        schema_simple.append({
            "tabla": t["nombre"],
            "columnas": cols[:12],
            "filas": len(t.get("muestra", []))
        })

    prompt_gpt = f"""Eres un experto en Power BI DAX. Genera queries VÁLIDAS y EJECUTABLES.

PREGUNTA DEL USUARIO:
{prompt}

SCHEMA DISPONIBLE (tablas y columnas reales):
{json.dumps(schema_simple, indent=2, ensure_ascii=False)}

REGLAS CRÍTICAS DE SINTAXIS DAX:
1. SIEMPRE usa comillas simples alrededor de nombres de tabla: 'NombreTabla'[Columna]
2. NUNCA pongas comillas alrededor de todo: 'Tabla[Columna]' ← INCORRECTO
3. Para filtros con dimensiones, USA CALCULATETABLE:
   CALCULATETABLE(SUMMARIZECOLUMNS(...), Filtro1, Filtro2)
4. Para fechas, USA DATE(): DATE(2025, 10, 27) no strings
5. VERIFICA que tablas y columnas existen en el schema

EJEMPLOS CORRECTOS:

// Agregación simple por persona
EVALUATE
SUMMARIZECOLUMNS(
    'stg RRHH_Users'[US_nombre],
    "Total Días", SUM('FACT_SaldoVacaciones'[SaldoVacaciones])
)

// Con filtros (usa CALCULATETABLE)
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        'stg RRHH_Users'[US_nombre],
        "Total", SUM('FACT_SaldoVacaciones'[SaldoVacaciones])
    ),
    'stg RRHH_Users'[US_nombre] = "Juan",
    'FechaCalendario'[Year] = 2025
)

// Valor único sin dimensiones
EVALUATE
ROW(
    "Total General", SUM('FACT_SaldoVacaciones'[SaldoVacaciones])
)

// Con filtro de fecha
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        'stg GEN_DptoRRHH'[DR_nombre],
        "Total", SUM('FACT_SaldoVacaciones'[SaldoVacaciones])
    ),
    'FACT_SaldoVacaciones'[fecha] = DATE(2025, 10, 27)
)

GENERA SOLO EL CÓDIGO DAX sin markdown ni explicaciones:"""

    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0,
            max_tokens=800,
            messages=[
                {
                    "role": "system",
                    "content": "Eres un experto en DAX de Power BI. Generas código ejecutable y correcto. Respondes SOLO código DAX sin explicaciones, markdown ni comentarios."
                },
                {"role": "user", "content": prompt_gpt}
            ]
        )

        dax = response["choices"][0]["message"]["content"].strip()

        # Limpiar markdown
        dax = re.sub(r'```dax\n?|```\n?', '', dax)
        dax = dax.strip()

        print(f"🤖 DAX generado por GPT:\n{dax[:200]}...")

        # ✅ CORRECCIÓN AUTOMÁTICA MEJORADA
        dax_corregido = _fix_table_quotes_v2(dax)

        if dax_corregido != dax:
            print(f"🔧 DAX corregido automáticamente (mostrando primeras diferencias)")

        df = _ejecutar_dax(dax_corregido, ctx["cm_seleccionado"])
        text = _resumir_resultado(df, prompt)

        return {
            "text": text,
            "query": dax_corregido,
            "preview": df.head(10).to_dict(orient="records"),
            "method": "gpt_fallback"
        }

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Fallback GPT falló: {error_msg}")

        # Dar feedback específico según el error
        if "No se encuentra la tabla" in error_msg:
            return {
                "text": f"❌ No pude encontrar las tablas necesarias en el modelo. Tablas disponibles: {', '.join([t['tabla'] for t in schema_simple[:5]])}",
                "query": dax_corregido if 'dax_corregido' in locals() else "",
                "preview": [],
                "method": "failed"
            }

        return {
            "text": f"❌ No pude generar una query válida. Error: {error_msg}",
            "query": dax_corregido if 'dax_corregido' in locals() else "",
            "preview": [],
            "method": "failed"
        }


# ============================================
# DETECCIÓN DE QUERIES MÚLTIPLES
# ============================================

def _requiere_multiples_queries(prompt: str) -> bool:
    """
    Detecta si la pregunta requiere múltiples queries para un análisis completo.
    """
    prompt_lower = prompt.lower()
    
    keywords_multi_query = [
        "porcentaje", "tasa", "ratio", "%", "por ciento",
        "comparar", "vs", "versus", "frente a", "diferencia",
        "cuántos de los", "qué parte", "proporción",
        "entre", "respecto", "en relación"
    ]
    
    return any(kw in prompt_lower for kw in keywords_multi_query)


def _generar_queries_complementarias(prompt: str, ctx: dict, query_principal: str, params_principales: dict) -> list[dict]:
    """
    Genera queries complementarias para análisis más completos.
    Por ejemplo, si la query principal cuenta finalizados, genera también el total.
    """
    
    dataset_name = ctx.get("cm_seleccionado", "")
    prompt_lower = prompt.lower()
    
    queries_complementarias = []
    
    # Detectar contexto de la pregunta
    es_porcentaje = any(k in prompt_lower for k in ["porcentaje", "tasa", "%", "por ciento", "proporción"])
    es_comparacion = any(k in prompt_lower for k in ["comparar", "vs", "versus", "diferencia"])
    
    if es_porcentaje:
        # Para porcentajes, necesitamos numerador y denominador
        tabla_principal = params_principales.get("tabla_principal", "")
        
        # Query 1: Total general (denominador)
        query_total = f"""EVALUATE
ROW(
    "Total", COUNTROWS('{tabla_principal}')
)"""
        queries_complementarias.append({
            "proposito": "total_general",
            "query": query_total,
            "descripcion": "Total de registros para calcular porcentaje"
        })
        
        # Query 2: Si hay filtros o condiciones específicas, contar con esas condiciones
        if "finalizado" in prompt_lower or "completado" in prompt_lower or "terminado" in prompt_lower:
            # Usar contexto filtrado
            ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))
            # Intentar encontrar columna de estado
            for t in ctx_filtrado:
                if t["nombre"] == tabla_principal and t.get("muestra"):
                    cols_raw = list(t["muestra"][0].keys())
                    # Limpiar nombres de columnas (eliminar prefijos de tabla)
                    cols = []
                    for col in cols_raw:
                        if "[" in col and "]" in col:
                            col_limpia = col.split("[")[-1].rstrip("]")
                        else:
                            col_limpia = col
                        cols.append(col_limpia)

                    col_estado = None
                    for col in cols:
                        if any(k in col.lower() for k in ["finalizado", "completado", "estado", "status", "apto"]):
                            col_estado = col
                            break

                    if col_estado:
                        query_condicion = f"""EVALUATE
ROW(
    "Total Con Condición", CALCULATE(
        COUNTROWS('{tabla_principal}'),
        '{tabla_principal}'[{col_estado}] = 1
    )
)"""
                        queries_complementarias.append({
                            "proposito": "con_condicion",
                            "query": query_condicion,
                            "descripcion": f"Registros donde {col_estado} = 1"
                        })
                        break
    
    elif es_comparacion:
        # Para comparaciones, puede necesitar datos de períodos diferentes o categorías
        pass
    
    return queries_complementarias[:2]  # Máximo 2 queries adicionales


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================
def analyze(user_prompt: str, ctx: dict, classifier_result: dict = None) -> dict:
    """
    Analiza la pregunta del usuario y ejecuta query(s) DAX contra Power BI
    
    Flujo:
    1. Detecta si requiere múltiples queries
    2. Selecciona patrón DAX apropiado para query principal
    3. Extrae parámetros con GPT
    4. Genera queries complementarias si es necesario
    5. Ejecuta todas las queries
    6. Resume resultados con contexto completo (como analista de FEMXA)
    """
    
    dataset_name = ctx.get("cm_seleccionado", "")
    
    if not dataset_name or dataset_name == "no es necesario CM":
        return {
            "text": "No se especificó el cuadro de mando o dataset.",
            "query": "",
            "preview": []
        }

    try:
        # 0️⃣ Detectar si requiere múltiples queries
        requiere_multi = _requiere_multiples_queries(user_prompt)
        print(f"{'🔢' if requiere_multi else '1️⃣'} {'Análisis multi-query' if requiere_multi else 'Query simple'}")
        
        # 1️⃣ Seleccionar patrón DAX para query principal
        pattern_name, pattern = _select_pattern(user_prompt)
        print(f"🎯 Patrón seleccionado: {pattern_name}")
        
        # 2️⃣ Extraer parámetros con GPT
        params = _extract_query_parameters(user_prompt, ctx, pattern_name)
        
        # 3️⃣ Construir DAX desde patrón
        dax_query = _build_dax_from_pattern(pattern, params)
        
        # 4️⃣ Ejecutar query principal
        df_principal = _ejecutar_dax(dax_query, dataset_name)
        
        resultados = {
            "principal": {
                "query": dax_query,
                "data": df_principal,
                "preview": df_principal.head(20).to_dict(orient="records")
            }
        }
        
        # 5️⃣ Generar y ejecutar queries complementarias si es necesario
        if requiere_multi:
            print("🔢 Generando queries complementarias...")
            queries_complementarias = _generar_queries_complementarias(user_prompt, ctx, dax_query, params)
            
            for i, q_info in enumerate(queries_complementarias, 1):
                try:
                    print(f"   Query {i}: {q_info['proposito']}")
                    df_comp = _ejecutar_dax(q_info["query"], dataset_name)
                    resultados[f"complementaria_{i}"] = {
                        "query": q_info["query"],
                        "data": df_comp,
                        "proposito": q_info["proposito"],
                        "descripcion": q_info["descripcion"],
                        "preview": df_comp.to_dict(orient="records")
                    }
                except Exception as e:
                    print(f"   ⚠️ Error en query complementaria {i}: {e}")
                    continue
        
        # 6️⃣ Resumir todos los resultados (CON CONTEXTO FEMXA)
        text = _resumir_resultado_multiple(df_principal, resultados, user_prompt) if requiere_multi else _resumir_resultado(df_principal, user_prompt)
        
        return {
            "text": text,
            "query": dax_query,
            "preview": df_principal.head(20).to_dict(orient="records"),
            "pattern_used": pattern_name,
            "parameters": params,
            "method": "pattern_based",
            "queries_adicionales": len(resultados) - 1 if requiere_multi else 0,
            "resultados_completos": resultados if requiere_multi else None
        }
        
    except Exception as e:
        print(f"❌ Error en analyzer (patrón): {e}")
        
        # Si es error de columna no encontrada, dar feedback útil
        if "No se encuentra la columna" in str(e) or "not found" in str(e).lower():
            # Extraer el nombre de la columna del error
            match = re.search(r"'([^']+)'", str(e))
            if match:
                col_problema = match.group(1)
                
                # Buscar columnas disponibles en todas las tablas
                cols_disponibles = []
                for t in ctx["contexto"]:
                    if t.get("muestra"):
                        tabla = t["nombre"]
                        cols = list(t["muestra"][0].keys())
                        cols_disponibles.extend([f"{tabla}.[{c}]" for c in cols[:5]])
                
                return {
                    "text": f"❌ No pude encontrar la columna '{col_problema}'. Algunas columnas disponibles: {', '.join(cols_disponibles[:10])}",
                    "query": dax_query if 'dax_query' in locals() else "",
                    "preview": [],
                    "method": "error_columna"
                }
        
        # Fallback: GPT genera DAX completo
        try:
            return _fallback_gpt_pure(user_prompt, ctx)
        except Exception as e2:
            return {
                "text": f"❌ No se pudo procesar la pregunta. Error: {str(e)}. Intenta reformular la pregunta.",
                "query": "",
                "preview": [],
                "method": "failed"
            }


# ============================================
# TEST LOCAL
# ============================================
if __name__ == "__main__":
    # Simular contexto para pruebas
    ctx = {
        "cm_seleccionado": "CM Gestión Horas y Vacaciones",
        "contexto": [
            {
                "nombre": "FACT_SaldoVacaciones",
                "muestra": [
                    {"EmpleadoID": 1, "SaldoVacaciones": 20, "Año": 2025},
                    {"EmpleadoID": 2, "SaldoVacaciones": 15, "Año": 2025}
                ]
            },
            {
                "nombre": "stg RRHH_Users",
                "muestra": [
                    {"EmpleadoID": 1, "Nombre": "Ana García"},
                    {"EmpleadoID": 2, "Nombre": "Juan Pérez"}
                ]
            },
            {
                "nombre": "FechaCalendario",
                "muestra": [{"Year": 2025, "MonthNumber": 10}]
            }
        ]
    }

    # Preguntas de prueba orientadas a FEMXA
    preguntas = [
        "¿Cuántas vacaciones tiene cada instructor en 2025?",
        "Dame el total de días de formación impartidos",
        "¿Quiénes son los 5 formadores con más horas lectivas?",
        "¿Qué porcentaje de alumnos han finalizado el curso?"
    ]
    
    for pregunta in preguntas:
        print(f"\n{'='*60}")
        print(f"PREGUNTA: {pregunta}")
        print('='*60)
        resultado = analyze(pregunta, ctx)
        print(f"\n📝 RESPUESTA: {resultado['text']}")
        print(f"\n🔧 QUERY:\n{resultado.get('query', 'N/A')}")
        print(f"\n📊 PREVIEW: {json.dumps(resultado.get('preview', []), indent=2, ensure_ascii=False)}")