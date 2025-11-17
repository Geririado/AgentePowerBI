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

try:
    from pathlib import Path
    current_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    
    from schemas import get_schema, get_columna_nombre_persona, get_metrica_principal, SCHEMAS
    SCHEMAS_DISPONIBLES = True
    print(f"✅ Schemas cargados: {len(SCHEMAS)} CMs disponibles")
except ImportError as e:
    print(f"⚠️ schemas.py no encontrado: {e}")
    SCHEMAS_DISPONIBLES = False
except Exception as e:
    print(f"⚠️ Error cargando schemas: {e}")
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
# DETECCIÓN INTELIGENTE DE COLUMNAS
# ============================================
def _detectar_columna_nombre_persona(ctx: dict) -> tuple[str, str] | None:
    """Detecta automáticamente la columna correcta para nombres de persona"""
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))
    
    # Prioridad de tablas
    prioridad_tablas = [
        "DIM_Responsable", 
        "DIM_Persona",
        "DIM_Empleado",
        "stg RRHH_Users", 
        "stg GEN_DptoRRHH"
    ]
    
    # Palabras clave para identificar columnas de nombre
    keywords_nombre = ["nombre", "name", "apellido", "completo", "full"]
    
    for tabla_name in prioridad_tablas:
        tabla = next((t for t in ctx_filtrado if t["nombre"] == tabla_name), None)
        if not tabla or not tabla.get("muestra"):
            continue
        
        for col, val in tabla["muestra"][0].items():
            col_lower = col.lower()
            
            # Debe ser string y contener keywords
            if isinstance(val, str) and any(kw in col_lower for kw in keywords_nombre):
                # NO debe ser ID o key
                if not any(k in col_lower for k in ["id", "key", "codigo", "code"]):
                    # Validar que tenga valores con espacios (nombre completo)
                    if " " in str(val):
                        print(f"🎯 Columna nombre detectada: '{tabla_name}'[{col}] = '{val}'")
                        return (tabla_name, col)
    
    # Buscar en todas las tablas si no se encontró en prioritarias
    for tabla in ctx_filtrado:
        if not tabla.get("muestra"):
            continue
        
        for col, val in tabla["muestra"][0].items():
            col_lower = col.lower()
            
            if isinstance(val, str) and any(kw in col_lower for kw in keywords_nombre):
                if not any(k in col_lower for k in ["id", "key", "codigo"]):
                    if " " in str(val):
                        print(f"🎯 Columna nombre detectada (secundaria): '{tabla['nombre']}'[{col}] = '{val}'")
                        return (tabla["nombre"], col)
    
    return None
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


def _filtrar_tablas_irrelevantes(contexto: list[dict]) -> list[dict]:
    """Filtra tablas autogeneradas por Power BI"""
    tablas_filtradas = []
    for t in contexto:
        nombre = t.get("nombre", "")
        if nombre.startswith("LocalDateTable_") or \
           nombre.startswith("DateTableTemplate_") or \
           nombre == "_Medidas" or nombre == "_medidas":
            continue
        tablas_filtradas.append(t)
    return tablas_filtradas


def _select_pattern(prompt: str) -> tuple[str, dict]:
    """Selecciona el patrón DAX más adecuado"""
    prompt_lower = prompt.lower()
    
    scores = {}
    for pattern_name, pattern_info in DAX_PATTERNS.items():
        score = sum(1 for kw in pattern_info["keywords"] if kw in prompt_lower)
        scores[pattern_name] = score
    
    best_pattern = max(scores, key=scores.get)
    if scores[best_pattern] > 0:
        return best_pattern, DAX_PATTERNS[best_pattern]
    
    if any(w in prompt_lower for w in ["cuántos hay", "número total"]):
        return "conteo_simple", DAX_PATTERNS["conteo_simple"]
    
    return "agregacion_simple", DAX_PATTERNS["agregacion_simple"]


def _corregir_sintaxis_referencia(ref: str) -> str:
    """Corrige sintaxis de referencia DAX a formato 'Tabla'[Columna]"""
    # 'Tabla[Columna]' → 'Tabla'[Columna]
    match_incorrecto = re.match(r"'([^']+)\[([^\]]+)\]'", ref)
    if match_incorrecto:
        tabla, columna = match_incorrecto.groups()
        if "[" in columna:
            columna = columna.split("[")[-1].rstrip("]")
        return f"'{tabla}'[{columna}]"

    # Tabla[Columna] → 'Tabla'[Columna]
    match_sin_comillas = re.match(r"^([A-Za-z_][A-Za-z0-9_\s]*)\[([^\]]+)\]$", ref)
    if match_sin_comillas and "'" not in ref:
        tabla, columna = match_sin_comillas.groups()
        tabla = tabla.strip()
        if "[" in columna:
            columna = columna.split("[")[-1].rstrip("]")
        return f"'{tabla}'[{columna}]"

    # Ya correcto: 'Tabla'[Columna]
    match_correcto = re.match(r"'([^']+)'\[([^\]]+)\]", ref)
    if match_correcto:
        tabla, columna = match_correcto.groups()
        if "[" in columna:
            columna = columna.split("[")[-1].rstrip("]")
            return f"'{tabla}'[{columna}]"
        return ref

    return ref


def _find_similar_column(target: str, available_cols: list) -> str | None:
    """Busca columna similar - retorna solo nombre sin prefijos"""
    target_lower = target.lower()
    
    def limpiar_nombre(col):
        if "[" in col and "]" in col:
            return col.split("[")[-1].rstrip("]")
        return col
    
    cols_limpias = [limpiar_nombre(col) for col in available_cols]
    
    # Coincidencia exacta
    for col in cols_limpias:
        if col.lower() == target_lower:
            return col
    
    # Contiene palabra
    for col in cols_limpias:
        if target_lower in col.lower():
            return col
    
    # Heurísticas para nombres de persona
    if target_lower in ["nombre", "name", "empleado", "persona", "usuario", "user"]:
        priority_keywords = ["nombre", "name", "apellido", "completo", "full"]
        for kw in priority_keywords:
            for col in cols_limpias:
                col_lower = col.lower()
                if kw in col_lower and not any(x in col_lower for x in ["id", "key", "codigo"]):
                    return col
    
    return None


def _validate_columns(params: dict, ctx: dict) -> dict:
    """Valida que columnas existan y corrige si es necesario"""
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))
    columnas_disponibles = {}
    tipos_columnas = {}  # 🆕 Guardar tipos
    
    for t in ctx_filtrado:
        if t.get("muestra") and len(t["muestra"]) > 0:
            tabla_nombre = t["nombre"]
            cols = list(t["muestra"][0].keys())
            columnas_disponibles[tabla_nombre] = cols
            # Guardar tipos de cada columna
            tipos_columnas[tabla_nombre] = {}
            for col, val in t["muestra"][0].items():
                tipos_columnas[tabla_nombre][col] = type(val).__name__

    # Validar dimension_columns
    dimension_cols_validadas = []
    for col_ref in params.get("dimension_columns", []):
        col_ref_corregido = _corregir_sintaxis_referencia(col_ref)
        if col_ref_corregido != col_ref:
            print(f"🔧 Corrigiendo sintaxis: {col_ref} → {col_ref_corregido}")
            col_ref = col_ref_corregido

        match = re.match(r"'([^']+)'\[([^\]]+)\]", col_ref)
        if match:
            tabla, columna = match.groups()
            if tabla in columnas_disponibles:
                if columna in columnas_disponibles[tabla]:
                    dimension_cols_validadas.append(col_ref)
                else:
                    col_similar = _find_similar_column(columna, columnas_disponibles[tabla])
                    if col_similar:
                        print(f"⚠️ Corrigiendo columna: {columna} → {col_similar}")
                        dimension_cols_validadas.append(f"'{tabla}'[{col_similar}]")

    params["dimension_columns"] = dimension_cols_validadas
    
    # Validar metric_expression
    metric = params.get("metric_expression", "")
    
    if "(" in metric:
        pattern_incorrecto = r"(SUM|COUNT|AVERAGE)\('([^']+)\[([^\]]+)\]'\)"
        match = re.search(pattern_incorrecto, metric)
        if match:
            func, tabla, columna = match.groups()
            metric_correcto = f"{func}('{tabla}'[{columna}])"
            print(f"🔧 Corrigiendo sintaxis métrica: {metric} → {metric_correcto}")
            params["metric_expression"] = metric_correcto
            metric = metric_correcto
    
    # 🆕 VALIDAR QUE COLUMNA TENGA AGREGACIÓN
    if "SUM(" in metric or "COUNT(" in metric or "AVERAGE(" in metric or "[" in metric:
        match = re.search(r"'([^']+)'\[([^\]]+)\]", metric)
        if match:
            tabla, columna = match.groups()
            
            # Si NO tiene función de agregación, añadir SUM()
            if not any(func in metric for func in ["SUM(", "COUNT(", "AVERAGE(", "MIN(", "MAX("]):
                print(f"⚠️ Métrica sin agregación detectada: {metric}")
                if tabla in columnas_disponibles:
                    if columna in columnas_disponibles[tabla]:
                        tipo_valor = tipos_columnas.get(tabla, {}).get(columna, "str")
                        
                        if tipo_valor in ["int", "int64", "float", "float64"]:
                            params["metric_expression"] = f"SUM('{tabla}'[{columna}])"
                            print(f"✅ Añadida agregación: SUM('{tabla}'[{columna}])")
                        else:
                            params["metric_expression"] = f"COUNTROWS('{tabla}')"
                            print(f"✅ Columna no numérica, usando: COUNTROWS('{tabla}')")
            elif "SUM(" in metric or "COUNT(" in metric or "AVERAGE(" in metric:
                # Ya tiene agregación, validar que columna exista
                if tabla in columnas_disponibles:
                    if columna not in columnas_disponibles[tabla]:
                        col_similar = _find_similar_column(columna, columnas_disponibles[tabla])
                        if col_similar:
                            print(f"⚠️ Corrigiendo columna en métrica: {columna} → {col_similar}")
                            func_match = re.match(r"(SUM|COUNT|AVERAGE)\('([^']+)'\[([^\]]+)\]\)", metric)
                            if func_match:
                                func, tabla_orig, _ = func_match.groups()
                                col_similar_limpia = col_similar.split("[")[-1].rstrip("]") if "[" in col_similar else col_similar
                                params["metric_expression"] = f"{func}('{tabla_orig}'[{col_similar_limpia}])"
                    else:
                        # Validar tipo de dato
                        tipo_valor = tipos_columnas.get(tabla, {}).get(columna, "str")
                        
                        if tipo_valor in ["str", "DateTime"] or "date" in columna.lower():
                            print(f"⚠️ No se puede hacer SUM de {columna} ({tipo_valor}). Buscando numérica.")
                            
                            col_numerica = None
                            for col, val in ctx_filtrado[0]["muestra"][0].items():
                                col_lower = col.lower()
                                tipo_col = tipos_columnas.get(tabla, {}).get(col, "str")
                                if tipo_col in ["int", "int64", "float", "float64"] and not any(k in col_lower for k in ["id", "key", "codigo"]):
                                    if any(kw in col_lower for kw in ["saldo", "dias", "horas", "importe", "total", "coste", "costo"]):
                                        col_numerica = col
                                        break
                            
                            if col_numerica:
                                params["metric_expression"] = f"SUM('{tabla}'[{col_numerica}])"
                                print(f"✅ Usando columna numérica: {col_numerica}")
                            else:
                                params["metric_expression"] = f"COUNTROWS('{tabla}')"
                                print(f"✅ Usando COUNTROWS")
    
    # 🆕 VALIDAR Y LIMPIAR FILTROS
    filtros_validados = []
    for filtro in params.get("filters", []):
        col_filtro = filtro.get("column", "")
        val_filtro = filtro.get("value", "")
        op_filtro = filtro.get("operator", "=")
        
        # 🚫 ELIMINAR FILTROS INVÁLIDOS
        # 1. Operador inválido "IS NOT NULL"
        if op_filtro.upper() in ["IS NOT NULL", "IS NULL", "IS"]:
            print(f"🚫 Filtro inválido eliminado: {col_filtro} {op_filtro} {val_filtro}")
            continue
        
        # 2. Valor None o "None"
        if val_filtro is None or str(val_filtro).strip().upper() == "NONE":
            print(f"🚫 Filtro con valor None eliminado: {col_filtro}")
            continue
        
        # 3. Verificar compatibilidad de tipo columna vs valor
        match = re.match(r"'([^']+)'\[([^\]]+)\]", col_filtro)
        if match:
            tabla, columna = match.groups()
            if tabla in tipos_columnas and columna in tipos_columnas[tabla]:
                tipo_columna = tipos_columnas[tabla][columna]
                
                # Si columna es numérica pero valor es fecha → ELIMINAR
                if tipo_columna in ["int", "int64", "float", "float64"]:
                    # Detectar si valor parece fecha
                    if isinstance(val_filtro, str) and ("/" in val_filtro or "-" in val_filtro):
                        try:
                            datetime.datetime.strptime(val_filtro.split()[0], "%d/%m/%Y")
                            print(f"🚫 Filtro incompatible eliminado: columna numérica {col_filtro} vs fecha '{val_filtro}'")
                            continue
                        except:
                            pass
                
                # Si columna es fecha, validar que valor sea fecha
                if tipo_columna == "DateTime" or "date" in columna.lower() or "fecha" in columna.lower():
                    if isinstance(val_filtro, str):
                        # Intentar parsear como fecha
                        fecha_valida = False
                        if "/" in val_filtro or "-" in val_filtro:
                            formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]
                            for fmt in formatos:
                                try:
                                    datetime.datetime.strptime(val_filtro.split()[0], fmt)
                                    fecha_valida = True
                                    break
                                except:
                                    pass
                        
                        if not fecha_valida:
                            print(f"🚫 Filtro de fecha inválido eliminado: {col_filtro} = '{val_filtro}'")
                            continue
        
        # Si pasa todas las validaciones, añadir
        filtros_validados.append(filtro)
    
    params["filters"] = filtros_validados
    
    if len(filtros_validados) < len(params.get("filters", [])):
        print(f"✅ Filtros limpiados: {len(params.get('filters', []))} → {len(filtros_validados)}")
    
    return params


# ============================================
# EXTRACCIÓN DE PARÁMETROS CON GPT
# ============================================
def _extract_query_parameters(prompt: str, ctx: dict, pattern_name: str) -> dict:
    """GPT identifica componentes necesarios (versión optimizada)"""
    dataset_name = ctx.get("cm_seleccionado", "")
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))

    # Obtener fecha actual
    fecha_hoy = datetime.datetime.now()
    contexto_temporal = f"\n📅 FECHA ACTUAL: {fecha_hoy.strftime('%d/%m/%Y')} (use esta fecha si preguntan 'hoy', 'ahora', 'actual')"

    # 🆕 DETECCIÓN AUTOMÁTICA DE COLUMNAS
    columna_nombre_auto = _detectar_columna_nombre_persona(ctx)
    
    # Sugerencias del schema
    schema_hints = ""
    if SCHEMAS_DISPONIBLES and dataset_name:
        persona_info = get_columna_nombre_persona(dataset_name)
        metrica_info = get_metrica_principal(dataset_name)
        if persona_info or metrica_info:
            schema_hints = "\n\nSUGERENCIAS DEL SCHEMA:"
            if persona_info:
                schema_hints += f"\n- Persona: '{persona_info[0]}'[{persona_info[1]}]"
            if metrica_info:
                schema_hints += f"\n- Métrica: SUM('{metrica_info[0]}'[{metrica_info[1]}])"
    
    # 🆕 Si detectamos columna de nombre automáticamente, priorizar sobre schema
    if columna_nombre_auto:
        if schema_hints:
            schema_hints = f"\n\n⚠️ COLUMNA DETECTADA EN DATOS (USAR ESTA):"
            schema_hints += f"\n- Persona: '{columna_nombre_auto[0]}'[{columna_nombre_auto[1]}]"
        else:
            schema_hints = f"\n\nCOLUMNA DETECTADA:"
            schema_hints += f"\n- Persona: '{columna_nombre_auto[0]}'[{columna_nombre_auto[1]}]"
    
    # Contexto simplificado
    tablas_disponibles = []
    for t in ctx_filtrado[:6]:
        if not t.get("muestra") or len(t["muestra"]) == 0:
            continue
        cols_con_ejemplo = []
        for col_name, col_value in list(t["muestra"][0].items())[:10]:
            cols_con_ejemplo.append({
                "nombre": col_name,
                "tipo": type(col_value).__name__
            })
        tablas_disponibles.append({
            "nombre": t["nombre"],
            "columnas": cols_con_ejemplo
        })
    
    prompt_gpt = f"""Experto en Power BI DAX. Identifica componentes para query.
{contexto_temporal}

PREGUNTA: "{prompt}"
TABLAS: {json.dumps(tablas_disponibles, ensure_ascii=False)}{schema_hints}

PATRÓN: {pattern_name}

REGLAS SINTAXIS CRÍTICAS:
1. 'Tabla'[Columna] ← correcto
2. 'Tabla[Columna]' ← incorrecto

3. ⚠️ MÉTRICAS - SIEMPRE CON AGREGACIÓN:
   - ❌ INCORRECTO: "metric_expression": "'Tabla'[Columna]"
   - ✅ CORRECTO: "metric_expression": "SUM('Tabla'[Columna])"
   - Funciones válidas: SUM(), COUNT(), AVERAGE(), MIN(), MAX()

4. ⚠️ FECHAS - MUY IMPORTANTE:
   - En JSON filters, pon valor como STRING simple: "15/11/2025"
   - ❌ NO pongas: "DATE(2025, 11, 15)" como string
   - ✅ CORRECTO: {{"column": "'Tabla'[Fecha]", "operator": "=", "value": "15/11/2025"}}

5. Para nombres: usa columnas descriptivas, NO códigos/IDs

JSON requerido:
{{
    "tabla_principal": "nombre_tabla",
    "dimension_columns": ["'Tabla'[Columna]"],
    "metric_expression": "SUM('Tabla'[Columna])",
    "metric_name": "Nombre resultado",
    "filters": [
        {{"column": "'Tabla'[Nombre]", "operator": "=", "value": "Juan"}},
        {{"column": "'Tabla'[Fecha]", "operator": "=", "value": "15/11/2025"}}
    ],
    "n": 10
}}

EJEMPLOS MÉTRICA CORRECTA:
- "metric_expression": "SUM('FACT_Costos'[Importe])"
- "metric_expression": "COUNT('Sales'[OrderID])"
- "metric_expression": "AVERAGE('Products'[Price])"

Solo JSON:"""
    
    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0,
            max_tokens=800,
            messages=[
                {"role": "system", "content": "Experto DAX. Respondes solo JSON válido."},
                {"role": "user", "content": prompt_gpt}
            ]
        )
        
        text = response["choices"][0]["message"]["content"].strip()
        text = re.sub(r'```json\n?|```\n?', '', text)
        match = re.search(r'\{.*\}', text, re.DOTALL)
        params = json.loads(match.group(0) if match else text)
        
        params.setdefault("dimension_columns", [])
        params.setdefault("metric_name", "Resultado")
        params.setdefault("filters", [])
        params.setdefault("n", 10)
        
        params = _validate_columns(params, ctx)
        print(f"✅ Parámetros extraídos")
        return params
        
    except Exception as e:
        print(f"❌ Error GPT: {e}")
        return _fallback_parameters(prompt, ctx)


def _fallback_parameters(prompt: str, ctx: dict) -> dict:
    """Extracción heurística si GPT falla"""
    print("⚠️ Usando extracción heurística")
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))
    dataset_name = ctx.get("cm_seleccionado", "")

    if SCHEMAS_DISPONIBLES and dataset_name:
        schema = get_schema(dataset_name)
        if schema:
            tabla_principal = schema.get("tabla_principal", "None")
            if tabla_principal == "None" and schema.get("tablas_fact"):
                tabla_principal = schema["tablas_fact"][0]

            dimension_col = None
            persona_info = get_columna_nombre_persona(dataset_name)
            if persona_info:
                dimension_col = f"'{persona_info[0]}'[{persona_info[1]}]"

            metric_col = None
            metrica_info = get_metrica_principal(dataset_name)
            if metrica_info:
                metric_col = f"SUM('{metrica_info[0]}'[{metrica_info[1]}])"

            if dimension_col or metric_col:
                return {
                    "tabla_principal": tabla_principal,
                    "dimension_columns": [dimension_col] if dimension_col else [],
                    "metric_expression": metric_col or f"COUNTROWS('{tabla_principal}')",
                    "metric_name": "Total",
                    "filters": [],
                    "n": 10
                }

    # Fallback sin schema
    tabla_principal = next(
        (t["nombre"] for t in ctx_filtrado if "fact" in t["nombre"].lower()),
        ctx_filtrado[0]["nombre"] if ctx_filtrado else ""
    )
    
    dimension_col = None
    prioridad_tablas = ["stg RRHH_Users", "DIM_Responsable", "stg GEN_DptoRRHH"]
    
    for tabla_name in prioridad_tablas:
        t = next((tab for tab in ctx_filtrado if tab["nombre"] == tabla_name), None)
        if t and t.get("muestra"):
            for col in t["muestra"][0].keys():
                col_lower = col.lower()
                if any(k in col_lower for k in ["nombre", "apellido", "usuario"]):
                    if not any(x in col_lower for x in ["id", "key"]):
                        dimension_col = f"'{t['nombre']}'[{col}]"
                        break
            if dimension_col:
                break

    metric_col = None
    keywords_metricas = ["saldo", "dias", "horas", "importe", "total"]
    for t in ctx_filtrado:
        if t.get("muestra"):
            for col, val in t["muestra"][0].items():
                if isinstance(val, (int, float)) and any(kw in col.lower() for kw in keywords_metricas):
                    if not any(k in col.lower() for k in ["id", "key"]):
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
# CONSTRUCCIÓN DE DAX
# ============================================
def _format_filter_value(val, col_ref=None, ctx=None):
    """Formatea valor de filtro para DAX detectando tipo de columna"""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return f"DATE({val.year}, {val.month}, {val.day})"
    elif isinstance(val, str):
        val_stripped = val.strip()
        
        # 🔥 CRÍTICO: Si ya es función DATE(), retornar sin comillas
        if val_stripped.startswith("DATE(") and val_stripped.endswith(")"):
            print(f"📅 Función DATE detectada, retornando sin comillas: {val_stripped}")
            return val_stripped
        
        # 🆕 DETECTAR TIPO DE COLUMNA si disponible
        if col_ref and ctx:
            match = re.match(r"'([^']+)'\[([^\]]+)\]", col_ref)
            if match:
                tabla, columna = match.groups()
                tabla_ctx = next((t for t in ctx.get("contexto", []) if t["nombre"] == tabla), None)
                if tabla_ctx and tabla_ctx.get("muestra"):
                    primer_valor = tabla_ctx["muestra"][0].get(columna)
                    tipo_columna = type(primer_valor).__name__
                    
                    # Si columna es integer/float pero valor es string, convertir
                    if tipo_columna in ["int", "int64", "float", "float64"]:
                        try:
                            num_val = float(val_stripped)
                            if num_val == int(num_val):
                                print(f"🔢 Convirtiendo '{val}' a {int(num_val)} (columna tipo {tipo_columna})")
                                return str(int(num_val))
                            print(f"🔢 Convirtiendo '{val}' a {num_val} (columna tipo {tipo_columna})")
                            return str(num_val)
                        except ValueError:
                            pass
                    
                    # Si columna es DateTime, convertir a DATE()
                    elif tipo_columna == "DateTime" or "date" in columna.lower() or "fecha" in columna.lower():
                        if "/" in val_stripped or "-" in val_stripped:
                            fecha_parte = val_stripped.split()[0] if " " in val_stripped else val_stripped
                            formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"]
                            for fmt in formatos:
                                try:
                                    dt = datetime.datetime.strptime(fecha_parte, fmt)
                                    print(f"📅 Fecha detectada: '{val}' → DATE({dt.year}, {dt.month}, {dt.day})")
                                    return f"DATE({dt.year}, {dt.month}, {dt.day})"
                                except ValueError:
                                    continue
        
        # Detectar número (sin contexto)
        if val_stripped.replace('.', '', 1).replace('-', '', 1).isdigit():
            try:
                num_val = float(val_stripped)
                if num_val == int(num_val):
                    return str(int(num_val))
                return str(num_val)
            except ValueError:
                pass
        
        # Parsear fecha (sin contexto) - PRIORIDAD ALTA
        if "/" in val_stripped or "-" in val_stripped:
            fecha_parte = val_stripped.split()[0] if " " in val_stripped else val_stripped
            formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%y"]
            for fmt in formatos:
                try:
                    dt = datetime.datetime.strptime(fecha_parte, fmt)
                    print(f"📅 Fecha detectada (sin contexto): '{val}' → DATE({dt.year}, {dt.month}, {dt.day})")
                    return f"DATE({dt.year}, {dt.month}, {dt.day})"
                except ValueError:
                    continue
        
        # String normal
        return f'"{val_stripped}"' if not val_stripped.startswith('"') else val_stripped
    elif isinstance(val, (int, float)):
        return str(val)
    return str(val)


def _build_dax_from_pattern(pattern: dict, params: dict, ctx: dict = None) -> str:
    """Construye query DAX desde patrón y parámetros"""
    template = pattern["template"]
    dimension_cols = ", ".join(params.get("dimension_columns", [])) if params.get("dimension_columns") else ""
    metric_expr = params.get("metric_expression", "1")
    metric_name = params.get("metric_name", "Resultado")
    tiene_filtros = bool(params.get("filters"))

    # Sin dimensiones → ROW()
    if not dimension_cols and "SUMMARIZECOLUMNS" in template:
        if tiene_filtros:
            filter_parts = []
            for f in params["filters"]:
                col = _corregir_sintaxis_referencia(f["column"])
                op = f.get("operator", "=")
                val = _format_filter_value(f["value"], col, ctx)
                filter_parts.append(f"{col} {op} {val}")
            filters_str = ",\n        " + ",\n        ".join(filter_parts)
            return f"""EVALUATE
ROW(
    "{metric_name}", CALCULATE(
        {metric_expr}{filters_str}
    )
)"""
        else:
            return f"""EVALUATE
ROW(
    "{metric_name}", {metric_expr}
)"""

    # Con filtros Y dimensiones → CALCULATETABLE
    if tiene_filtros and dimension_cols:
        filter_parts = []
        for f in params["filters"]:
            col = _corregir_sintaxis_referencia(f["column"])
            op = f.get("operator", "=")
            val = _format_filter_value(f["value"], col, ctx)
            filter_parts.append(f"{col} {op} {val}")
        filters_str = ",\n    " + ",\n    ".join(filter_parts)
        return f"""EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        {dimension_cols},
        "{metric_name}", {metric_expr}
    ){filters_str}
)"""

    # Sin filtros
    if dimension_cols:
        return f"""EVALUATE
SUMMARIZECOLUMNS(
    {dimension_cols},
    "{metric_name}", {metric_expr}
)"""

    # Fallback
    try:
        filters_str = ""
        if tiene_filtros:
            filter_parts = []
            for f in params["filters"]:
                col = _corregir_sintaxis_referencia(f["column"])
                op = f.get("operator", "=")
                val = _format_filter_value(f["value"], col, ctx)
                filter_parts.append(f"{col} {op} {val}")
            if filter_parts:
                filters_str = ",\n    " + ",\n    ".join(filter_parts)
        
        return template.format(
            dimension_columns=dimension_cols,
            filters=filters_str,
            metric_name=metric_name,
            metric_expression=metric_expr,
            table=params.get("tabla_principal", ""),
            n=params.get("n", 10)
        )
    except KeyError:
        return f"EVALUATE\nROW(\"{metric_name}\", {metric_expr})"


# ============================================
# EJECUCIÓN DAX
# ============================================
def _ejecutar_dax(dax_query: str, dataset_name: str) -> pd.DataFrame:
    """Ejecuta query DAX"""
    if not dax_query.strip().upper().startswith("EVALUATE"):
        raise ValueError("Query DAX debe comenzar con EVALUATE")
    
    connection_string = (
        f"Provider=MSOLAP;"
        f"Data Source={WORKSPACE_URL};"
        f"Initial Catalog={dataset_name};"
        f"Integrated Security=ClaimsToken;"
    )
    
    print(f"\n🔧 Ejecutando DAX:\n{dax_query[:300]}...\n")
    
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
        print(f"✅ Query ejecutada: {len(df)} filas")
        return df
        
    except Exception as e:
        error_msg = str(e)
        if "No se encuentra la tabla" in error_msg:
            print(f"❌ Tabla no encontrada")
        elif "No se encuentra la columna" in error_msg:
            print(f"❌ Columna no encontrada")
        raise e


# ============================================
# RESUMEN CON GPT
# ============================================
def _resumir_resultado(df: pd.DataFrame, user_prompt: str, todos_resultados: dict = None) -> str:
    """Genera resumen en lenguaje natural"""
    if df.empty:
        return "La consulta no devolvió resultados."

    if todos_resultados:
        contexto_datos = {"query_principal": {"datos": df.head(10).to_dict(orient="records")}}
        for key, res in todos_resultados.items():
            if key != "principal":
                contexto_datos[key] = {"datos": res.get("preview", [])}
        max_tokens = 500
    else:
        contexto_datos = df.head(20).to_dict(orient="records")
        max_tokens = 400

    prompt = f"""Analista de FEMXA (formación profesional).

PREGUNTA: "{user_prompt}"
DATOS: {json.dumps(contexto_datos, ensure_ascii=False, default=_to_json_safe)}

Responde directamente con datos. Usa terminología formativa: alumnos, cursos, instructores.
Máximo 4 líneas. Sé preciso con números.

Respuesta:"""

    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0.3,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": "Analista de datos FEMXA."},
                {"role": "user", "content": prompt}
            ]
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"⚠️ Error resumen: {e}")
        if len(df) == 1 and len(df.columns) == 1:
            return f"Resultado: {df.iloc[0, 0]}"
        return f"Se encontraron {len(df)} registros."


# ============================================
# FALLBACK GPT COMPLETO
# ============================================
def _fix_table_quotes_v2(dax: str) -> str:
    """Corrige sintaxis DAX - VERSIÓN MEJORADA v2"""
    
    # 1. CRÍTICO: Comillas faltantes al inicio de línea o después de coma
    # Patrón: ", stg RRHH_Users'[Col]" o "\n    stg RRHH_Users'[Col]"
    # → ", 'stg RRHH_Users'[Col]" o "\n    'stg RRHH_Users'[Col]"
    dax = re.sub(r'([,\n]\s*)([A-Za-z_][A-Za-z0-9_\s\-]+)\'\[', r"\1'\2'[", dax)
    
    # 2. Comillas faltantes al inicio absoluto (primera línea después de palabras clave)
    # Patrón después de "CALCULATETABLE(", "SUMMARIZECOLUMNS(", etc.
    dax = re.sub(r'(\()\s*([A-Za-z_][A-Za-z0-9_\s\-]+)\'\[', r"\1\n    '\2'[", dax)
    
    # 3. GUIDs cortados
    dax = re.sub(r"'([^']+)-'([^']+)'\[", r"'\1-\2'[", dax)
    
    # 4. Comillas anidadas
    for _ in range(3):
        dax = re.sub(r"'([^']+)\s+'([^']+)'\[", r"'\1 \2'[", dax)
    
    # 5. 'Tabla[Col]' → 'Tabla'[Col]
    dax = re.sub(r"'([^']+)\[([^\]]+)\]'", r"'\1'[\2]", dax)
    
    # 6. Tabla[Col] → 'Tabla'[Col] (solo si NO es función DAX)
    funciones_dax = ["SUM", "COUNT", "AVERAGE", "COUNTROWS", "CALCULATE", 
                     "SUMX", "FILTER", "DATE", "ROW", "SUMMARIZECOLUMNS",
                     "CALCULATETABLE", "TOPN", "IF", "AND", "OR", "EVALUATE"]
    pattern = r"(?<!')(?<![A-Z])(\b[A-Za-z_][A-Za-z0-9_\s\-]*)\[([^\]]+)\]"
    
    def replacer(match):
        tabla = match.group(1).strip()
        columna = match.group(2)
        if tabla.upper() in funciones_dax:
            return match.group(0)
        return f"'{tabla}'[{columna}]"
    
    dax = re.sub(pattern, replacer, dax)
    
    return dax


def _fallback_gpt_pure(prompt: str, ctx: dict) -> dict:
    """GPT genera DAX completo con detección mejorada de fechas"""
    print("⚠️ Fallback: GPT genera DAX completo")
    ctx_filtrado = _filtrar_tablas_irrelevantes(ctx.get("contexto", []))

    # Fecha actual para contexto
    fecha_hoy = datetime.datetime.now()
    contexto_temporal = f"📅 Fecha actual: {fecha_hoy.strftime('%d/%m/%Y')}"

    # Columnas de fecha detectadas en el contexto para guiar al modelo
    def _detectar_columnas_fecha(tablas_ctx):
        columnas_fecha = []
        formatos_fecha = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]

        for t in tablas_ctx:
            muestra = t.get("muestra", [])
            if not muestra:
                continue

            sample = muestra[0]
            for col_name, val in sample.items():
                col_lower = col_name.lower()
                tipo_val = type(val).__name__

                es_fecha = tipo_val == "datetime" or "fecha" in col_lower or "date" in col_lower
                if not es_fecha and isinstance(val, str):
                    valor_str = val.split()[0]
                    for fmt in formatos_fecha:
                        try:
                            datetime.datetime.strptime(valor_str, fmt)
                            es_fecha = True
                            break
                        except ValueError:
                            continue

                if es_fecha:
                    columnas_fecha.append(f"'{t['nombre']}'[{col_name}]")

        # Eliminar duplicados conservando orden
        columnas_unicas = []
        for c in columnas_fecha:
            if c not in columnas_unicas:
                columnas_unicas.append(c)
        return columnas_unicas

    columnas_fecha = _detectar_columnas_fecha(ctx_filtrado)
    hint_fechas = ""
    if columnas_fecha:
        listado = "\n- " + "\n- ".join(columnas_fecha[:8])
        hint_fechas = f"\nCOLUMNAS FECHA DISPONIBLES (usa estas para filtros de fecha):{listado}"

    schema_simple = []
    for t in ctx_filtrado[:6]:
        cols = list(t["muestra"][0].keys()) if t.get("muestra") else []
        schema_simple.append({"tabla": t["nombre"], "columnas": cols[:10]})

    prompt_gpt = f"""Experto DAX Power BI. Genera query VÁLIDA y EJECUTABLE.
{contexto_temporal}

PREGUNTA: {prompt}
SCHEMA: {json.dumps(schema_simple, ensure_ascii=False)}
{hint_fechas}

REGLAS CRÍTICAS:
1. 'NombreTabla'[Columna] ← correcto
2. 'Tabla[Columna]' ← incorrecto
3. Para filtros con dimensiones, USA CALCULATETABLE
4. FECHAS: SIEMPRE usa DATE(año, mes, día) - NUNCA strings
   - Correcto: 'Tabla'[Fecha] = DATE(2025, 11, 15)
   - Incorrecto: 'Tabla'[Fecha] = "2025-11-15"
5. Verifica que tablas/columnas existen

EJEMPLO CORRECTO con fecha:
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS(
        'stg RRHH_Users'[US_nombre],
        "Total", SUM('FACT'[Importe])
    ),
    'stg RRHH_Users'[US_nombre] = "Juan",
    'FACT'[Fecha] = DATE(2025, 11, 15)
)

Solo código DAX:"""

    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT_GPT4O,
            temperature=0,
            max_tokens=800,
            messages=[
                {"role": "system", "content": "Experto DAX. Solo código ejecutable sin markdown."},
                {"role": "user", "content": prompt_gpt}
            ]
        )
        
        dax = response["choices"][0]["message"]["content"].strip()
        dax = re.sub(r'```dax\n?|```\n?', '', dax).strip()
        
        print(f"🤖 DAX generado")
        
        # Detectar y corregir fechas mal formateadas en el DAX generado
        # Patrón: 'Columna' = "2025-11-15" o "15/11/2025"
        def fix_date_strings(match):
            col_ref = match.group(1)
            date_str = match.group(2)
            
            # Intentar parsear la fecha
            formatos = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
            for fmt in formatos:
                try:
                    dt = datetime.datetime.strptime(date_str, fmt)
                    date_func = f"DATE({dt.year}, {dt.month}, {dt.day})"
                    print(f"🔧 Corrigiendo fecha en DAX: '{date_str}' → {date_func}")
                    return f"{col_ref} = {date_func}"
                except ValueError:
                    continue
            return match.group(0)  # Si no se puede parsear, dejar original
        
        # Buscar patrones de fecha como string
        dax = re.sub(r"('[^']+'\[[^\]]+\])\s*=\s*\"(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})\"", 
                     fix_date_strings, dax)
        
        dax_corregido = _fix_table_quotes_v2(dax)
        
        if dax_corregido != dax:
            print(f"🔧 DAX corregido automáticamente")
        
        df = _ejecutar_dax(dax_corregido, ctx["cm_seleccionado"])
        text = _resumir_resultado(df, prompt)
        
        return {
            "text": text,
            "query": dax_corregido,
            "preview": df.head(10).to_dict(orient="records"),
            "method": "gpt_fallback"
        }
    
    except Exception as e:
        return {
            "text": f"❌ No pude generar query válida: {str(e)}",
            "query": "",
            "preview": [],
            "method": "failed"
        }


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================
def analyze(user_prompt: str, ctx: dict, classifier_result: dict = None) -> dict:
    """Analiza pregunta y ejecuta query DAX"""
    dataset_name = ctx.get("cm_seleccionado", "")
    
    if not dataset_name or dataset_name == "no es necesario CM":
        return {"text": "No se especificó dataset.", "query": "", "preview": []}

    try:
        pattern_name, pattern = _select_pattern(user_prompt)
        print(f"🎯 Patrón: {pattern_name}")
        
        params = _extract_query_parameters(user_prompt, ctx, pattern_name)
        dax_query = _build_dax_from_pattern(pattern, params, ctx)  # 🆕 Pasar contexto
        df_principal = _ejecutar_dax(dax_query, dataset_name)

        # Retry si devuelve vacío
        if df_principal.empty and params.get("filters"):
            print("⚠️ 0 resultados. Intentando fallback...")
            fallback_result = _fallback_gpt_pure(user_prompt, ctx)
            if fallback_result["method"] != "failed":
                return fallback_result

        text = _resumir_resultado(df_principal, user_prompt)
        
        return {
            "text": text,
            "query": dax_query,
            "preview": df_principal.head(20).to_dict(orient="records"),
            "pattern_used": pattern_name,
            "method": "pattern_based"
        }
        
    except Exception as e:
        print(f"❌ Error analyzer: {e}")
        try:
            return _fallback_gpt_pure(user_prompt, ctx)
        except:
            return {
                "text": f"❌ No se pudo procesar: {str(e)}",
                "query": "",
                "preview": [],
                "method": "failed"
            }


if __name__ == "__main__":
    ctx = {
        "cm_seleccionado": "CM Gestión Horas y Vacaciones",
        "contexto": [
            {"nombre": "FACT_SaldoVacaciones", 
             "muestra": [{"EmpleadoID": 1, "SaldoVacaciones": 20}]},
            {"nombre": "stg RRHH_Users",
             "muestra": [{"EmpleadoID": 1, "Nombre": "Ana García"}]}
        ]
    }
    
    resultado = analyze("¿Cuántas vacaciones tiene cada instructor?", ctx)
    print(f"\n📝 {resultado['text']}")
    print(f"\n🔧 {resultado.get('query', '')}")
