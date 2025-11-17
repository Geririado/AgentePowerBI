import os
import sys
import clr
import json
import re
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import openai

# ============================================
# CARGA AUTOMÁTICA DE ADOMD.NET
# ============================================
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
        print(f"✅ ADOMD.NET v{ver} detectado y cargado.")
        break

if not found:
    raise ImportError(
        "❌ No se encontró Microsoft.AnalysisServices.AdomdClient.dll.\n"
        "Descarga: https://learn.microsoft.com/en-us/analysis-services/client-libraries"
    )

from Microsoft.AnalysisServices.AdomdClient import AdomdConnection, AdomdCommand

# ============================================
# CONFIGURACIÓN INICIAL
# ============================================
load_dotenv()

openai.api_type = "azure"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai.api_version = "2024-06-01"
DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")

WORKSPACE_URL = "powerbi://api.powerbi.com/v1.0/myorg/Femxa_Aciturri_BI"
EXCEL_PATH = "LISTADO_CM_INICIALES_ASISTENTE_18_09_25.xlsx"

# ============================================
# FUNCIONES AUXILIARES
# ============================================
def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _find_col(df: pd.DataFrame, needle: str) -> str:
    for c in df.columns:
        if needle.lower() in str(c).lower():
            return c
    return None


def _pregunta_requiere_datos(prompt: str) -> bool:
    """Detecta si pregunta requiere datos cuantitativos"""
    prompt_l = _norm(prompt)
    palabras_dato = [
        "cuánt", "porcent", "total", "suma", "promedio", "media", "contar",
        "cuenta", "cuánto", "cifra", "número", "tasa", "evolución",
        "tendencia", "variación", "ranking", "compar", "incremento",
        "datos", "empleado", "vacacion", "salario", "curso", "alumno"
    ]
    return any(p in prompt_l for p in palabras_dato)


def _buscar_cm_relevante(df, prompt):
    """Busca CM más relevante por coincidencia de keywords"""
    prompt_l = _norm(prompt)
    df = df.copy()
    df["score"] = 0
    for i, row in df.iterrows():
        texto = _norm(str(row["indicadores_clave"]))
        coincidencias = sum(1 for w in prompt_l.split() if w in texto)
        df.at[i, "score"] = coincidencias
    df_sorted = df.sort_values(by="score", ascending=False)
    if df_sorted.iloc[0]["score"] > 0:
        return df_sorted.iloc[0]["nombre_cm"]
    return None


# ============================================
# LECTURA DEL MODELO DE POWER BI
# ============================================
def _leer_modelo_pbi(initial_catalog: str) -> dict:
    """Conecta al dataset y descarga hasta 10,000 filas por tabla + relaciones"""
    connection_string = (
        f"Provider=MSOLAP;"
        f"Data Source={WORKSPACE_URL};"
        f"Initial Catalog={initial_catalog};"
        f"Integrated Security=ClaimsToken;"
    )

    print(f"\n🔗 Conectando: {initial_catalog}")
    conn = AdomdConnection(connection_string)
    conn.Open()
    print("✅ Conexión abierta\n")

    # Listar tablas
    tablas = []
    cmd = AdomdCommand("SELECT [Name] FROM $SYSTEM.TMSCHEMA_TABLES", conn)
    reader = cmd.ExecuteReader()
    while reader.Read():
        tablas.append(reader.GetValue(0))
    reader.Close()
    print(f"📊 Tablas: {len(tablas)}\n")

    # Obtener relaciones
    relaciones = []
    qrels = """SELECT [FromTableID],[FromColumnID],[ToTableID],[ToColumnID],[IsActive]
    FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS"""
    cmd = AdomdCommand(qrels, conn)
    reader = cmd.ExecuteReader()
    while reader.Read():
        relaciones.append({
            "desde": reader.GetValue(0),
            "columna_desde": reader.GetValue(1),
            "hacia": reader.GetValue(2),
            "columna_hacia": reader.GetValue(3),
            "activa": reader.GetValue(4)
        })
    reader.Close()
    print(f"🔗 Relaciones: {len(relaciones)}\n")

    # Descargar datos
    contexto_tablas = []
    for tname in tablas:
        try:
            print(f"📥 Extrayendo: {tname}")
            query = f"EVALUATE '{tname}'"
            cmd = AdomdCommand(query, conn)
            reader = cmd.ExecuteReader()

            cols = [reader.GetName(i) for i in range(reader.FieldCount)]
            rows = []
            count = 0
            while reader.Read() and count < 10000:
                rows.append([reader.GetValue(i) for i in range(reader.FieldCount)])
                count += 1
            reader.Close()

            if not rows:
                print(f"⚠️ '{tname}' vacía\n")
                continue

            df = pd.DataFrame(rows, columns=cols)
            contexto_tablas.append({
                "nombre": tname,
                "num_filas": len(df),
                "muestra": df.to_dict(orient="records")
            })
            print(f"✅ {len(df)} filas\n")

        except Exception as e:
            print(f"⚠️ Error en '{tname}': {e}\n")
            continue

    conn.Close()
    print("📚 Conexión cerrada\n")
    return {"contexto": contexto_tablas, "relaciones": relaciones}


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================
def retrieve_context(prompt: str, classifier_result: dict = None, 
                     departamento: str = "general", nivel: str = "Baja") -> dict:
    """
    Selecciona CM más adecuado y descarga sus tablas.
    MEJORA: Valida si requiere CM ANTES de conectar a Power BI.
    """
    df = pd.read_excel(EXCEL_PATH, header=1)
    col_nombre = _find_col(df, "nombre")
    col_indicadores = _find_col(df, "indicador")
    col_dep = _find_col(df, "departamento")
    df = df[[col_nombre, col_indicadores, col_dep]].rename(
        columns={
            col_nombre: "nombre_cm", 
            col_indicadores: "indicadores_clave", 
            col_dep: "departamento_cm"
        }
    )

    dep_user = _norm(departamento)
    df_sin_filtrar = df.copy()

    if df.empty:
        return {
            "cm_seleccionado": "no es necesario CM",
            "justificacion": f"No hay cuadros para '{departamento}'.",
            "departamento": departamento,
            "contexto": [],
            "relaciones": []
        }

    requiere_datos = _pregunta_requiere_datos(prompt)

    # ✅ NUEVO: Evaluar si requiere CM ANTES de conectar
    if not requiere_datos:
        prompt_quick = f"""Pregunta: "{prompt}"

¿Requiere datos de cuadro de mando o puede responderse sin consultar datos?

Responde solo JSON:
{{"requiere_cm": true/false, "razon": "explicación breve"}}"""
        
        try:
            response = openai.ChatCompletion.create(
                engine=DEPLOYMENT,
                temperature=0,
                max_tokens=150,
                messages=[{"role": "user", "content": prompt_quick}]
            )
            text = response["choices"][0]["message"]["content"]
            match = re.search(r"\{.*\}", text, re.DOTALL)
            result = json.loads(match.group(0) if match else text)
            
            if not result.get("requiere_cm", False):
                print(f"ℹ️ No requiere CM: {result.get('razon', 'Pregunta conversacional')}")
                return {
                    "cm_seleccionado": "no es necesario CM",
                    "justificacion": result.get("razon", "No requiere datos"),
                    "departamento": departamento,
                    "contexto": [],
                    "relaciones": []
                }
        except Exception as e:
            print(f"⚠️ Error validación previa: {e}")

    # Selección de CM con GPT
    candidates = df.to_dict(orient="records")
    candidates_str = json.dumps(candidates, ensure_ascii=False, indent=2)
    prompt_gpt = f"""Experto en BI corporativo.

Pregunta: "{prompt}"
Cuadros disponibles:
{candidates_str}

{"IMPORTANTE: Requiere datos cuantitativos." if requiere_datos else ""}

Si no aplica: {{"nombre_cm":"no es necesario CM","razon":"..."}}
Solo JSON:"""

    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT,
            temperature=0.0,
            max_tokens=300,
            messages=[
                {"role": "system", "content": "Experto BI. Solo JSON."},
                {"role": "user", "content": prompt_gpt}
            ]
        )
        text = response["choices"][0]["message"]["content"]
        match = re.search(r"\{.*\}", text, re.DOTALL)
        result = json.loads(match.group(0) if match else text)
    except Exception:
        result = {"nombre_cm": "no es necesario CM", "razon": "Error GPT."}

    nombre_cm = result.get("nombre_cm", "").strip()
    justificacion = result.get("razon", "").strip() or "Seleccionado por coincidencia."

    # Fallback si requiere datos pero GPT dice "no CM"
    if requiere_datos and _norm(nombre_cm) in ["no cm", "no es necesario cm", "ninguno", "none"]:
        nombre_cm_buscado = _buscar_cm_relevante(df_sin_filtrar, prompt)
        if nombre_cm_buscado:
            nombre_cm = nombre_cm_buscado
            justificacion = "Pregunta requiere datos; CM más relacionado."
        else:
            return {
                "cm_seleccionado": "no es necesario CM",
                "justificacion": "No hay información para tu departamento.",
                "departamento": departamento,
                "contexto": [],
                "relaciones": [],
                "error_departamento": True
            }

    # ✅ Validar departamento del CM seleccionado
    if nombre_cm and _norm(nombre_cm) not in ["no cm", "no es necesario cm", "ninguno", "none", ""]:
        df_completo = pd.read_excel(EXCEL_PATH, header=1)
        col_n = _find_col(df_completo, "nombre")
        col_d = _find_col(df_completo, "departamento")
        df_completo = df_completo[[col_n, col_d]].rename(
            columns={col_n: "nombre_cm", col_d: "departamento_cm"}
        )
        
        print(f"🔍 Validando CM: '{nombre_cm}'")
        print(f"👤 Usuario dept: '{dep_user}' | Nivel: '{nivel}'")
        
        cm_row = df_completo[df_completo["nombre_cm"].astype(str).str.strip() == nombre_cm]
        if not cm_row.empty:
            departamento_cm = _norm(str(cm_row.iloc[0]["departamento_cm"]))
            print(f"📂 CM dept: '{departamento_cm}'")
            
            if nivel.lower() != "alta" and departamento_cm != dep_user:
                print(f"🚫 ACCESO DENEGADO - Departamentos no coinciden")
                return {
                    "cm_seleccionado": nombre_cm,
                    "justificacion": justificacion,
                    "departamento": departamento,
                    "contexto": [],
                    "relaciones": [],
                    "error_departamento": True
                }
            print(f"✅ ACCESO PERMITIDO")
        else:
            print(f"⚠️ CM '{nombre_cm}' no encontrado en Excel")

    # Si no requiere datos y GPT confirmó "no CM"
    if not requiere_datos and _norm(nombre_cm) in ["no cm", "no es necesario cm", "ninguno", "none"]:
        return {
            "cm_seleccionado": "no es necesario CM",
            "justificacion": justificacion,
            "departamento": departamento,
            "contexto": [],
            "relaciones": []
        }

    # ✅ DESCARGA DE DATOS (solo si es necesario)
    print(f"\n📊 Descargando datos de '{nombre_cm}'...")
    modelo = _leer_modelo_pbi(nombre_cm)

    print(f"🎯 CM seleccionado: {nombre_cm}")
    print(f"🧠 Razón: {justificacion}")
    print(f"📁 Departamento: {departamento}")
    print(f"📈 Tablas: {len(modelo['contexto'])}\n")

    return {
        "cm_seleccionado": nombre_cm,
        "justificacion": justificacion,
        "departamento": departamento,
        "contexto": modelo["contexto"],
        "relaciones": modelo["relaciones"]
    }


# ============================================
# TEST LOCAL
# ============================================
if __name__ == "__main__":
    pregunta = "¿Cuántos días de vacaciones tiene cada empleado?"
    resultado = retrieve_context(pregunta, {}, "Recursos Humanos")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
