import pandas as pd
import hashlib

EXCEL_PATH = "LISTADO_CM_INICIALES_ASISTENTE_18_09_25.xlsx"

USERS = {
    "admin": {"password": "admin123", "nivel": "Alta", "departamento": "Administrador"},
    "rrhh_user": {"password": "rrhh123", "nivel": "Baja", "departamento": "Recursos Humanos"},
    "pedagogia_user": {"password": "ped123", "nivel": "Baja", "departamento": "Pedagogía"},
    "empresas_user": {"password": "emp123", "nivel": "Baja", "departamento": "Empresas"},
    "sistemas_user": {"password": "sis123", "nivel": "Baja", "departamento": "Sistemas"},
    "atencion_user": {"password": "ate123", "nivel": "Baja", "departamento": "Atención al Alumno"}
}

def authenticate(username: str, password: str) -> dict | None:
    """Valida usuario y devuelve sus atributos si es correcto."""
    user = USERS.get(username)
    if not user:
        return None
    if hashlib.sha256(password.encode()).hexdigest() == hashlib.sha256(user["password"].encode()).hexdigest():
        return user
    return None


def load_permissions(nivel: str, departamento: str) -> list[dict]:
    """
    Lee el Excel (encabezados en fila 2, datos desde columna B)
    y devuelve las filas según el nivel de acceso y el departamento.
    """
    # Lee el Excel indicando que los encabezados están en la fila 2 (header=1)
    # y descartando la primera columna vacía
    df = pd.read_excel(EXCEL_PATH, header=1, usecols="B:I")

    # Normaliza los nombres de columna (minúsculas, sin espacios)
    df.columns = [c.strip().lower() for c in df.columns]

    # Identificamos las columnas relevantes
    col_crit = next((c for c in df.columns if "criticidad" in c), None)
    col_dep = next((c for c in df.columns if "departamento" in c), None)

    if not col_crit or not col_dep:
        raise ValueError(f"No se encontraron columnas 'Criticidad' o 'Departamento'. Columnas detectadas: {list(df.columns)}")

    # Normalizamos valores
    df[col_crit] = df[col_crit].astype(str).str.strip().str.lower()
    df[col_dep] = df[col_dep].astype(str).str.strip().str.lower()
    departamento = departamento.lower()

    # Filtrado por nivel y departamento
    if nivel.lower() == "alta":
        filtrado = df
    else:
        filtrado = df[
            (df[col_dep] == departamento)
        ]

    return filtrado.to_dict(orient="records")

