import os
import openai
from dotenv import load_dotenv
import json, re

load_dotenv()

openai.api_type = "azure"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai.api_version = "2024-06-01"

DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")


def classify(user_message: str, departamento: str = "general") -> dict:
    """
    Clasifica el mensaje usando GPT-3.5 y el contexto de departamento.
    Bloquea lenguaje inapropiado y valida relevancia temática.
    """

    prompt = f"""
Eres un clasificador que evalúa si una pregunta es apropiada para el chatbot corporativo del Cuadro de Mando FEMXA.

El usuario pertenece al departamento: **{departamento}**.

Tu tarea es decidir:
1. Si el mensaje es apropiado (sin lenguaje ofensivo, político o sexual).
2. Si está relacionado con el trabajo o los indicadores del departamento.
3. Si parece una pregunta analítica (por ejemplo, sobre cursos, alumnos, seguimiento, formación, etc.).

Responde siempre en formato JSON con este esquema:

{{
  "allowed": true/false,
  "confidence": 0.85,
  "name": "consulta_rrhh" o "fuera_de_tema",
  "reason": "explicación breve"
}}

Sé benevolente: si el mensaje es ambiguo pero podría tener relación con el trabajo del departamento o si es un breve saludo, permítelo.
"""

    try:
        response = openai.ChatCompletion.create(
            engine=DEPLOYMENT,
            temperature=0.2,
            max_tokens=200,
            messages=[
                {"role": "system", "content": "Eres un clasificador de prompts corporativos. Devuelves solo JSON."},
                {"role": "user", "content": prompt + "\n\nMensaje del usuario:\n" + user_message},
            ],
        )

        result_text = response["choices"][0]["message"]["content"].strip()

        try:
            result = json.loads(result_text)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", result_text, re.DOTALL)
            if match:
                result = json.loads(match.group(0))
            else:
                result = {
                    "allowed": True,
                    "confidence": 0.5,
                    "name": "indefinido",
                    "reason": "Respuesta no estructurada del modelo"
                }

        result.setdefault("allowed", True)
        result.setdefault("confidence", 0.5)
        result.setdefault("name", "indefinido")
        result.setdefault("reason", "Sin razón especificada")

        return result

    except Exception as e:
        return {
            "allowed": True,
            "confidence": 0.4,
            "name": "fallback",
            "reason": f"Error clasificando con GPT-3.5: {e}",
        }
