import streamlit as st
import pandas as pd
from src.utils.security import authenticate, load_permissions
from src.agent.classifier import classify
from src.agent.comparator import retrieve_context
from src.agent.analyzer import analyze
import base64

# ==============================
# CONFIGURACIÓN INICIAL
# ==============================
st.set_page_config(page_title="Chatbot Cuadro de Mando", page_icon="🤖", layout="centered")

if "page" not in st.session_state:
    st.session_state.page = "home"
if "user" not in st.session_state:
    st.session_state.user = None
if "tablas" not in st.session_state:
    st.session_state.tablas = []

# ==============================
# PANTALLA 1: LANDING
# ==============================
if st.session_state.page == "home":
    st.markdown("""
    <div style="border: 2px solid #0078d4; padding: 20px; border-radius: 8px;
                background-color: #f3f9ff; font-family: 'Segoe UI', sans-serif;">
      <h2 style="color: #0078d4;">🤖 ¡Nuevo Chatbot del Cuadro de Mando!</h2>
      <p>Ya está disponible el <strong>chatbot de ayuda para el Cuadro de Mando</strong>, diseñado para facilitarte el acceso a información personalizada sobre tus indicadores, métricas y seguimiento.</p>
      <ul>
        <li>Consulta tus datos de forma rápida y sencilla.</li>
        <li>Recibe asistencia sobre cómo interpretar los indicadores.</li>
        <li>Solicita informes o visualizaciones directamente desde el chat.</li>
      </ul>
      <p>Para comenzar, haz clic en el botón de abajo o accede desde Teams:</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
        div.stButton > button:first-child {
            background-color: #0078d4;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 6px;
            font-size: 16px;
            cursor: pointer;
        }
        div.stButton > button:hover {
            background-color: #005ea0;
        }
    </style>
    """, unsafe_allow_html=True)

    if st.button("Abrir Chatbot"):
        st.session_state.page = "login"
        st.rerun()

    st.markdown("""
    <p style="margin-top: 15px; font-size: 0.9em; color: #666;">
        Si tienes dudas o necesitas soporte, contacta con el equipo de Sistemas y Soporte.
    </p>
    """, unsafe_allow_html=True)

# ==============================
# PANTALLA 2: LOGIN
# ==============================
elif st.session_state.page == "login":
    st.title("🔐 Acceso al Agente Analítico de PowerBI")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Entrar"):
        user = authenticate(username, password)
        if user:
            st.session_state.user = user
            st.session_state.tablas = load_permissions(user["nivel"], user["departamento"])
            st.session_state.page = "chat"
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos")

    if st.button("Volver al inicio"):
        st.session_state.page = "home"
        st.rerun()

# ==============================
# PANTALLA 3: CHAT
# ==============================
elif st.session_state.page == "chat":
    user = st.session_state.user
    tablas = st.session_state.tablas

    st.sidebar.success(f"👤 {user['departamento']} | ")
    st.sidebar.markdown("### Cuadros de mando accesibles:")
    for t in tablas:
        nombre = t.get("nombre cm") or t.get("nombre") or t.get("indicadores clave") or "Sin nombre"
        st.sidebar.write(f"- **{str(nombre).title()}**")

    st.title("Agente Analítico de PowerBI")

    message = st.text_area("Escribe tu pregunta:", placeholder="Ej: ¿Cuántos alumnos han finalizado en 2024?")

    if st.button("Enviar"):
        if not message.strip():
            st.warning("Por favor, escribe una pregunta.")
            st.stop()

        try:
            # 1️⃣ Clasificación del mensaje
            try:
                cls = classify(message, user["departamento"])
                if not cls.get("allowed", True):
                    st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                    st.stop()
            except Exception as e:
                # Error en clasificación - mostrar mensaje genérico
                print(f"Error en clasificación: {e}")  # Log interno
                st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                st.stop()

            # 2️⃣ Recuperar contexto (comparador)
            try:
                with st.spinner("Conectando con Power BI..."):
                    ctx = retrieve_context(message, cls, user["departamento"], user["nivel"])
                    ctx["tablas_permitidas"] = tablas
                
                # 🚫 Verificar si hay error de departamento (fuera del spinner)
                if ctx.get("error_departamento", False):
                    st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                    st.stop()
                        
            except Exception as e:
                # Error en recuperación de contexto
                print(f"Error en retrieve_context: {e}")  # Log interno
                st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                st.stop()

            # ✅ Validaciones de seguridad y CM
            try:
                cm = ctx.get("cm_seleccionado")

                # ✅ Mostrar nombre del CM si es válido
                if cm and cm.lower() not in ["no cm", "no es necesario cm", "none", ""]:
                    st.markdown("### Cuadro de mando seleccionado:")
                    st.markdown(f"**{cm}**")
                    st.caption(ctx.get("justificacion", "Seleccionado automáticamente según la pregunta."))
                    st.divider()
                else:
                    st.info("ℹ️ No es necesario conectar con ningún cuadro de mando para esta pregunta.")
            except Exception as e:
                # Error en validaciones
                print(f"Error en validaciones: {e}")  # Log interno
                st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                st.stop()

            # 3️⃣ Analizar con el modelo
            try:
                with st.spinner("Analizando..."):
                    analysis = analyze(message, ctx, cls)
                
                # Verificar que la respuesta es válida
                if not analysis or not isinstance(analysis, dict):
                    raise ValueError("Respuesta inválida del analizador")
                
                # ✅ VERIFICAR FLAG DE ERROR PRIMERO
                if analysis.get("error", False):
                    # El analyzer detectó un error internamente
                    raise ValueError(f"Error en analyzer: {analysis.get('error_type', 'unknown')}")
                
                respuesta_texto = analysis.get("text", "").strip()
                
                # Verificar que hay texto en la respuesta
                if not respuesta_texto:
                    raise ValueError("Respuesta vacía del analizador")
                
            except Exception as e:
                # Error en análisis - cualquier tipo de error
                print(f"Error en analyze: {e}")  # Log interno para debugging
                st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                st.stop()

            # 4️⃣ Mostrar respuesta final (solo si todo fue exitoso)
            try:
                st.markdown("### Respuesta:")
                st.write(respuesta_texto)
            except Exception as e:
                # Error al mostrar respuesta
                print(f"Error mostrando respuesta: {e}")  # Log interno
                st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
                st.stop()

        except Exception as e:
            # Captura de CUALQUIER error no manejado previamente
            print(f"Error no capturado: {e}")  # Log interno para debugging
            st.error("Por favor, reformula la pregunta para que pueda ayudarte.")
            st.stop()

    if st.button("Cerrar sesión"):
        st.session_state.page = "home"
        st.session_state.user = None
        st.session_state["last_msg"] = ""
        st.rerun()
