"""
Metadatos de Cuadros de Mando de Power BI
Generado automáticamente desde schemas_powerbi.json
"""

SCHEMAS = {
    "Análisis Movimientos Admisión": {
        "tabla_principal": "Fact_Horas",
        "tabla_personas": "Dim_Personal",
        "tablas_fact": [
            "Fact_Horas",
            "Fact_Contactos",
        ],
        "mapeo_columnas": {
            "Dim_Personal": {
                "columna_nombre_persona": "Empleado",
            },
            "Fact_Horas": {
                "metrica_principal": "Total_Horas_Teorica",
                "columnas_numericas": ['Total_Horas_Teorica', 'Total_Horas_Reales', 'Diferencia_Horas'],
            },
            "Dim_Convocatoria": {
                "columna_nombre_persona": "nombreaccion",
            },
        },
        "medidas_principales": [
            "Horas Reales / Mes",
            "Contactos / hora real",
            "% Bajas Recuperadas/Preinscirtos Curso",
            "% Bajas Redirigidas",
            "% Difusión2",
        ],
    },
    "CDM Gestión de Costes": {
        "tabla_principal": "None",
        "tabla_personas": "stg RRHH_Users",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "stg RRHH_Users": {
                "columna_nombre_persona": "US_GlobeEnterprise",
                "metrica_principal": "US_costeMes",
                "columnas_numericas": ['US_costeMes'],
            },
            "stg GEN_DptoRRHH": {
                "columna_nombre_persona": "DR_nombre",
            },
            "stg GEN_PuestoRRHH": {
                "columna_nombre_persona": "PR_nombre",
            },
            "stg RRHH_userLevels": {
                "columna_nombre_persona": "UL_nombre",
            },
            "DIM_Responsable": {
                "columna_nombre_persona": "US_nombre",
            },
            "stg RRHH_Empresas": {
                "columna_nombre_persona": "EMP_nombre",
            },
            "stg fx_ContratosDocentesPersonal": {
                "columna_nombre_persona": "Numero",
                "metrica_principal": "Importe",
                "columnas_numericas": ['Importe', 'HorasSemanales', 'Horas'],
            },
            "stg contratostipos": {
                "columna_nombre_persona": "nombre",
            },
            "stg Sociedades": {
                "columna_nombre_persona": "NombreFiscal",
            },
        },
        "medidas_principales": [
            "Coste Anual",
            "%Temporales",
            "FechaTXT",
            "CostesMesAnterior",
            "Personas con discapacidad",
        ],
    },
    "CDM Gestión Horas y Vacaciones": {
        "tabla_principal": "FACT_SaldoHoras",
        "tabla_personas": "stg RRHH_Users",
        "tablas_fact": [
            "FACT_SaldoHoras",
            "FACT_SaldoVacaciones",
        ],
        "mapeo_columnas": {
            "FACT_SaldoHoras": {
                "columna_nombre_persona": "Nombre",
                "metrica_principal": "Horas",
                "columnas_numericas": ['Horas'],
            },
            "FACT_SaldoVacaciones": {
                "metrica_principal": "SaldoVacaciones",
                "columnas_numericas": ['SaldoVacaciones'],
            },
            "stg RRHH_Users": {
                "columna_nombre_persona": "US_GlobeEnterprise",
                "metrica_principal": "US_costeMes",
                "columnas_numericas": ['US_costeMes'],
            },
            "stg GEN_DptoRRHH": {
                "columna_nombre_persona": "DR_nombre",
            },
            "stg GEN_PuestoRRHH": {
                "columna_nombre_persona": "PR_nombre",
            },
            "stg RRHH_userLevels": {
                "columna_nombre_persona": "UL_nombre",
            },
            "DIM_Responsable": {
                "columna_nombre_persona": "US_nombre",
            },
        },
        "medidas_principales": [
            "HorasDiaAnterior",
            "Diferencia",
            "FechaTXT",
            "VacacionesDiaAnterior",
            "DiferenciaVacas",
        ],
    },
    "CM Control Contratos Docentes Aitor": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "Dim_Familia": {
                "columna_nombre_persona": "Familia_Nombre",
            },
            "ContratosDocentes_CIF": {
                "columna_nombre_persona": "nombre",
                "metrica_principal": "DiasTotalesNoSolapados",
                "columnas_numericas": ['DiasTotalesNoSolapados', 'DiasTotalesNoSolapados_PersonaCIF'],
            },
        },
        "medidas_principales": [
            "Fecha inicio",
            "Fecha fin",
            "Dias Contratados",
            "Dias Descansados",
            "FechaNumero_e",
        ],
    },
    "CM Control de horarios por docente": {
        "tabla_principal": "Fact_ConvocatoriasContenidosHorarios",
        "tabla_personas": "None",
        "tablas_fact": [
            "Fact_ConvocatoriasContenidosHorarios",
        ],
        "mapeo_columnas": {
            "Dim_Plan": {
                "columna_nombre_persona": "nombreplan",
            },
            "Dim_docentes": {
                "columna_nombre_persona": "NombreDocente",
            },
            "Dim_Familia": {
                "columna_nombre_persona": "Familia_Nombre",
            },
        },
        "medidas_principales": [
            "Horas Totales",
            "Dia de la semana",
            "suma Horas totales",
            "HoraInicioMañanaDocente",
            "HoraFinMañanaDocente",
        ],
    },
    "CM Control Documentación laboral": {
        "tabla_principal": "Fact_Documentos",
        "tabla_personas": "Dim_ContratosDocentesPersonal",
        "tablas_fact": [
            "Fact_Documentos",
            "Fact_ControlDocumentos",
        ],
        "mapeo_columnas": {
            "Fact_Documentos": {
                "columna_nombre_persona": "NombreTipoDocumento",
            },
            "Dim_ContratosDocentesPersonal": {
                "columna_nombre_persona": "Gestor Contrato",
            },
            "Dim_personal": {
                "columna_nombre_persona": "nombreper",
                "metrica_principal": "CosteHora",
                "columnas_numericas": ['CosteHora', 'PermiteHorasAdicionales', 'HorasAnuales'],
            },
            "Fact_ControlDocumentos": {
                "columna_nombre_persona": "NombreArchivo",
            },
            "Dim_TheCategory": {
                "columna_nombre_persona": "TableName",
            },
        },
        "medidas_principales": [
            "Numero de Documentos",
            "Total Numero de Documentos",
            "% No Documents",
            "Numero de DNIs",
            "Total Numero de DNIs",
        ],
    },
    "CM Control Formación Obligatoria": {
        "tabla_principal": "Fact_Cursos",
        "tabla_personas": "None",
        "tablas_fact": [
            "Fact_Cursos",
            "Fact_Examenes",
            "Fact_Lecciones",
        ],
        "mapeo_columnas": {
            "Dim_Usuarios": {
                "columna_nombre_persona": "usernameUsuario",
            },
            "Fact_Cursos": {
                "columna_nombre_persona": "horaFinUsuario",
            },
            "Aux_cursos_Flag": {
                "columna_nombre_persona": "Usuario_Curso",
                "metrica_principal": "NumCursosTotales",
                "columnas_numericas": ['NumCursosTotales'],
            },
            "Aux_cursos_Compliance_Flag": {
                "columna_nombre_persona": "Usuario_Curso",
                "metrica_principal": "NumCursosTotales",
                "columnas_numericas": ['NumCursosTotales'],
            },
            "Aux_Usuarios_Flag": {
                "columna_nombre_persona": "Flag",
            },
        },
        "medidas_principales": [
            "MatriculacionesMes",
            "NumExamenes",
            "NumLecciones",
            "NumExamenesTotales",
            "NumLeccionesTotales",
        ],
    },
    "CM Evolucion Caidas Ejecucion_1": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "Centros": {
                "columna_nombre_persona": "NombreLugar",
            },
            "Alumnos": {
                "columna_nombre_persona": "NombreAlumno",
            },
            "AccionesFormativas": {
                "columna_nombre_persona": "NombreAno",
                "metrica_principal": "ImportePorAlumno",
                "columnas_numericas": ['ImportePorAlumno'],
            },
            "Fecha": {
                "columna_nombre_persona": "Nombremes",
            },
            "AlumnoCurso": {
                "columna_nombre_persona": "NombreEstado",
                "metrica_principal": "Importe Caídas",
                "columnas_numericas": ['Importe Caídas', 'Importe Abandonos', 'Importe No Inicia'],
            },
            "FechasFinCurso": {
                "columna_nombre_persona": "NombreMes",
            },
        },
        "medidas_principales": [
            "% Caidas AAFF",
            "Diferencia Caidas AAFF",
            "Objetivo Abandono",
            "% NO INICIA",
            "% ABANDONOS",
        ],
    },
    "EvolucionMatriculaciones": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "Matriculaciones": {
                "columna_nombre_persona": "UsuarioCambio",
                "metrica_principal": "HorasAccion",
                "columnas_numericas": ['HorasAccion', 'HorasMes', 'HorasTareas'],
            },
            "Fecha Preinscripcion": {
                "columna_nombre_persona": "Nombre Sem Preinscripcion",
            },
            "Usuarios": {
                "columna_nombre_persona": "Nombre",
                "metrica_principal": "CosteMes",
                "columnas_numericas": ['CosteMes'],
            },
            "Tareas": {
                "columna_nombre_persona": "NombreSemana",
                "metrica_principal": "nHoras",
                "columnas_numericas": ['nHoras', 'CosteUsuario'],
            },
            "Equipos Sharepoint": {
                "columna_nombre_persona": "Nombre",
                "metrica_principal": "HorasMes",
                "columnas_numericas": ['HorasMes', 'CosteMes'],
            },
            "StockPreinscripciones": {
                "columna_nombre_persona": "Desempleados",
                "metrica_principal": "Total",
                "columnas_numericas": ['Total'],
            },
            "Objetivos de matriculación": {
                "columna_nombre_persona": "Nombre Semana",
            },
            "Coste Mensual AAA": {
                "metrica_principal": "Mes",
                "columnas_numericas": ['Mes', 'Coste', 'Objetivo'],
            },
            "Objetivos de matriculación 2º Semestre ajustado": {
                "columna_nombre_persona": "Nombre Mes",
            },
        },
        "medidas_principales": [
            "NumeroPreinscripciones",
            "HorasTareasAñoSemana",
            "CosteUsuario",
            "NumeroDesempleadosMatriculados",
            "NumeroOcupadosMatriculados",
        ],
    },
    "CM Recuento Alumnos y Docentes": {
        "tabla_principal": "Fact_alumnoscurso",
        "tabla_personas": "None",
        "tablas_fact": [
            "Fact_alumnoscurso",
            "Fact_docentescurso",
        ],
        "mapeo_columnas": {
            "Dim_cursosdeaccion": {
                "columna_nombre_persona": "NombreSimple",
            },
            "Dim_convocatoria": {
                "columna_nombre_persona": "nombreaccion",
            },
        },
        "medidas_principales": [
            "Alumnos",
            "Alumnos Distintos",
            "Docentes",
            "Docentes Distintos",
            "Prom. Alumnos",
        ],
    },
    "CM Seguimiento Coste Docentes Histórico": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "vw_m_plan_convocatorias": {
                "columna_nombre_persona": "Nombre Convocatoria",
            },
            "vw_f_coste_docentes": {
                "columna_nombre_persona": "nombreaño",
                "metrica_principal": "CosteLaboralDocente",
                "columnas_numericas": ['CosteLaboralDocente', 'HorasDocenteCurso', 'CosteHoraDocente'],
            },
            "vw_f_importes_computables": {
                "columna_nombre_persona": "nombreaño",
                "metrica_principal": "euroalumno",
                "columnas_numericas": ['euroalumno', 'computablesprevistos', 'Importes computables previstos'],
            },
            "vw_link_costes_importes": {
                "columna_nombre_persona": "nombreaño",
                "metrica_principal": "% COSTE DOCENCIA / COMPUTABLES PREVISTOS DIFF ?",
                "columnas_numericas": ['% COSTE DOCENCIA / COMPUTABLES PREVISTOS DIFF ?'],
            },
            "stg docentes": {
                "columna_nombre_persona": "nombre",
            },
            "stg docentescurso": {
                "metrica_principal": "SumaCosteLaboralTotal",
                "columnas_numericas": ['SumaCosteLaboralTotal'],
            },
        },
        "medidas_principales": [
            "COSTE LABORAL DOCENTE",
            "COSTE LABORAL DOCENTE COMPARACION",
            "COSTE LABORAL DOCENTE DIFF",
            "COSTE LABORAL DOCENTE EVOLUTION",
            "COSTE LABORAL DOCENTE DIFF %",
        ],
    },
    "CM Seguimiento F2024_OCU_Estatal": {
        "tabla_principal": "Fact_PlanesTipos_PlanesFormativos_Comunidades",
        "tabla_personas": "None",
        "tablas_fact": [
            "Fact_PlanesTipos_PlanesFormativos_Comunidades",
            "Fact_fx_ProyectosCentrosLineas_Valoracion",
            "Fact_Preinscripciones",
            "Fact_AlumnosGF",
        ],
        "mapeo_columnas": {
            "Dim_PlanesTipos_PlanesFormativos": {
                "columna_nombre_persona": "nombreplan",
            },
            "Dim_PlanesTipos": {
                "columna_nombre_persona": "Nombre",
            },
            "Fact_fx_ProyectosCentrosLineas_Valoracion": {
                "metrica_principal": "ParticipantesPrevistosAdaptacion",
                "columnas_numericas": ['ParticipantesPrevistosAdaptacion', 'Participantes', 'DuracionTotal'],
            },
            "Fact_Preinscripciones": {
                "columna_nombre_persona": "Desempleado",
            },
            "Dim_Provincia": {
                "columna_nombre_persona": "Nombre",
            },
            "Dim_fx_ProyectosCentrosLineas_Valoracion": {
                "metrica_principal": "DuracionTotal",
                "columnas_numericas": ['DuracionTotal'],
            },
            "Control Cierre Acciones": {
                "columna_nombre_persona": "preinscripcionesdesempleadosformula",
                "metrica_principal": "preinscripcionestotal",
                "columnas_numericas": ['preinscripcionestotal', 'preinscripcionestotalformula', 'preinscripcionestotalsinbajas'],
            },
            "Fact_AlumnosGF": {
                "metrica_principal": "CC_DuracionTotal",
                "columnas_numericas": ['CC_DuracionTotal'],
            },
        },
        "medidas_principales": [
            "cantidad",
            "Total Cantidad Comunidad",
            "% Cantidad",
            "cantidad (%)",
            "Cantidad Ocupados Sector",
        ],
    },
    "CM_Consulta Presencia": {
        "tabla_principal": "Fact_Ausencias",
        "tabla_personas": "Dim_Empleados",
        "tablas_fact": [
            "Fact_Ausencias",
        ],
        "mapeo_columnas": {
            "Fact_Ausencias": {
                "metrica_principal": "ABS_totalHours",
                "columnas_numericas": ['ABS_totalHours', 'Horas'],
            },
            "Dim_Empleados": {
                "columna_nombre_persona": "Empleado",
            },
        },
        "medidas_principales": [
            "Ausencia justificada",
            "Suma de Horas",
            "Acumulado de horas",
        ],
    },
    "CM_Ocupación Aulas": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "Ocupacion Aulas": {
                "columna_nombre_persona": "NombrePlan",
            },
        },
        "medidas_principales": [
            "RecuentoNombrePlan",
        ],
    },
    "CM_Seguimiento Proyectos Virtualización": {
        "tabla_principal": "Fact_Proyectos",
        "tabla_personas": "None",
        "tablas_fact": [
            "Fact_Proyectos",
            "Fact_Tareas",
            "Fact_Asignaciones",
            "Fact_PrevisionesDeLaAsignación",
            "Fact_PrevisionesDelProyecto",
            "Fact_PrevisionesDeLaTarea",
            "Fact_LíneasDelParteDeHoras",
            "Fact_ConjuntoDeDatosRealesDeLaLíneaDelParteDeHoras",
        ],
        "mapeo_columnas": {
            "Fact_Proyectos": {
                "columna_nombre_persona": "NombreDelTipoDeProyectoEmpresarial",
                "metrica_principal": "CosteRealDelProyecto",
                "columnas_numericas": ['CosteRealDelProyecto', 'CosteRealDeHorasExtraDelProyecto', 'TrabajoRealDeHorasExtraDelProyecto'],
            },
            "Fact_Tareas": {
                "columna_nombre_persona": "NombreDeLaTareaPrincipal",
                "metrica_principal": "CosteRealDeLaTarea",
                "columnas_numericas": ['CosteRealDeLaTarea', 'CosteFijoRealDeLaTarea', 'CosteDeHorasExtraRealDeLaTarea'],
            },
            "Fact_Asignaciones": {
                "columna_nombre_persona": "NombreDeLaReservaDeLaAsignación",
                "metrica_principal": "CosteRealDeLaAsignación",
                "columnas_numericas": ['CosteRealDeLaAsignación', 'CosteDeHorasExtraRealDeLaAsignación', 'TrabajoRealDeHorasExtraDeLaAsignación'],
            },
            "Fact_PrevisionesDeLaAsignación": {
                "columna_nombre_persona": "NombreDeProyecto",
                "metrica_principal": "CosteDelPresupuestoPrevistoDeLaAsignación",
                "columnas_numericas": ['CosteDelPresupuestoPrevistoDeLaAsignación', 'CostePrevistoDeLaAsignación'],
            },
            "Fact_PrevisionesDelProyecto": {
                "columna_nombre_persona": "NombreDeProyecto",
                "metrica_principal": "CostePresupuestadoPrevistoDelProyecto",
                "columnas_numericas": ['CostePresupuestadoPrevistoDelProyecto', 'CostePrevistoDelProyecto', 'CosteFijoPrevistoDelProyecto'],
            },
            "Fact_PrevisionesDeLaTarea": {
                "columna_nombre_persona": "NombreDeProyecto",
                "metrica_principal": "CosteDelPresupuestoPrevistoDeLaTarea",
                "columnas_numericas": ['CosteDelPresupuestoPrevistoDeLaTarea', 'CostePrevistoDeLaTarea', 'CosteFijoPrevistoDeLaTarea'],
            },
            "Fact_LíneasDelParteDeHoras": {
                "columna_nombre_persona": "NombreDeProyecto",
                "metrica_principal": "TrabajoRealDeHorasExtraFacturable",
                "columnas_numericas": ['TrabajoRealDeHorasExtraFacturable', 'TrabajoDeHorasExtraRealNoFacturable', 'TrabajoRealFacturable'],
            },
            "Fact_ConjuntoDeDatosRealesDeLaLíneaDelParteDeHoras": {
                "columna_nombre_persona": "NombreDelRecursoQueHaRealizadoElÚltimoCambio",
                "metrica_principal": "ÍndiceDeAjuste",
                "columnas_numericas": ['ÍndiceDeAjuste', 'TrabajoRealDeHorasExtraFacturable', 'TrabajoDeHorasExtraRealNoFacturable'],
            },
            "Dim_PartesDeHoras": {
                "columna_nombre_persona": "NombreDelPeríodo",
            },
            "Dim_Proyectos": {
                "columna_nombre_persona": "NombreDeProyecto",
            },
            "Dim_Tareas": {
                "columna_nombre_persona": "NombreDeLaTarea",
            },
            "Dim_Asignaciones": {
                "columna_nombre_persona": "NombreDeRecurso",
            },
        },
        "medidas_principales": [
            "Trabajo Del Proyecto",
            "Trabajo Real Del Proyecto",
            "Coste Real Del Proyecto",
            "PorcentajeCompletadoDelProyecto",
            "Coste Restante Del Proyecto",
        ],
    },
    "InformeSoporteTickets": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "Informe_SoporteTickets": {
                "columna_nombre_persona": "Nombre Mes Creación",
                "metrica_principal": "T. Resolucion Calculado (Horas)",
                "columnas_numericas": ['T. Resolucion Calculado (Horas)', 'T. Primera Respuesta Calculado (Horas)'],
            },
        },
        "medidas_principales": [
        ],
    },
    "Seguimiento Avance Ejecución": {
        "tabla_principal": "None",
        "tabla_personas": "None",
        "tablas_fact": [
        ],
        "mapeo_columnas": {
            "IMPORTES": {
                "metrica_principal": "ImporteSolicitadoHoraAlumno",
                "columnas_numericas": ['ImporteSolicitadoHoraAlumno', 'DuracionTotal', 'Participantes'],
            },
            "ProyectosCentros": {
                "columna_nombre_persona": "NombreAgrupacionSectorial",
            },
            "ImportesComputables": {
                "columna_nombre_persona": "NumeroDesempleadosVFVAAESA",
                "metrica_principal": "ImporteAlumnosVFVAAESA",
                "columnas_numericas": ['ImporteAlumnosVFVAAESA', 'ImporteTotalComputablesPrevistos', 'ImporteComputablesPrevistos'],
            },
        },
        "medidas_principales": [
            "% Comprometido / Obtenido",
            "DiferenciaImportesObtenido",
            "DiferenciaImportesComprometido",
            "DiferenciaCompro/Obteni",
            "FechaCompTXT",
        ],
    },
}


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
