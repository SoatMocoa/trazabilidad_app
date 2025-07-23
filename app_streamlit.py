import streamlit as st
from datetime import datetime, timedelta, date
from backend import database_operations as db_ops
import pandas as pd
import os
from utils.io_utils import export_df_to_csv
from dateutil.rrule import rrule, DAILY
from utils.date_utils import sumar_dias_habiles, calcular_dias_habiles_entre_fechas, parse_date, validate_future_date
from config.constants import (
    FACTURADORES, EPS_OPCIONES, AREA_SERVICIO_OPCIONES,
    ESTADO_AUDITORIA_OPCIONES, TIPO_ERROR_OPCIONES
)

# Configuración de la página de Streamlit
st.set_page_config(layout="wide")

# Inicializar tablas de la base de datos
db_ops.crear_tablas()

# Crear directorio 'data' si no existe
if not os.path.exists('data'):
    os.makedirs('data')

# Inicializar estados de sesión para la UI
if 'selected_invoice_input_key' not in st.session_state:
    st.session_state.selected_invoice_input_key = 0
if 'filter_text_key' not in st.session_state:
    st.session_state.filter_text_key = 0
if 'filter_select_key' not in st.session_state:
    st.session_state.filter_select_key = 0
if 'editing_factura_id' not in st.session_state:
    st.session_state.editing_factura_id = None
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'refacturar_mode' not in st.session_state:
    st.session_state.refacturar_mode = False
if 'current_invoice_data' not in st.session_state:
    st.session_state.current_invoice_data = None
if 'form_key' not in st.session_state:
    st.session_state.form_key = 0
if 'confirm_delete_id' not in st.session_state:
    st.session_state.confirm_delete_id = None

# Nuevas claves de sesión para los selectores de carga masiva
if 'bulk_facturador_key' not in st.session_state:
    st.session_state.bulk_facturador_key = 0
if 'bulk_eps_key' not in st.session_state:
    st.session_state.bulk_eps_key = 0
if 'bulk_area_servicio_key' not in st.session_state:
    st.session_state.bulk_area_servicio_key = 0


def login_page():
    """
    Muestra la página de inicio de sesión para la aplicación.
    """
    st.title("Iniciar Sesión - Trazabilidad de Facturas")
    with st.form("login_form"):
        username = st.text_input("Usuario:")
        password = st.text_input("Contraseña:", type="password")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            user_data = db_ops.obtener_credenciales_usuario(username)
            if user_data:
                db_password, user_role = user_data
                # NOTA: En un entorno real, esto DEBE ser un hash de la contraseña.
                if password == db_password: # Comparación de texto plano por ahora
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.session_state['user_role'] = user_role
                    st.success(f"Bienvenido, {username}! Tu rol es: {user_role}")
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta.")
            else:
                st.error("Usuario no encontrado.")

def main_app_page():
    """
    Muestra la página principal de la aplicación después de un inicio de sesión exitoso.
    """
    st.title("Trazabilidad de Facturas - Hospital Jose Maria Hernandez de Mocoa")
    user_role = st.session_state.get('user_role', 'guest')
    st.sidebar.header(f"Bienvenido, {st.session_state.get('username')} ({user_role})")

    if st.sidebar.button("Cerrar Sesión"):
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.session_state['user_role'] = None
        st.rerun()

    # Pestañas principales de la aplicación
    tab1, tab2, tab3 = st.tabs(["Ingreso Individual", "Carga Masiva", "Estadísticas"])

    with tab1:
        st.header("Ingreso de Factura Individual")
        display_invoice_entry_form(user_role)

    with tab2:
        st.header("Carga Masiva (Solo Número y Fecha)")
        display_bulk_load_section()

    with tab3:
        st.header("Estadísticas por Legalizador y EPS")
        display_statistics()

    st.header("Facturas Registradas")
    display_invoice_table(user_role)

def get_selectbox_default_index(options_list, current_value):
    """
    Calcula el índice por defecto para un st.selectbox.
    Retorna el índice del valor actual + 1 (por la opción vacía inicial),
    o 0 si no se encuentra o no hay valor actual.
    """
    if current_value:
        try:
            return options_list.index(current_value) + 1
        except ValueError:
            pass # Valor no encontrado, se usará el índice 0 (opción vacía)
    return 0

def _ensure_datetime_objects(factura_data):
    """
    Asegura que los campos de fecha/hora en los datos de la factura sean objetos datetime.date o datetime.datetime.
    Esto es necesario porque los datos de la base de datos pueden venir como cadenas.
    """
    if factura_data:
        # fecha_generacion (DATE)
        if isinstance(factura_data.get('fecha_generacion'), str):
            factura_data['fecha_generacion'] = parse_date(factura_data['fecha_generacion'])
        elif factura_data.get('fecha_generacion') is None:
            factura_data['fecha_generacion'] = date.today() # Valor por defecto si es None

        # fecha_reemplazo (DATE)
        if isinstance(factura_data.get('fecha_reemplazo'), str):
            factura_data['fecha_reemplazo'] = parse_date(factura_data['fecha_reemplazo'])
        
        # fecha_hora_entrega (TIMESTAMP)
        if isinstance(factura_data.get('fecha_hora_entrega'), str) and factura_data['fecha_hora_entrega']:
            try:
                factura_data['fecha_hora_entrega'] = datetime.strptime(factura_data['fecha_hora_entrega'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                factura_data['fecha_hora_entrega'] = None
        elif not isinstance(factura_data.get('fecha_hora_entrega'), datetime):
            factura_data['fecha_hora_entrega'] = None

        # fecha_entrega_radicador (TIMESTAMP)
        if isinstance(factura_data.get('fecha_entrega_radicador'), str) and factura_data['fecha_entrega_radicador']:
            try:
                factura_data['fecha_entrega_radicador'] = datetime.strptime(factura_data['fecha_entrega_radicador'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                factura_data['fecha_entrega_radicador'] = None
        elif not isinstance(factura_data.get('fecha_entrega_radicador'), datetime):
            factura_data['fecha_entrega_radicador'] = None

        # fecha_gen_original_linked (DATE)
        if isinstance(factura_data.get('fecha_gen_original_linked'), str) and factura_data['fecha_gen_original_linked']:
            factura_data['fecha_gen_original_linked'] = parse_date(factura_data['fecha_gen_original_linked'])
        elif not isinstance(factura_data.get('fecha_gen_original_linked'), date):
            factura_data['fecha_gen_original_linked'] = None # O date.today() si prefieres un valor por defecto

    return factura_data

def display_invoice_entry_form(user_role):
    """
    Muestra el formulario para ingresar o editar una factura individual.
    """
    # Determinar valores por defecto para el formulario en modo edición/refacturación
    current_data = st.session_state.current_invoice_data
    
    # Asegurarse de que las fechas sean objetos date o cadenas vacías
    fecha_generacion_val = current_data['fecha_generacion'].strftime('%Y-%m-%d') if current_data and isinstance(current_data.get('fecha_generacion'), date) else ""

    with st.form(key=f"invoice_entry_form_{st.session_state.form_key}", clear_on_submit=False):
        # Campos del formulario
        facturador = st.selectbox("Legalizador:", options=[""] + FACTURADORES, 
                                  index=get_selectbox_default_index(FACTURADORES, current_data.get('facturador')) if current_data and not st.session_state.refacturar_mode else 0, 
                                  disabled=st.session_state.refacturar_mode)

        eps = st.selectbox("EPS:", options=[""] + EPS_OPCIONES, 
                           index=get_selectbox_default_index(EPS_OPCIONES, current_data.get('eps')) if current_data and not st.session_state.refacturar_mode else 0, 
                           disabled=st.session_state.refacturar_mode)

        numero_factura = st.text_input("Número de Factura:", value=current_data['numero_factura'] if current_data and not st.session_state.refacturar_mode else "", disabled=st.session_state.refacturar_mode)
        fecha_generacion = st.text_input("Fecha de Generación (YYYY-MM-DD o DD/MM/YYYY):", value=fecha_generacion_val, disabled=st.session_state.refacturar_mode)

        area_servicio = st.selectbox("Área de Servicio:", options=[""] + AREA_SERVICIO_OPCIONES, 
                                     index=get_selectbox_default_index(AREA_SERVICIO_OPCIONES, current_data.get('area_servicio')) if current_data and not st.session_state.refacturar_mode else 0, 
                                     disabled=st.session_state.refacturar_mode)

        # Sección de refacturación
        if st.session_state.refacturar_mode:
            st.markdown("---")
            st.subheader("Datos de Refacturación")
            new_numero_factura = st.text_input("Nuevo Número de Factura:")
            # La fecha de reemplazo debería ser la fecha de generación de la nueva factura
            fecha_reemplazo_factura = st.text_input("Fecha de Generación de la Nueva Factura (YYYY-MM-DD o DD/MM/YYYY):", value=datetime.now().strftime('%Y-%m-%d'))
        else:
            new_numero_factura = None
            fecha_reemplazo_factura = None

        # Botón para marcar como corregida (solo para legalizadores y facturas devueltas)
        if user_role == 'legalizador' and current_data and current_data['estado_auditoria'] == 'Devuelta por Auditor':
            if st.form_submit_button("Marcar como Corregida"):
                marcar_como_corregida_action(st.session_state.editing_factura_id, current_data['observacion_auditor'], current_data['tipo_error'])
                st.rerun()

        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.refacturar_mode:
                submitted = st.form_submit_button("Guardar Factura Reemplazo")
            elif st.session_state.edit_mode:
                submitted = st.form_submit_button("Actualizar Factura")
            else:
                submitted = st.form_submit_button("Guardar Factura")
        with col2:
            if st.session_state.edit_mode or st.session_state.refacturar_mode:
                if st.form_submit_button("Cancelar Edición"):
                    cancelar_edicion_action()
                    st.rerun()

        # Lógica de envío del formulario
        if submitted:
            if st.session_state.refacturar_mode:
                guardar_factura_reemplazo_action(st.session_state.editing_factura_id, new_numero_factura, fecha_reemplazo_factura, facturador, eps, area_servicio)
            elif st.session_state.edit_mode:
                # Obtener datos originales para campos no editables en este formulario
                original_data = db_ops.obtener_factura_por_id(st.session_state.editing_factura_id)
                if original_data:
                    # Asegurarse de que los tipos de datos de fecha/hora sean correctos para la base de datos
                    original_data = _ensure_datetime_objects(original_data)
                    actualizar_factura_action(
                        st.session_state.editing_factura_id,
                        numero_factura,
                        area_servicio,
                        facturador,
                        fecha_generacion, # Viene del input del formulario
                        eps,
                        original_data['fecha_hora_entrega'],
                        original_data['tiene_correccion'],
                        original_data['descripcion_devolucion'],
                        original_data['fecha_devolucion_lider'],
                        original_data['revisado'],
                        original_data['factura_original_id'],
                        original_data['estado'],
                        original_data['reemplazada_por_numero_factura'],
                        original_data['estado_auditoria'], # Estos 3 son actualizados por auditor, no por este form
                        original_data['observacion_auditor'],
                        original_data['tipo_error'],
                        original_data['fecha_reemplazo']
                    )
                else:
                    st.error("Error: No se pudo recuperar la factura original para actualizar.")
            else:
                guardar_factura_action(facturador, eps, numero_factura, fecha_generacion, area_servicio)
            st.rerun()

def display_bulk_load_section():
    """
    Muestra la sección para la carga masiva de facturas desde un archivo CSV.
    """
    with st.form("bulk_load_form"):
        st.write("Por favor, selecciona el Legalizador, EPS y Área de Servicio para todas las facturas del CSV.")
        # Se añaden claves a los selectbox para poder resetearlos
        facturador_bulk = st.selectbox("Legalizador (CSV):", options=[""] + FACTURADORES, key=f"bulk_facturador_selector_{st.session_state.bulk_facturador_key}")
        eps_bulk = st.selectbox("EPS (CSV):", options=[""] + EPS_OPCIONES, key=f"bulk_eps_selector_{st.session_state.bulk_eps_key}")
        area_servicio_bulk = st.selectbox("Área de Servicio (CSV):", options=[""] + AREA_SERVICIO_OPCIONES, key=f"bulk_area_servicio_selector_{st.session_state.bulk_area_servicio_key}")
        
        st.write("Cargar archivo CSV (columnas requeridas: Numero de Factura, Fecha de Generacion)")
        uploaded_file = st.file_uploader("Cargar archivo CSV", type=["csv"])
        bulk_submitted = st.form_submit_button("Cargar desde CSV")

        if bulk_submitted and uploaded_file is not None:
            if not facturador_bulk or not eps_bulk or not area_servicio_bulk:
                st.error("Por favor, selecciona Legalizador, EPS y Área de Servicio para la carga masiva.")
                return

            inserted_count = 0
            skipped_count = 0
            total_rows = 0
            df = pd.read_csv(uploaded_file)

            required_columns_csv = ['Numero de Factura', 'Fecha de Generacion'] # Only these are required in CSV now
            if not all(col in df.columns for col in required_columns_csv):
                st.error(f"El archivo CSV debe contener las columnas: {', '.join(required_columns_csv)}.")
                return

            st.write(f"Iniciando carga masiva desde: {uploaded_file.name}")
            st.write(f"Facturador global: {facturador_bulk}, EPS global: {eps_bulk}, Área de Servicio global: {area_servicio_bulk}")

            for index, row in df.iterrows():
                total_rows += 1
                numero_factura_csv = str(row['Numero de Factura']).strip()
                fecha_str_csv = str(row['Fecha de Generacion']).strip()

                if not numero_factura_csv.isdigit():
                    st.warning(f"Fila {index+2}: Número de factura '{numero_factura_csv}' no es numérico. Saltando.")
                    skipped_count += 1
                    continue

                fecha_generacion_csv_obj = parse_date(fecha_str_csv, f"Fecha de Generación (Fila {index+2})")
                if fecha_generacion_csv_obj is None:
                    skipped_count += 1
                    continue
                if not validate_future_date(fecha_generacion_csv_obj, f"Fecha de Generación (Fila {index+2})"):
                    skipped_count += 1
                    continue

                # Convertir fecha_generacion_csv_obj a datetime.date para la DB
                fecha_generacion_db = fecha_generacion_csv_obj

                # fecha_hora_entrega debe ser un objeto datetime
                fecha_hora_entrega_db = datetime.now()

                # Usar la función guardar_factura con los valores globales seleccionados
                factura_id = db_ops.guardar_factura(
                    numero_factura=numero_factura_csv, 
                    area_servicio=area_servicio_bulk, 
                    facturador=facturador_bulk, 
                    fecha_generacion=fecha_generacion_db, 
                    eps=eps_bulk, 
                    fecha_hora_entrega=fecha_hora_entrega_db
                )

                if factura_id:
                    if area_servicio_bulk == "SOAT":
                        # Usar la función guardar_detalles_soat (ya no insertar_detalles_soat_bulk)
                        db_ops.guardar_detalles_soat(factura_id, fecha_generacion_db)
                    inserted_count += 1
                else:
                    skipped_count += 1
                    st.info(f"Fila {index+2}: Factura '{numero_factura_csv}' ya existe o hubo un error al insertar. Saltando.")

            st.success(f"Carga masiva finalizada.\nTotal de filas procesadas: {total_rows}\nFacturas insertadas: {inserted_count}\nFacturas omitidas (duplicadas/errores): {skipped_count}")
            invalidate_facturas_cache()
            # Resetear claves de selectbox para limpiar sus valores
            st.session_state.bulk_facturador_key += 1
            st.session_state.bulk_eps_key += 1
            st.session_state.bulk_area_servicio_key += 1
            st.rerun()

def display_statistics():
    """
    Muestra las estadísticas generales y por legalizador/EPS.
    """
    st.subheader("Estadísticas Generales de Facturas")
    col_radicadas, col_errores, col_pendientes, col_total = st.columns(4)

    total_radicadas = db_ops.obtener_conteo_facturas_radicadas_ok()
    total_errores = db_ops.obtener_conteo_facturas_con_errores()
    total_pendientes = db_ops.obtener_conteo_facturas_pendientes_global()
    total_general = db_ops.obtener_conteo_total_facturas()

    with col_radicadas:
        st.metric(label="Facturas Radicadas (OK)", value=total_radicadas)
    with col_errores:
        st.metric(label="Facturas con Errores", value=total_errores)
    with col_pendientes:
        st.metric(label="Facturas Pendientes", value=total_pendientes)
    with col_total:
        st.metric(label="Total General de Facturas", value=total_general)

    st.markdown("---")
    st.subheader("Conteo por Legalizador y EPS (Facturas Pendientes)")
    stats = db_ops.obtener_conteo_facturas_por_legalizador_y_eps()

    if stats:
        df_stats = pd.DataFrame(stats, columns=["Legalizador", "EPS", "Facturas Pendientes"])
        st.dataframe(df_stats, use_container_width=True, hide_index=True)
    else:
        st.info("No hay estadísticas disponibles de facturas pendientes.")

def highlight_rows(row):
    """
    Define el estilo de resaltado para las filas de la tabla de facturas.
    """
    styles = [''] * len(row)
    if row["Estado Auditoria"] == 'Devuelta por Auditor':
        styles = ['background-color: lightblue'] * len(row)
    elif row["Estado Auditoria"] == 'Corregida por Legalizador':
        styles = ['background-color: lightsalmon'] * len(row)
    elif row["Días Restantes"] == "Refacturar":
        styles = ['background-color: salmon'] * len(row)
    elif isinstance(row["Días Restantes"], (int, float)):
        if 1 <= row["Días Restantes"] <= 3:
            styles = ['background-color: yellow'] * len(row)
        elif row["Días Restantes"] > 3:
            styles = ['background-color: lightgreen'] * len(row)
    return styles

@st.cache_data(ttl=60)
def get_cached_facturas(search_term, search_column):
    """
    Obtiene las facturas de la base de datos y las almacena en caché.
    """
    return db_ops.cargar_facturas(search_term, search_column)

def invalidate_facturas_cache():
    """
    Invalida la caché de facturas para forzar una nueva carga de datos.
    """
    get_cached_facturas.clear()

def display_invoice_table(user_role):
    """
    Muestra la tabla de facturas registradas con opciones de búsqueda y acciones.
    """
    col_search, col_criteria = st.columns([3, 2])
    with col_search:
        search_term_input = st.text_input("Buscar:", value="", key=f"search_input_widget_{st.session_state.filter_text_key}")
    with col_criteria:
        options_criteria = ["Numero de Factura", "Legalizador", "EPS", "Area de Servicio", "Estado Auditoria"]
        search_criterion_selectbox = st.selectbox("Buscar por:", options=options_criteria, index=0, key=f"search_criteria_widget_{st.session_state.filter_select_key}")

    current_search_term = st.session_state.get(f'search_input_widget_{st.session_state.filter_text_key}', '').strip()
    current_search_criterion = st.session_state.get(f'search_criteria_widget_{st.session_state.filter_select_key}', 'Numero de Factura')

    db_column_name = {
        "Numero de Factura": "numero_factura",
        "Legalizador": "facturador",
        "EPS": "eps",
        "Area de Servicio": "area_servicio",
        "Estado Auditoria": "estado_auditoria"
    }.get(current_search_criterion)

    facturas_raw = get_cached_facturas(search_term=current_search_term, search_column=db_column_name)

    processed_facturas = []
    hoy_obj = date.today() # Usar date.today() para comparar solo fechas

    for factura in facturas_raw:
        # Asegurar que los objetos de fecha/hora sean del tipo correcto
        factura = _ensure_datetime_objects(factura)

        # Acceder a los datos por nombre de columna (diccionario)
        factura_id = factura.get('id')
        numero_factura_base = factura.get('numero_factura', '')
        area_servicio = factura.get('area_servicio', '')
        facturador_nombre = factura.get('facturador', '')
        fecha_generacion_base_obj = factura.get('fecha_generacion') 
        eps_nombre = factura.get('eps', '')
        fecha_hora_entrega = factura.get('fecha_hora_entrega') 
        estado_factura = factura.get('estado', '')
        reemplazada_por_numero = factura.get('reemplazada_por_numero_factura', '')
        estado_auditoria_db = factura.get('estado_auditoria', 'Pendiente')
        observacion_auditor_db = factura.get('observacion_auditor', '')
        tipo_error_db = factura.get('tipo_error', '')
        fecha_reemplazo_db_val = factura.get('fecha_reemplazo') 
        num_fact_original_linked = factura.get('num_fact_original_linked', '')
        fecha_gen_original_linked_obj = factura.get('fecha_gen_original_linked') 
        fecha_entrega_radicador_db = factura.get('fecha_entrega_radicador') 

        # Cálculo de días restantes
        fecha_limite_liquidacion_obj = sumar_dias_habiles(fecha_generacion_base_obj, 21)
        dias_restantes_liquidacion = 0
        if hoy_obj <= fecha_limite_liquidacion_obj:
            dias_restantes_liquidacion = calcular_dias_habiles_entre_fechas(hoy_obj, fecha_limite_liquidacion_obj)
        else:
            dias_pasados_del_limite = calcular_dias_habiles_entre_fechas(fecha_limite_liquidacion_obj, hoy_obj)
            dias_restantes_liquidacion = -dias_pasados_del_limite

        # Lógica para mostrar números y fechas de factura original/reemplazo
        display_numero_factura_col = ""
        display_numero_reemplazo_col = ""
        display_fecha_generacion_actual_col = ""
        display_fecha_reemplazo_display = ""

        if factura.get('factura_original_id') is not None: # Es una factura de reemplazo
            display_numero_factura_col = num_fact_original_linked
            display_numero_reemplazo_col = numero_factura_base
            display_fecha_generacion_actual_col = fecha_gen_original_linked_obj.strftime('%Y-%m-%d') if fecha_gen_original_linked_obj else ""
            display_fecha_reemplazo_display = fecha_generacion_base_obj.strftime('%Y-%m-%d') if fecha_generacion_base_obj else "" # Fecha de la nueva factura
        elif estado_factura == 'Reemplazada': # Es la factura original que fue reemplazada
            display_numero_factura_col = numero_factura_base
            display_numero_reemplazo_col = reemplazada_por_numero
            display_fecha_generacion_actual_col = fecha_generacion_base_obj.strftime('%Y-%m-%d') if fecha_generacion_base_obj else ""
            display_fecha_reemplazo_display = fecha_reemplazo_db_val.strftime('%Y-%m-%d') if fecha_reemplazo_db_val else ""
        else: # Factura normal, no de reemplazo ni reemplazada
            display_numero_factura_col = numero_factura_base
            display_numero_reemplazo_col = ""
            display_fecha_generacion_actual_col = fecha_generacion_base_obj.strftime('%Y-%m-%d') if fecha_generacion_base_obj else ""
            display_fecha_reemplazo_display = ""

        display_dias_restantes = dias_restantes_liquidacion
        display_estado_for_tree = estado_factura

        if dias_restantes_liquidacion < 0 and estado_auditoria_db not in ['Devuelta por Auditor', 'Corregida por Legalizador']:
            display_dias_restantes = "Refacturar"
            display_estado_for_tree = "Vencidas"
        elif dias_restantes_liquidacion == 0 and estado_auditoria_db not in ['Devuelta por Auditor', 'Corregida por Legalizador']:
            display_dias_restantes = "Hoy Vence"
            display_estado_for_tree = "Vencidas" # O un estado específico para hoy vence

        processed_facturas.append({
            "ID": factura_id,
            "Área de Servicio": area_servicio,
            "Facturador": facturador_nombre,
            "EPS": eps_nombre,
            "Número de Factura": display_numero_factura_col,
            "Número Reemplazo Factura": display_numero_reemplazo_col,
            "Fecha Generación": display_fecha_generacion_actual_col,
            "Fecha Reemplazo Factura": display_fecha_reemplazo_display,
            "Fecha de Entrega": fecha_hora_entrega.strftime('%Y-%m-%d %H:%M:%S') if fecha_hora_entrega else "",
            "Días Restantes": display_dias_restantes,
            "Estado": display_estado_for_tree,
            "Estado Auditoria": estado_auditoria_db,
            "Tipo de Error": tipo_error_db,
            "Observación Auditor": observacion_auditor_db,
            "Fecha Entrega Radicador": fecha_entrega_radicador_db.strftime('%Y-%m-%d %H:%M:%S') if fecha_entrega_radicador_db else ""
        })
    df_facturas = pd.DataFrame(processed_facturas)

    if not df_facturas.empty:
        def get_sort_key(row):
            if row["Estado Auditoria"] == 'Devuelta por Auditor': return 1
            elif row["Estado Auditoria"] == 'Corregida por Legalizador': return 2
            elif row["Días Restantes"] == "Refacturar": return 3
            else: return 4
        
        df_facturas['sort_key'] = df_facturas.apply(get_sort_key, axis=1)
        df_facturas = df_facturas.sort_values(by=['sort_key', 'Fecha Generación'], ascending=[True, False])
        df_facturas = df_facturas.drop(columns=['sort_key'])
        
        st.dataframe(df_facturas.style.apply(highlight_rows, axis=1), use_container_width=True, hide_index=True)
    else:
        st.info("No hay facturas registradas que coincidan con los criterios de búsqueda.")

    # Sección de acciones para factura seleccionada
    col_export, col_edit, col_refacturar, col_delete_placeholder = st.columns(4)
    with col_export:
        if st.button("Exportar a CSV"):
            export_df_to_csv(df_facturas)

    selected_invoice_id = st.number_input("ID de Factura para Acción:", min_value=0, step=1, key=f"selected_invoice_id_input_{st.session_state.selected_invoice_input_key}")

    if selected_invoice_id > 0:
        factura_data_for_action = db_ops.obtener_factura_por_id(selected_invoice_id)
        if factura_data_for_action:
            # Asegurar que los tipos de datos de fecha/hora sean correctos al cargar en session_state
            factura_data_for_action = _ensure_datetime_objects(factura_data_for_action)

            st.session_state.current_invoice_data = factura_data_for_action
            with col_edit:
                if st.button("Cargar para Edición", key="edit_button"):
                    cargar_factura_para_edicion_action(selected_invoice_id)
                    st.rerun()
            with col_refacturar:
                # Solo mostrar botón "Refacturar" si la factura está vencida
                if not df_facturas.empty and selected_invoice_id in df_facturas['ID'].values:
                    # Obtener el estado de "Días Restantes" de la factura seleccionada
                    dias_restantes_df = df_facturas[df_facturas['ID'] == selected_invoice_id]['Días Restantes'].iloc[0]
                    if dias_restantes_df == "Refacturar":
                        if st.button("Refacturar", key="refacturar_button"):
                            cargar_factura_para_refacturar_action(selected_invoice_id)
                            st.rerun()
            
            # Acciones de auditoría (solo para rol 'auditor')
            if user_role == 'auditor':
                st.markdown("---")
                st.subheader("Acciones de Auditoría para Factura Seleccionada")
                
                estado_auditoria_default_index = 0
                if st.session_state.current_invoice_data and st.session_state.current_invoice_data['estado_auditoria']:
                    try:
                        estado_auditoria_default_index = ESTADO_AUDITORIA_OPCIONES.index(st.session_state.current_invoice_data['estado_auditoria'])
                    except ValueError:
                        estado_auditoria_default_index = 0
                
                tipo_error_default_index = 0
                if st.session_state.current_invoice_data and st.session_state.current_invoice_data['tipo_error']:
                    try:
                        tipo_error_default_index = TIPO_ERROR_OPCIONES.index(st.session_state.current_invoice_data['tipo_error'])
                    except ValueError:
                        tipo_error_default_index = 0

                with st.form(key=f"auditoria_form_{selected_invoice_id}", clear_on_submit=False):
                    estado_auditoria_input = st.selectbox("Estado Auditoría:", options=ESTADO_AUDITORIA_OPCIONES, index=estado_auditoria_default_index, key=f"estado_auditoria_{selected_invoice_id}")
                    tipo_error_input = st.selectbox("Tipo de Error:", options=TIPO_ERROR_OPCIONES, index=tipo_error_default_index, key=f"tipo_error_{selected_invoice_id}")
                    observacion_auditor_input = st.text_area("Observación Auditor:", value=st.session_state.current_invoice_data['observacion_auditor'] if st.session_state.current_invoice_data and st.session_state.current_invoice_data['observacion_auditor'] else "", key=f"observacion_auditor_{selected_invoice_id}")
                    
                    submit_auditoria = st.form_submit_button("Auditar Factura")
                    if submit_auditoria:
                        auditar_factura_action(selected_invoice_id, estado_auditoria_input, observacion_auditor_input, tipo_error_input)
                        st.rerun()

                # Checkbox para fecha de entrega al radicador
                fecha_entrega_radicador_val = st.session_state.current_invoice_data['fecha_entrega_radicador']
                fecha_entrega_radicador_checked = st.checkbox(
                    "Factura Entregada al Radicador",
                    value=bool(fecha_entrega_radicador_val),
                    key=f"radicador_checkbox_{selected_invoice_id}"
                )
                # Si el estado del checkbox cambia, actualizar la DB
                if fecha_entrega_radicador_checked != bool(fecha_entrega_radicador_val):
                    actualizar_fecha_entrega_radicador_action(selected_invoice_id, fecha_entrega_radicador_checked)
                    st.rerun()

                # Botón de eliminar factura
                if st.button("Eliminar Factura", key=f"delete_button_{selected_invoice_id}"):
                    st.session_state.confirm_delete_id = selected_invoice_id
                
                # Confirmación de eliminación (modal simulada)
                if 'confirm_delete_id' in st.session_state and st.session_state.confirm_delete_id == selected_invoice_id:
                    st.warning(f"¿Estás seguro de que quieres eliminar la factura ID: {selected_invoice_id}?\nEsta acción es irreversible.")
                    col_confirm_del, col_cancel_del = st.columns(2)
                    with col_confirm_del:
                        if st.button("Confirmar Eliminación", key="confirm_delete_button_modal"):
                            success = eliminar_factura_action(selected_invoice_id)
                            if success:
                                st.success(f"Factura ID: {selected_invoice_id} eliminada correctamente.")
                                st.session_state.confirm_delete_id = None
                                invalidate_facturas_cache()
                                cancelar_edicion_action() # Resetear el formulario y la selección
                            else:
                                st.error("No se pudo eliminar la factura.")
                            st.rerun()
                    with col_cancel_del:
                        if st.button("Cancelar", key="cancel_delete_button_modal"):
                            st.info("Eliminación cancelada.")
                            st.session_state.confirm_delete_id = None
                            st.rerun()
        else:
            st.warning("ID de factura no encontrado.")
            st.session_state.current_invoice_data = None
    else:
        # Asegurarse de que el estado de la factura seleccionada se limpie si el ID es 0 o no válido
        if st.session_state.current_invoice_data is not None and selected_invoice_id == 0:
            cancelar_edicion_action() # Limpiar el estado si el usuario deselecciona la factura
            st.rerun()


def guardar_factura_action(facturador, eps, numero_factura, fecha_generacion_str, area_servicio):
    """
    Función de acción para guardar una nueva factura.
    """
    if not all([facturador, eps, numero_factura, fecha_generacion_str, area_servicio]):
        st.error("Todos los campos son obligatorios.")
        return

    if not numero_factura.isdigit():
        st.error("El campo 'Número de Factura' debe contener solo números.")
        return

    fecha_generacion_obj = parse_date(fecha_generacion_str, "Fecha de Generación")
    if fecha_generacion_obj is None:
        return
    if not validate_future_date(fecha_generacion_obj, "Fecha de Generación"):
        return

    # Convertir a datetime.date para la base de datos
    fecha_generacion_db = fecha_generacion_obj

    # fecha_hora_entrega debe ser un objeto datetime
    fecha_hora_entrega = datetime.now()

    factura_id = db_ops.guardar_factura(
        numero_factura=numero_factura, 
        area_servicio=area_servicio, 
        facturador=facturador, 
        fecha_generacion=fecha_generacion_db, 
        eps=eps, 
        fecha_hora_entrega=fecha_hora_entrega
    )

    if factura_id:
        if area_servicio == "SOAT":
            db_ops.guardar_detalles_soat(factura_id, fecha_generacion_db)
        st.success("Factura guardada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action() # Resetear el formulario después de guardar
    else:
        st.error(f"La factura con número '{numero_factura}' ya existe con la misma combinación de Legalizador, EPS y Área de Servicio.")

def actualizar_factura_action(factura_id, numero_factura, area_servicio, facturador, fecha_generacion_str, eps,
                               fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                               fecha_devolucion_lider, revisado, factura_original_id, estado,
                               reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
    """
    Función de acción para actualizar una factura existente.
    """
    if not all([factura_id, numero_factura, area_servicio, facturador, fecha_generacion_str, eps]):
        st.error("Todos los campos son obligatorios para la actualización.")
        return

    if not numero_factura.isdigit():
        st.error("El campo 'Número de Factura' debe contener solo números.")
        return

    fecha_generacion_obj = parse_date(fecha_generacion_str, "Fecha de Generación")
    if fecha_generacion_obj is None:
        return
    if not validate_future_date(fecha_generacion_obj, "Fecha de Generación"):
        return

    # Convertir a datetime.date para la base de datos
    fecha_generacion_db = fecha_generacion_obj

    success = db_ops.actualizar_factura(
        factura_id,
        numero_factura,
        area_servicio,
        facturador,
        fecha_generacion_db, # Usar el objeto date
        eps,
        fecha_hora_entrega,
        tiene_correccion,
        descripcion_devolucion,
        fecha_devolucion_lider,
        revisado,
        factura_original_id,
        estado,
        reemplazada_por_numero_factura,
        estado_auditoria,
        observacion_auditor,
        tipo_error,
        fecha_reemplazo # Ya es un objeto date/None
    )

    if success:
        st.success("Factura actualizada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error(f"No se pudo actualizar la factura. El número de factura '{numero_factura}' ya podría existir con la misma combinación de Legalizador, EPS y Área de Servicio.")

def cargar_factura_para_edicion_action(factura_id):
    """
    Carga los datos de una factura para su edición en el formulario.
    """
    factura_data = db_ops.obtener_factura_por_id(factura_id)
    if factura_data:
        # Asegurar que los tipos de datos de fecha/hora sean correctos al cargar en session_state
        factura_data = _ensure_datetime_objects(factura_data)

        st.session_state.editing_factura_id = factura_id
        st.session_state.edit_mode = True
        st.session_state.refacturar_mode = False
        st.session_state.current_invoice_data = factura_data
        st.session_state.form_key += 1 # Cambiar la clave del formulario para forzar su re-renderizado
        st.success(f"Factura {factura_data['numero_factura']} cargada para edición.")
    else:
        st.error("No se pudo cargar la factura para edición.")

def cargar_factura_para_refacturar_action(factura_id):
    """
    Carga los datos de una factura para el proceso de refacturación.
    """
    factura_data = db_ops.obtener_factura_por_id(factura_id)
    if factura_data:
        # Asegurar que los tipos de datos de fecha/hora sean correctos al cargar en session_state
        factura_data = _ensure_datetime_objects(factura_data)

        st.session_state.editing_factura_id = factura_id
        st.session_state.edit_mode = False
        st.session_state.refacturar_mode = True
        st.session_state.current_invoice_data = factura_data
        st.session_state.form_key += 1 # Cambiar la clave del formulario para forzar su re-renderizado
        st.warning(f"Factura {factura_data['numero_factura']} cargada para refacturar. Ingrese el nuevo número de factura.")
    else:
        st.error("No se pudo cargar la factura para refacturar.")

def auditar_factura_action(factura_id, nuevo_estado_auditoria, observacion, tipo_error):
    """
    Función de acción para que un auditor actualice el estado de una factura.
    """
    if st.session_state.user_role != 'auditor':
        st.error("Permiso Denegado. Solo los auditores pueden auditar facturas.")
        return

    if nuevo_estado_auditoria == "Devuelta por Auditor" and not tipo_error:
        st.error("Si el estado es 'Devuelta por Auditor', debe especificar el 'Tipo de Error'.")
        return

    observacion_to_save = observacion if observacion else None
    tipo_error_to_save = tipo_error if tipo_error else None

    success = db_ops.actualizar_estado_auditoria_factura(factura_id, nuevo_estado_auditoria, observacion_to_save, tipo_error_to_save)
    if success:
        st.success(f"Estado de auditoría de factura actualizado a '{nuevo_estado_auditoria}'.")
        invalidate_facturas_cache()
        cancelar_edicion_action() # Resetear el formulario y la selección
    else:
        st.error("No se pudo actualizar el estado de auditoría de la factura.")

def eliminar_factura_action(factura_id):
    """
    Función de acción para eliminar una factura.
    """
    if st.session_state.user_role != 'auditor':
        st.error("Permiso Denegado. Solo los auditores pueden eliminar facturas.")
        return False
    
    success = db_ops.eliminar_factura(factura_id)
    return success

def guardar_factura_reemplazo_action(old_factura_id, new_numero_factura, fecha_reemplazo_factura_str, facturador, eps, area_servicio):
    """
    Función de acción para guardar una factura de reemplazo.
    """
    if not new_numero_factura:
        st.error("El campo 'Nuevo Número de Factura' es obligatorio.")
        return
    if not new_numero_factura.isdigit():
        st.error("El 'Nuevo Número de Factura' debe contener solo números.")
        return

    fecha_reemplazo_factura_obj = parse_date(fecha_reemplazo_factura_str, "Fecha de Generación de la Nueva Factura")
    if fecha_reemplazo_factura_obj is None:
        return
    if not validate_future_date(fecha_reemplazo_factura_obj, "Fecha de Generación de la Nueva Factura"):
        return
    
    # new_fecha_generacion es la fecha de generación de la NUEVA factura (objeto date)
    new_fecha_generacion_db = fecha_reemplazo_factura_obj

    factura_original_data = db_ops.obtener_factura_por_id(old_factura_id)
    if not factura_original_data:
        st.error("No se pudo obtener la información de la factura original para el reemplazo.")
        return

    # Los campos area_servicio, facturador, eps para la nueva factura de reemplazo
    # se toman del formulario, no de la factura original si el usuario los cambió.
    # La fecha_reemplazo es la fecha en que se realiza el reemplazo (fecha de la nueva factura).
    success = db_ops.guardar_factura_reemplazo(
        old_factura_id,
        new_numero_factura,
        new_fecha_generacion_db, # Fecha de generación de la nueva factura (objeto date)
        factura_original_data['area_servicio'], # Mantener el área de servicio de la original
        factura_original_data['facturador'], # Mantener el facturador de la original
        factura_original_data['eps'], # Mantener la EPS de la original
        datetime.now().date() # Fecha actual del reemplazo (objeto date)
    )

    if success:
        st.success(f"Factura reemplazada por {new_numero_factura} correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error(f"No se pudo guardar la factura de reemplazo. El número '{new_numero_factura}' ya podría existir.")

def marcar_como_corregida_action(factura_id, observacion_actual, tipo_error_actual):
    """
    Función de acción para que un legalizador marque una factura como corregida.
    """
    if st.session_state.user_role != 'legalizador':
        st.error("Permiso Denegado. Solo los legalizadores pueden marcar facturas como corregidas.")
        return
    
    success = db_ops.actualizar_estado_auditoria_factura(factura_id, "Corregida por Legalizador", observacion_actual, tipo_error_actual)
    if success:
        st.success(f"Factura ID: {factura_id} marcada como 'Corregida por Legalizador'.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error("No se pudo marcar la factura como corregida.")

def actualizar_fecha_entrega_radicador_action(factura_id, set_date):
    """
    Función de acción para actualizar la fecha de entrega al radicador.
    """
    fecha_entrega = datetime.now() if set_date else None # Objeto datetime o None
    success = db_ops.actualizar_fecha_entrega_radicador(factura_id, fecha_entrega)
    if success:
        st.success("Fecha de entrega al radicador actualizada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error("No se pudo actualizar la fecha de entrega al radicador.")

def cancelar_edicion_action():
    """
    Resetea el estado de edición/refacturación y limpia los datos del formulario.
    """
    st.session_state.editing_factura_id = None
    st.session_state.edit_mode = False
    st.session_state.refacturar_mode = False
    st.session_state.current_invoice_data = None
    st.session_state.form_key += 1 # Forzar la recreación del formulario
    if 'confirm_delete_id' in st.session_state:
        st.session_state.confirm_delete_id = None
    
    # Incrementar las claves de los widgets de búsqueda para resetearlos visualmente
    st.session_state.selected_invoice_input_key += 1
    st.session_state.filter_text_key += 1
    st.session_state.filter_select_key += 1

# Lógica principal de la aplicación: mostrar login o la app principal
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if st.session_state['logged_in']:
    main_app_page()
else:
    login_page()
