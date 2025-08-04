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

st.set_page_config(layout="wide")

# Inicializa las tablas y los usuarios por defecto
db_ops.crear_tablas()

if not os.path.exists('data'):
    os.makedirs('data')

# --- Gestión del Estado de la Sesión ---
# Centralizamos la inicialización del estado en una sola función para mayor claridad
def initialize_session_state():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if 'username' not in st.session_state:
        st.session_state['username'] = None
    if 'user_role' not in st.session_state:
        st.session_state['user_role'] = None
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
    if 'bulk_facturador' not in st.session_state:
        st.session_state.bulk_facturador = ""
    if 'bulk_eps' not in st.session_state:
        st.session_state.bulk_eps = ""
    if 'bulk_area_servicio' not in st.session_state:
        st.session_state.bulk_area_servicio = ""
    if 'search_term' not in st.session_state:
        st.session_state.search_term = ""
    if 'search_criteria' not in st.session_state:
        st.session_state.search_criteria = "Numero de Factura"

initialize_session_state()

def login_page():
    st.title("Iniciar Sesión - Trazabilidad de Facturas")
    with st.form("login_form"):
        username = st.text_input("Usuario:")
        password = st.text_input("Contraseña:", type="password")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            user_data = db_ops.obtener_credenciales_usuario(username)
            if user_data:
                db_password_hashed, user_role = user_data
                # --- CAMBIO CRÍTICO DE SEGURIDAD ---
                # Usamos la función de bcrypt para verificar la contraseña hasheada.
                if db_ops.check_password(password, db_password_hashed):
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
    st.title("Trazabilidad de Facturas - Hospital Jose Maria Hernandez de Mocoa")
    user_role = st.session_state.get('user_role', 'guest')
    st.sidebar.header(f"Bienvenido, {st.session_state.get('username')} ({user_role})")

    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.clear()
        st.rerun()

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
    if current_value:
        try:
            # Aseguramos que la lista tenga un valor por defecto al principio.
            return options_list.index(current_value) + 1
        except ValueError:
            pass
    return 0

def _ensure_datetime_objects(factura_data):
    if not factura_data:
        return {}

    # Convertir las fechas a objetos datetime si son strings
    for key in ['fecha_generacion', 'fecha_reemplazo']:
        if isinstance(factura_data.get(key), str):
            factura_data[key] = parse_date(factura_data[key])
        elif factura_data.get(key) is None:
            factura_data[key] = date.today()

    for key in ['fecha_hora_entrega', 'fecha_entrega_radicador']:
        value = factura_data.get(key)
        if isinstance(value, str):
            try:
                factura_data[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                factura_data[key] = None
        elif not isinstance(value, datetime):
            factura_data[key] = None

    if isinstance(factura_data.get('fecha_gen_original_linked'), str):
        factura_data['fecha_gen_original_linked'] = parse_date(factura_data['fecha_gen_original_linked'])
    elif not isinstance(factura_data.get('fecha_gen_original_linked'), date):
        factura_data['fecha_gen_original_linked'] = None

    return factura_data

def _process_factura_for_display(factura):
    factura = _ensure_datetime_objects(factura)

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

    hoy_obj = date.today()

    dias_restantes_liquidacion = "N/A"
    display_estado_for_tree = estado_factura
    
    if fecha_generacion_base_obj:
        fecha_limite_liquidacion_obj = sumar_dias_habiles(fecha_generacion_base_obj, 21)
        if hoy_obj <= fecha_limite_liquidacion_obj:
            dias_restantes_liquidacion = calcular_dias_habiles_entre_fechas(hoy_obj, fecha_limite_liquidacion_obj)
        else:
            dias_pasados_del_limite = calcular_dias_habiles_entre_fechas(fecha_limite_liquidacion_obj, hoy_obj)
            dias_restantes_liquidacion = -dias_pasados_del_limite
            
        if dias_restantes_liquidacion < 0 and estado_auditoria_db not in ['Devuelta por Auditor', 'Corregida por Legalizador', 'En Radicador']:
            dias_restantes_liquidacion = "Refacturar"
            display_estado_for_tree = "Vencidas"
        elif dias_restantes_liquidacion == 0 and estado_auditoria_db not in ['Devuelta por Auditor', 'Corregida por Legalizador', 'En Radicador']:
            dias_restantes_liquidacion = "Hoy Vence"
            display_estado_for_tree = "Vencidas"

    display_numero_factura_col = numero_factura_base
    display_numero_reemplazo_col = ""
    display_fecha_generacion_actual_col = fecha_generacion_base_obj.strftime('%Y-%m-%d') if fecha_generacion_base_obj else ""
    display_fecha_reemplazo_display = ""
    
    if factura.get('factura_original_id') is not None:
        display_numero_factura_col = num_fact_original_linked or ""
        display_numero_reemplazo_col = numero_factura_base
        display_fecha_generacion_actual_col = fecha_gen_original_linked_obj.strftime('%Y-%m-%d') if fecha_gen_original_linked_obj else ""
        display_fecha_reemplazo_display = fecha_generacion_base_obj.strftime('%Y-%m-%d') if fecha_generacion_base_obj else ""
    elif estado_factura == 'Reemplazada':
        display_numero_factura_col = numero_factura_base
        display_numero_reemplazo_col = reemplazada_por_numero or ""
        display_fecha_generacion_actual_col = fecha_generacion_base_obj.strftime('%Y-%m-%d') if fecha_generacion_base_obj else ""
        display_fecha_reemplazo_display = fecha_reemplazo_db_val.strftime('%Y-%m-%d') if fecha_reemplazo_db_val else ""
    
    return {
        "ID": factura_id,
        "Área de Servicio": area_servicio,
        "Facturador": facturador_nombre,
        "EPS": eps_nombre,
        "Número de Factura": display_numero_factura_col,
        "Número Reemplazo Factura": display_numero_reemplazo_col,
        "Fecha Generación": display_fecha_generacion_actual_col,
        "Fecha Reemplazo Factura": display_fecha_reemplazo_display,
        "Fecha de Entrega": fecha_hora_entrega.strftime('%Y-%m-%d %H:%M:%S') if fecha_hora_entrega else "",
        "Días Restantes": dias_restantes_liquidacion,
        "Estado": display_estado_for_tree,
        "Estado Auditoria": estado_auditoria_db,
        "Tipo de Error": tipo_error_db,
        "Observación Auditor": observacion_auditor_db,
        "Fecha Entrega Radicador": fecha_entrega_radicador_db.strftime('%Y-%m-%d %H:%M:%S') if fecha_entrega_radicador_db else ""
    }

def display_invoice_entry_form(user_role):
    # Lógica de carga de datos iniciales
    if st.session_state.edit_mode or st.session_state.refacturar_mode:
        current_data = st.session_state.current_invoice_data
        fecha_generacion_val = current_data['fecha_generacion'].strftime('%Y-%m-%d') if isinstance(current_data.get('fecha_generacion'), date) else ""
        new_form_key = st.session_state.form_key
    else:
        current_data = {}
        fecha_generacion_val = datetime.now().strftime('%Y-%m-%d')
        new_form_key = st.session_state.form_key

    with st.form(key=f"invoice_entry_form_{new_form_key}", clear_on_submit=True):
        facturador = st.selectbox("Legalizador:", options=[""] + FACTURADORES,
                                  index=get_selectbox_default_index(FACTURADORES, current_data.get('facturador')),
                                  disabled=st.session_state.refacturar_mode)
        eps = st.selectbox("EPS:", options=[""] + EPS_OPCIONES,
                           index=get_selectbox_default_index(EPS_OPCIONES, current_data.get('eps')),
                           disabled=st.session_state.refacturar_mode)
        numero_factura = st.text_input("Número de Factura:", value=current_data.get('numero_factura', ""),
                                       disabled=st.session_state.refacturar_mode)
        fecha_generacion = st.text_input("Fecha de Generación (YYYY-MM-DD o DD/MM/YYYY):", value=fecha_generacion_val,
                                         disabled=st.session_state.refacturar_mode)
        area_servicio = st.selectbox("Área de Servicio:", options=[""] + AREA_SERVICIO_OPCIONES,
                                     index=get_selectbox_default_index(AREA_SERVICIO_OPCIONES, current_data.get('area_servicio')),
                                     disabled=st.session_state.refacturar_mode)

        if st.session_state.refacturar_mode:
            st.markdown("---")
            st.subheader("Datos de Refacturación")
            new_numero_factura = st.text_input("Nuevo Número de Factura:", key=f"new_num_fact_{new_form_key}")
            fecha_reemplazo_factura = st.text_input("Fecha de Generación de la Nueva Factura (YYYY-MM-DD o DD/MM/YYYY):",
                                                   value=datetime.now().strftime('%Y-%m-%d'),
                                                   key=f"new_fecha_gen_{new_form_key}")
        else:
            new_numero_factura = None
            fecha_reemplazo_factura = None

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

        if submitted:
            if st.session_state.refacturar_mode:
                guardar_factura_reemplazo_action(st.session_state.editing_factura_id, new_numero_factura,
                                                 fecha_reemplazo_factura, facturador, eps, area_servicio)
            elif st.session_state.edit_mode:
                original_data = db_ops.obtener_factura_por_id(st.session_state.editing_factura_id)
                if original_data:
                    original_data = _ensure_datetime_objects(original_data)
                    actualizar_factura_action(
                        st.session_state.editing_factura_id, numero_factura, area_servicio, facturador,
                        fecha_generacion, eps, original_data.get('fecha_hora_entrega'),
                        original_data.get('tiene_correccion'), original_data.get('descripcion_devolucion'),
                        original_data.get('fecha_devolucion_lider'), original_data.get('revisado'),
                        original_data.get('factura_original_id'), original_data.get('estado'),
                        original_data.get('reemplazada_por_numero_factura'), original_data.get('estado_auditoria'),
                        original_data.get('observacion_auditor'), original_data.get('tipo_error'),
                        original_data.get('fecha_reemplazo')
                    )
            else:
                guardar_factura_action(facturador, eps, numero_factura, fecha_generacion, area_servicio)
            st.session_state.form_key += 1
            st.rerun()
        
        # Lógica para marcar como corregida dentro del formulario de edición
        if user_role == 'legalizador' and st.session_state.edit_mode and current_data.get('estado_auditoria') == 'Devuelta por Auditor':
            if st.button("Marcar como Corregida"):
                marcar_como_corregida_action(st.session_state.editing_factura_id, current_data.get('observacion_auditor'), current_data.get('tipo_error'))
                st.rerun()

def display_bulk_load_section():
    with st.form("bulk_load_form"):
        st.write("Por favor, selecciona el Legalizador, EPS y Área de Servicio para todas las facturas del CSV.")
        facturador_bulk = st.selectbox("Legalizador (CSV):", options=[""] + FACTURADORES,
                                       key="bulk_facturador", index=get_selectbox_default_index(FACTURADORES, st.session_state.get('bulk_facturador', "")))
        eps_bulk = st.selectbox("EPS (CSV):", options=[""] + EPS_OPCIONES,
                                key="bulk_eps", index=get_selectbox_default_index(EPS_OPCIONES, st.session_state.get('bulk_eps', "")))
        area_servicio_bulk = st.selectbox("Área de Servicio (CSV):", options=[""] + AREA_SERVICIO_OPCIONES,
                                          key="bulk_area_servicio", index=get_selectbox_default_index(AREA_SERVICIO_OPCIONES, st.session_state.get('bulk_area_servicio', "")))

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
            required_columns_csv = ['Numero de Factura', 'Fecha de Generacion']
            
            if not all(col in df.columns for col in required_columns_csv):
                st.error(f"El archivo CSV debe contener las columnas: {', '.join(required_columns_csv)}.")
                return

            st.write(f"Iniciando carga masiva desde: {uploaded_file.name}")
            st.write(f"Facturador global: {facturador_bulk}, EPS global: {eps_bulk}, Área de Servicio global: {area_servicio_bulk}")

            for index, row in df.iterrows():
                total_rows += 1
                numero_factura_csv = str(row.get('Numero de Factura', '')).strip()
                fecha_str_csv = str(row.get('Fecha de Generacion', '')).strip()

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

                factura_id = db_ops.guardar_factura(
                    numero_factura=numero_factura_csv,
                    area_servicio=area_servicio_bulk,
                    facturador=facturador_bulk,
                    fecha_generacion=fecha_generacion_csv_obj,
                    eps=eps_bulk,
                    fecha_hora_entrega=datetime.now()
                )

                if factura_id:
                    if area_servicio_bulk == "SOAT":
                        db_ops.guardar_detalles_soat(factura_id, fecha_generacion_csv_obj)
                    inserted_count += 1
                else:
                    skipped_count += 1
                    st.info(f"Fila {index+2}: Factura '{numero_factura_csv}' ya existe o hubo un error al insertar. Saltando.")

            st.success(f"Carga masiva finalizada.\nTotal de filas procesadas: {total_rows}\nFacturas insertadas: {inserted_count}\nFacturas omitidas (duplicadas/errores): {skipped_count}")
            invalidate_facturas_cache()
            st.session_state.bulk_facturador = ""
            st.session_state.bulk_eps = ""
            st.session_state.bulk_area_servicio = ""
            st.rerun()

def display_statistics():
    st.subheader("Estadísticas Generales de Facturas")
    
    # Usar las funciones cacheadas del backend
    total_pendientes = db_ops.obtener_conteo_facturas_pendientes_global()
    total_lista_para_radicar = db_ops.obtener_conteo_facturas_lista_para_radicar()
    total_en_radicador = db_ops.obtener_conteo_facturas_en_radicador()
    total_errores = db_ops.obtener_conteo_facturas_con_errores()
    total_vencidas = db_ops.obtener_conteo_facturas_vencidas() # Usar la función del backend
    total_general = db_ops.obtener_conteo_total_facturas()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Facturas Pendientes", value=total_pendientes)
        st.metric(label="Facturas En Radicador", value=total_en_radicador)
    with col2:
        st.metric(label="Facturas Lista para Radicar", value=total_lista_para_radicar)
        st.metric(label="Facturas con Errores", value=total_errores)
    with col3:
        st.metric(label="Facturas Vencidas (Refacturar)", value=total_vencidas)
    
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
    return db_ops.cargar_facturas(search_term, search_column)

def invalidate_facturas_cache():
    # Invalida solo la caché de la función que carga las facturas
    get_cached_facturas.clear()
    db_ops.obtener_conteo_facturas_por_legalizador_y_eps.clear()
    db_ops.obtener_conteo_facturas_lista_para_radicar.clear()
    db_ops.obtener_conteo_facturas_en_radicador.clear()
    db_ops.obtener_conteo_facturas_con_errores.clear()
    db_ops.obtener_conteo_facturas_pendientes_global.clear()
    db_ops.obtener_conteo_facturas_vencidas.clear()
    db_ops.obtener_conteo_total_facturas.clear()

def display_invoice_table(user_role):
    col_search, col_criteria = st.columns([3, 2])
    with col_search:
        search_term_input = st.text_input("Buscar:", value=st.session_state.search_term, key="search_input")
    with col_criteria:
        options_criteria = ["Numero de Factura", "Legalizador", "EPS", "Area de Servicio", "Estado Auditoria"]
        search_criterion_selectbox = st.selectbox("Buscar por:", options=options_criteria, index=options_criteria.index(st.session_state.search_criteria), key="search_criteria_select")

    # Guardar los valores de búsqueda en el estado de la sesión
    st.session_state.search_term = search_term_input
    st.session_state.search_criteria = search_criterion_selectbox

    db_column_name = {
        "Numero de Factura": "numero_factura",
        "Legalizador": "facturador",
        "EPS": "eps",
        "Area de Servicio": "area_servicio",
        "Estado Auditoria": "estado_auditoria"
    }.get(st.session_state.search_criteria)

    facturas_raw = get_cached_facturas(search_term=st.session_state.search_term.strip(), search_column=db_column_name)

    if facturas_raw:
        processed_facturas = [_process_factura_for_display(factura) for factura in facturas_raw]
        df_facturas = pd.DataFrame(processed_facturas)
    else:
        df_facturas = pd.DataFrame()

    if not df_facturas.empty:
        # Se movió la lógica de ordenamiento fuera de la función de procesamiento
        def get_sort_key(row):
            sort_order = {
                'Devuelta por Auditor': 1, 'Corregida por Legalizador': 2, 'Refacturar': 3,
                'Hoy Vence': 4, 'Pendiente': 5, 'Lista para Radicar': 6, 'En Radicador': 7
            }
            estado = row["Estado Auditoria"]
            if row["Días Restantes"] == "Refacturar":
                estado = "Refacturar"
            elif row["Días Restantes"] == "Hoy Vence":
                estado = "Hoy Vence"
            
            return sort_order.get(estado, 99)
        
        df_facturas['sort_key'] = df_facturas.apply(get_sort_key, axis=1)
        df_facturas = df_facturas.sort_values(by=['sort_key', 'Fecha Generación'], ascending=[True, False])
        df_facturas = df_facturas.drop(columns=['sort_key'])

        st.dataframe(df_facturas.style.apply(highlight_rows, axis=1), use_container_width=True, hide_index=True)
    else:
        st.info("No hay facturas registradas que coincidan con los criterios de búsqueda.")

    # Acciones principales
    col_export, col_edit, col_refacturar = st.columns(3)
    with col_export:
        if not df_facturas.empty and st.download_button(
            label="Exportar a CSV",
            data=df_facturas.to_csv(index=False).encode('utf-8'),
            file_name=f'facturas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            mime='text/csv'
        ):
            st.success("Archivo CSV exportado exitosamente.")

    selected_invoice_id = st.number_input("ID de Factura para Acción:", min_value=0, step=1, key="selected_invoice_id_input")

    if selected_invoice_id > 0:
        factura_data_for_action = db_ops.obtener_factura_por_id(selected_invoice_id)
        if factura_data_for_action:
            st.session_state.current_invoice_data = _ensure_datetime_objects(factura_data_for_action)

            with col_edit:
                if st.button("Cargar para Edición", key="edit_button"):
                    cargar_factura_para_edicion_action(selected_invoice_id)
                    st.rerun()

            with col_refacturar:
                dias_restantes_df = "N/A"
                if not df_facturas.empty:
                    dias_restantes_row = df_facturas[df_facturas['ID'] == selected_invoice_id]['Días Restantes']
                    if not dias_restantes_row.empty:
                        dias_restantes_df = dias_restantes_row.iloc[0]

                if dias_restantes_df == "Refacturar":
                    if st.button("Refacturar", key="refacturar_button"):
                        cargar_factura_para_refacturar_action(selected_invoice_id)
                        st.rerun()

            # Lógica para auditoría y eliminación
            if user_role == 'auditor':
                st.markdown("---")
                st.subheader("Acciones de Auditoría para Factura Seleccionada")
                display_auditor_actions(selected_invoice_id)
        else:
            st.warning("ID de factura no encontrado.")
            st.session_state.current_invoice_data = None
    else:
        if st.session_state.current_invoice_data is not None:
            cancelar_edicion_action()
            st.rerun()

def display_auditor_actions(selected_invoice_id):
    current_data = st.session_state.current_invoice_data

    estado_auditoria_options_filtered = [opt for opt in ESTADO_AUDITORIA_OPCIONES if opt != 'Radicada y Aceptada']
    estado_auditoria_default_index = get_selectbox_default_index(estado_auditoria_options_filtered, current_data.get('estado_auditoria', ''))
    
    tipo_error_default_index = get_selectbox_default_index(TIPO_ERROR_OPCIONES, current_data.get('tipo_error', ''))

    with st.form(key=f"auditoria_form_{selected_invoice_id}", clear_on_submit=False):
        estado_auditoria_input = st.selectbox("Estado Auditoría:", options=[""] + estado_auditoria_options_filtered, index=estado_auditoria_default_index, key=f"estado_auditoria_{selected_invoice_id}")
        tipo_error_input = st.selectbox("Tipo de Error:", options=[""] + TIPO_ERROR_OPCIONES, index=tipo_error_default_index, key=f"tipo_error_{selected_invoice_id}")
        observacion_auditor_input = st.text_area("Observación Auditor:", value=current_data.get('observacion_auditor', ''), key=f"observacion_auditor_{selected_invoice_id}")
        
        submit_auditoria = st.form_submit_button("Auditar Factura")
        
        if submit_auditoria:
            auditar_factura_action(selected_invoice_id, estado_auditoria_input, observacion_auditor_input, tipo_error_input)
            st.rerun()

    if current_data.get('estado_auditoria') in ['Lista para Radicar', 'En Radicador']:
        fecha_entrega_radicador_val = current_data.get('fecha_entrega_radicador')
        fecha_entrega_radicador_checked = st.checkbox(
            "Factura Entregada al Radicador",
            value=bool(fecha_entrega_radicador_val),
            key=f"radicador_checkbox_{selected_invoice_id}"
        )
        if fecha_entrega_radicador_checked != bool(fecha_entrega_radicador_val):
            actualizar_fecha_entrega_radicador_action(selected_invoice_id, fecha_entrega_radicador_checked)
            st.rerun()

    if st.button("Eliminar Factura", key=f"delete_button_{selected_invoice_id}"):
        st.session_state.confirm_delete_id = selected_invoice_id

    if st.session_state.get('confirm_delete_id') == selected_invoice_id:
        st.warning(f"¿Estás seguro de que quieres eliminar la factura ID: {selected_invoice_id}?\nEsta acción es irreversible.")
        col_confirm_del, col_cancel_del = st.columns(2)
        with col_confirm_del:
            if st.button("Confirmar Eliminación", key="confirm_delete_button_modal"):
                success = eliminar_factura_action(selected_invoice_id)
                if success:
                    st.success(f"Factura ID: {selected_invoice_id} eliminada correctamente.")
                    st.session_state.confirm_delete_id = None
                    invalidate_facturas_cache()
                    cancelar_edicion_action()
                else:
                    st.error("No se pudo eliminar la factura.")
                st.rerun()
        with col_cancel_del:
            if st.button("Cancelar", key="cancel_delete_button_modal"):
                st.info("Eliminación cancelada.")
                st.session_state.confirm_delete_id = None
                st.rerun()


def guardar_factura_action(facturador, eps, numero_factura, fecha_generacion_str, area_servicio):
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

    factura_id = db_ops.guardar_factura(
        numero_factura=numero_factura,
        area_servicio=area_servicio,
        facturador=facturador,
        fecha_generacion=fecha_generacion_obj,
        eps=eps,
        fecha_hora_entrega=datetime.now()
    )

    if factura_id:
        if area_servicio == "SOAT":
            db_ops.guardar_detalles_soat(factura_id, fecha_generacion_obj)
        st.success("Factura guardada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error(f"La factura con número '{numero_factura}' ya existe con la misma combinación de Legalizador, EPS y Área de Servicio.")

def actualizar_factura_action(factura_id, numero_factura, area_servicio, facturador, fecha_generacion_str, eps,
                               fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                               fecha_devolucion_lider, revisado, factura_original_id, estado,
                               reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
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

    success = db_ops.actualizar_factura(
        factura_id, numero_factura, area_servicio, facturador, fecha_generacion_obj, eps,
        fecha_hora_entrega, tiene_correccion, descripcion_devolucion, fecha_devolucion_lider,
        revisado, factura_original_id, estado, reemplazada_por_numero_factura, estado_auditoria,
        observacion_auditor, tipo_error, fecha_reemplazo
    )

    if success:
        st.success("Factura actualizada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error(f"No se pudo actualizar la factura. El número de factura '{numero_factura}' ya podría existir con la misma combinación de Legalizador, EPS y Área de Servicio.")

def cargar_factura_para_edicion_action(factura_id):
    factura_data = db_ops.obtener_factura_por_id(factura_id)
    if factura_data:
        st.session_state.editing_factura_id = factura_id
        st.session_state.edit_mode = True
        st.session_state.refacturar_mode = False
        st.session_state.current_invoice_data = factura_data
        st.success(f"Factura {factura_data['numero_factura']} cargada para edición.")
    else:
        st.error("No se pudo cargar la factura para edición.")

def cargar_factura_para_refacturar_action(factura_id):
    factura_data = db_ops.obtener_factura_por_id(factura_id)
    if factura_data:
        st.session_state.editing_factura_id = factura_id
        st.session_state.edit_mode = False
        st.session_state.refacturar_mode = True
        st.session_state.current_invoice_data = factura_data
        st.warning(f"Factura {factura_data['numero_factura']} cargada para refacturar. Ingrese el nuevo número de factura.")
    else:
        st.error("No se pudo cargar la factura para refacturar.")

def auditar_factura_action(factura_id, nuevo_estado_auditoria, observacion, tipo_error):
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
        cancelar_edicion_action()
    else:
        st.error("No se pudo actualizar el estado de auditoría de la factura.")

def eliminar_factura_action(factura_id):
    if st.session_state.user_role != 'auditor':
        st.error("Permiso Denegado. Solo los auditores pueden eliminar facturas.")
        return False
    
    success = db_ops.eliminar_factura(factura_id)
    return success

def guardar_factura_reemplazo_action(old_factura_id, new_numero_factura, fecha_reemplazo_factura_str, facturador, eps, area_servicio):
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

    factura_original_data = db_ops.obtener_factura_por_id(old_factura_id)
    if not factura_original_data:
        st.error("No se pudo obtener la información de la factura original para el reemplazo.")
        return

    success = db_ops.guardar_factura_reemplazo(
        old_factura_id,
        new_numero_factura,
        fecha_reemplazo_factura_obj,
        factura_original_data['area_servicio'],
        factura_original_data['facturador'],
        factura_original_data['eps'],
        datetime.now().date()
    )

    if success:
        st.success(f"Factura reemplazada por {new_numero_factura} correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else:
        st.error(f"No se pudo guardar la factura de reemplazo. El número '{new_numero_factura}' ya podría existir.")

def marcar_como_corregida_action(factura_id, observacion_actual, tipo_error_actual):
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
    fecha_entrega = datetime.now() if set_date else None
    success = db_ops.actualizar_fecha_entrega_radicador(factura_id, fecha_entrega)
    if success:
        st.success("Fecha de entrega al radicador actualizada correctamente.")
        invalidate_facturas_cache()
    else:
        st.error("No se pudo actualizar la fecha de entrega al radicador.")

def cancelar_edicion_action():
    st.session_state.editing_factura_id = None
    st.session_state.edit_mode = False
    st.session_state.refacturar_mode = False
    st.session_state.current_invoice_data = None
    st.session_state.form_key += 1
    st.session_state.confirm_delete_id = None
    st.session_state.selected_invoice_input_key = 0 # No es necesario en el código revisado

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if st.session_state['logged_in']:
    main_app_page()
else:
    login_page()