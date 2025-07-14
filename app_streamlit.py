import streamlit as st
from datetime import datetime, timedelta
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
db_ops.crear_tablas()
if not os.path.exists('data'): os.makedirs('data')
if 'selected_invoice_input_key' not in st.session_state: st.session_state.selected_invoice_input_key = 0
if 'filter_text_key' not in st.session_state: st.session_state.filter_text_key = 0
if 'filter_select_key' not in st.session_state: st.session_state.filter_select_key = 0
def login_page():
    st.title("Iniciar Sesion - Trazabilidad de Facturas")
    with st.form("login_form"):
        username = st.text_input("Usuario:")
        password = st.text_input("Contraseña:", type="password")
        submitted = st.form_submit_button("Entrar")
        if submitted:
            user_data = db_ops.obtener_credenciales_usuario(username)
            if user_data:
                db_password, user_role = user_data
                if password == db_password:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.session_state['user_role'] = user_role
                    st.success(f"Bienvenido, {username}! Tu rol es: {user_role}")
                    st.rerun()
                else: st.error("Contraseña incorrecta.")
            else: st.error("Usuario no encontrado.")
def main_app_page():
    st.title("Trazabilidad de Facturas - Hospital Jose Maria Hernandez de Mocoa")
    user_role = st.session_state.get('user_role', 'guest')
    st.sidebar.header(f"Bienvenido, {st.session_state.get('username')} ({user_role})")
    if st.sidebar.button("Cerrar Sesion"):
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.session_state['user_role'] = None
        st.rerun()
    tab1, tab2, tab3 = st.tabs(["Ingreso Individual", "Carga Masiva", "Estadisticas"])
    with tab1:
        st.header("Ingreso de Factura Individual")
        display_invoice_entry_form(user_role)
    with tab2:
        st.header("Carga Masiva (Solo Numero y Fecha)")
        display_bulk_load_section()
    with tab3:
        st.header("Estadisticas por Legalizador y EPS")
        display_statistics()
    st.header("Facturas Registradas")
    display_invoice_table(user_role)
def display_invoice_entry_form(user_role):
    if 'editing_factura_id' not in st.session_state: st.session_state.editing_factura_id = None
    if 'edit_mode' not in st.session_state: st.session_state.edit_mode = False
    if 'refacturar_mode' not in st.session_state: st.session_state.refacturar_mode = False
    if 'current_invoice_data' not in st.session_state: st.session_state.current_invoice_data = None
    if 'form_key' not in st.session_state: st.session_state.form_key = 0
    with st.form(key=f"invoice_entry_form_{st.session_state.form_key}", clear_on_submit=False):
        facturador_default_index = 0
        if st.session_state.current_invoice_data and not st.session_state.refacturar_mode:
            try: facturador_default_index = FACTURADORES.index(st.session_state.current_invoice_data[3]) + 1
            except ValueError: facturador_default_index = 0
        facturador = st.selectbox("Legalizador:", options=[""] + FACTURADORES, index=facturador_default_index, disabled=st.session_state.refacturar_mode)
        eps_default_index = 0
        if st.session_state.current_invoice_data and not st.session_state.refacturar_mode:
            try: eps_default_index = EPS_OPCIONES.index(st.session_state.current_invoice_data[5]) + 1
            except ValueError: eps_default_index = 0
        eps = st.selectbox("EPS:", options=[""] + EPS_OPCIONES, index=eps_default_index, disabled=st.session_state.refacturar_mode)
        numero_factura = st.text_input("Numero de Factura:", value=st.session_state.current_invoice_data[1] if st.session_state.current_invoice_data and not st.session_state.refacturar_mode else "", disabled=st.session_state.refacturar_mode)
        fecha_generacion = st.text_input("Fecha de Generacion (YYYY-MM-DD o DD/MM/YYYY):", value=st.session_state.current_invoice_data[4] if st.session_state.current_invoice_data and not st.session_state.refacturar_mode else "", disabled=st.session_state.refacturar_mode)
        area_servicio_default_index = 0
        if st.session_state.current_invoice_data and not st.session_state.refacturar_mode:
            try: area_servicio_default_index = AREA_SERVICIO_OPCIONES.index(st.session_state.current_invoice_data[2]) + 1
            except ValueError: area_servicio_default_index = 0
        area_servicio = st.selectbox("Area de Servicio:", options=[""] + AREA_SERVICIO_OPCIONES, index=area_servicio_default_index, disabled=st.session_state.refacturar_mode)
        if st.session_state.refacturar_mode:
            st.markdown("---")
            st.subheader("Datos de Refacturacion")
            new_numero_factura = st.text_input("Nuevo Numero de Factura:")
            fecha_reemplazo_factura = st.text_input("Fecha Reemplazo Factura (YYYY-MM-DD o DD/MM/YYYY):", value=st.session_state.current_invoice_data[4] if st.session_state.current_invoice_data else "")
        else:
            new_numero_factura = None
            fecha_reemplazo_factura = None
        if user_role == 'legalizador' and st.session_state.current_invoice_data and st.session_state.current_invoice_data[14] == 'Devuelta por Auditor':
            if st.form_submit_button("Marcar como Corregida"):
                marcar_como_corregida_action(st.session_state.editing_factura_id, st.session_state.current_invoice_data[15], st.session_state.current_invoice_data[16])
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.refacturar_mode: submitted = st.form_submit_button("Guardar Factura Reemplazo")
            elif st.session_state.edit_mode: submitted = st.form_submit_button("Actualizar Factura")
            else: submitted = st.form_submit_button("Guardar Factura")
        with col2:
            if st.session_state.edit_mode or st.session_state.refacturar_mode:
                if st.form_submit_button("Cancelar Edicion"):
                    cancelar_edicion_action()
                    st.rerun()
        if submitted:
            if st.session_state.refacturar_mode:
                guardar_factura_reemplazo_action(st.session_state.editing_factura_id, new_numero_factura, fecha_reemplazo_factura, facturador, eps, area_servicio)
            elif st.session_state.edit_mode:
                if user_role != 'auditor' and st.session_state.current_invoice_data:
                    estado_auditoria = st.session_state.current_invoice_data[14]
                    observacion_auditor = st.session_state.current_invoice_data[15]
                    tipo_error = st.session_state.current_invoice_data[16]
                else:
                    estado_auditoria = None
                    observacion_auditor = None
                    tipo_error = None
                actualizar_factura_action(st.session_state.editing_factura_id, numero_factura, area_servicio, facturador, fecha_generacion, eps, estado_auditoria, observacion_auditor, tipo_error)
            else:
                guardar_factura_action(facturador, eps, numero_factura, fecha_generacion, area_servicio)
            st.rerun()
def display_bulk_load_section():
    with st.form("bulk_load_form"):
        st.write("Por favor, selecciona el Legalizador, EPS y Area de Servicio para todas las facturas del CSV.")
        facturador_bulk = st.selectbox("Legalizador (CSV):", options=[""] + FACTURADORES)
        eps_bulk = st.selectbox("EPS (CSV):", options=[""] + EPS_OPCIONES)
        area_servicio_bulk = st.selectbox("Area de Servicio (CSV):", options=[""] + AREA_SERVICIO_OPCIONES)
        uploaded_file = st.file_uploader("Cargar archivo CSV (columnas: Numero de Factura, Fecha de Generacion)", type=["csv"])
        bulk_submitted = st.form_submit_button("Cargar desde CSV")
        if bulk_submitted and uploaded_file is not None:
            if not facturador_bulk or not eps_bulk or not area_servicio_bulk:
                st.error("Por favor, selecciona Legalizador, EPS y Area de Servicio para la carga masiva.")
                return
            inserted_count = 0
            skipped_count = 0
            total_rows = 0
            df = pd.read_csv(uploaded_file)
            if 'Numero de Factura' not in df.columns or 'Fecha de Generacion' not in df.columns:
                st.error("El archivo CSV debe contener las columnas 'Numero de Factura' y 'Fecha de Generacion'.")
                return
            st.write(f"Iniciando carga masiva desde: {uploaded_file.name}")
            st.write(f"Facturador: {facturador_bulk}, EPS: {eps_bulk}, Area de Servicio: {area_servicio_bulk}")
            for index, row in df.iterrows():
                total_rows += 1
                numero_factura_csv = str(row['Numero de Factura']).strip()
                fecha_str_csv = str(row['Fecha de Generacion']).strip()                
                if not numero_factura_csv.isdigit():
                    st.warning(f"Fila {index+2}: Numero de factura '{numero_factura_csv}' no es numerico. Saltando.")
                    skipped_count += 1
                    continue
                fecha_generacion_csv_obj = parse_date(fecha_str_csv, f"Fecha de Generacion (Fila {index+2})")
                if fecha_generacion_csv_obj is None:
                    skipped_count += 1
                    continue
                if not validate_future_date(fecha_generacion_csv_obj, f"Fecha de Generacion (Fila {index+2})"):
                    skipped_count += 1
                    continue
                current_time = datetime.now().time()
                fecha_hora_entrega_obj = datetime.combine(fecha_generacion_csv_obj, current_time)
                fecha_hora_entrega_db = fecha_hora_entrega_obj.strftime('%Y-%m-%d %H:%M:%S')
                fecha_generacion_db = fecha_generacion_csv_obj.strftime('%Y-%m-%d')
                factura_id = db_ops.insertar_factura_bulk(numero_factura_csv, area_servicio_bulk, facturador_bulk, fecha_generacion_db, eps_bulk, fecha_hora_entrega_db)
                if factura_id:
                    if area_servicio_bulk == "SOAT": db_ops.insertar_detalles_soat_bulk(factura_id, fecha_generacion_db)
                    inserted_count += 1
                else:
                    skipped_count += 1
                    st.info(f"Fila {index+2}: Factura '{numero_factura_csv}' ya existe o hubo un error al insertar. Saltando.")
            st.success(f"Carga masiva finalizada.\nTotal de filas procesadas: {total_rows}\nFacturas insertadas: {inserted_count}\nFacturas omitidas (duplicadas/errores): {skipped_count}")
            invalidate_facturas_cache()
            st.rerun()
def display_statistics():
    st.subheader("Estadísticas Generales de Facturas")
    col_radicadas, col_errores, col_pendientes, col_total = st.columns(4)
    total_radicadas = db_ops.obtener_conteo_facturas_radicadas_ok()
    total_errores = db_ops.obtener_conteo_facturas_con_errores()
    total_pendientes = db_ops.obtener_conteo_facturas_pendientes_global()
    total_general = db_ops.obtener_conteo_total_facturas()
    with col_radicadas: st.metric(label="Facturas Radicadas (OK)", value=total_radicadas)
    with col_errores: st.metric(label="Facturas con Errores", value=total_errores)
    with col_pendientes: st.metric(label="Facturas Pendientes", value=total_pendientes)
    with col_total: st.metric(label="Total General de Facturas", value=total_general)
    st.markdown("---")
    st.subheader("Conteo por Legalizador y EPS (Facturas Pendientes)")
    stats = db_ops.obtener_conteo_facturas_por_legalizador_y_eps()
    if stats:
        df_stats = pd.DataFrame(stats, columns=["Legalizador", "EPS", "Facturas Pendientes"])
        st.dataframe(df_stats, use_container_width=True, hide_index=True)
    else: st.info("No hay estadisticas disponibles de facturas pendientes.")
def highlight_rows(row):
    styles = [''] * len(row)
    if row["Estado Auditoria"] == 'Devuelta por Auditor': styles = ['background-color: lightblue'] * len(row)
    elif row["Estado Auditoria"] == 'Corregida por Legalizador': styles = ['background-color: lightsalmon'] * len(row)
    elif row["Dias Restantes"] == "Refacturar": styles = ['background-color: salmon'] * len(row)
    elif isinstance(row["Dias Restantes"], (int, float)):
        if 1 <= row["Dias Restantes"] <= 3: styles = ['background-color: yellow'] * len(row)
        elif row["Dias Restantes"] > 3: styles = ['background-color: lightgreen'] * len(row)
    return styles
@st.cache_data(ttl=60)
def get_cached_facturas(search_term, search_column): return db_ops.cargar_facturas(search_term, search_column)
def invalidate_facturas_cache(): get_cached_facturas.clear()
def display_invoice_table(user_role):
    col_search, col_criteria = st.columns([3, 2])
    with col_search:
        search_term_input = st.text_input("Buscar:", value="", key=f"search_input_widget_{st.session_state.filter_text_key}")
    with col_criteria:
        options_criteria = ["Numero de Factura", "Legalizador", "EPS", "Area de Servicio", "Estado Auditoria"]
        search_criterion_selectbox = st.selectbox("Buscar por:", options=options_criteria, index=0, key=f"search_criteria_widget_{st.session_state.filter_select_key}")
    current_search_term = st.session_state.get(f'search_input_widget_{st.session_state.filter_text_key}', '').strip()
    current_search_criterion = st.session_state.get(f'search_criteria_widget_{st.session_state.filter_select_key}', 'Numero de Factura')
    db_column_name = {"Numero de Factura": "numero_factura", "Legalizador": "facturador", "EPS": "eps", "Area de Servicio": "area_servicio", "Estado Auditoria": "estado_auditoria"}.get(current_search_criterion)
    facturas_raw = get_cached_facturas(search_term=current_search_term, search_column=db_column_name)
    processed_facturas = []
    hoy_obj = datetime.now().date()
    for factura in facturas_raw:
        factura_id = factura[0]
        numero_factura_base = factura[1]
        area_servicio = factura[2]
        facturador_nombre = factura[3]
        fecha_generacion_base_str = factura[4]
        eps_nombre = factura[5]
        fecha_hora_entrega = factura[6]
        estado_factura = factura[12]
        reemplazada_por_numero = factura[13] if factura[13] else ""
        estado_auditoria_db = factura[14] if factura[14] else "Pendiente"
        observacion_auditor_db = factura[15] if factura[15] else ""
        tipo_error_db = factura[16] if factura[16] else ""
        fecha_reemplazo_db_val = factura[17] if factura[17] else ""
        num_fact_original_linked = factura[18] if factura[18] else ""
        fecha_gen_original_linked_str = factura[19] if factura[19] else ""
        fecha_entrega_radicador_db = factura[20] if factura[20] else ""
        try: fecha_generacion_obj = datetime.strptime(fecha_generacion_base_str, '%Y-%m-%d').date()
        except ValueError: fecha_generacion_obj = hoy_obj
        fecha_limite_liquidacion_obj = sumar_dias_habiles(fecha_generacion_obj, 21)
        dias_restantes_liquidacion = 0
        if hoy_obj <= fecha_limite_liquidacion_obj: dias_restantes_liquidacion = calcular_dias_habiles_entre_fechas(hoy_obj, fecha_limite_liquidacion_obj)
        else:
            dias_pasados_del_limite = calcular_dias_habiles_entre_fechas(fecha_limite_liquidacion_obj + timedelta(days=1), hoy_obj)
            dias_restantes_liquidacion = -dias_pasados_del_limite
        display_numero_factura_col = ""
        display_numero_reemplazo_col = ""
        display_fecha_generacion_actual_col = ""
        display_fecha_reemplazo_display = ""
        if factura[11] is not None:
            display_numero_factura_col = num_fact_original_linked
            display_numero_reemplazo_col = numero_factura_base
            display_fecha_generacion_actual_col = fecha_gen_original_linked_str
            display_fecha_reemplazo_display = fecha_gen_original_linked_str
        elif estado_factura == 'Reemplazada':
            display_numero_factura_col = numero_factura_base
            display_numero_reemplazo_col = reemplazada_por_numero
            display_fecha_generacion_actual_col = fecha_generacion_base_str
            display_fecha_reemplazo_display = fecha_reemplazo_db_val
        else:
            display_numero_factura_col = numero_factura_base
            display_numero_reemplazo_col = ""
            display_fecha_generacion_actual_col = fecha_generacion_base_str
            display_fecha_reemplazo_display = ""
        display_dias_restantes = dias_restantes_liquidacion
        display_estado_for_tree = estado_factura
        if dias_restantes_liquidacion <= 0 and estado_auditoria_db != 'Devuelta por Auditor' and estado_auditoria_db != 'Corregida por Legalizador':
            display_dias_restantes = "Refacturar"
            display_estado_for_tree = "Vencidas"
        processed_facturas.append({
            "ID": factura_id, "Area de Servicio": area_servicio, "Facturador": facturador_nombre, "EPS": eps_nombre,
            "Numero de Factura": display_numero_factura_col, "Numero Reemplazo Factura": display_numero_reemplazo_col,
            "Fecha Generacion": display_fecha_generacion_actual_col, "Fecha Reemplazo Factura": display_fecha_reemplazo_display,
            "Fecha de Entrega": fecha_hora_entrega, "Dias Restantes": display_dias_restantes, "Estado": display_estado_for_tree,
            "Estado Auditoria": estado_auditoria_db, "Tipo de Error": tipo_error_db, "Observacion Auditor": observacion_auditor_db,
            "Fecha Entrega Radicador": fecha_entrega_radicador_db})
    df_facturas = pd.DataFrame(processed_facturas)
    if not df_facturas.empty:
        def get_sort_key(row):
            if row["Estado Auditoria"] == 'Devuelta por Auditor': return 1
            elif row["Estado Auditoria"] == 'Corregida por Legalizador': return 2
            elif row["Dias Restantes"] == "Refacturar": return 3
            else: return 4
        df_facturas['sort_key'] = df_facturas.apply(get_sort_key, axis=1)
        df_facturas = df_facturas.sort_values(by=['sort_key', 'Fecha Generacion'], ascending=[True, False])
        df_facturas = df_facturas.drop(columns=['sort_key'])
        st.dataframe(df_facturas.style.apply(highlight_rows, axis=1), use_container_width=True, hide_index=True)
    else: st.info("No hay facturas registradas que coincidan con los criterios de búsqueda.")
    col_export, col_edit, col_refacturar, col_delete_placeholder = st.columns(4)
    with col_export:
        if st.button("Exportar a CSV"): export_df_to_csv(df_facturas)
    selected_invoice_id = st.number_input("ID de Factura para Accion:", min_value=0, step=1, key=f"selected_invoice_id_input_{st.session_state.selected_invoice_input_key}")
    if selected_invoice_id > 0:
        factura_data_for_action = db_ops.obtener_factura_por_id(selected_invoice_id)
        if factura_data_for_action:
            st.session_state.current_invoice_data = factura_data_for_action
            with col_edit:
                if st.button("Cargar para Edicion", key="edit_button"):
                    cargar_factura_para_edicion_action(selected_invoice_id)
                    st.rerun()
            with col_refacturar:
                if not df_facturas.empty and selected_invoice_id in df_facturas['ID'].values and df_facturas[df_facturas['ID'] == selected_invoice_id]['Dias Restantes'].iloc[0] == "Refacturar":
                    if st.button("Refacturar", key="refacturar_button"):
                        cargar_factura_para_refacturar_action(selected_invoice_id)
                        st.rerun()
            if user_role == 'auditor':
                st.markdown("---")
                st.subheader("Acciones de Auditoría para Factura Seleccionada")
                estado_auditoria_default_index = 0
                if st.session_state.current_invoice_data and st.session_state.current_invoice_data[14]:
                    try: estado_auditoria_default_index = ESTADO_AUDITORIA_OPCIONES.index(st.session_state.current_invoice_data[14])
                    except ValueError: estado_auditoria_default_index = 0
                tipo_error_default_index = 0
                if st.session_state.current_invoice_data and st.session_state.current_invoice_data[16]:
                    try: tipo_error_default_index = TIPO_ERROR_OPCIONES.index(st.session_state.current_invoice_data[16])
                    except ValueError: tipo_error_default_index = 0
                with st.form(key=f"auditoria_form_{selected_invoice_id}", clear_on_submit=False):
                    estado_auditoria_input = st.selectbox("Estado Auditoria:", options=ESTADO_AUDITORIA_OPCIONES, index=estado_auditoria_default_index, key=f"estado_auditoria_{selected_invoice_id}")
                    tipo_error_input = st.selectbox("Tipo de Error:", options=TIPO_ERROR_OPCIONES, index=tipo_error_default_index, key=f"tipo_error_{selected_invoice_id}")
                    observacion_auditor_input = st.text_area("Observacion Auditor:", value=st.session_state.current_invoice_data[15] if st.session_state.current_invoice_data and st.session_state.current_invoice_data[15] else "", key=f"observacion_auditor_{selected_invoice_id}")
                    submit_auditoria = st.form_submit_button("Auditar Factura")
                    if submit_auditoria:
                        auditar_factura_action(selected_invoice_id, estado_auditoria_input, observacion_auditor_input, tipo_error_input)
                        st.rerun()
                fecha_entrega_radicador_val = st.session_state.current_invoice_data[20] if st.session_state.current_invoice_data else None
                fecha_entrega_radicador_checked = st.checkbox("Factura Entregada al Radicador", value=bool(fecha_entrega_radicador_val), key=f"radicador_checkbox_{selected_invoice_id}")
                if fecha_entrega_radicador_checked != bool(fecha_entrega_radicador_val):
                    actualizar_fecha_entrega_radicador_action(selected_invoice_id, fecha_entrega_radicador_checked)
                    st.rerun()
                if st.button("Eliminar Factura", key=f"delete_button_{selected_invoice_id}"):
                    st.session_state.confirm_delete_id = selected_invoice_id
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
                                cancelar_edicion_action()
                            else: st.error("No se pudo eliminar la factura.")
                            st.rerun()
                    with col_cancel_del:
                        if st.button("Cancelar", key="cancel_delete_button_modal"):
                            st.info("Eliminación cancelada.")
                            st.session_state.confirm_delete_id = None
                            st.rerun()
        else:
            st.warning("ID de factura no encontrado.")
            st.session_state.current_invoice_data = None
def guardar_factura_action(facturador, eps, numero_factura, fecha_generacion_str, area_servicio):
    if not all([facturador, eps, numero_factura, fecha_generacion_str, area_servicio]):
        st.error("Todos los campos son obligatorios.")
        return
    if not numero_factura.isdigit():
        st.error("El campo 'Numero de Factura' debe contener solo numeros.")
        return
    fecha_generacion_obj = parse_date(fecha_generacion_str, "Fecha de Generacion")
    if fecha_generacion_obj is None:
        return
    if not validate_future_date(fecha_generacion_obj, "Fecha de Generacion"):
        return
    fecha_generacion_db = fecha_generacion_obj.strftime('%Y-%m-%d')
    fecha_hora_entrega = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    factura_id = db_ops.guardar_factura(facturador, eps, numero_factura, fecha_generacion_db, area_servicio, fecha_hora_entrega)
    if factura_id:
        if area_servicio == "SOAT": db_ops.guardar_detalles_soat(factura_id, fecha_generacion_db)
        st.success("Factura guardada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else: st.error(f"La factura con numero '{numero_factura}' ya existe.")
def actualizar_factura_action(factura_id, numero_factura, area_servicio, facturador, fecha_generacion_str, eps, estado_auditoria, observacion_auditor, tipo_error):
    if not all([factura_id, numero_factura, area_servicio, facturador, fecha_generacion_str, eps]):
        st.error("Todos los campos son obligatorios para la actualizacion.")
        return
    if not numero_factura.isdigit():
        st.error("El campo 'Numero de Factura' debe contener solo numeros.")
        return
    fecha_generacion_obj = parse_date(fecha_generacion_str, "Fecha de Generacion")
    if fecha_generacion_obj is None:
        return
    if not validate_future_date(fecha_generacion_obj, "Fecha de Generacion"):
        return
    fecha_generacion_db = fecha_generacion_obj.strftime('%Y-%m-%d')
    original_data = db_ops.obtener_factura_por_id(factura_id)
    if not original_data:
        st.error("No se pudo recuperar la factura original para actualizar.")
        return
    estado_auditoria_to_save = original_data[14]
    observacion_auditor_to_save = original_data[15]
    tipo_error_to_save = original_data[16]
    success = db_ops.actualizar_factura(
        factura_id, numero_factura, area_servicio, facturador, fecha_generacion_db,
        eps, original_data[6], original_data[7], original_data[8], original_data[9],
        original_data[10], original_data[11], original_data[12], original_data[13],
        estado_auditoria_to_save, observacion_auditor_to_save, tipo_error_to_save,
        original_data[17]
    )
    if success:
        st.success("Factura actualizada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else: st.error(f"No se pudo actualizar la factura. El numero de factura '{numero_factura}' ya podria existir.")
def cargar_factura_para_edicion_action(factura_id):
    factura_data = db_ops.obtener_factura_por_id(factura_id)
    if factura_data:
        st.session_state.editing_factura_id = factura_id
        st.session_state.edit_mode = True
        st.session_state.refacturar_mode = False
        st.session_state.current_invoice_data = factura_data
        st.session_state.form_key += 1
        st.success(f"Factura {factura_data[1]} cargada para edicion.")
    else: st.error("No se pudo cargar la factura para edicion.")
def cargar_factura_para_refacturar_action(factura_id):
    factura_data = db_ops.obtener_factura_por_id(factura_id)
    if factura_data:
        st.session_state.editing_factura_id = factura_id
        st.session_state.edit_mode = False
        st.session_state.refacturar_mode = True
        st.session_state.current_invoice_data = factura_data
        st.session_state.form_key += 1
        st.warning(f"Factura {factura_data[1]} cargada para refacturar. Ingrese el nuevo numero de factura.")
    else: st.error("No se pudo cargar la factura para refacturar.")
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
        st.success(f"Estado de auditoria de factura actualizado a '{nuevo_estado_auditoria}'.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else: st.error("No se pudo actualizar el estado de auditoria de la factura.")
def eliminar_factura_action(factura_id):
    if st.session_state.user_role != 'auditor':
        st.error("Permiso Denegado. Solo los auditores pueden eliminar facturas.")
        return False
    success = db_ops.eliminar_factura(factura_id)
    return success
def guardar_factura_reemplazo_action(old_factura_id, new_numero_factura, fecha_reemplazo_factura_str, facturador, eps, area_servicio):
    if not new_numero_factura:
        st.error("El campo 'Nuevo Numero de Factura' es obligatorio.")
        return
    if not new_numero_factura.isdigit():
        st.error("El 'Nuevo Numero de Factura' debe contener solo numeros.")
        return
    fecha_reemplazo_factura_obj = parse_date(fecha_reemplazo_factura_str, "Fecha Reemplazo Factura")
    if fecha_reemplazo_factura_obj is None:
        return
    if not validate_future_date(fecha_reemplazo_factura_obj, "Fecha Reemplazo Factura"):
        return
    fecha_reemplazo_db = fecha_reemplazo_factura_obj.strftime('%Y-%m-%d')
    factura_original_data = db_ops.obtener_factura_por_id(old_factura_id)
    if not factura_original_data:
        st.error("No se pudo obtener la informacion de la factura original para el reemplazo.")
        return
    new_fecha_generacion_automatica = datetime.now().strftime('%Y-%m-%d')
    success = db_ops.guardar_factura_reemplazo(
        old_factura_id, new_numero_factura, new_fecha_generacion_automatica,
        factura_original_data[2], factura_original_data[3], factura_original_data[5],
        fecha_reemplazo_db
    )
    if success:
        st.success(f"Factura reemplazada por {new_numero_factura} correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else: st.error(f"No se pudo guardar la factura de reemplazo. El numero '{new_numero_factura}' ya podria existir.")
def marcar_como_corregida_action(factura_id, observacion_actual, tipo_error_actual):
    if st.session_state.user_role != 'legalizador':
        st.error("Permiso Denegado. Solo los legalizadores pueden marcar facturas como corregidas.")
        return
    success = db_ops.actualizar_estado_auditoria_factura(factura_id, "Corregida por Legalizador", observacion_actual, tipo_error_actual)
    if success:
        st.success(f"Factura ID: {factura_id} marcada como 'Corregida por Legalizador'.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else: st.error("No se pudo marcar la factura como corregida.")
def actualizar_fecha_entrega_radicador_action(factura_id, set_date):
    fecha_entrega = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if set_date else None
    success = db_ops.actualizar_fecha_entrega_radicador(factura_id, fecha_entrega)
    if success:
        st.success("Fecha de entrega al radicador actualizada correctamente.")
        invalidate_facturas_cache()
        cancelar_edicion_action()
    else: st.error("No se pudo actualizar la fecha de entrega al radicador.")
def cancelar_edicion_action():
    st.session_state.editing_factura_id = None
    st.session_state.edit_mode = False
    st.session_state.refacturar_mode = False
    st.session_state.current_invoice_data = None
    st.session_state.form_key += 1
    if 'confirm_delete_id' in st.session_state: st.session_state.confirm_delete_id = None
    st.session_state.selected_invoice_input_key += 1
    st.session_state.filter_text_key += 1
    st.session_state.filter_select_key += 1

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if st.session_state['logged_in']: main_app_page()
else: login_page()
