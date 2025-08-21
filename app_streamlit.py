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
import numpy as np

st.set_page_config(layout="wide")
db_ops.crear_tablas()

if not os.path.exists('data'):
    os.makedirs('data')

def initialize_session_state():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
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
    if 'bulk_facturador_key' not in st.session_state:
        st.session_state.bulk_facturador_key = 0
    if 'bulk_eps_key' not in st.session_state:
        st.session_state.bulk_eps_key = 0
    if 'bulk_area_servicio_key' not in st.session_state:
        st.session_state.bulk_area_servicio_key = 0

initialize_session_state()

@st.cache_data(ttl=60)
def get_cached_facturas(search_term, search_column):
    return db_ops.cargar_facturas(search_term, search_column)

@st.cache_data(ttl=300)
def get_cached_statistics():
    return {
        "total_pendientes": db_ops.obtener_conteo_facturas_pendientes_global() or 0,
        "total_lista_para_radicar": db_ops.obtener_conteo_facturas_lista_para_radicar() or 0,
        "total_en_radicador": db_ops.obtener_conteo_facturas_en_radicador() or 0,
        "total_errores": db_ops.obtener_conteo_facturas_con_errores() or 0,
        "total_general": db_ops.obtener_conteo_total_facturas() or 0,
        "stats_por_legalizador_eps": db_ops.obtener_conteo_facturas_por_legalizador_y_eps() or []
    }

def invalidate_all_caches():
    get_cached_facturas.clear()
    get_cached_statistics.clear()
    keys_to_remove = [key for key in st.session_state.keys() if key.startswith('df_cache_')]
    for key in keys_to_remove:
        del st.session_state[key]
    if 'last_search_tuple' in st.session_state:
        del st.session_state['last_search_tuple']

def get_selectbox_default_index(options_list, current_value):
    if current_value:
        try:
            return options_list.index(current_value) + 1
        except ValueError:
            pass
    return 0

def _process_factura_for_display_df(df_raw):
    if df_raw is None or len(df_raw) == 0:
        return pd.DataFrame(columns=[
            'ID', 'Área de Servicio', 'Facturador', 'EPS', 'Número de Factura',
            'Número Reemplazo Factura', 'Fecha Generación', 'Fecha Reemplazo Factura',
            'Fecha de Entrega', 'Días Restantes', 'Estado', 'Estado Auditoria',
            'Tipo de Error', 'Observación Auditor', 'Fecha Entrega Radicador'
        ])

    if not isinstance(df_raw, pd.DataFrame):
        df = pd.DataFrame(df_raw)
    else:
        df = df_raw.copy()

    hoy = pd.Timestamp('today').normalize()

    date_columns = ['fecha_generacion', 'fecha_reemplazo', 'fecha_hora_entrega', 'fecha_entrega_radicador', 'fecha_gen_original_linked']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize() # Usamos normalize() para quedarnos solo con la fecha

    df['fecha_base_calculo'] = df['fecha_reemplazo'].combine_first(df['fecha_generacion'])

    df['fecha_limite_liquidacion_obj'] = df['fecha_base_calculo'].apply(
        lambda x: sumar_dias_habiles(x, 21) if not pd.isnull(x) else None
    )

    def calcular_dias_para_fila(fecha_limite):
        if pd.isnull(fecha_limite):
            return None
        dias = calcular_dias_habiles_entre_fechas(hoy, fecha_limite)
        if fecha_limite < hoy:
            return dias * -1
        return dias

    df['Días Restantes'] = df['fecha_limite_liquidacion_obj'].apply(calcular_dias_para_fila)

    cond_vencidas = (
        (df['Días Restantes'] < 0) &
        (~df['estado_auditoria'].isin(['Devuelta por Auditor', 'Corregida por Legalizador', 'En Radicador', 'Radicada y Aceptada'])) &
        (df['estado'] != 'Reemplazada')
    )
    cond_hoy_vence = (
        (df['Días Restantes'] == 0) &
        (~df['estado_auditoria'].isin(['Devuelta por Auditor', 'Corregida por Legalizador', 'En Radicador', 'Radicada y Aceptada'])) &
        (df['estado'] != 'Reemplazada')
    )
    df.loc[cond_vencidas, 'Días Restantes'] = "Refacturar"
    df.loc[cond_hoy_vence, 'Días Restantes'] = "Hoy Vence"

    df['Número de Factura'] = np.where(
        df['factura_original_id'].notnull(),
        df['num_fact_original_linked'],
        df['numero_factura']
    )
    df['Número Reemplazo Factura'] = np.where(
        df['factura_original_id'].notnull(),
        df['numero_factura'],
        np.where(
            df['estado'] == 'Reemplazada',
            df['reemplazada_por_numero_factura'],
            ""
        )
    )
    df['Fecha Generación'] = np.where(
        df['factura_original_id'].notnull(),
        df['fecha_gen_original_linked'],
        df['fecha_generacion']
    )
    df['Fecha Reemplazo Factura'] = np.where(
        df['factura_original_id'].notnull(),
        df['fecha_generacion'].dt.strftime('%Y-%m-%d'),
        np.where(
            df['estado'] == 'Reemplazada',
            df['fecha_reemplazo'].dt.strftime('%Y-%m-%d'),
            ""
        )
    )
    df['Estado'] = np.where(df['factura_original_id'].notnull(), "Reemplazada", df['estado'])

    cond_estado_vencidas = (df['Días Restantes'].isin(["Refacturar", "Hoy Vence"]))
    df['Estado'] = np.where(cond_estado_vencidas, "Vencidas", df['Estado'])

    df['Fecha Generación'] = df['Fecha Generación'].dt.strftime('%Y-%m-%d').fillna('')
    df['Fecha de Entrega'] = df['fecha_hora_entrega'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    df['Fecha Entrega Radicador'] = df['fecha_entrega_radicador'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

    df = df.rename(columns={
        'id': 'ID',
        'area_servicio': 'Área de Servicio',
        'facturador': 'Facturador',
        'eps': 'EPS',
        'estado_auditoria': 'Estado Auditoria',
        'tipo_error': 'Tipo de Error',
        'observacion_auditor': 'Observación Auditor'
    })

    columnas_finales = [
        'ID', 'Área de Servicio', 'Facturador', 'EPS', 'Número de Factura',
        'Número Reemplazo Factura', 'Fecha Generación', 'Fecha Reemplazo Factura',
        'Fecha de Entrega', 'Días Restantes', 'Estado', 'Estado Auditoria',
        'Tipo de Error', 'Observación Auditor', 'Fecha Entrega Radicador'
    ]
    for col in columnas_finales:
        if col not in df.columns:
            df[col] = None

    return df[columnas_finales]

def login_page():
    st.title("Iniciar Sesión - Trazabilidad de Facturas")
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
                else:
                    st.error("Contraseña incorrecta.")
            else:
                st.error("Usuario no encontrado.")

def main_app_page():
    st.title("Trazabilidad de Facturas - Hospital Jose Maria Hernandez de Mocoa")
    user_role = st.session_state.get('user_role', 'guest')
    st.sidebar.header(f"Bienvenido, {st.session_state.get('username')} ({user_role})")
    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.logged_in = False
        st.session_state.username = None
        st.session_state.user_role = None
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

def display_invoice_entry_form(user_role):
    current_data = st.session_state.current_invoice_data
    fecha_generacion_val = current_data['fecha_generacion'].strftime('%Y-%m-%d') if current_data and isinstance(current_data.get('fecha_generacion'), date) else ""

    with st.form(key=f"invoice_entry_form_{st.session_state.form_key}", clear_on_submit=False):
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
        if st.session_state.refacturar_mode:
            st.markdown("---")
            st.subheader("Datos de Refacturación")
            new_numero_factura = st.text_input("Nuevo Número de Factura:")
            fecha_reemplazo_factura = st.text_input("Fecha de Generación de la Nueva Factura (YYYY-MM-DD o DD/MM/YYYY):", value=datetime.now().strftime('%Y-%m-%d'))
        else:
            new_numero_factura = None
            fecha_reemplazo_factura = None

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

        if submitted:
            if st.session_state.refacturar_mode:
                guardar_factura_reemplazo_action(st.session_state.editing_factura_id, new_numero_factura, fecha_reemplazo_factura, facturador, eps, area_servicio)
            elif st.session_state.edit_mode:
                original_data = db_ops.obtener_factura_por_id(st.session_state.editing_factura_id)
                if original_data:
                    actualizar_factura_action(
                        st.session_state.editing_factura_id,
                        numero_factura,
                        area_servicio,
                        facturador,
                        fecha_generacion,
                        eps,
                        original_data.get('fecha_hora_entrega'),
                        original_data.get('tiene_correccion'),
                        original_data.get('descripcion_devolucion'),
                        original_data.get('fecha_devolucion_lider'),
                        original_data.get('revisado'),
                        original_data.get('factura_original_id'),
                        original_data.get('estado'),
                        original_data.get('reemplazada_por_numero_factura'),
                        original_data.get('estado_auditoria'),
                        original_data.get('observacion_auditor'),
                        original_data.get('tipo_error'),
                        original_data.get('fecha_reemplazo')
                    )
                else:
                    st.error("Error: No se pudo recuperar la factura original para actualizar.")
            else:
                guardar_factura_action(facturador, eps, numero_factura, fecha_generacion, area_servicio)
            st.rerun()

def display_bulk_load_section():
    with st.form("bulk_load_form"):
        st.write("Por favor, selecciona el Legalizador, EPS y Área de Servicio para todas las facturas del CSV.")
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
            inserted_count, skipped_count, total_rows = 0, 0, 0
            df = pd.read_csv(uploaded_file)
            required_columns_csv = ['Numero de Factura', 'Fecha de Generacion']
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
                
                factura_existente = db_ops.obtener_factura_por_numero(numero_factura_csv)
                if factura_existente:
                    st.info(f"Fila {index+2}: Factura '{numero_factura_csv}' ya existe. Saltando para evitar duplicados.")
                    skipped_count += 1
                    continue

                fecha_generacion_csv_obj = parse_date(fecha_str_csv, f"Fecha de Generación (Fila {index+2})")
                if fecha_generacion_csv_obj is None or not validate_future_date(fecha_generacion_csv_obj, f"Fecha de Generación (Fila {index+2})"):
                    skipped_count += 1
                    continue
                fecha_generacion_db = fecha_generacion_csv_obj
                fecha_hora_entrega_db = datetime.now()

                if area_servicio_bulk in ["Hospitalizacion", "Urgencias"]:
                    estado_auditoria_automatico_masivo = "Lista para Radicar"
                else:
                    estado_auditoria_automatico_masivo = "Pendiente"
                
                factura_id = db_ops.guardar_factura(
                    numero_factura=numero_factura_csv,
                    area_servicio=area_servicio_bulk,
                    facturador=facturador_bulk,
                    fecha_generacion=fecha_generacion_db,
                    eps=eps_bulk,
                    fecha_hora_entrega=fecha_hora_entrega_db,
                    estado_auditoria=estado_auditoria_automatico_masivo  # <-- Pasa el nuevo parámetro
                )

                if factura_id:
                    if area_servicio_bulk == "SOAT":
                        db_ops.guardar_detalles_soat(factura_id, fecha_generacion_db)
                    inserted_count += 1
                else:
                    skipped_count += 1
                    st.warning(f"Fila {index+2}: Error al insertar la factura '{numero_factura_csv}'. Saltando.")

            st.success(f"Carga masiva finalizada.\nTotal de filas procesadas: {total_rows}\nFacturas insertadas: {inserted_count}\nFacturas omitidas (duplicadas/errores): {skipped_count}")
            invalidate_all_caches()
            st.session_state.bulk_facturador_key += 1
            st.session_state.bulk_eps_key += 1
            st.session_state.bulk_area_servicio_key += 1
            st.rerun()

def display_statistics():
    st.subheader("Estadísticas Generales de Facturas")
    stats_data = get_cached_statistics()
    total_pendientes = stats_data["total_pendientes"]
    total_lista_para_radicar = stats_data["total_lista_para_radicar"]
    total_en_radicador = stats_data["total_en_radicador"]
    total_errores = stats_data["total_errores"]
    total_general = stats_data["total_general"]
    df_stats = pd.DataFrame(stats_data["stats_por_legalizador_eps"], columns=["Legalizador", "EPS", "Facturas Pendientes"])
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Facturas Pendientes", value=total_pendientes)
        st.metric(label="Facturas En Radicador", value=total_en_radicador)
    with col2:
        st.metric(label="Facturas Lista para Radicar", value=total_lista_para_radicar)
        st.metric(label="Facturas con Errores", value=total_errores)
    with col3:
        facturas_raw_for_vencidas = get_cached_facturas(search_term="", search_column="")
        processed_facturas_df = _process_factura_for_display_df(facturas_raw_for_vencidas)
        total_vencidas = (processed_facturas_df["Días Restantes"] == "Refacturar").sum()
        st.metric(label="Facturas Vencidas (Refacturar)", value=total_vencidas)
    
    st.metric(label="Total General de Facturas", value=total_general)
    st.markdown("---")
    st.subheader("Conteo por Legalizador y EPS (Facturas Pendientes)")
    if not df_stats.empty:
        st.dataframe(df_stats, use_container_width=True, hide_index=True)
    else:
        st.info("No hay estadísticas disponibles de facturas pendientes.")

def highlight_rows(row):
    columns_to_colorize = [
        'Días Restantes',
        'Estado',
        'Estado Auditoria',
        'Tipo de Error',
        'Observación Auditor'
    ]
    styles_list = [''] * len(row)
    
    base_color = ''
    if row["Estado Auditoria"] == 'Devuelta por Auditor':
        base_color = 'background-color: lightblue'
    elif row["Estado Auditoria"] == 'Corregida por Legalizador':
        base_color = 'background-color: lightsalmon'
    elif row["Días Restantes"] == "Refacturar":
        base_color = 'background-color: salmon'
    else:
        try:
            dias_restantes_num = int(row["Días Restantes"])
            if 1 <= dias_restantes_num <= 3:
                base_color = 'background-color: yellow'
            elif dias_restantes_num > 3:
                base_color = 'background-color: lightgreen'
        except (ValueError, TypeError):
            pass

    for i, col_name in enumerate(row.index):
        if col_name in columns_to_colorize:
            styles_list[i] = base_color
            
    return styles_list

def display_invoice_table(user_role):
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

    cache_key = f"df_cache_{current_search_term}_{db_column_name}"
    current_search_tuple = (current_search_term, db_column_name)

    if (cache_key not in st.session_state or 
        st.session_state.get('last_search_tuple') != current_search_tuple):
        
        facturas_raw = get_cached_facturas(search_term=current_search_term, 
                                         search_column=db_column_name)
        st.session_state[cache_key] = _process_factura_for_display_df(facturas_raw)
        st.session_state['last_search_tuple'] = current_search_tuple
        st.session_state['current_page'] = 0  # Resetear a primera página al buscar nuevo término

    df_facturas = st.session_state[cache_key]

    rows_per_page = 20
    total_rows = len(df_facturas)
    total_pages = max(1, (total_rows + rows_per_page - 1) // rows_per_page)

    if 'current_page' not in st.session_state:
        st.session_state.current_page = 0
    else:
        st.session_state.current_page = max(0, min(st.session_state.current_page, total_pages - 1))

    if not df_facturas.empty:
        start_idx = st.session_state.current_page * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)
        df_page = df_facturas.iloc[start_idx:end_idx].copy()

        df_page['sort_key'] = df_page.apply(
            lambda row: 1 if row["Estado Auditoria"] == 'Devuelta por Auditor' 
            else 2 if row["Estado Auditoria"] == 'Corregida por Legalizador' 
            else 3 if row["Días Restantes"] == "Refacturar" 
            else 4, axis=1
        )
        df_page = df_page.sort_values(by=['sort_key', 'Fecha Generación'], ascending=[True, False])
        df_page = df_page.drop(columns=['sort_key'])

        st.dataframe(df_page.style.apply(highlight_rows, axis=1), 
                    use_container_width=True, hide_index=True)

        col_prev, col_page_info, col_next = st.columns([1, 3, 1])
        with col_prev:
            if st.button("⏪ Anterior", disabled=(st.session_state.current_page == 0)):
                st.session_state.current_page -= 1
                st.rerun()
        with col_page_info:
            st.markdown(f"**Página {st.session_state.current_page + 1} de {total_pages}** | **Filas: {start_idx + 1}-{end_idx} de {total_rows}**")
        with col_next:
            if st.button("Siguiente ⏩", disabled=(st.session_state.current_page >= total_pages - 1)):
                st.session_state.current_page += 1
                st.rerun()
    else:
        st.info("No hay facturas registradas que coincidan con los criterios de búsqueda.")

    if not df_facturas.empty and user_role == 'auditor':
        st.markdown("### Entrega Masiva al Radicador")
        selectable_ids = df_facturas.loc[
            (df_facturas['Estado Auditoria'].isin(['Lista para Radicar', 'En Radicador'])) &
            (df_facturas['Fecha Entrega Radicador'].isna() | (df_facturas['Fecha Entrega Radicador'] == '')),
            'ID'
        ].tolist()
        
        with st.form("entrega_masiva_form"):
            selected_ids = st.multiselect("Seleccione las facturas a marcar como entregadas:", selectable_ids)
            submitted = st.form_submit_button("Marcar seleccionadas como entregadas")
    
            if submitted and selected_ids:
                fecha_entrega = datetime.now()
                entregadas_count = db_ops.entregar_facturas_radicador(selected_ids, fecha_entrega)
                
                if entregadas_count > 0:
                    st.success(f"{entregadas_count} facturas marcadas como entregadas.")
                else:
                    st.warning("No se pudieron marcar facturas como entregadas.")
                
                invalidate_all_caches()
                if cache_key in st.session_state:
                    del st.session_state[cache_key]
                st.rerun()

    col_export, col_edit, col_refacturar, col_delete_placeholder = st.columns(4)
    with col_export:
        if st.button("Exportar a CSV"):
            export_df_to_csv(df_facturas)  # Exporta TODAS las facturas filtradas, no solo la página

    selected_invoice_id = st.number_input("ID de Factura para Acción:", 
                                        min_value=0, 
                                        step=1, 
                                        key=f"selected_invoice_id_input_{st.session_state.selected_invoice_input_key}")

    if selected_invoice_id > 0:
        factura_data_for_action = db_ops.obtener_factura_por_id(selected_invoice_id)
        if factura_data_for_action:
            st.session_state.current_invoice_data = factura_data_for_action
            
            with col_edit:
                if st.button("Cargar para Edición", key="edit_button"):
                    cargar_factura_para_edicion_action(selected_invoice_id)
                    st.rerun()
                    
            with col_refacturar:
                if selected_invoice_id in df_facturas['ID'].values:
                    dias_restantes_df = df_facturas[df_facturas['ID'] == selected_invoice_id]['Días Restantes'].iloc[0]
                    if dias_restantes_df == "Refacturar":
                        if st.button("Refacturar", key="refacturar_button"):
                            cargar_factura_para_refacturar_action(selected_invoice_id)
                            st.rerun()

            if user_role == 'auditor':
                st.markdown("---")
                st.subheader("Acciones de Auditoría para Factura Seleccionada")
                
                estado_auditoria_default_index = 0
                if st.session_state.current_invoice_data['estado_auditoria']:
                    try: 
                        estado_auditoria_default_index = ESTADO_AUDITORIA_OPCIONES.index(
                            st.session_state.current_invoice_data['estado_auditoria'])
                    except ValueError: 
                        estado_auditoria_default_index = 0
                
                tipo_error_default_index = 0
                if st.session_state.current_invoice_data['tipo_error']:
                    try: 
                        tipo_error_default_index = TIPO_ERROR_OPCIONES.index(
                            st.session_state.current_invoice_data['tipo_error'])
                    except ValueError: 
                        tipo_error_default_index = 0
                
                with st.form(key=f"auditoria_form_{selected_invoice_id}", clear_on_submit=False):
                    estado_auditoria_options_filtered = [opt for opt in ESTADO_AUDITORIA_OPCIONES 
                                                       if opt != 'Radicada y Aceptada']
                    estado_auditoria_input = st.selectbox("Estado Auditoría:", 
                                                        options=estado_auditoria_options_filtered, 
                                                        index=estado_auditoria_default_index, 
                                                        key=f"estado_auditoria_{selected_invoice_id}")
                    
                    tipo_error_input = st.selectbox("Tipo de Error:", 
                                                  options=TIPO_ERROR_OPCIONES, 
                                                  index=tipo_error_default_index, 
                                                  key=f"tipo_error_{selected_invoice_id}")
                    
                    observacion_auditor_input = st.text_area("Observación Auditor:", 
                                                           value=st.session_state.current_invoice_data['observacion_auditor'] or "", 
                                                           key=f"observacion_auditor_{selected_invoice_id}")
                    
                    submit_auditoria = st.form_submit_button("Auditar Factura")
                    if submit_auditoria:
                        auditar_factura_action(selected_invoice_id, estado_auditoria_input, 
                                             observacion_auditor_input, tipo_error_input)
                        st.rerun()
                
                if factura_data_for_action['estado_auditoria'] in ['Lista para Radicar', 'En Radicador']:
                    fecha_entrega_radicador_val = factura_data_for_action['fecha_entrega_radicador']
                    fecha_entrega_radicador_checked = st.checkbox(
                        "Factura Entregada al Radicador",
                        value=bool(fecha_entrega_radicador_val),
                        key=f"radicador_checkbox_{selected_invoice_id}"
                    )
                    if fecha_entrega_radicador_checked != bool(fecha_entrega_radicador_val):
                        actualizar_fecha_entrega_radicador_action(selected_invoice_id, 
                                                                fecha_entrega_radicador_checked)
                        st.rerun()
                
                if st.button("Eliminar Factura", key=f"delete_button_{selected_invoice_id}"):
                    st.session_state.confirm_delete_id = selected_invoice_id
                
                if ('confirm_delete_id' in st.session_state and 
                    st.session_state.confirm_delete_id == selected_invoice_id):
                    
                    st.warning(f"¿Estás seguro de que quieres eliminar la factura ID: {selected_invoice_id}?\nEsta acción es irreversible.")
                    col_confirm_del, col_cancel_del = st.columns(2)
                    with col_confirm_del:
                        if st.button("Confirmar Eliminación", key="confirm_delete_button_modal"):
                            success = eliminar_factura_action(selected_invoice_id)
                            if success:
                                st.success(f"Factura ID: {selected_invoice_id} eliminada correctamente.")
                                st.session_state.confirm_delete_id = None
                                invalidate_all_caches()
                                if cache_key in st.session_state:
                                    del st.session_state[cache_key]
                                cancelar_edicion_action()
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
        if st.session_state.current_invoice_data is not None and selected_invoice_id == 0:
            cancelar_edicion_action()
            st.rerun()

def guardar_factura_action(facturador, eps, numero_factura, fecha_generacion_str, area_servicio):
    if not all([facturador, eps, numero_factura, fecha_generacion_str, area_servicio]):
        st.error("Todos los campos son obligatorios.")
        return
    if not numero_factura.isdigit():
        st.error("El campo 'Número de Factura' debe contener solo números.")
        return
    
    factura_existente = db_ops.obtener_factura_por_numero(numero_factura)
    if factura_existente:
        st.error(f"Error: La factura con el número '{numero_factura}' ya existe en la base de datos.")
        return

    fecha_generacion_obj = parse_date(fecha_generacion_str, "Fecha de Generación")
    if fecha_generacion_obj is None:
        return
    if not validate_future_date(fecha_generacion_obj, "Fecha de Generación"):
        return
    fecha_generacion_db = fecha_generacion_obj
    fecha_hora_entrega = datetime.now()

    if area_servicio in ["Hospitalizacion", "Urgencias"]:
        estado_auditoria_automatico = "Lista para Radicar"
    else:
        estado_auditoria_automatico = "Pendiente"

    factura_id = db_ops.guardar_factura(
        numero_factura=numero_factura,
        area_servicio=area_servicio,
        facturador=facturador,
        fecha_generacion=fecha_generacion_db,
        eps=eps,
        fecha_hora_entrega=fecha_hora_entrega,
        estado_auditoria=estado_auditoria_automatico  # <-- Nuevo parámetro
    )
    if factura_id:
        if area_servicio == "SOAT":
            db_ops.guardar_detalles_soat(factura_id, fecha_generacion_db)
        st.success(f"Factura guardada correctamente. Estado: {estado_auditoria_automatico}")
        invalidate_all_caches()
        cancelar_edicion_action()
    else:
        st.error(f"Error al guardar la factura. Esto puede ocurrir si el número de factura ya existe. Por favor, verifique y vuelva a intentarlo.")

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
    if fecha_generacion_obj is None: return
    if not validate_future_date(fecha_generacion_obj, "Fecha de Generación"): return
    fecha_generacion_db = fecha_generacion_obj
    success = db_ops.actualizar_factura(
        factura_id, numero_factura, area_servicio, facturador, fecha_generacion_db, eps,
        fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
        fecha_devolucion_lider, revisado, factura_original_id, estado,
        reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo
    )
    if success:
        st.success("Factura actualizada correctamente.")
        invalidate_all_caches()
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
        st.session_state.form_key += 1
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
        st.session_state.form_key += 1
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
        invalidate_all_caches()
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
    
    factura_existente = db_ops.obtener_factura_por_numero(new_numero_factura)
    if factura_existente:
        st.error(f"Error: El nuevo número de factura '{new_numero_factura}' ya existe en la base de datos. Por favor, ingrese un número diferente.")
        return

    fecha_reemplazo_factura_obj = parse_date(fecha_reemplazo_factura_str, "Fecha de Generación de la Nueva Factura")
    if fecha_reemplazo_factura_obj is None: return
    if not validate_future_date(fecha_reemplazo_factura_obj, "Fecha de Generación de la Nueva Factura"): return
    
    success = db_ops.guardar_factura_reemplazo(
        old_factura_id,
        new_numero_factura,
        fecha_reemplazo_factura_obj
    )
    
    if success:
        st.success(f"Factura ID: {old_factura_id} actualizada como reemplazada por {new_numero_factura} correctamente.")
        invalidate_all_caches()
        cancelar_edicion_action()
    else:
        st.error(f"No se pudo guardar la factura de reemplazo.")

def marcar_como_corregida_action(factura_id, observacion_actual, tipo_error_actual):
    if st.session_state.user_role != 'legalizador':
        st.error("Permiso Denegado. Solo los legalizadores pueden marcar facturas como corregidas.")
        return
    success = db_ops.actualizar_estado_auditoria_factura(factura_id, "Corregida por Legalizador", observacion_actual, tipo_error_actual)
    if success:
        st.success(f"Factura ID: {factura_id} marcada como 'Corregida por Legalizador'.")
        invalidate_all_caches()
        cancelar_edicion_action()
    else:
        st.error("No se pudo marcar la factura como corregida.")

def actualizar_fecha_entrega_radicador_action(factura_id, set_date):
    fecha_entrega = datetime.now() if set_date else None
    success = db_ops.actualizar_fecha_entrega_radicador(factura_id, fecha_entrega)
    if success:
        st.success("Fecha de entrega al radicador actualizada correctamente.")
        invalidate_all_caches()
        cancelar_edicion_action()
    else:
        st.error("No se pudo actualizar la fecha de entrega al radicador.")

def cancelar_edicion_action():
    st.session_state.editing_factura_id = None
    st.session_state.edit_mode = False
    st.session_state.refacturar_mode = False
    st.session_state.current_invoice_data = None
    st.session_state.form_key += 1
    if 'confirm_delete_id' in st.session_state:
        st.session_state.confirm_delete_id = None
    st.session_state.selected_invoice_input_key += 1
    st.session_state.filter_text_key += 1
    st.session_state.filter_select_key += 1

if st.session_state['logged_in']:
    main_app_page()
else:
    login_page()
