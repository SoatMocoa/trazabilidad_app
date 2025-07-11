import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import pytz # Para manejar zonas horarias, si es necesario

# Asegúrate de que las rutas de importación sean correctas
import backend.database_operations as db_ops
import services.invoice_service as invoice_service
from utils.date_utils import parse_date, calculate_business_days_difference, validate_future_date
import io_utils
import constants

# --- Configuración Inicial ---
st.set_page_config(layout="wide", page_title="Sistema de Trazabilidad de Facturas")

# --- Funciones de Utilidad de la Aplicación ---

# Función para invalidar el caché de Streamlit para las facturas
def invalidate_facturas_cache():
    if 'facturas_df' in st.session_state:
        del st.session_state['facturas_df']
    if 'facturas_data_raw' in st.session_state:
        del st.session_state['facturas_data_raw']
    print("DEBUG: Caché de facturas invalidado.")

# Función para cargar facturas y almacenarlas en caché
@st.cache_data(ttl=300, show_spinner="Cargando facturas...")
def get_facturas_data_cached(search_term=None, search_column=None):
    data = db_ops.cargar_facturas(search_term, search_column)
    if data:
        # Los índices deben coincidir con la consulta SELECT en obtener_factura_por_id
        df = pd.DataFrame(data, columns=[
            'id', 'numero_factura', 'area_servicio', 'facturador', 'fecha_generacion', 'eps',
            'fecha_hora_entrega', 'tiene_correccion', 'descripcion_devolucion',
            'fecha_devolucion_lider', 'revisado', 'factura_original_id', 'estado',
            'reemplazada_por_numero_factura', 'estado_auditoria', 'observacion_auditor',
            'tipo_error', 'fecha_reemplazo',
            'num_fact_original_linked', 'fecha_gen_original_linked', 'fecha_entrega_radicador'
        ])
        # Convertir a datetime si es posible para facilitar cálculos
        df['fecha_generacion'] = pd.to_datetime(df['fecha_generacion'], errors='coerce').dt.date
        df['fecha_hora_entrega'] = pd.to_datetime(df['fecha_hora_entrega'], errors='coerce')
        df['fecha_devolucion_lider'] = pd.to_datetime(df['fecha_devolucion_lider'], errors='coerce').dt.date
        df['fecha_reemplazo'] = pd.to_datetime(df['fecha_reemplazo'], errors='coerce').dt.date
        df['fecha_gen_original_linked'] = pd.to_datetime(df['fecha_gen_original_linked'], errors='coerce').dt.date
        df['fecha_entrega_radicador'] = pd.to_datetime(df['fecha_entrega_radicador'], errors='coerce').dt.date
    else:
        df = pd.DataFrame(columns=[
            'id', 'numero_factura', 'area_servicio', 'facturador', 'fecha_generacion', 'eps',
            'fecha_hora_entrega', 'tiene_correccion', 'descripcion_devolucion',
            'fecha_devolucion_lider', 'revisado', 'factura_original_id', 'estado',
            'reemplazada_por_numero_factura', 'estado_auditoria', 'observacion_auditor',
            'tipo_error', 'fecha_reemplazo',
            'num_fact_original_linked', 'fecha_gen_original_linked', 'fecha_entrega_radicador'
        ])
    return data, df

def update_facturas_df():
    st.session_state['facturas_data_raw'], st.session_state['facturas_df'] = get_facturas_data_cached(
        st.session_state.get('search_term'), st.session_state.get('search_column')
    )

def cancelar_edicion_action():
    st.session_state['editing_factura_id'] = None
    st.session_state['show_edit_form'] = False
    invalidate_facturas_cache()
    st.rerun()

def get_status_color(row):
    # Lógica para determinar el color de la fila (rojo, amarillo, verde)
    # Convertir a objetos date si no lo son
    fecha_generacion = row['fecha_generacion']
    fecha_entrega_radicador = row['fecha_entrega_radicador']

    # Si la fecha de generación es un valor no válido (NaT), retornar vacío
    if pd.isna(fecha_generacion):
        return ""

    if row['estado_auditoria'] == 'Radicada OK':
        return "background-color: #d4edda" # Verde claro para OK
    elif row['estado_auditoria'] == 'Devuelta por Auditor' or row['estado_auditoria'] == 'Corregida por Legalizador':
        return "background-color: #fff3cd" # Amarillo claro para errores/correcciones
    elif row['estado'] == 'Reemplazada':
        return "background-color: #f8d7da" # Rojo claro si está reemplazada (aunque ahora no creamos nuevas)

    # Si tiene fecha de entrega radicador, el cálculo de los 22 días ya no aplica para "vencimiento"
    if pd.notna(fecha_entrega_radicador):
        return "background-color: #d4edda" # Considerar verde si ya se entregó al radicador

    # Calcular los días hábiles solo si fecha_generacion es una fecha válida
    # Ajustar para la zona horaria de Colombia
    timezone = pytz.timezone('America/Bogota') # Zona horaria de Mocoa, Putumayo, Colombia
    current_date = datetime.now(timezone).date()

    dias_habiles = calculate_business_days_difference(fecha_generacion, current_date)

    if dias_habiles > 22:
        return "background-color: #f8d7da"  # Rojo para vencidas
    else:
        return "background-color: #d1ecf1"  # Azul claro/verde para activas dentro del plazo

# --- Diseño de la Interfaz ---

def main_app_page():
    st.sidebar.title("Menú")
    user_role = st.session_state.get('user_role', 'guest')
    st.sidebar.write(f"Rol: **{user_role.capitalize()}**")

    # Contadores y estadísticas
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Facturas Pendientes de Auditoría", value=db_ops.obtener_conteo_facturas_pendientes_global())
    with col2:
        st.metric(label="Facturas Radicadas OK", value=db_ops.obtener_conteo_facturas_radicadas_ok())
    with col3:
        st.metric(label="Facturas con Errores/Corregidas", value=db_ops.obtener_conteo_facturas_con_errores())
    with col4:
        st.metric(label="Total de Facturas", value=db_ops.obtener_conteo_total_facturas())

    if user_role == 'legalizador':
        st.sidebar.header("Acciones de Legalizador")
        if st.sidebar.button("Registrar Nueva Factura"):
            st.session_state['show_entry_form'] = True
            st.session_state['show_edit_form'] = False # Ocultar edición si se muestra entrada
        if st.sidebar.button("Ver/Editar Facturas"):
            st.session_state['show_entry_form'] = False
            st.session_state['show_edit_form'] = False # Asegurarse de que no esté en modo edición al principio

    elif user_role == 'auditor':
        st.sidebar.header("Acciones de Auditor")
        if st.sidebar.button("Ver/Auditar Facturas"):
            st.session_state['show_entry_form'] = False
            st.session_state['show_edit_form'] = False

    st.title("Gestión de Facturas")

    # Mostrar formulario de registro si el botón fue presionado
    if st.session_state.get('show_entry_form', False) and user_role == 'legalizador':
        display_invoice_entry_form(user_role)
    elif st.session_state.get('show_edit_form', False) and st.session_state.get('editing_factura_id') is not None:
        display_invoice_edit_form(st.session_state['editing_factura_id'], user_role)
    else:
        display_invoice_table(user_role)

def display_invoice_entry_form(user_role):
    st.header("Registrar Nueva Factura")

    with st.form("new_invoice_form", clear_on_submit=True):
        facturador = st.text_input("Facturador", help="Nombre del facturador, por ejemplo, 'ALEXIS ERAZO'")
        eps = st.text_input("EPS", help="Nombre de la EPS, por ejemplo, 'ASOCIACION MUTUAL SER'")
        numero_factura = st.text_input("Número de Factura", help="Número único de la factura")
        fecha_generacion = st.date_input("Fecha de Generación", value=datetime.now(), format="YYYY-MM-DD").strftime('%Y-%m-%d')
        area_servicio = st.text_input("Área de Servicio", help="Área a la que pertenece la factura")

        # Campo para detalles SOAT
        needs_soat = st.checkbox("¿Necesita detalles SOAT?")
        selected_soat_date = None
        if needs_soat:
            selected_soat_date = st.date_input("Fecha de Generación SOAT", value=datetime.now(), format="YYYY-MM-DD").strftime('%Y-%m-%d')

        submitted = st.form_submit_button("Guardar Factura")
        if submitted:
            invoice_service.guardar_factura_action(facturador, eps, numero_factura, fecha_generacion, area_servicio, invalidate_facturas_cache, lambda: st.session_state.update({'show_entry_form': False, 'form_cleared': True}), selected_soat_date)
            # st.session_state.update({'show_entry_form': False, 'form_cleared': True}) # Esto puede ser manejado por el callback
            st.rerun()

    if st.button("Cancelar"):
        st.session_state['show_entry_form'] = False
        st.rerun()

def display_invoice_edit_form(factura_id, user_role):
    st.header(f"Editar Factura ID: {factura_id}")
    factura_data = db_ops.obtener_factura_por_id(factura_id)

    if not factura_data:
        st.warning("Factura no encontrada. Por favor, seleccione una factura válida.")
        st.session_state['editing_factura_id'] = None
        st.session_state['show_edit_form'] = False
        st.rerun()
        return

    # Mapear los datos de la tupla a nombres de variables legibles
    # Esto debe coincidir con el orden del SELECT en obtener_factura_por_id
    (
        id_factura, numero_factura_actual, area_servicio_actual, facturador_actual, fecha_generacion_actual, eps_actual,
        fecha_hora_entrega_actual, tiene_correccion_actual, descripcion_devolucion_actual,
        fecha_devolucion_lider_actual, revisado_actual, factura_original_id_actual, estado_actual,
        reemplazada_por_numero_factura_actual, estado_auditoria_actual, observacion_auditor_actual,
        tipo_error_actual, fecha_reemplazo_actual, num_fact_original_linked, fecha_gen_original_linked,
        fecha_entrega_radicador_actual
    ) = factura_data

    # Formato de fechas para los widgets de Streamlit
    fecha_generacion_dt = pd.to_datetime(fecha_generacion_actual).date() if fecha_generacion_actual else None
    fecha_hora_entrega_dt = pd.to_datetime(fecha_hora_entrega_actual) if fecha_hora_entrega_actual else None
    fecha_devolucion_lider_dt = pd.to_datetime(fecha_devolucion_lider_actual).date() if fecha_devolucion_lider_actual else None
    fecha_reemplazo_dt = pd.to_datetime(fecha_reemplazo_actual).date() if fecha_reemplazo_actual else None
    fecha_entrega_radicador_dt = pd.to_datetime(fecha_entrega_radicador_actual).date() if fecha_entrega_radicador_actual else None

    # Datos SOAT (si existen)
    soat_details = db_ops.obtener_detalles_soat_por_factura_id(factura_id)
    fecha_generacion_soat_dt = None
    if soat_details:
        fecha_generacion_soat_dt = pd.to_datetime(soat_details[2]).date() if soat_details[2] else None


    if user_role == 'legalizador':
        st.subheader("Datos de la Factura")
        with st.form("edit_invoice_form"):
            col1, col2 = st.columns(2)
            with col1:
                edited_numero_factura = st.text_input("Número de Factura", value=numero_factura_actual)
                edited_facturador = st.text_input("Facturador", value=facturador_actual)
                edited_fecha_generacion = st.date_input("Fecha de Generación", value=fecha_generacion_dt, format="YYYY-MM-DD")
                edited_area_servicio = st.text_input("Área de Servicio", value=area_servicio_actual)
            with col2:
                edited_eps = st.text_input("EPS", value=eps_actual)
                edited_fecha_hora_entrega = st.text_input("Fecha y Hora de Entrega", value=fecha_hora_entrega_actual)
                edited_tiene_correccion = st.checkbox("¿Tiene Corrección?", value=tiene_correccion_actual)
                edited_descripcion_devolucion = st.text_area("Descripción Devolución", value=descripcion_devolucion_actual)
                edited_fecha_devolucion_lider = st.date_input("Fecha Devolución Líder", value=fecha_devolucion_lider_dt, format="YYYY-MM-DD") if fecha_devolucion_lider_dt else st.date_input("Fecha Devolución Líder", value=None, format="YYYY-MM-DD")

            st.markdown("---")
            st.subheader("Detalles SOAT (si aplica)")
            if soat_details:
                st.write(f"Fecha de Generación SOAT Actual: {fecha_generacion_soat_dt}")
            else:
                st.info("No hay detalles SOAT registrados para esta factura.")
            
            # Opción para refacturar (usando la misma lógica de actualizar)
            st.markdown("---")
            st.subheader("Opciones de Refacturación")
            st.warning("Usar esta sección para 'Refacturar' una factura. Se actualizará la factura actual con los nuevos datos.")
            new_numero_factura_reemplazo = st.text_input("Nuevo Número de Factura para Refacturar", key="new_num_reemplazo")
            fecha_refacturacion = st.date_input("Fecha de Refacturación (nueva fecha de generación)", value=datetime.now(), format="YYYY-MM-DD", key="fecha_reemp_input")

            submitted_update = st.form_submit_button("Actualizar Factura")
            submitted_refactor = st.form_submit_button("Refacturar y Reiniciar")


            if submitted_update:
                # Lógica para la actualización normal (sin refacturar)
                success = db_ops.actualizar_factura(
                    factura_id,
                    edited_numero_factura,
                    edited_area_servicio,
                    edited_facturador,
                    edited_fecha_generacion.strftime('%Y-%m-%d'), # Formato a string
                    edited_eps,
                    edited_fecha_hora_entrega,
                    edited_tiene_correccion,
                    edited_descripcion_devolucion,
                    edited_fecha_devolucion_lider.strftime('%Y-%m-%d') if edited_fecha_devolucion_lider else None, # Formato a string
                    revisado_actual, # Mantener revisado_actual para edición normal
                    factura_original_id_actual,
                    estado_actual,
                    reemplazada_por_numero_factura_actual,
                    estado_auditoria_actual,
                    observacion_auditor_actual,
                    tipo_error_actual,
                    fecha_reemplazo_actual.strftime('%Y-%m-%d') if fecha_reemplazo_actual else None, # Formato a string
                    fecha_entrega_radicador_actual.strftime('%Y-%m-%d') if fecha_entrega_radicador_actual else None
                )
                if success:
                    st.success("Factura actualizada correctamente.")
                    cancelar_edicion_action() # Recarga la tabla
                else:
                    st.error("Error al actualizar la factura. El número de factura podría estar duplicado.")

            elif submitted_refactor:
                # Lógica para la refacturación (usando la nueva función del service)
                if not new_numero_factura_reemplazo:
                    st.error("Para refacturar, el 'Nuevo Número de Factura para Refacturar' es obligatorio.")
                else:
                    invoice_service.realizar_refacturacion_action(
                        factura_id_a_refacturar=factura_id,
                        nuevo_numero_factura_str=new_numero_factura_reemplazo,
                        fecha_refacturacion_str=fecha_refacturacion.strftime('%Y-%m-%d'),
                        invalidate_cache_callback=invalidate_facturas_cache,
                        cancel_edit_callback=cancelar_edicion_action
                    )


    elif user_role == 'auditor':
        st.subheader("Auditoría de Factura")
        with st.form("audit_form"):
            current_estado_auditoria = estado_auditoria_actual if estado_auditoria_actual else 'Pendiente'
            current_observacion_auditor = observacion_auditor_actual if observacion_auditor_actual else ""
            current_tipo_error = tipo_error_actual if tipo_error_actual else ""

            # Mostrar campos no editables para el auditor
            st.text_input("Número de Factura", value=numero_factura_actual, disabled=True)
            st.text_input("Facturador", value=facturador_actual, disabled=True)
            st.text_input("EPS", value=eps_actual, disabled=True)
            st.date_input("Fecha de Generación", value=fecha_generacion_dt, disabled=True, format="YYYY-MM-DD")
            st.text_input("Área de Servicio", value=area_servicio_actual, disabled=True)
            st.text_input("Fecha y Hora de Entrega", value=fecha_hora_entrega_actual, disabled=True)

            # Campos editables por el auditor
            new_estado_auditoria = st.selectbox("Estado de Auditoría", options=constants.ESTADOS_AUDITORIA, index=constants.ESTADOS_AUDITORIA.index(current_estado_auditoria))
            new_observacion_auditor = st.text_area("Observación del Auditor", value=current_observacion_auditor)
            new_tipo_error = st.selectbox("Tipo de Error", options=constants.TIPOS_ERROR, index=constants.TIPOS_ERROR.index(current_tipo_error))

            submitted_audit = st.form_submit_button("Actualizar Auditoría")
            if submitted_audit:
                invoice_service.auditar_factura_action(factura_id, new_estado_auditoria, new_observacion_auditor, new_tipo_error, invalidate_facturas_cache, cancelar_edicion_action)

            if new_estado_auditoria == "Radicada OK":
                st.markdown("---")
                st.subheader("Registro de Radicación OK")
                fecha_entrega_radicador_value = fecha_entrega_radicador_dt if fecha_entrega_radicador_dt else datetime.now().date()
                new_fecha_entrega_radicador = st.date_input("Fecha de Entrega al Radicador", value=fecha_entrega_radicador_value, format="YYYY-MM-DD")
                if st.button("Guardar Fecha de Entrega al Radicador"):
                    invoice_service.actualizar_fecha_entrega_action(factura_id, new_fecha_entrega_radicador.strftime('%Y-%m-%d'), invalidate_facturas_cache, cancelar_edicion_action)

    if st.button("Cancelar Edición/Auditoría"):
        cancelar_edicion_action()

def display_invoice_table(user_role):
    st.subheader("Listado de Facturas")

    # Búsqueda
    search_col, filter_col = st.columns([3, 1])
    with search_col:
        search_term = st.text_input("Buscar factura por número o facturador", key="search_input")
    with filter_col:
        search_column = st.selectbox("Buscar en", options=["numero_factura", "facturador", "eps", "area_servicio", "estado_auditoria"], key="search_column")

    # Actualizar la sesión para que el caché sepa qué buscar
    st.session_state['search_term'] = search_term
    st.session_state['search_column'] = search_column

    # Cargar datos (usando la función que maneja el caché)
    facturas_data_raw, facturas_df = get_facturas_data_cached(search_term, search_column)
    st.session_state['facturas_data_raw'] = facturas_data_raw # Guardar para posible uso futuro
    st.session_state['facturas_df'] = facturas_df # Guardar el DataFrame en session_state

    if not facturas_df.empty:
        # Añadir columna de "Días Hábiles" y "Color de Estado" para la visualización
        facturas_df['Días Hábiles'] = facturas_df.apply(lambda row: calculate_business_days_difference(row['fecha_generacion'], datetime.now().date()) if pd.notna(row['fecha_generacion']) and pd.isna(row['fecha_entrega_radicador']) else None, axis=1)
        
        # Aplicar el estilo de color
        styled_df = facturas_df.style.apply(get_status_color, axis=1)

        # Mostrar tabla interactiva (sin posibilidad de edición directa aquí)
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        # Selección para edición/eliminación
        selected_invoice_id = st.selectbox(
            "Seleccionar Factura por ID para Editar/Auditar/Eliminar:",
            options=[None] + facturas_df['id'].tolist(),
            format_func=lambda x: f"ID: {x} - {facturas_df[facturas_df['id'] == x]['numero_factura'].iloc[0]}" if x else "Seleccione una factura"
        )

        if selected_invoice_id:
            st.session_state['editing_factura_id'] = selected_invoice_id
            st.session_state['show_edit_form'] = True
            st.rerun()

    else:
        st.info("No hay facturas registradas que coincidan con los criterios de búsqueda.")

# --- Lógica de Autenticación ---

def login_page():
    st.title("Iniciar Sesión")
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        with st.form("login_form"):
            username = st.text_input("Usuario")
            password = st.text_input("Contraseña", type="password")
            submitted = st.form_submit_button("Entrar")

            if submitted:
                user_data = db_ops.obtener_credenciales_usuario(username)
                if user_data and user_data[0] == password:
                    st.session_state['logged_in'] = True
                    st.session_state['user_role'] = user_data[1]
                    st.success(f"¡Bienvenido, {username} ({user_data[1]})!")
                    # Inicializar otros estados para la app principal
                    st.session_state['show_entry_form'] = False
                    st.session_state['show_edit_form'] = False
                    st.session_state['editing_factura_id'] = None
                    update_facturas_df() # Cargar las facturas inicialmente
                    st.rerun()
                else:
                    st.error("Usuario o contraseña incorrectos.")
    else:
        st.sidebar.button("Cerrar Sesión", on_click=logout)
        main_app_page()

def logout():
    st.session_state['logged_in'] = False
    st.session_state['user_role'] = None
    if 'facturas_df' in st.session_state:
        del st.session_state['facturas_df']
    if 'facturas_data_raw' in st.session_state:
        del st.session_state['facturas_data_raw']
    st.rerun()

# --- Punto de Entrada de la Aplicación ---
if __name__ == "__main__":
    db_ops.crear_tablas() # Asegurar que las tablas existan al iniciar
    login_page()