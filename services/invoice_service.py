import streamlit as st
from datetime import datetime

import backend.database_operations as db_ops
from utils.date_utils import parse_date, validate_future_date
import constants

# --- Funciones de acción para el servicio de facturas ---

def guardar_factura_action(facturador, eps, numero_factura, fecha_generacion, area_servicio, invalidate_cache_callback, reset_form_callback, selected_soat_date=None):
    if not all([numero_factura, fecha_generacion, facturador, eps, area_servicio]):
        st.error("Todos los campos obligatorios deben ser llenados.")
        return False
    
    if not numero_factura.isdigit():
        st.error("El 'Número de Factura' debe contener solo números.")
        return False

    fecha_generacion_obj = parse_date(fecha_generacion, "Fecha de Generación")
    if fecha_generacion_obj is None:
        return False

    if not validate_future_date(fecha_generacion_obj, "Fecha de Generación"):
        return False

    fecha_hora_entrega = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    factura_id = db_ops.guardar_factura(facturador, eps, numero_factura, fecha_generacion_obj.strftime('%Y-%m-%d'), area_servicio, fecha_hora_entrega)

    if factura_id:
        if selected_soat_date:
            soat_date_obj = parse_date(selected_soat_date, "Fecha de Generación SOAT")
            if soat_date_obj:
                db_ops.guardar_detalles_soat(factura_id, soat_date_obj.strftime('%Y-%m-%d'))
        st.success(f"Factura {numero_factura} guardada exitosamente con ID: {factura_id}")
        invalidate_cache_callback()
        reset_form_callback()
        return True
    else:
        st.error(f"Error al guardar la factura {numero_factura}. Posiblemente el número de factura ya existe.")
        return False

def auditar_factura_action(factura_id, estado_auditoria, observacion, tipo_error, invalidate_cache_callback, cancel_edit_callback):
    # Validaciones para el auditor
    if estado_auditoria == "Devuelta por Auditor" and (not observacion or not tipo_error):
        st.error("Para el estado 'Devuelta por Auditor', 'Observación Auditor' y 'Tipo de Error' son obligatorios.")
        return

    if db_ops.actualizar_estado_auditoria_factura(factura_id, estado_auditoria, observacion, tipo_error):
        st.success(f"Factura ID {factura_id} actualizada a estado '{estado_auditoria}' correctamente.")
        invalidate_cache_callback()
        cancel_edit_callback()
    else:
        st.error("Error al actualizar el estado de auditoría de la factura.")

def eliminar_factura_action(factura_id, invalidate_cache_callback):
    if db_ops.eliminar_factura(factura_id):
        st.success(f"Factura ID {factura_id} eliminada correctamente.")
        invalidate_cache_callback()
    else:
        st.error(f"Error al eliminar la factura ID {factura_id}.")

def actualizar_fecha_entrega_action(factura_id, fecha_entrega_radicador_str, invalidate_cache_callback, cancel_edit_callback):
    if not fecha_entrega_radicador_str:
        st.error("La fecha de entrega al radicador es obligatoria.")
        return

    fecha_entrega_obj = parse_date(fecha_entrega_radicador_str, "Fecha Entrega Radicador")
    if fecha_entrega_obj is None:
        return

    # Se permite que la fecha de entrega sea pasada o actual, pero no futura.
    if not validate_future_date(fecha_entrega_obj, "Fecha Entrega Radicador", allow_future=False):
        st.error("La fecha de entrega al radicador no puede ser una fecha futura.")
        return

    if db_ops.actualizar_fecha_entrega_radicador(factura_id, fecha_entrega_obj.strftime('%Y-%m-%d')):
        st.success(f"Fecha de entrega al radicador actualizada para factura ID {factura_id}.")
        invalidate_cache_callback()
        cancel_edit_callback()
    else:
        st.error(f"Error al actualizar la fecha de entrega al radicador para factura ID {factura_id}.")

# Esta es la nueva función clave para la "refacturación"
def realizar_refacturacion_action(factura_id_a_refacturar, nuevo_numero_factura_str, fecha_refacturacion_str, invalidate_cache_callback, cancel_edit_callback):
    """
    Función para "refacturar" una factura existente.
    Actualiza la factura original con un nuevo número y fecha de generación,
    y resetea sus estados a "Pendiente", manteniendo los demás datos.
    """
    if not nuevo_numero_factura_str:
        st.error("El campo 'Nuevo Número de Factura' es obligatorio.")
        return
    if not nuevo_numero_factura_str.isdigit():
        st.error("El 'Nuevo Número de Factura' debe contener solo números.")
        return

    # Asegúrate de usar fecha_refacturacion_str aquí para parsear
    fecha_refacturacion_obj = parse_date(fecha_refacturacion_str, "Fecha de Refacturación")
    if fecha_refacturacion_obj is None:
        return

    # La fecha de refacturación NO debería ser una fecha futura
    # (ya que representa la nueva fecha de generación de la factura)
    if not validate_future_date(fecha_refacturacion_obj, "Fecha de Refacturación", allow_future=False):
        st.error("La fecha de refacturación no puede ser una fecha futura.")
        return

    # PASO CLAVE: Obtener TODOS los datos actuales de la factura que vamos a refacturar
    factura_original_data = db_ops.obtener_factura_por_id(factura_id_a_refacturar)
    if not factura_original_data:
        st.error(f"Error: No se pudo encontrar la factura con ID {factura_id_a_refacturar} para refacturar. Asegúrate que el ID sea correcto.")
        return

    # Extraer los datos que queremos mantener de la factura original
    # Los índices corresponden al SELECT en obtener_factura_por_id
    current_area_servicio = factura_original_data[2]
    current_facturador = factura_original_data[3]
    current_eps = factura_original_data[5]
    # Puedes decidir si quieres mantener la fecha_hora_entrega original o resetearla a la actual.
    # Para la refacturación, a menudo se reinicia con la fecha/hora de la acción.
    # current_fecha_hora_entrega = factura_original_data[6] # Si quisieras mantener la original

    # Nuevos valores para la factura refacturada
    new_fecha_generacion_for_db = fecha_refacturacion_obj.strftime('%Y-%m-%d') # Esta es la nueva fecha de generación
    new_fecha_hora_entrega_for_db = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Fecha y hora actuales de la refacturación

    # Llamar a la función de actualización en la base de datos (db_ops.actualizar_factura)
    success = db_ops.actualizar_factura(
        factura_id=factura_id_a_refacturar,
        new_numero_factura=nuevo_numero_factura_str,
        new_area_servicio=current_area_servicio,
        new_facturador=current_facturador,
        new_fecha_generacion=new_fecha_generacion_for_db, # AQUÍ REINICIAMOS EL PLAZO DE 22 DÍAS
        new_eps=current_eps,
        new_fecha_hora_entrega=new_fecha_hora_entrega_for_db,
        # Reseteamos estos campos al estado inicial de una factura "nueva"
        tiene_correccion=False,
        descripcion_devolucion=None,
        fecha_devolucion_lider=None,
        revisado=False,
        factura_original_id=None, # Ya no es un reemplazo de nada
        estado='Activa', # La factura vuelve a estar activa
        reemplazada_por_numero_factura=None, # No la reemplaza nadie
        estado_auditoria='Pendiente', # Vuelve a estado pendiente de auditoría
        observacion_auditor=None,
        tipo_error=None,
        fecha_reemplazo=fecha_refacturacion_obj.strftime('%Y-%m-%d'), # Fecha en que se refacturó
        fecha_entrega_radicador=None # Resetear este campo si aplica
    )

    if success:
        st.success(f"Factura ID {factura_id_a_refacturar} refacturada con el nuevo número {nuevo_numero_factura_str} correctamente.")
        invalidate_cache_callback()
        cancel_edit_callback()
    else:
        st.error(f"No se pudo refacturar la factura ID {factura_id_a_refacturar}. El número '{nuevo_numero_factura_str}' ya podría existir o hubo un error en la base de datos.")