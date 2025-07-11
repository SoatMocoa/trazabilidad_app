# services/invoice_service.py

import streamlit as st
from datetime import datetime

import backend.database_operations as db_ops
from utils.date_utils import parse_date, validate_future_date

def auditar_factura_action(factura_id, nuevo_estado_auditoria, observacion, tipo_error, invalidate_cache_callback, cancel_edit_callback):
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
        invalidate_cache_callback() # Usar callback
        cancel_edit_callback()      # Usar callback
    else: st.error("No se pudo actualizar el estado de auditoria de la factura.")

def eliminar_factura_action(factura_id): # Esta función devuelve un booleano, no interactúa con UI directamente
    if st.session_state.user_role != 'auditor':
        st.error("Permiso Denegado. Solo los auditores pueden eliminar facturas.")
        return False
    success = db_ops.eliminar_factura(factura_id)
    return success

def guardar_factura_reemplazo_action(old_factura_id, new_numero_factura, fecha_reemplazo_factura_str, invalidate_cache_callback, cancel_edit_callback):
    # NOTA: He eliminado facturador, eps, area_servicio de los parámetros directos,
    # ya que estos se obtendrán de la factura original de la DB.
    # Si tu UI permite CAMBIAR estos campos al refacturar, tendrías que volver a pasarlos
    # desde la UI y priorizarlos. Por ahora, asumimos que se mantienen los originales.

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
        # Esto podría ser una validación a reconsiderar si la fecha de reemplazo puede ser "hoy" o pasada
        # para una refacturación. Para propósitos de este error, la mantendremos.
        return

    # >>> PUNTO CLAVE: OBTENER LOS DATOS COMPLETOS DE LA FACTURA ORIGINAL <<<
    factura_original_data = db_ops.obtener_factura_por_id(old_factura_id)
    if not factura_original_data:
        st.error("No se pudo obtener la información de la factura original para el reemplazo.")
        return

    # Asumiendo que factura_original_data es una tupla con el orden de `SELECT` en `obtener_factura_por_id`
    # El orden es crucial aquí: f.id (0), f.numero_factura (1), f.area_servicio (2), f.facturador (3), f.fecha_generacion (4), f.eps (5), ...
    original_area_servicio = factura_original_data[2]
    original_facturador = factura_original_data[3]
    original_eps = factura_original_data[5]
    # new_fecha_generacion_automatica se mantiene como la fecha actual o la fecha de reemplazo si quieres que sea la nueva "fecha_generacion"
    new_fecha_generacion_automatica = fecha_reemplazo_factura_obj.strftime('%Y-%m-%d') # Usamos la fecha de reemplazo como la nueva fecha de generación

    # Llama a la función de la base de datos para refacturar (actualizar) la factura existente
    success = db_ops.refacturar_factura(
        old_factura_id,
        new_numero_factura,
        new_fecha_generacion_automatica, # Esta es la nueva fecha_generacion
        original_area_servicio,
        original_facturador,
        original_eps,
        fecha_reemplazo_factura_obj.strftime('%Y-%m-%d') # Esta es la fecha_reemplazo
    )
    if success:
        st.success(f"Factura ID: {old_factura_id} refacturada con el número {new_numero_factura} correctamente.")
        invalidate_cache_callback()
        cancel_edit_callback()
    else:
        st.error(f"No se pudo refacturar la factura. El numero '{new_numero_factura}' ya podría existir o hubo un error en la base de datos.")

def marcar_como_corregida_action(factura_id, observacion_actual, tipo_error_actual, invalidate_cache_callback, cancel_edit_callback):
    if st.session_state.user_role != 'legalizador':
        st.error("Permiso Denegado. Solo los legalizadores pueden marcar facturas como corregidas.")
        return
    success = db_ops.actualizar_estado_auditoria_factura(factura_id, "Corregida por Legalizador", observacion_actual, tipo_error_actual)
    if success:
        st.success(f"Factura ID: {factura_id} marcada como 'Corregida por Legalizador'.")
        invalidate_cache_callback()
        cancel_edit_callback()
    else: st.error("No se pudo marcar la factura como corregida.")

def actualizar_fecha_entrega_radicador_action(factura_id, set_date, invalidate_cache_callback, cancel_edit_callback):
    fecha_entrega = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if set_date else None
    success = db_ops.actualizar_fecha_entrega_radicador(factura_id, fecha_entrega)
    if success:
        st.success("Fecha de entrega al radicador actualizada correctamente.")
        invalidate_cache_callback()
        cancel_edit_callback()
    else: st.error("No se pudo actualizar la fecha de entrega al radicador.")