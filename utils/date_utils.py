# utils/date_utils.py
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY

def sumar_dias_habiles(fecha_inicio_obj, num_dias_habiles):
    current_date = fecha_inicio_obj
    dias_sumados = 0
    while dias_sumados < num_dias_habiles:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5: dias_sumados += 1
    return current_date

def calcular_dias_habiles_entre_fechas(fecha_inicio_obj, fecha_fin_obj):
    if fecha_inicio_obj > fecha_fin_obj: return 0
    dias_habiles = 0
    for dt in rrule(DAILY, dtstart=fecha_inicio_obj, until=fecha_fin_obj):
        if dt.weekday() < 5: dias_habiles += 1
    return dias_habiles

def parse_date(date_string, field_name="Fecha"):
    """
    Intenta parsear una cadena de fecha en formatos YYYY-MM-DD o DD/MM/YYYY.
    Muestra un error de Streamlit si el formato es inválido.
    Retorna un objeto date o None si hay un error.
    """
    if not date_string:
        st.error(f"El campo '{field_name}' es obligatorio.")
        return None
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        try:
            return datetime.strptime(date_string, '%d/%m/%Y').date()
        except ValueError:
            st.error(f"El campo '{field_name}' debe tener el formato 'YYYY-MM-DD' o 'DD/MM/YYYY'.")
            return None

def validate_future_date(date_obj, field_name="Fecha"):
    """
    Valida si una fecha es futura. Muestra un error de Streamlit si lo es.
    Retorna True si la fecha es válida (no futura), False si es futura.
    """
    hoy_obj = datetime.now().date()
    if date_obj > hoy_obj:
        st.error(f"La '{field_name}' no puede ser una fecha futura.")
        return False
    return True
