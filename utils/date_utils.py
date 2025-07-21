import streamlit as st # ¡Añade esta línea!
from datetime import datetime, timedelta, date
from dateutil.rrule import rrule, DAILY

# Lista de días festivos en Colombia para 2025 (ejemplo, ajustar según sea necesario)
# Fuente: https://www.dias-festivos.com/dias-festivos-colombia-2025.html
DIAS_FESTIVOS_2025 = [
    date(2025, 1, 1),  # Año Nuevo
    date(2025, 1, 6),  # Día de Reyes
    date(2025, 3, 24), # Día de San José
    date(2025, 4, 17), # Jueves Santo
    date(2025, 4, 18), # Viernes Santo
    date(2025, 5, 1),  # Día del Trabajo
    date(2025, 5, 26), # Día de la Ascensión
    date(2025, 6, 16), # Corpus Christi
    date(2025, 6, 23), # Sagrado Corazón
    date(2025, 7, 20), # Día de la Independencia
    date(2025, 8, 7),  # Batalla de Boyacá
    date(2025, 8, 18), # Asunción de la Virgen
    date(2025, 10, 13),# Día de la Raza
    date(2025, 11, 3), # Día de Todos los Santos
    date(2025, 11, 17),# Independencia de Cartagena
    date(2025, 12, 8), # Día de la Inmaculada Concepción
    date(2025, 12, 25) # Navidad
]

def es_dia_habil(fecha):
    """Verifica si una fecha es un día hábil (no fin de semana ni festivo)."""
    return fecha.weekday() < 5 and fecha not in DIAS_FESTIVOS_2025

def sumar_dias_habiles(fecha_inicio, dias):
    """Suma un número de días hábiles a una fecha dada."""
    dias_sumados = 0
    fecha_actual = fecha_inicio
    while dias_sumados < dias:
        fecha_actual += timedelta(days=1)
        if es_dia_habil(fecha_actual):
            dias_sumados += 1
    return fecha_actual

def calcular_dias_habiles_entre_fechas(fecha_inicio, fecha_fin):
    """Calcula el número de días hábiles entre dos fechas (inclusive la fecha_inicio, exclusiva la fecha_fin)."""
    if fecha_inicio > fecha_fin:
        return -calcular_dias_habiles_entre_fechas(fecha_fin, fecha_inicio) # Manejar orden inverso
    
    dias_habiles = 0
    for dt in rrule(DAILY, dtstart=fecha_inicio, until=fecha_fin - timedelta(days=1)):
        if es_dia_habil(dt.date()):
            dias_habiles += 1
    return dias_habiles

def parse_date(date_str, field_name="Fecha"):
    """
    Intenta parsear una cadena de fecha en varios formatos.
    Muestra un mensaje de error en Streamlit si el formato es inválido.
    Retorna un objeto date o None si falla.
    """
    formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    st.error(f"Formato de '{field_name}' inválido: '{date_str}'. Use YYYY-MM-DD o DD/MM/YYYY.")
    return None

def validate_future_date(input_date, field_name):
    """
    Valida que la fecha no sea una fecha futura.
    Muestra un mensaje de error en Streamlit si la fecha es futura.
    """
    if input_date and input_date > date.today():
        st.error(f"La '{field_name}' no puede ser una fecha futura.")
        return False
    return True
