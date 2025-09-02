import streamlit as st
from datetime import datetime, timedelta, date
from dateutil.rrule import rrule, DAILY

DIAS_FESTIVOS_2025 = [
    date(2025, 1, 1),
    date(2025, 1, 6),
    date(2025, 3, 24),
    date(2025, 4, 17),
    date(2025, 4, 18),
    date(2025, 5, 1),
    date(2025, 5, 26),
    date(2025, 6, 16),
    date(2025, 6, 23),
    date(2025, 7, 20),
    date(2025, 8, 7),
    date(2025, 8, 18),
    date(2025, 10, 13),
    date(2025, 11, 3),
    date(2025, 11, 17),
    date(2025, 12, 8),
    date(2025, 12, 25)
]

def es_dia_habil(fecha):
    return fecha.weekday() < 5 and fecha not in DIAS_FESTIVOS_2025

def sumar_dias_habiles(fecha_inicio, dias):
    dias_sumados = 0
    fecha_actual = fecha_inicio
    while dias_sumados < dias:
        fecha_actual += timedelta(days=1)
        if es_dia_habil(fecha_actual):
            dias_sumados += 1
    return fecha_actual

def calcular_dias_habiles_entre_fechas(fecha_inicio, fecha_fin):
    if fecha_inicio > fecha_fin:
        return -calcular_dias_habiles_entre_fechas(fecha_fin, fecha_inicio) # Manejar orden inverso
    
    dias_habiles = 0
    for dt in rrule(DAILY, dtstart=fecha_inicio, until=fecha_fin - timedelta(days=1)):
        if es_dia_habil(dt.date()):
            dias_habiles += 1
    return dias_habiles

def parse_date(date_str, field_name="Fecha"):
    from datetime import datetime
    
    formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    st.error(f"Formato de '{field_name}' inválido: '{date_str}'. Use YYYY-MM-DD o DD/MM/YYYY.")
    return None

def validate_future_date(input_date, field_name):
    if input_date and input_date > date.today():
        st.error(f"La '{field_name}' no puede ser una fecha futura.")
        return False
    return True
