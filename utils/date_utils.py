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