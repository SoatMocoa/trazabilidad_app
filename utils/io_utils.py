# utils/io_utils.py
import streamlit as st
import pandas as pd # Necesitarás pandas aquí porque df es un DataFrame
from backend import database_operations as db_ops

def export_df_to_csv(df):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Descargar CSV", data=csv, file_name="facturas_trazabilidad.csv", mime="text/csv")

def generar_reporte_carga_masiva(numero_lote, facturador, eps, area_servicio, dataframe_facturas, fecha_hora_carga, ids_facturas):
    """
    Genera HTML para el reporte de relación de carga masiva
    """
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Relación de Carga Masiva - {numero_lote}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ text-align: center; margin-bottom: 20px; }}
            .info {{ margin-bottom: 15px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>RELACIÓN DE CARGA MASIVA</h2>
            <h3>Hospital Jose Maria Hernandez de Mocoa</h3>
        </div>
        
        <div class="info">
            <p><strong>Número de Lote:</strong> {numero_lote}</p>
            <p><strong>Legalizador:</strong> {facturador}</p>
            <p><strong>EPS:</strong> {eps}</p>
            <p><strong>Área de Servicio:</strong> {area_servicio}</p>
            <p><strong>Fecha y Hora de Carga:</strong> {fecha_hora_carga.strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Total de Facturas Cargadas:</strong> {len(dataframe_facturas)}</p>
        </div>
        
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>ID Factura</th>
                    <th>Número de Factura</th>
                    <th>Fecha de Generación</th>
                    <th>Estado Auditoría</th>  <!-- NUEVA COLUMNA -->
                </tr>
            </thead>
            <tbody>
    """
    
    # Obtener los estados de auditoría de la base de datos
    estados_auditoria = []
    for factura_id in ids_facturas:
        factura_data = db_ops.obtener_factura_por_id(factura_id)
        estados_auditoria.append(factura_data['estado_auditoria'] if factura_data else 'N/A')
    
    # Agregar filas por cada factura - CON los IDs y estado de auditoría
    for i, (_, row) in enumerate(dataframe_facturas.iterrows(), 1):
        html += f"""
                <tr>
                    <td>{i}</td>
                    <td>{ids_facturas[i-1]}</td>
                    <td>{row['Numero de Factura']}</td>
                    <td>{row['Fecha de Generacion']}</td>
                    <td>{estados_auditoria[i-1]}</td>  <!-- NUEVO DATO -->
                </tr>
        """
    
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    return html

def generar_reporte_carga_individual(facturador, eps, area_servicio, factura_data, fecha_hora_carga):
    """
    Genera HTML para el reporte de relación de carga individual
    """
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Relación de Carga Individual</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ text-align: center; margin-bottom: 20px; }}
            .info {{ margin-bottom: 15px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>RELACIÓN DE CARGA INDIVIDUAL</h2>
            <h3>Hospital Jose Maria Hernandez de Mocoa</h3>
        </div>
        
        <div class="info">
            <p><strong>Legalizador:</strong> {facturador}</p>
            <p><strong>EPS:</strong> {eps}</p>
            <p><strong>Área de Servicio:</strong> {area_servicio}</p>
            <p><strong>Fecha y Hora de Carga:</strong> {fecha_hora_carga.strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <table>
            <thead>
                <tr>
                    <th>ID Factura</th>
                    <th>Número de Factura</th>
                    <th>Fecha de Generación</th>
                    <th>Estado Auditoría</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{factura_data['id']}</td>
                    <td>{factura_data['numero_factura']}</td>
                    <td>{factura_data['fecha_generacion'].strftime('%Y-%m-%d')}</td>
                    <td>{factura_data['estado_auditoria']}</td>
                </tr>
            </tbody>
        </table>
    </body>
    </html>
    """
    return html
