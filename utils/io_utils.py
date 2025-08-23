# utils/io_utils.py
import streamlit as st
import pandas as pd # Necesitarás pandas aquí porque df es un DataFrame

def export_df_to_csv(df):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Descargar CSV", data=csv, file_name="facturas_trazabilidad.csv", mime="text/csv")

def generar_reporte_carga_masiva(numero_lote, facturador, eps, area_servicio, dataframe_facturas, fecha_hora_carga, ids_facturas):  # <-- PARÁMETRO NUEVO AQUÍ
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
                </tr>
            </thead>
            <tbody>
    """
    
    # Agregar filas por cada factura - CON los IDs de la base de datos
    for i, (_, row) in enumerate(dataframe_facturas.iterrows(), 1):
        html += f"""
                <tr>
                    <td>{i}</td>
                    <td>{ids_facturas[i-1]}</td>  <!-- ID de la base de datos -->
                    <td>{row['Numero de Factura']}</td>
                    <td>{row['Fecha de Generacion']}</td>
                </tr>
        """
    
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    return html