# utils/io_utils.py
import streamlit as st
import pandas as pd # Necesitarás pandas aquí porque df es un DataFrame

def export_df_to_csv(df):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Descargar CSV", data=csv, file_name="facturas_trazabilidad.csv", mime="text/csv")