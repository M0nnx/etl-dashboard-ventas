import pandas as pd
import os
from datetime import datetime

def exportar_excel(df: pd.DataFrame, resumen_df: pd.DataFrame, ruta_base: str = "reportes"):
    fecha = datetime.now().strftime('%Y-%m-%d')
    carpeta = os.path.join(ruta_base, fecha)
    os.makedirs(carpeta, exist_ok=True)

    ruta_archivo = os.path.join(carpeta, f"reporte_ventas_{fecha}.xlsx")

    with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos Filtrados y limpiados')
        resumen_df.to_excel(writer, index=True, sheet_name='Resumen por Categoría')

    print(f"Reporte exportado exitosamente a: {ruta_archivo}")

from etl import ejecutar_etl

df = ejecutar_etl("data/Ventas_limpio.csv")
resumen = df.groupby("Product Category")["Total Amount"].sum().reset_index()
exportar_excel(df, resumen)
