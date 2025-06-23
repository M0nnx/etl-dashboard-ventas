import pandas as pd 
from datetime import datetime
ruta_csv = "data/Ventas.csv"


def cargar_datos(ruta_csv:str)-> pd.DataFrame:
    df = pd.read_csv(ruta_csv)
    df = df.drop(columns=["Unnamed: 0"], errors='ignore')
    return df

def limpiar_transformar(df: pd.DataFrame) -> pd.DataFrame:
    df['Date'] = pd.to_datetime(df['Date'])
    df['Month'] = df['Date'].dt.month
    df['Year'] = df['Date'].dt.year
    df['Ticket Promedio'] = df['Total Amount'] / df['Quantity']
    df = df[df['Quantity'] > 0]
    df = df[df['Precio x Unidad'] > 0]
    return df

def ejecutar_etl(ruta_csv: str) -> pd.DataFrame:
    df = cargar_datos(ruta_csv)
    df = limpiar_transformar(df)
    return df

df_final = ejecutar_etl(ruta_csv)
df_final.to_csv("data/Ventas_limpio.csv", index=False)
print(df_final.head())