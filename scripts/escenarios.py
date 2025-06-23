import pandas as pd

def simular_escenario_precio(df: pd.DataFrame, incremento_pct: float) -> pd.DataFrame:
    df_copy = df.copy()
    factor = 1 + incremento_pct / 100
    df_copy['Total Escenario'] = df_copy['Quantity'] * (df_copy['Precio x Unidad'] * factor)
    return df_copy

from etl import ejecutar_etl
from escenarios import simular_escenario_precio


df = ejecutar_etl("data/ventas.csv")
df_simulado = simular_escenario_precio(df, 10)
df_simulado.to_csv("data/ventas_simuladas.csv", index=False)
