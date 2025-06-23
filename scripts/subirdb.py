import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

df = pd.read_csv("data/Ventas_limpio.csv")
df['Date'] = pd.to_datetime(df['Date'])

df.rename(columns={
    'product_category': 'Product Category',
    'precio_x_unidad': 'Precio x Unidad',
    'total_amount': 'Total Amount'
}, inplace=True)

columnas_existentes = ['Date', 'Gender', 'Age', 'Product Category', 'Quantity', 'Precio x Unidad', 'Total Amount']
df_filtrado = df[columnas_existentes]

connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = %s
          AND table_name = 'ventas' 
          AND column_name = 'MyUnknownColumn';
    """), (db_name,))
    count = result.scalar()
    if count > 0:
        conn.execute(text("ALTER TABLE ventas DROP COLUMN MyUnknownColumn;"))
        print("Columna MyUnknownColumn eliminada.")
    else:
        print("Columna MyUnknownColumn no existe.")

    conn.execute(text("TRUNCATE TABLE ventas;"))

df_filtrado.to_sql('ventas', con=engine, if_exists='append', index=False)

print("Datos actualizados correctamente en MySQL")