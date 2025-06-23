import pandas as pd
from sqlalchemy import create_engine, text

df = pd.read_csv("data/Ventas_limpio.csv")
df['Date'] = pd.to_datetime(df['Date'])

df.rename(columns={
    'product_category': 'Product Category',
    'precio_x_unidad': 'Precio x Unidad',
    'total_amount': 'Total Amount'
}, inplace=True)

columnas_existentes = ['Date', 'Gender', 'Age', 'Product Category', 'Quantity', 'Precio x Unidad', 'Total Amount']
df_filtrado = df[columnas_existentes]



engine = create_engine("mysql+pymysql://usersql:root@localhost:3306/customer_retail_purchase_data")

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = 'customer_retail_purchase_data' 
          AND table_name = 'ventas' 
          AND column_name = 'MyUnknownColumn';
    """))
    count = result.scalar()
    if count > 0:
        conn.execute(text("ALTER TABLE ventas DROP COLUMN MyUnknownColumn;"))
        print("Columna MyUnknownColumn eliminada.")
    else:
        print("Columna MyUnknownColumn no existe.")

    conn.execute(text("TRUNCATE TABLE ventas;"))

df_filtrado.to_sql('ventas', con=engine, if_exists='append', index=False)

print("Datos actualizados correctamente en MySQL")
