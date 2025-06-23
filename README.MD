# 🧠 Proyecto ETL y Dashboard de Ventas

Este proyecto simula un entorno real de análisis de datos de ventas, implementando un pipeline completo: desde la ingesta y limpieza de datos, hasta la generación de reportes automatizados y dashboards visuales interactivos.

---

## 📌 Objetivo

Desarrollar un flujo de trabajo completo para automatizar la transformación y visualización de datos de ventas con herramientas modernas del stack de datos. El resultado permite tomar decisiones estratégicas basadas en información clara, limpia y actualizada.

---

## 🔁 Flujo de trabajo

```mermaid
graph TD
    A[CSV original (Ventas.csv)] --> B[ETL con Python (etl.py)]
    B --> C[CSV limpio (Ventas_limpio.csv)]
    C --> D[Carga a MySQL]
    C --> E[Exportación a Excel (reportes)]
    D --> F[Dashboard en Power BI]
    E --> F

🧰 Stack tecnológico
Fase	Herramientas
Ingesta y limpieza	Python, Pandas
Exportación	CSV, Excel (openpyxl)
Base de datos	MySQL, SQLAlchemy, pymysql
Visualización	Power BI, Excel