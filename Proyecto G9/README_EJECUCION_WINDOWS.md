# Pipeline Instacart - versión Windows sin escritura Hadoop

Esta versión mantiene PySpark para el procesamiento masivo, pero evita
`df.write.parquet()` y `df.write.csv()` en el equipo local.

## Flujo

CSV originales
→ Validación
→ Limpieza con PySpark
→ Integración con PySpark
→ Transformación con PySpark
→ Agregaciones con PySpark
→ Resultados pequeños a Pandas
→ CSV normales
→ Matplotlib

## Dataset

Coloque los seis archivos en `data/raw/`:

- `orders.csv`
- `order_products__prior.csv`
- `order_products__train.csv`
- `products.csv`
- `aisles.csv`
- `departments.csv`

## Instalar dependencias

```powershell
python -m pip install -r requirements.txt
```

## Ejecutar paso a paso

```powershell
python src\01_validate_data.py
python src\02_process_data.py
python src\03_visualizations.py
```

Cuando los tres funcionen individualmente:

```powershell
python scripts\run_pipeline.py
```

## Resultados

En `results/`:

- `top_products.csv`
- `top_departments.csv`
- `top_aisles.csv`
- `orders_by_hour.csv`
- `orders_by_day.csv`
- `reorder_analysis.csv`
- `basket_size_analysis.csv`
- `basket_by_day.csv`
- `average_days_between_orders.csv`
- `orders_time_period.csv`

En `visualizations/`:

- `top_products.png`
- `orders_by_hour.png`
- `orders_by_day.png`
- `reorder_rate.png`
- `top_departments.png`

## Nota

Esta versión resuelve la prueba local en Windows evitando la escritura
intermedia mediante Hadoop. La arquitectura cloud final del curso todavía
debe incluir almacenamiento y base de datos según la consigna del proyecto.
