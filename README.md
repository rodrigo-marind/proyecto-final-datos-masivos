# proyecto-final-datos-masivos
Proyecto de análisis de datos masivos para identificar patrones de compra, demanda de productos y recompra utilizando el dataset Instacart Market Basket Analysis.
# Análisis de patrones de compra en un supermercado en línea

Proyecto desarrollado para el curso **Datos Masivos**, orientado al procesamiento y análisis de grandes volúmenes de datos mediante servicios en la nube.

## Integrantes

* Rodrigo Marín Díaz
* José Julián Castro Montero
* Verónica Dávila Molina

---

## 1. Descripción del proyecto

Este proyecto analiza los patrones de compra y recompra de los usuarios de un supermercado en línea utilizando el dataset público **Instacart Market Basket Analysis**.

El conjunto de datos contiene millones de registros relacionados con pedidos, productos, pasillos, departamentos, horarios de compra y comportamiento de recompra.

Debido al volumen de información, se implementó una arquitectura de procesamiento de datos en la nube que permite almacenar los archivos originales, procesarlos mediante PySpark, generar resultados analíticos, almacenarlos en una base de datos y visualizarlos mediante un dashboard.

El flujo implementado comprende las siguientes etapas:

**Ingesta → Almacenamiento → Procesamiento → Base de datos → Visualización**

---

## 2. Objetivo general

Diseñar e implementar una arquitectura de procesamiento de datos en la nube para analizar los patrones de compra y recompra de los usuarios de un supermercado en línea.

Entre los principales objetivos se encuentran:

* Almacenar los archivos originales del dataset en la nube.
* Validar y limpiar los datos.
* Integrar los diferentes archivos mediante identificadores comunes.
* Procesar millones de registros utilizando PySpark.
* Identificar productos, departamentos y pasillos con mayor demanda.
* Analizar los días y horarios con mayor cantidad de pedidos.
* Determinar los productos con mayor tasa de recompra.
* Calcular métricas relacionadas con el tamaño de los pedidos.
* Almacenar los resultados analíticos en BigQuery.
* Generar visualizaciones mediante Looker Studio.

---

## 3. Dataset

### Instacart Market Basket Analysis

**Fuente:** Kaggle
**Fuente original:** Instacart
**Formato:** CSV
**Tamaño aproximado:** 207 MB
**Cantidad de archivos:** 6

Enlace:

https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis

El dataset contiene los siguientes archivos:

| Archivo                     | Descripción                                                     |
| --------------------------- | --------------------------------------------------------------- |
| `orders.csv`                | Información general de los pedidos realizados por los usuarios. |
| `order_products__prior.csv` | Productos incluidos en pedidos históricos.                      |
| `order_products__train.csv` | Productos incluidos en el conjunto de entrenamiento.            |
| `products.csv`              | Catálogo de productos.                                          |
| `aisles.csv`                | Información de los pasillos o subcategorías.                    |
| `departments.csv`           | Información de los departamentos o categorías generales.        |

El archivo `order_products__prior.csv` contiene más de **32 millones de registros**, lo que permite trabajar con un volumen de datos adecuado para aplicar técnicas de procesamiento de datos masivos.

Los archivos originales no se almacenan directamente en este repositorio debido a su tamaño. Para ejecutar el proyecto deben descargarse desde Kaggle y colocarse en:

```text
data/raw/
```

---

## 4. Arquitectura implementada

La arquitectura final utilizada en el proyecto es:

```text
Kaggle
   ↓
Google Cloud Storage
   ↓
Google Colab + PySpark
   ↓
BigQuery
   ↓
Looker Studio
```

### 4.1 Kaggle

Kaggle funciona como fuente del dataset original **Instacart Market Basket Analysis**.

### 4.2 Google Cloud Storage

Los seis archivos CSV originales se almacenan en un bucket de Google Cloud Storage.

El Data Lake utiliza la siguiente organización:

```text
raw/
processed/
results/
```

* `raw/`: contiene los datos originales sin modificar.
* `processed/`: zona destinada a datos intermedios procesados.
* `results/`: contiene los resultados analíticos generados después del procesamiento.

### 4.3 Google Colab + PySpark

Google Colab se utiliza como entorno de procesamiento.

PySpark permite realizar operaciones sobre millones de registros, incluyendo:

* carga de datos;
* validación;
* limpieza;
* filtrado;
* integración;
* transformación;
* agregación;
* generación de métricas analíticas.

### 4.4 BigQuery

Los resultados generados por PySpark se almacenan como tablas analíticas dentro del dataset:

```text
instacart_analytics
```

Entre las tablas almacenadas se encuentran:

```text
top_products
reorder_analysis
top_departments
top_aisles
orders_by_hour
orders_by_day
average_days_between_orders
basket_size_analysis
basket_by_day
```

### 4.5 Looker Studio

BigQuery se conecta con Looker Studio para construir un dashboard con los principales resultados del análisis.

Las visualizaciones desarrolladas incluyen:

* productos más comprados;
* pedidos según hora del día;
* pedidos según día de la semana;
* productos con mayor tasa de recompra;
* demanda por departamento.

---

## 5. Pipeline de procesamiento

El pipeline implementado sigue el siguiente flujo:

```text
Dataset Instacart
        ↓
Carga desde Cloud Storage
        ↓
Validación
        ↓
Limpieza y filtrado
        ↓
Integración
        ↓
Transformación
        ↓
Agregación
        ↓
Resultados analíticos
        ↓
BigQuery
        ↓
Looker Studio
```

### Validación

Se verifica:

* existencia de los seis archivos;
* columnas requeridas;
* identificadores principales;
* valores nulos;
* estructura general del dataset.

Los principales identificadores utilizados son:

```text
order_id
product_id
aisle_id
department_id
```

### Limpieza

Se aplican operaciones como:

* validación de identificadores;
* eliminación de registros inválidos;
* validación de `order_dow`;
* validación de `order_hour_of_day`;
* validación de `add_to_cart_order`;
* validación de la variable `reordered`.

### Integración

Los diferentes archivos se relacionan mediante sus identificadores:

```text
orders
   ↓ order_id
order_products
   ↓ product_id
products
   ↓
aisles + departments
```

### Agregación

Después de procesar los registros se generan tablas analíticas de menor tamaño que permiten responder las preguntas del proyecto.

---

## 6. Preguntas analíticas

El proyecto busca responder preguntas como:

1. ¿Cuáles son los productos que aparecen en la mayor cantidad de pedidos?
2. ¿Qué departamentos concentran la mayor demanda?
3. ¿Qué pasillos presentan mayor actividad?
4. ¿Cuáles son las horas del día en las que se realizan más pedidos?
5. ¿Cómo varía la cantidad de pedidos según el día de la semana?
6. ¿Cuáles productos presentan la mayor tasa de recompra?
7. ¿Cuántos productos contiene, en promedio, cada pedido?
8. ¿Cuánto tiempo transcurre, en promedio, entre un pedido y el siguiente?
9. ¿Cómo cambia el tamaño promedio de los pedidos según el día de la semana?

---

## 7. Resultados principales

El procesamiento permitió transformar millones de registros originales en tablas analíticas listas para consulta y visualización.

Entre los principales resultados se identificó que:

* Los productos relacionados con frutas y productos frescos aparecen entre los artículos con mayor frecuencia de compra.
* Productos como **Banana** y **Bag of Organic Bananas** se encuentran entre los productos más comprados.
* La actividad de pedidos aumenta durante la mañana y se concentra principalmente entre media mañana y la tarde.
* Existen diferencias en la cantidad de pedidos según el día de la semana.
* La tasa de recompra permite identificar productos que los usuarios adquieren nuevamente con alta frecuencia.
* El departamento **produce** presenta la mayor demanda dentro del dataset.
* Otros departamentos con alta actividad incluyen **dairy eggs** y **snacks**.

Los resultados completos se encuentran en:

```text
results/
```

y las visualizaciones locales generadas se encuentran en:

```text
visualizations/
```

---

## 8. Estructura del repositorio

```text
instacart-big-data-analysis/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   └── README.md
│
├── src/
│   ├── 01_validate_data.py
│   ├── 02_process_data.py
│   └── 03_visualizations.py
│
├── scripts/
│   └── run_pipeline.py
│
├── results/
│   ├── top_products.csv
│   ├── reorder_analysis.csv
│   ├── top_departments.csv
│   ├── top_aisles.csv
│   ├── orders_by_hour.csv
│   ├── orders_by_day.csv
│   ├── average_days_between_orders.csv
│   ├── basket_size_analysis.csv
│   └── basket_by_day.csv
│
├── visualizations/
│   ├── top_products.png
│   ├── orders_by_hour.png
│   ├── orders_by_day.png
│   ├── reorder_rate.png
│   └── top_departments.png
│
└── docs/
    ├── architecture.png
    └── screenshots/
```

---

## 9. Tecnologías utilizadas

### Lenguajes y procesamiento

* Python
* PySpark
* Pandas
* Matplotlib

### Cloud

* Google Cloud Platform
* Google Cloud Storage
* BigQuery

### Entorno de procesamiento

* Google Colab
* Visual Studio Code

### Visualización

* Looker Studio
* Matplotlib

### Control de versiones

* Git
* GitHub

### Fuente de datos

* Kaggle

---

## 10. Instalación local

### Requisitos

Se recomienda disponer de:

* Python 3
* Java 17
* PySpark
* Pandas
* Matplotlib

Clonar el repositorio:

```bash
git clone URL_DEL_REPOSITORIO
cd instacart-big-data-analysis
```

Instalar las dependencias:

```bash
python -m pip install -r requirements.txt
```

---

## 11. Preparación del dataset

Descargar el dataset desde:

https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis

Colocar los seis CSV dentro de:

```text
data/raw/
```

La estructura debe ser:

```text
data/
└── raw/
    ├── orders.csv
    ├── order_products__prior.csv
    ├── order_products__train.csv
    ├── products.csv
    ├── aisles.csv
    └── departments.csv
```

---

## 12. Ejecución local

### Paso 1 — Validación

```bash
python src/01_validate_data.py
```

Este script verifica la estructura de los archivos, columnas requeridas y valores nulos.

### Paso 2 — Procesamiento

```bash
python src/02_process_data.py
```

Este script realiza las principales operaciones de:

```text
Limpieza
→ Filtrado
→ Integración
→ Transformación
→ Agregación
→ Generación de resultados
```

Los resultados se almacenan en:

```text
results/
```

### Paso 3 — Visualizaciones

```bash
python src/03_visualizations.py
```

Las gráficas generadas se almacenan en:

```text
visualizations/
```

### Pipeline completo

También puede ejecutarse el pipeline completo mediante:

```bash
python scripts/run_pipeline.py
```

---

## 13. Ejecución en la nube

La implementación cloud sigue los siguientes pasos:

1. Descargar el dataset desde Kaggle.
2. Cargar los seis CSV originales en Google Cloud Storage.
3. Organizar el Data Lake mediante las carpetas `raw/`, `processed/` y `results/`.
4. Conectar Google Colab con Cloud Storage.
5. Descargar temporalmente los datos al entorno de procesamiento.
6. Procesar los archivos mediante PySpark.
7. Generar las tablas analíticas.
8. Guardar una copia de los resultados en Cloud Storage.
9. Cargar los resultados en BigQuery.
10. Conectar BigQuery con Looker Studio.
11. Construir el dashboard final.

---

## 14. Tablas analíticas generadas

| Tabla                         | Descripción                                    |
| ----------------------------- | ---------------------------------------------- |
| `top_products`                | Productos con mayor cantidad de compras.       |
| `reorder_analysis`            | Productos con mayor tasa de recompra.          |
| `top_departments`             | Demanda agrupada por departamento.             |
| `top_aisles`                  | Demanda agrupada por pasillo.                  |
| `orders_by_hour`              | Cantidad de pedidos según la hora del día.     |
| `orders_by_day`               | Cantidad de pedidos según el día de la semana. |
| `average_days_between_orders` | Promedio de días entre pedidos.                |
| `basket_size_analysis`        | Promedio de productos por pedido.              |
| `basket_by_day`               | Tamaño promedio del pedido según el día.       |

---

## 15. Visualizaciones

El dashboard desarrollado en Looker Studio incluye cinco visualizaciones principales:

### Productos más comprados

Permite identificar los productos que aparecen con mayor frecuencia dentro de los pedidos.

### Pedidos según hora del día

Permite observar las horas con mayor actividad de compra.

### Pedidos según día de la semana

Permite comparar la cantidad de pedidos entre los distintos días.

### Productos con mayor tasa de recompra

Permite identificar los productos que los usuarios tienden a comprar nuevamente.

### Demanda por departamento

Permite comparar la actividad entre las principales categorías del supermercado.

---

## 16. Limitaciones

El dataset no contiene información relacionada con:

* precios;
* ingresos;
* monto total de los pedidos;
* costos;
* ganancias.

Por esta razón, el proyecto se concentra en:

* frecuencia de compra;
* cantidad de pedidos;
* productos;
* categorías;
* comportamiento temporal;
* recompra.

No se realizan análisis de rentabilidad ni ingresos monetarios.

---

## 17. Conclusión

El proyecto demuestra la implementación de una arquitectura completa para el procesamiento de datos masivos utilizando servicios en la nube.

Google Cloud Storage permite conservar los datos originales, mientras que Google Colab y PySpark permiten realizar el procesamiento de millones de registros. Los resultados analíticos son almacenados posteriormente en BigQuery y consumidos desde Looker Studio para generar visualizaciones que facilitan la interpretación de los patrones de compra.

La solución implementada permite transformar grandes volúmenes de registros en información útil sobre productos, departamentos, horarios y comportamiento de recompra, demostrando el funcionamiento completo del flujo:

```text
Kaggle
→ Cloud Storage
→ Google Colab + PySpark
→ BigQuery
→ Looker Studio
```
