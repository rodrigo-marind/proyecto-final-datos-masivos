import os

# IMPORTANT:
# Spark local mode runs the driver/executor in the same JVM.
# Give that JVM more heap BEFORE Spark starts.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--driver-memory 4g pyspark-shell"
)

from pathlib import Path
import csv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
RESULTS_DIR = ROOT / "results"

# Schemas explícitos: evita que Spark tenga que inferir tipos recorriendo
# archivos CSV muy grandes antes del procesamiento.
ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", IntegerType(), True),
    StructField("order_hour_of_day", IntegerType(), True),
    StructField("days_since_prior_order", DoubleType(), True),
])

ORDER_PRODUCTS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
])

AISLES_SCHEMA = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True),
])

DEPARTMENTS_SCHEMA = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
])


def read_csv(spark, filename, schema):
    path = RAW_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"No se encontró: {path}")

    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(str(path))
    )


def save_rows(df, filename):
    """
    El DataFrame recibido DEBE ser un resultado agregado pequeño.
    Se usa collect() + csv estándar, evitando toPandas/PyArrow.
    """
    output = RESULTS_DIR / filename
    rows = df.collect()
    fieldnames = df.columns

    with output.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        for row in rows:
            writer.writerow([row[c] for c in fieldnames])

    print(f"[OK] Resultado guardado: {output}")


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Instacart-02-Process-LowMemory")
        # No usar local[*]: en esta PC Spark lanzó ~11 tareas simultáneas y
        # todas competían por el mismo heap. Dos hilos reducen presión de RAM.
        .master("local[2]")
        # 16 particiones era muy poco para >33 millones de filas.
        # Más particiones = bloques de shuffle más pequeños.
        .config("spark.sql.shuffle.partitions", "128")
        .config("spark.default.parallelism", "128")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Evitar Arrow: no hace falta para resultados pequeños y no requiere pyarrow.
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== PROCESAMIENTO INSTACART - MODO MEMORIA REDUCIDA ===")
    print("PySpark procesa los datos grandes; solo resultados pequeños vuelven a Python.\n")

    # ------------------------------------------------------------------
    # 1. CARGA
    # ------------------------------------------------------------------
    print("[1/5] Cargando CSV con esquemas explícitos...")

    orders = read_csv(spark, "orders.csv", ORDERS_SCHEMA)
    prior = read_csv(spark, "order_products__prior.csv", ORDER_PRODUCTS_SCHEMA)
    train = read_csv(spark, "order_products__train.csv", ORDER_PRODUCTS_SCHEMA)
    products = read_csv(spark, "products.csv", PRODUCTS_SCHEMA)
    aisles = read_csv(spark, "aisles.csv", AISLES_SCHEMA)
    departments = read_csv(spark, "departments.csv", DEPARTMENTS_SCHEMA)

    # ------------------------------------------------------------------
    # 2. LIMPIEZA
    # ------------------------------------------------------------------
    print("[2/5] Aplicando limpieza y filtros...")

    # Tablas pequeñas / medianas: deduplicación segura.
    orders = (
        orders
        .dropDuplicates(["order_id"])
        .filter(
            col("order_id").isNotNull()
            & col("user_id").isNotNull()
            & col("order_dow").between(0, 6)
            & col("order_hour_of_day").between(0, 23)
        )
    )

    products = (
        products
        .dropDuplicates(["product_id"])
        .filter(
            col("product_id").isNotNull()
            & col("aisle_id").isNotNull()
            & col("department_id").isNotNull()
        )
    )

    aisles = aisles.dropDuplicates(["aisle_id"])
    departments = departments.dropDuplicates(["department_id"])

    # IMPORTANTE:
    # No hacemos dropDuplicates() sobre las tablas de 32M+ y 1.3M filas
    # en esta versión local, porque provoca un shuffle completo muy costoso
    # en una sola PC. Sí filtramos identificadores/rangos inválidos.
    prior = (
        prior
        .filter(
            col("order_id").isNotNull()
            & col("product_id").isNotNull()
            & (col("add_to_cart_order") > 0)
            & col("reordered").isin(0, 1)
        )
        .select("order_id", "product_id", "reordered")
    )

    train = (
        train
        .filter(
            col("order_id").isNotNull()
            & col("product_id").isNotNull()
            & (col("add_to_cart_order") > 0)
            & col("reordered").isin(0, 1)
        )
        .select("order_id", "product_id", "reordered")
    )

    order_products = prior.unionByName(train)

    # ------------------------------------------------------------------
    # 3. MÉTRICAS DE PRODUCTO
    # ------------------------------------------------------------------
    print("[3/5] Calculando métricas de productos/categorías...")

    # Primero agregamos 33M+ filas a ~50K productos.
    # SOLO DESPUÉS hacemos joins con dimensiones pequeñas.
    product_metrics_base = (
        order_products
        .groupBy("product_id")
        .agg(
            count("*").alias("product_order_count"),
            avg(col("reordered").cast("double")).alias("reorder_rate")
        )
    )

    product_metrics = (
        product_metrics_base
        .join(
            broadcast(
                products.select(
                    "product_id", "product_name", "aisle_id", "department_id"
                )
            ),
            on="product_id",
            how="left"
        )
    )

    top_products = (
        product_metrics
        .select("product_id", "product_name", "product_order_count")
        .orderBy(col("product_order_count").desc())
        .limit(20)
    )

    reorder_analysis = (
        product_metrics
        .filter(col("product_order_count") >= 100)
        .select(
            "product_id", "product_name",
            "product_order_count", "reorder_rate"
        )
        .orderBy(col("reorder_rate").desc())
        .limit(20)
    )

    # Demanda por departamento usando conteos ya agregados por producto.
    top_departments = (
        product_metrics
        .groupBy("department_id")
        .agg(
            spark_sum("product_order_count")
            .alias("department_order_count")
        )
        .join(
            broadcast(departments),
            on="department_id",
            how="left"
        )
        .select(
            "department_id", "department", "department_order_count"
        )
        .orderBy(col("department_order_count").desc())
    )

    # Demanda por pasillo usando conteos ya agregados por producto.
    top_aisles = (
        product_metrics
        .groupBy("aisle_id")
        .agg(
            spark_sum("product_order_count")
            .alias("aisle_order_count")
        )
        .join(
            broadcast(aisles),
            on="aisle_id",
            how="left"
        )
        .select("aisle_id", "aisle", "aisle_order_count")
        .orderBy(col("aisle_order_count").desc())
        .limit(20)
    )

    # Ejecutar/guardar estos resultados antes de iniciar otra agregación grande.
    save_rows(top_products, "top_products.csv")
    save_rows(reorder_analysis, "reorder_analysis.csv")
    save_rows(top_departments, "top_departments.csv")
    save_rows(top_aisles, "top_aisles.csv")

    # ------------------------------------------------------------------
    # 4. MÉTRICAS DE PEDIDOS
    # ------------------------------------------------------------------
    print("[4/5] Calculando métricas temporales y de pedidos...")

    orders_by_hour = (
        orders
        .groupBy("order_hour_of_day")
        .agg(count("*").alias("order_count"))
        .orderBy("order_hour_of_day")
    )

    orders_by_day = (
        orders
        .groupBy("order_dow")
        .agg(count("*").alias("order_count"))
        .orderBy("order_dow")
    )

    average_days_between_orders = (
        orders
        .agg(
            avg("days_since_prior_order")
            .alias("average_days_between_orders")
        )
    )

    save_rows(orders_by_hour, "orders_by_hour.csv")
    save_rows(orders_by_day, "orders_by_day.csv")
    save_rows(
        average_days_between_orders,
        "average_days_between_orders.csv"
    )

    # ------------------------------------------------------------------
    # 5. TAMAÑO DE CESTA
    # ------------------------------------------------------------------
    print("[5/5] Calculando tamaño promedio de los pedidos...")

    # Esta es la segunda agregación grande, pero sin el join gigante del código anterior.
    basket_size = (
        order_products
        .groupBy("order_id")
        .agg(count("*").alias("total_products_order"))
    )

    basket_size_analysis = (
        basket_size
        .agg(
            avg("total_products_order")
            .alias("average_products_per_order")
        )
    )

    basket_by_day = (
        basket_size
        .join(
            orders.select("order_id", "order_dow"),
            on="order_id",
            how="inner"
        )
        .groupBy("order_dow")
        .agg(
            avg("total_products_order")
            .alias("average_products_per_order")
        )
        .orderBy("order_dow")
    )

    save_rows(basket_size_analysis, "basket_size_analysis.csv")
    save_rows(basket_by_day, "basket_by_day.csv")

    print("\n[OK] Procesamiento completado correctamente.")
    print(f"Resultados: {RESULTS_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
