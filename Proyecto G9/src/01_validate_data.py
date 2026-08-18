from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"

EXPECTED_COLUMNS = {
    "orders.csv": [
        "order_id", "user_id", "eval_set", "order_number",
        "order_dow", "order_hour_of_day", "days_since_prior_order"
    ],
    "order_products__prior.csv": [
        "order_id", "product_id", "add_to_cart_order", "reordered"
    ],
    "order_products__train.csv": [
        "order_id", "product_id", "add_to_cart_order", "reordered"
    ],
    "products.csv": [
        "product_id", "product_name", "aisle_id", "department_id"
    ],
    "aisles.csv": ["aisle_id", "aisle"],
    "departments.csv": ["department_id", "department"],
}

def main():
    spark = (
        SparkSession.builder
        .appName("Instacart-01-Validate")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== VALIDACIÓN INICIAL DEL DATASET ===")
    print(f"Ruta de datos: {RAW_DIR}")

    missing_files = []
    validation_errors = []

    for filename, expected_cols in EXPECTED_COLUMNS.items():
        path = RAW_DIR / filename
        if not path.exists():
            missing_files.append(filename)
            print(f"[ERROR] No existe: {path}")
            continue

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(path))
        )

        current_cols = df.columns
        missing_cols = [c for c in expected_cols if c not in current_cols]
        extra_cols = [c for c in current_cols if c not in expected_cols]

        print(f"\nArchivo: {filename}")
        print(f"Filas: {df.count():,}")
        print(f"Columnas encontradas: {current_cols}")

        if missing_cols:
            validation_errors.append((filename, missing_cols))
            print(f"[ERROR] Columnas faltantes: {missing_cols}")
        else:
            print("[OK] Todas las columnas esperadas están presentes.")

        if extra_cols:
            print(f"[INFO] Columnas adicionales: {extra_cols}")

        null_counts = df.select([
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in current_cols
        ])
        print("Valores nulos:")
        null_counts.show(truncate=False)

    if missing_files:
        spark.stop()
        raise FileNotFoundError(
            "Faltan archivos requeridos: " + ", ".join(missing_files)
        )

    if validation_errors:
        spark.stop()
        details = "; ".join(f"{name}: {cols}" for name, cols in validation_errors)
        raise ValueError(f"Se detectaron columnas faltantes: {details}")

    print("\n[OK] Validación inicial completada correctamente.")
    spark.stop()

if __name__ == "__main__":
    main()
