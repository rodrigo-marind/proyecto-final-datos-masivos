from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results"
VIS_DIR = ROOT / "visualizations"


def read_result(filename):
    path = RESULTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"No existe {path}. Ejecuta primero src/02_process_data.py"
        )
    return pd.read_csv(path)


def save_plot(filename):
    VIS_DIR.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(VIS_DIR / filename, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"[OK] Gráfica: {VIS_DIR / filename}")


def main():
    print("\n=== GENERACIÓN DE VISUALIZACIONES ===")

    # 1. Top 10 productos
    top_products = read_result("top_products.csv").head(10).copy()
    top_products = top_products.dropna(
        subset=["product_name", "product_order_count"]
    )
    top_products["product_name"] = top_products["product_name"].astype(str)

    plt.figure(figsize=(10, 6))
    plt.barh(
        top_products["product_name"][::-1],
        top_products["product_order_count"][::-1]
    )
    plt.xlabel("Cantidad de compras")
    plt.ylabel("Producto")
    plt.title("Top 10 productos más comprados")
    save_plot("top_products.png")

    # 2. Pedidos por hora
    orders_by_hour = read_result("orders_by_hour.csv").dropna(
        subset=["order_hour_of_day", "order_count"]
    )

    plt.figure(figsize=(10, 5))
    plt.plot(
        orders_by_hour["order_hour_of_day"],
        orders_by_hour["order_count"],
        marker="o"
    )
    plt.xlabel("Hora del día")
    plt.ylabel("Cantidad de pedidos")
    plt.title("Pedidos según hora del día")
    plt.xticks(range(0, 24))
    save_plot("orders_by_hour.png")

    # 3. Pedidos por día
    orders_by_day = read_result("orders_by_day.csv").dropna(
        subset=["order_dow", "order_count"]
    )

    plt.figure(figsize=(8, 5))
    plt.bar(
        orders_by_day["order_dow"].astype(int).astype(str),
        orders_by_day["order_count"]
    )
    plt.xlabel("Día de la semana (0-6)")
    plt.ylabel("Cantidad de pedidos")
    plt.title("Pedidos según día de la semana")
    save_plot("orders_by_day.png")

    # 4. Tasa de recompra
    reorder = read_result("reorder_analysis.csv").head(10).copy()
    reorder = reorder.dropna(
        subset=["product_name", "reorder_rate"]
    )
    reorder["product_name"] = reorder["product_name"].astype(str)

    plt.figure(figsize=(10, 6))
    plt.barh(
        reorder["product_name"][::-1],
        reorder["reorder_rate"][::-1]
    )
    plt.xlabel("Tasa de recompra")
    plt.ylabel("Producto")
    plt.title("Productos con mayor tasa de recompra")
    save_plot("reorder_rate.png")

    # 5. Demanda por departamento
    departments = read_result("top_departments.csv").copy()

    # El CSV puede contener una fila sin nombre de departamento
    # si algún product_id no encontró coincidencia en el catálogo.
    departments = departments.dropna(
        subset=["department", "department_order_count"]
    )
    departments["department"] = departments["department"].astype(str)

    plt.figure(figsize=(10, 7))
    plt.barh(
        departments["department"][::-1],
        departments["department_order_count"][::-1]
    )
    plt.xlabel("Cantidad de productos comprados")
    plt.ylabel("Departamento")
    plt.title("Demanda por departamento")
    save_plot("top_departments.png")

    print("\n[OK] Todas las visualizaciones fueron generadas.")


if __name__ == "__main__":
    main()
