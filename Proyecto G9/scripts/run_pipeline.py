from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]

STEPS = [
    ROOT / "src" / "01_validate_data.py",
    ROOT / "src" / "02_process_data.py",
    ROOT / "src" / "03_visualizations.py",
]

def run_step(script):
    print("\n" + "=" * 72)
    print(f"EJECUTANDO: {script.name}")
    print("=" * 72)

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(ROOT)
    )

    if result.returncode != 0:
        print(f"\n[ERROR] El pipeline se detuvo en {script.name}")
        sys.exit(result.returncode)

def main():
    print("\nPIPELINE INSTACART - VERSIÓN WINDOWS")
    print("Validación → Procesamiento PySpark → Resultados CSV → Visualizaciones")

    for step in STEPS:
        run_step(step)

    print("\n" + "=" * 72)
    print("[OK] PIPELINE COMPLETADO CORRECTAMENTE")
    print(f"Resultados: {ROOT / 'results'}")
    print(f"Gráficas:   {ROOT / 'visualizations'}")
    print("=" * 72)

if __name__ == "__main__":
    main()
