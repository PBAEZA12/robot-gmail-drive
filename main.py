
import subprocess
import sys

def run_script(script_name):
    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    print(f"\n--- Salida de {script_name} ---")
    print(result.stdout)
    if result.stderr:
        print(f"\n--- Errores de {script_name} ---")
        print(result.stderr)

if __name__ == "__main__":
    run_script("search_custodia_instrumentos.py")
    run_script("search_movimientos_y_saldos.py")
