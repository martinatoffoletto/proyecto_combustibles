import subprocess
import sys

def run_command(cmd):
    print(f"Ejecutando: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error al ejecutar '{cmd}':\n{result.stderr}")
        sys.exit(result.returncode)

if __name__ == "__main__":
    run_command("python prepare_data.py")
    run_command("dbt seed")   
    run_command("dbt run")
    print("Pipeline completado correctamente.")
