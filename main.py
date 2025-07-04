import subprocess
import sys
import duckdb

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
    
    
    ###PARA VERIFICAR LA TABLA FINAL CARGADA EN DUCKDB###
    print("\nVerificando tabla final 'final_combustible_ipc'...\n")
    con = duckdb.connect("duckdb.db")  

    try:
        df = con.execute("SELECT * FROM final_combustible_ipc LIMIT 5").fetchdf()
        print(df)
        print("\n✔ Verificación exitosa: tabla cargada correctamente.")
    except Exception as e:
        print(f"\n Error al consultar la tabla final: {e}")
    

