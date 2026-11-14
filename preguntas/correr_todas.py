"""Corre P1..P6 en orden y arma resultados/tiempos.md (la tabla de tiempos del informe).

    python -m preguntas.correr_todas
"""
import json
import os
import platform

from pipeline.comun import RESULTADOS, escribir, tabla_md
from preguntas import p1_ahora, p2_ventana, p3_rango, p4_dos_patrones, p5_hotspotting, p6_propia


def main():
    (RESULTADOS / "tiempos.json").unlink(missing_ok=True)
    for i, mod in enumerate((p1_ahora, p2_ventana, p3_rango, p4_dos_patrones, p5_hotspotting, p6_propia), 1):
        print(f"\n######## P{i} ########")
        mod.main()
    tiempos = json.loads((RESULTADOS / "tiempos.json").read_text(encoding="utf-8"))
    texto = f"""# Tiempos (mediana de 5 corridas, la primera aparte)

Maquina: {platform.system()} {platform.release()} - {platform.machine()} - {os.cpu_count()} hilos - Python {platform.python_version()}
(Redis 7.4 y HBase 2.5 standalone en Docker en la misma maquina)

{tabla_md(["consulta", "mediana (ms)", "primera corrida (ms)"],
          [[k, f"{v['mediana_ms']:.2f}", f"{v['primera_ms']:.2f}"] for k, v in tiempos.items()])}
"""
    print(texto)
    escribir("tiempos.md", texto)


if __name__ == "__main__":
    main()
