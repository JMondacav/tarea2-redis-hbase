"""B.2 La hora en vivo, de punta a punta: deja Redis limpio, crea los dos grupos, lanza los consumidores
(ingesta y tablero, procesos aparte) y corre el productor a 20x. Termina cuando los dos grupos vaciaron el stream.

    python -m pipeline.vivo
"""
import subprocess
import sys
import threading
import time

from pipeline.comun import (GRUPO_INGESTA, GRUPO_TABLERO, MAXLEN, N_VIVO, PASO_VIVO, RAIZ, STREAM,
                            conectar_redis, escribir, tabla_md)
from pipeline.productor import producir


def lanzar(modulo, *args):
    return subprocess.Popen([sys.executable, "-m", modulo, *args], cwd=RAIZ)


def main():
    r = conectar_redis()
    r.flushdb()   # Redis es solo del pipeline: se parte de cero para que sea reproducible
    for g in (GRUPO_INGESTA, GRUPO_TABLERO):
        r.xgroup_create(STREAM, g, id="0", mkstream=True)   # grupos antes de producir: ven todo

    procs = [lanzar("pipeline.ingesta", "--nombre", "ingesta-1"),
             lanzar("pipeline.tablero", "--nombre", "tablero-1")]

    lag_max = {GRUPO_INGESTA: 0, GRUPO_TABLERO: 0}
    parar = threading.Event()

    def monitor():
        rm = conectar_redis()
        while not parar.is_set():
            try:
                for g in rm.xinfo_groups(STREAM):
                    lag = (g.get("lag") or 0) + g["pending"]   # sin leer + leido sin confirmar
                    lag_max[g["name"]] = max(lag_max.get(g["name"], 0), lag)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.2)

    th = threading.Thread(target=monitor, daemon=True)
    th.start()

    def prog(n):
        if n % 1000 == 0:
            print(f"  productor: {n:,}/{N_VIVO:,}", flush=True)

    print(f"Productor a 20x sobre {STREAM} (MAXLEN ~{MAXLEN:,}) ...")
    n, seg = producir(STREAM, PASO_VIVO, prog)
    print(f"productor: {n:,} eventos en {seg:.1f} s ({n / seg:.1f} ev/s)")

    ini = time.perf_counter()
    for p in procs:
        try:
            p.wait(timeout=300)
        except subprocess.TimeoutExpired:
            p.kill()
            print("un consumidor no termino a tiempo", file=sys.stderr)
    drenaje = time.perf_counter() - ini
    parar.set()
    th.join()

    info = r.xinfo_stream(STREAM)
    grupos = r.xinfo_groups(STREAM)
    texto = f"""# B.2 Hora en vivo

- Productor: **{n:,} eventos en {seg:.1f} s** ({n / seg:.1f} ev/s, objetivo 40 ev/s = 20x)
- Los consumidores terminaron {drenaje:.1f} s despues del ultimo XADD
- Stream `{STREAM}`: XLEN {info['length']:,} - entries-added {info['entries-added']:,} - MAXLEN ~{MAXLEN:,}

{tabla_md(["grupo", "entries-read", "pending", "lag final", "atraso maximo observado (entradas)"],
          [[g['name'], g['entries-read'], g['pending'], g['lag'], lag_max.get(g['name'], 0)] for g in grupos])}

El atraso maximo es lo que el MAXLEN tiene que aguantar: si el atraso llegara a MAXLEN, el recorte
borraria entradas que el grupo aun no lee.
"""
    print(texto)
    escribir("b2_vivo.md", texto)


if __name__ == "__main__":
    main()
