"""B.2 productor: lee la hora en vivo y la escribe en el stream con XADD a 20x (un evento cada 25 ms).

    python -m pipeline.productor              # 20x sobre sensores:t2
    python -m pipeline.productor --rapido     # sin pausa (solo para experimentos)
"""
import argparse
import time

import redis

from pipeline.comun import MAXLEN, PASO_VIVO, STREAM, conectar_redis, vivo


def xadd_con_reintento(r, stream, campos, maxlen):
    """Si Redis se reinicia (D.2) el productor no pierde el evento: lo reintenta hasta que entra.
    (Si el XADD alcanzo a entrar y se corto la respuesta, puede quedar repetido: abajo da igual, la clave es idempotente.)"""
    while True:
        try:
            p = r.pipeline(transaction=False)
            p.xadd(stream, campos, maxlen=maxlen, approximate=True)
            p.incr(f"{stream}:producidos")
            return p.execute()[0]
        except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError):
            time.sleep(0.2)


def producir(stream=STREAM, paso=PASO_VIVO, progreso=None, eventos=None, seguir=None):
    """seguir: threading.Event opcional; si se limpia, el productor se queda esperando (lo usa D.2 para la foto)."""
    r = conectar_redis()
    n = 0
    ini = time.perf_counter()
    for n, ev in enumerate(eventos if eventos is not None else vivo(), start=1):
        if seguir is not None:
            seguir.wait()
        if paso:
            espera = ini + (n - 1) * paso - time.perf_counter()   # reloj absoluto: no acumula deriva
            if espera > 0:
                time.sleep(espera)
        campos = {"estacion_id": ev["estacion_id"], "tipo_sensor": ev["tipo_sensor"],
                  "valor": ev["valor"], "timestamp": ev["timestamp"], "n": n}
        xadd_con_reintento(r, stream, campos, MAXLEN)
        if progreso:
            progreso(n)
    # marca de fin: los consumidores terminan cuando la ven y ya no les queda nada
    while True:
        try:
            r.set(f"{stream}:fin", n)
            break
        except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError):
            time.sleep(0.2)
    return n, time.perf_counter() - ini


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--rapido", action="store_true")
    a = ap.parse_args()

    def prog(n):
        if n % 1000 == 0:
            print(f"  producidos {n:,}", flush=True)

    n, seg = producir(a.stream, 0 if a.rapido else PASO_VIVO, prog)
    print(f"productor: {n:,} eventos en {seg:.1f} s ({n / seg:,.1f} ev/s)")


if __name__ == "__main__":
    main()
