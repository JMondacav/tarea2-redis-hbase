"""P6 La propia: "Hace cuanto que no habia este flujo?"

Para cada estacion de flujo vehicular:
  - Redis decide el QUE: el promedio de sus ultimos 10 min de medicion (ventana:<est>, anclado a tablero:ts_max),
    que ordena las estaciones de mas a menos cargada ahora.
  - HBase responde el CUANDO: scan inverso en `lecturas` desde el inicio de la ventana actual hacia atras,
    calculando el promedio movil de 10 min, hasta encontrar la ultima ventana anterior con promedio >= al de ahora.
Es lo que mostraria un tablero como "el flujo de 10 min mas alto en N horas" al lado del valor en vivo.
"""
from collections import deque
from datetime import timedelta

from pipeline.comun import VENTANA_MS, conectar_hbase, conectar_redis, escribir, ms_a_iso, tabla_md, ts_ms
from preguntas.medir import fmt_t, guardar_tiempo, medir

MIN_LECTURAS = 30   # una ventana de 10 min tiene ~75 lecturas; menos que esto es un borde de los datos


def ahora_redis(r):
    """Promedio de 10 min de cada estacion de flujo, ordenado de mayor a menor (como P2, pero es el insumo)."""
    flujo = sorted(r.smembers("tipo:flujo_vehicular"))
    ancla = int(r.get("tablero:ts_max"))
    p = r.pipeline(transaction=False)
    for e in flujo:
        p.zrangebyscore(f"ventana:{e}", f"({ancla - VENTANA_MS}", ancla)
    prom = {e: sum(float(m.split(":", 1)[1]) for m in ms) / len(ms) for e, ms in zip(flujo, p.execute())}
    return ancla, sorted(prom.items(), key=lambda x: -x[1])


def ultima_ventana_mayor(t, est, umbral, ancla):
    """Ventanas (a-10min, a] con a <= ancla-10min (no se traslapan con la de ahora), de la mas nueva a la mas vieja."""
    inicio = f"{est}#{ms_a_iso(ancla - VENTANA_MS)}".encode()
    ventana, suma, recorridas = deque(), 0.0, 0     # ventana[0] = el ancla candidata (la mas nueva)
    for k, d in t.scan(row_start=inicio, row_stop=f"{est}#".encode(), reverse=True,
                       columns=[b"d:valor"], batch_size=200):   # chico: casi siempre para en < 200 filas
        recorridas += 1
        ts, v = ts_ms(k.decode().split("#", 1)[1]), float(d[b"d:valor"])
        # la fila nueva ya quedo fuera de la ventana de ventana[0]: esa ventana esta completa, se evalua
        while ventana and ts <= ventana[0][0] - VENTANA_MS:
            if len(ventana) >= MIN_LECTURAS and suma / len(ventana) >= umbral:
                return ventana[0][0], suma / len(ventana), len(ventana), recorridas
            suma -= ventana.popleft()[1]
        ventana.append((ts, v))
        suma += v
    return None, None, None, recorridas


def consulta(r, t):
    ancla, ranking = ahora_redis(r)                              # Redis: el que
    return ancla, [(e, prom, *ultima_ventana_mayor(t, e, prom, ancla)) for e, prom in ranking]   # HBase: el cuando


def main():
    r, conn = conectar_redis(), conectar_hbase()
    t = conn.table("lecturas")
    (ancla, res), m = medir(lambda: consulta(r, t))
    guardar_tiempo("P6 Redis (que) + HBase (cuando)", m)
    filas = []
    for i, (est, prom, a, prom_h, n_h, rec) in enumerate(res, 1):
        if a:
            dif = timedelta(milliseconds=ancla - a)
            horas = dif.total_seconds() / 3600
            hace = f"{horas:.1f} h" if horas >= 1 else f"{dif.total_seconds() / 60:.0f} min"
            filas.append([i, est, f"{prom:.2f}", ms_a_iso(a)[:19].replace("T", " "), f"{prom_h:.2f}", hace, f"{rec:,}"])
        else:
            filas.append([i, est, f"{prom:.2f}", "nunca (record de los 5 dias)", "-", "-", f"{rec:,}"])
    total = sum(x[5] for x in res)
    texto = f"""# P6 La propia: "hace cuanto que no habia este flujo?"

Para cada estacion de flujo: el promedio de sus ultimos 10 min (ahora) y la ultima vez en la historia que tuvo una
ventana de 10 min con promedio igual o mayor. Ancla (ahora) = {ms_a_iso(ancla)}.

- **Redis (el que)**: `SMEMBERS tipo:flujo_vehicular`, `GET tablero:ts_max` y `ZRANGEBYSCORE ventana:<est>` -> promedio de
  ahora de cada estacion, que define el orden y el umbral de cada una.
- **HBase (el cuando)**: `scan(row_start=b'<est>#<ancla - 10 min>', row_stop=b'<est>#', reverse=True)` en `lecturas`,
  con un promedio movil de 10 min (deque + suma) que se detiene en la primera ventana con promedio >= umbral.
  Solo ventanas que terminan antes de que empiece la de ahora (no se traslapan).

{tabla_md(["rank", "estacion", "prom. 10 min ahora (Redis)", "ultima ventana >= (fin, HBase)", "su promedio", "hace", "filas recorridas"], filas)}

Tiempo (todo junto, 8 estaciones): {fmt_t(m)} - {total:,} filas recorridas en HBase.

Como se lee: "10 min" quiere decir que la ventana justo anterior ya era igual o mas alta (la estacion viene
bajando o esta estable); las que salen con horas estan en un pico que no se veia hace rato, que es justo lo que
le interesa a quien mira el tablero.

**Por que necesita a los dos**: el promedio de ahora cambia cada pocos segundos y sale de la ventana que mantiene
el tablero con el stream (ultimo por medicion, tardias en su lugar): eso es Redis, y leerlo cuesta ~2 ms (P2). La
pregunta "desde cuando" necesita dias de historia por estacion, que en Redis no estan (P2: costaria GB de RAM al
año) y que en HBase estan ordenados por estacion y tiempo: el scan inverso desde "ahora" los recorre en el orden
justo y se detiene apenas encuentra la respuesta. Por eso el costo no es fijo: depende de que tan lejos este la
respuesta; si la estacion esta en su record hay que leer toda su historia (~54 mil filas en 5 dias, y creciendo).
"""
    print(texto)
    escribir("p6_propia.md", texto)


if __name__ == "__main__":
    main()
