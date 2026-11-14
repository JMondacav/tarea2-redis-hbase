"""P2 Ventana de 10 minutos - Redis.

Promedio de flujo de los ultimos 10 min de medicion (anclados a la medicion mas reciente del stream,
tablero:ts_max) por estacion de flujo, leyendo los ventana:* con ZRANGEBYSCORE. Y cuanta memoria se iria
si la ventana no se recortara (MEMORY USAGE).
"""
from pipeline.comun import STREAM, VENTANA_MS, conectar_redis, escribir, ms_a_iso, tabla_md, ts_ms
from preguntas.medir import fmt_t, guardar_tiempo, medir


def promedios(r):
    flujo = sorted(r.smembers("tipo:flujo_vehicular"))
    ancla = int(r.get("tablero:ts_max"))
    p = r.pipeline(transaction=False)
    for e in flujo:
        p.zrangebyscore(f"ventana:{e}", f"({ancla - VENTANA_MS}", ancla)   # (ancla-10min, ancla]
    out = {}
    for e, miembros in zip(flujo, p.execute()):
        vals = [float(m.split(":", 1)[1]) for m in miembros]
        out[e] = (len(vals), sum(vals) / len(vals) if vals else None)
    return ancla, out


def main():
    r = conectar_redis()
    (ancla, res), m = medir(lambda: promedios(r))
    guardar_tiempo("P2 Redis (8 ventanas)", m)
    filas = [[e, n, f"{v:.2f}"] for e, (n, v) in res.items()]

    # --- memoria: ventana recortada vs. la misma ventana sin recortar (toda la hora en vivo) ---
    flujo = sorted(res)
    for e in flujo:
        r.delete(f"exp:sin_recorte:{e}")
    p = r.pipeline(transaction=False)
    for _, c in r.xrange(STREAM, "-", "+"):
        if c["tipo_sensor"] == "flujo_vehicular":
            t = ts_ms(c["timestamp"])
            p.zadd(f"exp:sin_recorte:{c['estacion_id']}", {f"{t}:{c['valor']}": t})
    p.execute()
    filas_m, tot_c, tot_s, n_c, n_s = [], 0, 0, 0, 0
    for e in flujo:
        kc, ks = f"ventana:{e}", f"exp:sin_recorte:{e}"
        mc, ms_ = r.memory_usage(kc, samples=0), r.memory_usage(ks, samples=0)
        cc, cs = r.zcard(kc), r.zcard(ks)
        tot_c, tot_s, n_c, n_s = tot_c + mc, tot_s + ms_, n_c + cc, n_s + cs
        filas_m.append([e, cc, mc, r.object("encoding", kc), cs, ms_, r.object("encoding", ks)])
    for e in flujo:
        r.delete(f"exp:sin_recorte:{e}")
    b_por_entrada = tot_s / n_s
    # ritmo real: 2 ev/s entre 16 estaciones -> 0,125 ev/s por estacion = 10.800 lecturas/dia
    dia = 10_800 * b_por_entrada * 8 / 1e6
    anio = dia * 365 / 1e3

    texto = f"""# P2 Ventana de 10 minutos - Redis

Ancla = medicion mas reciente del stream (`tablero:ts_max`) = **{ms_a_iso(ancla)}**.
Ventana = (ancla - 10 min, ancla]. Por estacion: `ZRANGEBYSCORE ventana:<est> ({ancla - VENTANA_MS} {ancla}`
(los 8 en un pipeline); el miembro es `"<ts>:<valor>"` y el promedio se saca de esos valores.

{tabla_md(["estacion", "lecturas en la ventana", "promedio flujo (10 min)"], filas)}

Tiempo: {fmt_t(m)}

## Por que un sorted set con el ts de medicion como score

- `ZRANGEBYSCORE` saca un rango de tiempo en O(log n + k), sin recorrer todo.
- Recortar es un solo `ZREMRANGEBYSCORE -inf <ancla-10min>`, tambien por score.
- Ordena por **medicion** y no por llegada: una lectura tardia cae en su lugar y una que llega con mas de 10 min
  de atraso respecto al ancla ni entra.
- El miembro lleva el ts, asi un evento repetido tal cual es el mismo miembro (ZADD no lo duplica).

## Memoria (MEMORY USAGE ... SAMPLES 0)

{tabla_md(["estacion", "entradas recortada", "bytes recortada", "encoding", "entradas sin recortar (1 h)", "bytes sin recortar", "encoding"], filas_m)}

Total: recortadas **{tot_c:,} bytes** ({n_c} entradas) vs sin recortar **{tot_s:,} bytes** ({n_s} entradas), o sea
{tot_s / tot_c:.1f} veces mas con solo 1 hora. Sin recortar ademas pasa de `listpack` (compacto, hasta 128 entradas)
a `skiplist` (~{b_por_entrada:.0f} bytes por entrada). A ritmo real (0,125 lecturas/s por estacion) seria ~{dia:.1f} MB
por dia para las 8 estaciones y ~{anio:.1f} GB al año, creciendo para siempre en RAM: la historia larga no va en Redis,
va en HBase.

**Interpretacion**: la ventana recortada es chica y de tamano fijo (~75 lecturas por estacion), por eso responde en
{m['mediana_ms']:.1f} ms y cabe en memoria siempre; sin recorte crece lineal con el tiempo.
"""
    print(texto)
    escribir("p2_ventana.md", texto)


if __name__ == "__main__":
    main()
