"""P1 "Ahora" - Redis contra HBase.

Ultimo valor de las 16 estaciones + top 5 de flujo vehicular, desde Redis (tablero) y desde HBase (lecturas).
En HBase, con clave estacion#ts lo ultimo es la ULTIMA fila del rango de la estacion: se pide con un scan
inverso (reverse=True, limit=1) por estacion = 16 RPC. Ademas: la estacion cuyo "ultimo" cambia segun se tome
por medicion o por llegada (la llegada sale del orden del stream).
"""
from pipeline.comun import STREAM, TIPO_DE, TIPOS, conectar_hbase, conectar_redis, escribir, ms_a_iso, tabla_md
from preguntas.medir import fmt_t, guardar_tiempo, medir

ESTACIONES = sorted(TIPO_DE)


def desde_redis(r):
    estaciones = sorted(r.sunion([f"tipo:{t}" for t in TIPOS]))       # catalogo
    p = r.pipeline(transaction=False)
    for e in estaciones:
        p.hgetall(f"ultimo:{e}")
    p.zrevrange("ranking:flujo", 0, 4, withscores=True)
    res = p.execute()
    ult = {e: (float(h["valor"]), int(h["ts"])) for e, h in zip(estaciones, res[:-1])}
    return ult, [(e, v) for e, v in res[-1]]


def desde_hbase(conn):
    t = conn.table("lecturas")
    ult = {}
    for e in ESTACIONES:
        pref = f"{e}#".encode()
        for k, d in t.scan(row_start=pref + b"\xff", row_stop=pref, reverse=True, limit=1, batch_size=1,
                           columns=[b"d:valor"]):
            ult[e] = (float(d[b"d:valor"]), k.decode().split("#", 1)[1])
    flujo = sorted(((e, v) for e, (v, _) in ult.items() if TIPO_DE[e] == "flujo_vehicular"),
                   key=lambda x: -x[1])[:5]
    return ult, flujo


def ultimo_por_llegada(r):
    """Recorre el stream de atras para adelante (orden de llegada) hasta ver las 16 estaciones."""
    vistos, fin = {}, "+"
    while len(vistos) < 16:
        tramo = r.xrevrange(STREAM, max=fin, min="-", count=500)
        if not tramo:
            break
        for i, c in tramo:
            vistos.setdefault(c["estacion_id"], (i, c))
        fin = "(" + tramo[-1][0]
    return vistos


def main():
    r, conn = conectar_redis(), conectar_hbase()
    (ult_r, top_r), m_r = medir(lambda: desde_redis(r))
    (ult_h, top_h), m_h = medir(lambda: desde_hbase(conn))
    guardar_tiempo("P1 Redis (16 ultimos + top5)", m_r)
    guardar_tiempo("P1 HBase (16 scans inversos)", m_h)

    filas = []
    for e in ESTACIONES:
        vr, tsr = ult_r[e]
        vh, tsh = ult_h[e]
        filas.append([e, TIPO_DE[e], vr, ms_a_iso(tsr)[11:23], vh, tsh[11:23], "si" if vr == vh else "NO"])
    igual_top = [e for e, _ in top_r] == [e for e, _ in top_h]

    llegada = ultimo_por_llegada(r)
    distintas = []
    for e in ESTACIONES:
        sid, c = llegada[e]
        if c["timestamp"] != ms_a_iso(ult_r[e][1]) or float(c["valor"]) != ult_r[e][0]:
            distintas.append((e, sid, c))
    filas_d = []
    for e, sid, c in distintas:
        filas_d.append([e, "por medicion (tablero, HBase)", ult_r[e][0], ms_a_iso(ult_r[e][1]), "-"])
        filas_d.append([e, "por llegada (ultima entrada del stream)", c["valor"], c["timestamp"],
                        f"{sid} (evento n={c['n']})"])

    veces = m_h["mediana_ms"] / m_r["mediana_ms"]
    texto = f"""# P1 "Ahora" - Redis contra HBase

**Redis**: `SUNION tipo:*` (catalogo) + un pipeline con 16 `HGETALL ultimo:<est>` y `ZREVRANGE ranking:flujo 0 4 WITHSCORES`.
**HBase**: por cada estacion `scan(row_start=b'<est>#\\xff', row_stop=b'<est>#', reverse=True, limit=1)` sobre `lecturas`
(la ultima fila del rango de la estacion = la medicion mas nueva). 16 scans; el top 5 se ordena en Python.

{tabla_md(["estacion", "tipo", "valor Redis", "ts Redis", "valor HBase", "ts HBase", "igual"], filas)}

Top 5 flujo vehicular (Redis): {", ".join(f"{e} = {v}" for e, v in top_r)}
Top 5 flujo vehicular (HBase): {", ".join(f"{e} = {v}" for e, v in top_h)} -> {"mismo resultado" if igual_top else "DISTINTO"}

## Tiempos

- Redis: {fmt_t(m_r)}
- HBase: {fmt_t(m_h)}
- HBase tarda **{veces:.0f} veces** lo que tarda Redis.

## Ultimo por medicion vs ultimo por llegada

{tabla_md(["estacion", "criterio", "valor", "timestamp de medicion", "entrada del stream"], filas_d) if filas_d else "ninguna estacion cambia"}

**Interpretacion**: un dashboard deberia leer de Redis: responde en {m_r['mediana_ms']:.1f} ms con 2 idas y vueltas
contra {m_h['mediana_ms']:.1f} ms y 16 scans en HBase, y su costo no depende de cuanta historia haya. En
{", ".join(e for e, _, _ in distintas) or "-"} la ultima lectura que llego es una lectura tardia (medida antes que otra que ya
habia llegado): si el tablero usara "lo ultimo que llego" mostraria un valor viejo. Por eso el tablero compara el ts
de medicion antes de escribir. HBase da lo mismo que Redis porque su clave ordena por medicion, pero no sabe nada
del orden de llegada (eso solo queda en el stream).
"""
    print(texto)
    escribir("p1_ahora.md", texto)


if __name__ == "__main__":
    main()
