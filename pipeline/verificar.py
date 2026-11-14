"""B.3 Verificacion: compara lo que quedo en Redis y HBase con los numeros de control (y con lo que deberia
haber en el tablero, recalculado desde el generador).

    python -m pipeline.verificar
"""
import sys

from pipeline.comun import (CONTROL, GRUPO_INGESTA, GRUPO_TABLERO, N_HIST, N_VIVO, STREAM, TIPO_DE, VENTANA_MS,
                            conectar_hbase, conectar_redis, contar_filas, escribir, historico, tabla_md, ts_ms,
                            vivo)


def control_desde_generador():
    """Recalcula repetidos y claves distintas desde el generador (no confia en las constantes)."""
    out = {}
    todos_c, todas_k = set(), set()
    for nombre, it in (("hist", historico()), ("vivo", vivo())):
        n, completos, claves = 0, set(), set()
        for e in it:
            n += 1
            t = (e["estacion_id"], e["tipo_sensor"], e["valor"], e["timestamp"])
            completos.add(t)
            claves.add((e["estacion_id"], e["timestamp"]))
        out[nombre] = {"eventos": n, "repetidos": n - len(completos), "claves": len(claves)}
        todos_c |= completos
        todas_k |= claves
    out["total"] = {"eventos": out["hist"]["eventos"] + out["vivo"]["eventos"],
                    "repetidos": N_HIST + N_VIVO - len(todos_c), "claves": len(todas_k)}
    return out


def tablero_esperado():
    """Ultimo por medicion (empate de ms -> el ultimo que llego) y ventanas, recalculados de la hora en vivo."""
    ultimo, por_ts = {}, {}
    for e in vivo():
        est, ts = e["estacion_id"], ts_ms(e["timestamp"])
        if est not in ultimo or ts >= ultimo[est][1]:
            ultimo[est] = (e["valor"], ts, e["tipo_sensor"])
        por_ts[(est, ts)] = e["valor"]
    ancla = max(ts for _, ts in por_ts)
    ventanas = {}
    for (est, ts), v in por_ts.items():
        if TIPO_DE[est] == "flujo_vehicular" and ts > ancla - VENTANA_MS:
            ventanas.setdefault(est, set()).add(f"{ts}:{v}")
    return ultimo, ventanas, ancla


def main():
    r = conectar_redis()
    conn = conectar_hbase()
    lineas, problemas = [], []

    def chequeo(desc, obtenido, esperado, explicacion=""):
        ok = obtenido == esperado
        lineas.append([desc, f"{obtenido:,}" if isinstance(obtenido, int) else obtenido,
                       f"{esperado:,}" if isinstance(esperado, int) else esperado, "OK" if ok else "NO CALZA"])
        if not ok:
            problemas.append(f"{desc}: obtenido {obtenido}, esperado {esperado}. {explicacion}")

    # 1. numeros de control recalculados desde el generador
    gen = control_desde_generador()
    for parte in ("hist", "vivo", "total"):
        for k in ("eventos", "repetidos", "claves"):
            chequeo(f"generador {parte}: {k}", gen[parte][k], CONTROL[parte][k],
                    "el generador o la semilla no son los de la tarea")

    # 2. stream y grupos
    producidos = int(r.get(f"{STREAM}:producidos") or 0)
    info = r.xinfo_stream(STREAM)
    chequeo("eventos producidos (contador del productor)", producidos, N_VIVO)
    chequeo("entries-added del stream", info["entries-added"], N_VIVO)
    grupos = {g["name"]: g for g in r.xinfo_groups(STREAM)}
    for g in (GRUPO_INGESTA, GRUPO_TABLERO):
        gi = grupos.get(g, {})
        proc = int(r.get(f"stats:{STREAM}:{g}:procesadas") or 0)
        chequeo(f"grupo {g}: entradas procesadas (XACK)", proc, N_VIVO,
                "si es mayor, hubo reprocesos (no es grave: es idempotente)")
        chequeo(f"grupo {g}: entries-read", gi.get("entries-read"), N_VIVO)
        chequeo(f"grupo {g}: XPENDING", r.xpending(STREAM, g)["pending"], 0,
                "quedaron entradas leidas sin XACK: un consumidor murio y nadie las rescato")

    # 3. HBase
    for t in ("lecturas", "lecturas_tipo"):
        chequeo(f"filas HBase {t}", contar_filas(conn, t), CONTROL["total"]["claves"],
                "mas filas = la clave no es idempotente (usa la llegada); menos = se perdieron eventos")

    # 4. tablero
    ultimo, ventanas, ancla = tablero_esperado()
    malos = []
    for est, (v, ts, tipo) in sorted(ultimo.items()):
        h = r.hgetall(f"ultimo:{est}")
        if not h or float(h["valor"]) != v or int(h["ts"]) != ts or h["tipo"] != tipo:
            malos.append(est)
    chequeo("tablero: ultimo:<est> correctos (por medicion)", f"{16 - len(malos)}/16", "16/16",
            f"no calzan: {malos}")
    rank = {m: s for m, s in r.zrange("ranking:flujo", 0, -1, withscores=True)}
    rank_esp = {e: x[0] for e, x in ultimo.items() if x[2] == "flujo_vehicular"}
    chequeo("tablero: ranking:flujo", "OK" if rank == rank_esp else str(rank), "OK")
    v_ok = sum(1 for est, s in ventanas.items() if set(r.zrange(f"ventana:{est}", 0, -1)) == s)
    chequeo("tablero: ventana:<est> = ultimos 10 min de medicion", f"{v_ok}/{len(ventanas)}", f"{len(ventanas)}/8")
    cat = {t: r.smembers(f"tipo:{t}") for t in set(TIPO_DE.values())}
    cat_esp = {t: {e for e, x in TIPO_DE.items() if x == t} for t in set(TIPO_DE.values())}
    chequeo("tablero: catalogo tipo:<tipo>", "OK" if cat == cat_esp else str(cat), "OK")

    texto = "# B.3 Verificacion\n\n" + tabla_md(["chequeo", "obtenido", "esperado", ""], lineas) + "\n\n"
    if problemas:
        texto += "## Lo que no calza\n\n" + "\n".join(f"- {p}" for p in problemas) + "\n"
    else:
        texto += ("Todo calza. HBase tiene 853.953 filas en cada tabla = 846.906 claves del historico + 7.047 de la "
                  "hora en vivo. La diferencia con los 871.200 eventos son los 17.243 repetidos tal cual + 4 "
                  "choques de clave (misma estacion y mismo ms, distinto valor) que quedan con el ultimo valor "
                  "escrito.\n")
    print(texto)
    escribir("b3_verificacion.md", texto)
    return 1 if problemas else 0


if __name__ == "__main__":
    sys.exit(main())
