"""P5 Hotspotting y salting - HBase.

La hora en vivo (7.200 eventos, en orden de llegada) escrita en tres tablas con la misma informacion y distinta clave:
  p5_est_ts   estacion#ts           pre-dividida por estacion (los mismos cortes que lecturas)
  p5_ts_est   ts#estacion           pre-dividida por dia
  p5_salt     salt#ts#estacion      salt = crc32(estacion) mod 4, pre-dividida por salt
Se mide cuantas escrituras recibio cada region (JMX writeRequestCount) y el tiempo de leer todas las estaciones
entre 00:10 y 00:20 del 07-nov (16 rangos / 1 rango / 4 rangos).
"""
import time
from collections import Counter

from pipeline.comun import (SPLITS, TIPO_DE, conectar_hbase, contar_filas, escribir, recrear_tablas, salt,
                            tabla_md, vivo)
from preguntas.medir import fmt_t, guardar_tiempo, jmx_regiones, medir

ESTACIONES = sorted(TIPO_DE)
DIAS = ["2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07", "2026-11-08"]
TABLAS = {
    "p5_est_ts": (SPLITS["lecturas"], lambda e, ts: f"{e}#{ts}"),
    "p5_ts_est": (DIAS, lambda e, ts: f"{ts}#{e}"),
    "p5_salt": (["1", "2", "3"], lambda e, ts: f"{salt(e)}#{ts}#{e}"),
}
DESDE, HASTA = "2026-11-07T00:10:00.000-03:00", "2026-11-07T00:20:00.000-03:00"
LOTE = 50   # como llegan en vivo: lotes chicos, en orden de llegada


def escribir_hora(conn):
    eventos = list(vivo())
    for nombre, (_, clave) in TABLAS.items():
        t = conn.table(nombre)
        for i in range(0, len(eventos), LOTE):
            with t.batch() as b:
                for ev in eventos[i:i + LOTE]:
                    b.put(clave(ev["estacion_id"], ev["timestamp"]).encode(),
                          {b"d:valor": str(ev["valor"]).encode(), b"d:tipo": ev["tipo_sensor"].encode()})


def escrituras_por_region(conn, nombre):
    beans = jmx_regiones()
    filas = []
    for reg in conn.table(nombre).regions():
        cod = reg["name"].decode().rstrip(".").rsplit(".", 1)[1]
        clave = f"Namespace_default_table_{nombre}_region_{cod}_metric_writeRequestCount"
        filas.append((reg["start_key"].decode() or "(inicio)", reg["end_key"].decode() or "(fin)", beans.get(clave)))
    return filas


def leer(conn, nombre):
    t = conn.table(nombre)
    if nombre == "p5_est_ts":
        rangos = [(f"{e}#{DESDE}", f"{e}#{HASTA}") for e in ESTACIONES]                # 16 rangos
    elif nombre == "p5_ts_est":
        rangos = [(DESDE, HASTA)]                                                        # 1 rango
    else:
        rangos = [(f"{s}#{DESDE}", f"{s}#{HASTA}") for s in range(4)]                    # 4 rangos
    n = 0
    for a, b in rangos:
        n += sum(1 for _ in t.scan(row_start=a.encode(), row_stop=b.encode(), batch_size=1000))
    return n, len(rangos)


def main():
    conn = conectar_hbase()
    print("Creando las 3 tablas ...")
    recrear_tablas(conn, [(n, s) for n, (s, _) in TABLAS.items()], versiones=1)
    print("Escribiendo la hora en vivo en las 3 tablas ...")
    escribir_hora(conn)
    print("Esperando que JMX publique (~10 s) ...")
    time.sleep(15)
    regiones = {n: escrituras_por_region(conn, n) for n in TABLAS}
    for _ in range(6):   # si todavia no aparecen todas, esperar otro poco
        if all(c is not None for f in regiones.values() for *_, c in f) and \
           all(sum(c for *_, c in f) >= 7_000 for f in regiones.values()):
            break
        time.sleep(5)
        regiones = {n: escrituras_por_region(conn, n) for n in TABLAS}

    secciones, resumen = [], []
    for n, filas in regiones.items():
        tot = sum(c or 0 for *_, c in filas)
        mx = max(c or 0 for *_, c in filas)
        secciones.append(f"**{n}** ({len(filas)} regiones, {contar_filas(conn, n):,} filas)\n\n" + tabla_md(
            ["region (inicio)", "region (fin)", "escrituras", "% del total"],
            [[a, b, f"{c:,}" if c is not None else "?", f"{100 * (c or 0) / tot:.1f}%"] for a, b, c in filas]))
        resumen.append([n, len(filas), sum(1 for *_, c in filas if c), f"{100 * mx / tot:.0f}%"])

    lecturas = {}
    for n in TABLAS:
        (cant, nr), m = medir(lambda n=n: leer(conn, n))
        lecturas[n] = (cant, nr, m)
        guardar_tiempo(f"P5 {n} ({nr} rango{'s' if nr > 1 else ''})", m)

    reparto = Counter(salt(e) for e in ESTACIONES)
    texto = f"""# P5 Hotspotting y salting - HBase

La hora en vivo (7.200 eventos -> 7.047 claves) escrita en orden de llegada, en lotes de {LOTE}, en tres tablas.
Escrituras por region = `writeRequestCount` del JMX (`/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions`),
leido despues de esperar que se publique (~10 s). Suma un poco mas que 7.047 y no 7.200: happybase junta en una sola
mutacion los puts a la misma fila dentro de un lote, y un repetido llega pegado al original (casi siempre en el mismo
lote); solo cuentan doble los pocos que quedaron en el borde entre dos lotes.

{(chr(10) + chr(10)).join(secciones)}

## Tabla 3 x regiones (resumen)

{tabla_md(["tabla", "regiones", "regiones que recibieron escrituras", "la region mas cargada se llevo"], resumen)}

Reparto de estaciones por salt (crc32 mod 4): {dict(sorted(reparto.items()))}

## "Todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov"

{tabla_md(["tabla", "rangos", "lecturas", "mediana (ms)", "primera (ms)"],
          [[n, nr, c, f"{m['mediana_ms']:.2f}", f"{m['primera_ms']:.2f}"] for n, (c, nr, m) in lecturas.items()])}

{chr(10).join(f"- {n}: {fmt_t(m)}" for n, (_, _, m) in lecturas.items())}

**Interpretacion**: con `ts#estacion` todas las escrituras de la hora caen en la region del 07-nov (mas unas pocas
tardias en la del 06): una sola region, y en un cluster un solo servidor, recibe todo = hotspot. Con
`estacion#ts` y con salt la carga se reparte. Para leer "todo un intervalo" pasa al reves: `ts#estacion` lo
resuelve en 1 rango ({lecturas['p5_ts_est'][2]['mediana_ms']:.0f} ms), salt en 4 ({lecturas['p5_salt'][2]['mediana_ms']:.0f} ms)
y `estacion#ts` en 16 ({lecturas['p5_est_ts'][2]['mediana_ms']:.0f} ms): cada rango es un RPC mas, y con
~1.200 filas en una sola maquina lo que pesa es la cantidad de idas y vueltas, no el volumen. Elegiriamos `estacion#ts` (que ya es `lecturas`): reparte bien las
escrituras y la pregunta tipica es por estacion. Cambiariamos a salt si hubiera muchas consultas "todas las
estaciones en un intervalo" con un cluster de verdad, y `ts#estacion` solo serviria si la tasa de escritura fuera
baja y casi todas las consultas fueran por intervalo de tiempo.
"""
    print(texto)
    escribir("p5_hotspotting.md", texto)


if __name__ == "__main__":
    main()
