"""P4 Dos patrones de acceso - catalogo en Redis + HBase.

PM2.5 de cada estacion de calidad del aire entre 07:00 y 09:00 del 05-nov: promedio, maximo y hora del maximo
(primera ocurrencia).
 (a) lecturas (estacion primero): la lista de estaciones sale de Redis (SMEMBERS tipo:calidad_aire), un rango por estacion
 (b) lecturas_tipo (tipo primero): un solo scan
"""
import subprocess

from pipeline.comun import RAIZ, conectar_hbase, conectar_redis, contar_filas, escribir, hbase_shell, tabla_md
from preguntas.medir import fmt_t, guardar_tiempo, medir

DESDE, HASTA = "2026-11-05T07:00:00.000-03:00", "2026-11-05T09:00:00.000-03:00"


def resumir(lecturas_por_est):
    out = {}
    for est, filas in sorted(lecturas_por_est.items()):
        vmax, tmax = None, None
        for ts, v in filas:                 # vienen en orden de tiempo: el primer maximo queda
            if vmax is None or v > vmax:
                vmax, tmax = v, ts
        out[est] = (len(filas), sum(v for _, v in filas) / len(filas), vmax, tmax)
    return out


def estacion_primero(r, t):
    estaciones = r.smembers("tipo:calidad_aire")       # el catalogo vive en Redis
    datos = {}
    for e in estaciones:
        datos[e] = [(k.decode().split("#", 1)[1], float(d[b"d:valor"]))
                    for k, d in t.scan(row_start=f"{e}#{DESDE}".encode(), row_stop=f"{e}#{HASTA}".encode(),
                                       columns=[b"d:valor"], batch_size=1000)]
    return resumir(datos)


def tipo_primero(t):
    datos = {}
    for k, d in t.scan(row_start=f"calidad_aire#{DESDE}".encode(), row_stop=f"calidad_aire#{HASTA}".encode(),
                       columns=[b"d:valor"], batch_size=1000):
        _, ts, est = k.decode().split("#")
        datos.setdefault(est, []).append((ts, float(d[b"d:valor"])))
    return resumir(datos)


def tamano_en_disco(tabla):
    r = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "du", "-sb", f"/data/hbase/data/default/{tabla}"],
                       capture_output=True, text=True, cwd=RAIZ)
    return int(r.stdout.split()[0])


def main():
    r, conn = conectar_redis(), conectar_hbase()
    t_lect, t_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")
    res_a, m_a = medir(lambda: estacion_primero(r, t_lect))
    res_b, m_b = medir(lambda: tipo_primero(t_tipo))
    guardar_tiempo("P4 (a) catalogo Redis + 4 rangos en lecturas", m_a)
    guardar_tiempo("P4 (b) 1 scan en lecturas_tipo", m_b)

    # costo de la segunda tabla: espacio (flush para que la MemStore baje a HFiles y se pueda medir)
    hbase_shell("flush 'lecturas'\nflush 'lecturas_tipo'\n")
    disco = {t: tamano_en_disco(t) for t in ("lecturas", "lecturas_tipo")}
    filas = {t: contar_filas(conn, t) for t in ("lecturas", "lecturas_tipo")}

    filas_r = [[e, n, f"{p:.2f}", vmax, tmax[11:23]] for e, (n, p, vmax, tmax) in res_a.items()]
    texto = f"""# P4 Dos patrones de acceso - catalogo Redis + HBase

Rango [07:00, 09:00) del 05-nov, solo estaciones de calidad del aire (PM2.5).

(a) `SMEMBERS tipo:calidad_aire` en Redis -> {len(res_a)} estaciones -> un `scan` por estacion en `lecturas`
    (`<est>#{DESDE}` .. `<est>#{HASTA}`).
(b) un solo `scan` en `lecturas_tipo` de `calidad_aire#{DESDE}` a `calidad_aire#{HASTA}`.

{tabla_md(["estacion", "lecturas", "promedio PM2.5", "maximo", "hora del maximo (1a vez)"], filas_r)}

(a) y (b) dan {"exactamente lo mismo" if res_a == res_b else "DISTINTO"}.

## Tiempos

- (a) catalogo Redis + {len(res_a)} rangos: {fmt_t(m_a)}
- (b) un scan en lecturas_tipo: {fmt_t(m_b)}

## Lo que cuesta la segunda tabla

{tabla_md(["tabla", "filas", "bytes en disco (despues de flush)"], [[t, f"{filas[t]:,}", f"{disco[t]:,}"] for t in disco])}

- Escrituras: cada evento se escribe 2 veces. En la carga historica fueron 1.728.000 puts en vez de 864.000,
  y en vivo 14.400 en vez de 7.200.
- Disco: lecturas_tipo ocupa {disco['lecturas_tipo'] / disco['lecturas']:.2f} veces lo de lecturas (la clave es mas larga
  porque lleva el nombre del tipo y la estacion) -> en total se paga ~{(disco['lecturas'] + disco['lecturas_tipo']) / disco['lecturas']:.1f}x.
- Si falla la segunda escritura: HBase no tiene transacciones entre tablas. En `ingesta` el lote se escribe en
  `lecturas` y despues en `lecturas_tipo`; si la segunda falla se lanza la excepcion, **no hay XACK**, el lote queda
  pendiente y se reintenta entero. La primera tabla recibe el mismo put otra vez (misma clave, no duplica) y al final las
  dos quedan iguales. Mientras tanto hay un rato en que `lecturas` tiene el evento y `lecturas_tipo` no.

**Interpretacion**: aqui la segunda tabla casi no gana nada: hay solo {len(res_a)} estaciones de calidad del aire, asi que
(a) son {len(res_a)} scans cortos y (b) uno solo, y los dos estan en el orden de los milisegundos. No valdria la pena
cuando hay pocas estaciones por tipo (como aqui) o la consulta por tipo es rara, porque se paga el doble de
escrituras y de disco siempre. Valdria la pena con miles de estaciones por tipo, donde (a) serian miles de RPC.
"""
    print(texto)
    escribir("p4_dos_patrones.md", texto)


if __name__ == "__main__":
    main()
