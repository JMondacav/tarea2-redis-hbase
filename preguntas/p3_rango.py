"""P3 Un rango - HBase.

Lecturas de CC-001 el 04-nov entre 08:00 (incluido) y 10:00 (excluido): cuantas y promedio por cuarto de hora.
(a) scan por rango de clave  (b) recorrido de la tabla entera con un filtro de servidor (RowFilter regex).
"""
from collections import defaultdict

from pipeline.comun import conectar_hbase, contar_filas, escribir, tabla_md
from preguntas.medir import fmt_t, guardar_tiempo, medir

INI = b"CC-001#2026-11-04T08:00:00.000-03:00"
FIN = b"CC-001#2026-11-04T10:00:00.000-03:00"
FILTRO = b"RowFilter(=, 'regexstring:^CC-001#2026-11-04T0[89]:')"


def agrupar(filas):
    g = defaultdict(list)
    for k, d in filas:
        ts = k.decode().split("#", 1)[1]
        hh, mm = ts[11:13], int(ts[14:16])
        g[f"{hh}:{mm // 15 * 15:02d}"].append(float(d[b"d:valor"]))
    return sum(len(v) for v in g.values()), {q: (len(v), sum(v) / len(v)) for q, v in sorted(g.items())}


def por_rango(t):
    return agrupar(t.scan(row_start=INI, row_stop=FIN, columns=[b"d:valor"], batch_size=1000))


def tabla_entera(t):
    return agrupar(t.scan(filter=FILTRO, columns=[b"d:valor"], batch_size=1000))


def main():
    conn = conectar_hbase()
    t = conn.table("lecturas")
    (n_a, g_a), m_a = medir(lambda: por_rango(t))
    (n_b, g_b), m_b = medir(lambda: tabla_entera(t))
    guardar_tiempo("P3 HBase scan por rango", m_a)
    guardar_tiempo("P3 HBase tabla entera + filtro", m_b)
    total = contar_filas(conn, "lecturas")
    veces = m_b["mediana_ms"] / m_a["mediana_ms"]
    filas = [[q, n, f"{v:.2f}"] for q, (n, v) in g_a.items()]
    texto = f"""# P3 Un rango - HBase

(a) `scan(row_start=b'{INI.decode()}', row_stop=b'{FIN.decode()}')` -> rango semiabierto [08:00, 10:00).
(b) `scan(filter="{FILTRO.decode()}")` -> el filtro corre en el servidor pero igual hay que leer las {total:,} filas.

**{n_a} lecturas** (por rango) / **{n_b} lecturas** (tabla entera) -> {"mismo resultado" if g_a == g_b else "DISTINTO"}.

{tabla_md(["cuarto de hora", "lecturas", "promedio"], filas)}

## Tiempos

- (a) scan por rango: {fmt_t(m_a)}
- (b) tabla entera + filtro: {fmt_t(m_b)}
- (b) es **{veces:.0f} veces** mas lento.

**Interpretacion**: el scan por rango va directo a la region de CC-001 y lee solo las ~{n_a} filas que pide
(la clave estacion#ts las deja juntas y ordenadas); el filtro tiene que pasar por las {total:,} filas.
Con cinco años en vez de cinco dias (365 veces mas datos) el scan por rango tardaria casi lo mismo, porque
depende de cuantas filas devuelve y no del tamano de la tabla, mientras que el recorrido entero creceria ~365 veces
(de {m_b['mediana_ms'] / 1000:.1f} s a ~{m_b['mediana_ms'] * 365 / 60000:.0f} min en esta maquina). En un cluster el recorrido se
repartiria entre RegionServers, pero seguiria siendo lineal en el tamano de la tabla.
"""
    print(texto)
    escribir("p3_rango.md", texto)


if __name__ == "__main__":
    main()
