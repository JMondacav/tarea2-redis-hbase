"""B.1 Carga historica: crea las dos tablas pre-divididas y carga los 864.000 eventos directo a HBase (con batch).

    python -m pipeline.carga_historica
"""
import time

from pipeline.comun import (N_HIST, SPLITS, conectar_hbase, contar_filas, escribir, historico,
                            recrear_tablas, tabla_md)
from pipeline.hbase_io import put_evento

FILAS_POR_RPC = 5_000   # filas por mutateRows; cada fila son 2 celdas


def main():
    conn = conectar_hbase()
    print("Creando tablas lecturas y lecturas_tipo con SPLITS ...")
    recrear_tablas(conn, [("lecturas", SPLITS["lecturas"]), ("lecturas_tipo", SPLITS["lecturas_tipo"])])
    for t in ("lecturas", "lecturas_tipo"):
        print(f"  {t}: {len(conn.table(t).regions())} regiones")

    t_lect, t_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")
    print(f"Cargando {N_HIST:,} eventos historicos (batch de {FILAS_POR_RPC} filas) ...")
    ini = time.perf_counter()
    n = 0
    # batch_size de happybase cuenta celdas, no filas -> 2 celdas por fila
    with t_lect.batch(batch_size=FILAS_POR_RPC * 2) as b_lect, t_tipo.batch(batch_size=FILAS_POR_RPC * 2) as b_tipo:
        for ev in historico():
            put_evento(b_lect, b_tipo, ev)
            n += 1
            if n % 100_000 == 0:
                s = time.perf_counter() - ini
                print(f"  {n:>7,} eventos  {s:6.1f} s  ({n / s:,.0f} ev/s)", flush=True)
    seg = time.perf_counter() - ini

    print("Contando filas ...")
    ini_c = time.perf_counter()
    filas = {t: contar_filas(conn, t) for t in ("lecturas", "lecturas_tipo")}
    seg_c = time.perf_counter() - ini_c

    puts = 2 * n
    texto = f"""# B.1 Carga historica

- Eventos leidos del generador: **{n:,}**
- Puts enviados: **{puts:,}** ({n:,} por tabla), en batch de {FILAS_POR_RPC:,} filas por RPC
- Tiempo total de carga: **{seg:.1f} s** -> **{n / seg:,.0f} eventos/s** ({puts / seg:,.0f} puts/s)
- Conteo con `FirstKeyOnlyFilter() AND KeyOnlyFilter()`: {seg_c:.1f} s

{tabla_md(["tabla", "filas", "esperado (claves distintas historico)", "ok"],
          [[t, f"{c:,}", "846,906", "si" if c == 846_906 else "NO"] for t, c in filas.items()])}

Las {n - 846_906:,} filas que "faltan" son los 17.090 repetidos tal cual + 4 choques de clave
(misma estacion y mismo ms con distinto valor): caen en la misma fila, por eso no suman.
"""
    print(texto)
    escribir("b1_carga_historica.md", texto)


if __name__ == "__main__":
    main()
