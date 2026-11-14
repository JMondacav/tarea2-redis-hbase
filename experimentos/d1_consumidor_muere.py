"""D.1 El consumidor muere a mitad de lote.

Sobre un stream y tablas propias del experimento (para no tocar el pipeline principal):
  1. se producen los 7.200 eventos de la hora en vivo en sensores:t2:d1
  2. ingesta-1 hace os._exit(1) en el lote 5, DESPUES del put en HBase y ANTES del XACK
  3. se mira XPENDING: cuantas entradas y de quien
  4. ingesta-2 las rescata con XAUTOCLAIM y termina el stream
  5. HBase queda con exactamente 7.047 filas (numero de control de la hora en vivo) y se miran las versiones
  6. contraprueba: lo mismo pero con el XACK ANTES del put -> se pierden eventos

    python -m experimentos.d1_consumidor_muere
"""
import subprocess
import sys
import time
from collections import Counter

from pipeline.comun import (CONTROL, RAIZ, SPLITS, clave_lecturas, conectar_hbase, conectar_redis, contar_filas,
                            escribir, recrear_tablas, tabla_md)
from pipeline.productor import producir

LOTE, MUERE_EN = 200, 5


def preparar(r, conn, stream, pref):
    recrear_tablas(conn, [(f"{pref}lecturas", SPLITS["lecturas"]), (f"{pref}lecturas_tipo", SPLITS["lecturas_tipo"])])
    r.delete(stream, f"{stream}:fin", f"{stream}:producidos", f"stats:{stream}:ingesta:procesadas")
    r.xgroup_create(stream, "ingesta", id="0", mkstream=True)
    producir(stream, paso=0)    # sin pausa: el momento de la muerte depende del lote, no del ritmo


def ingesta(stream, pref, nombre, *extra):
    p = subprocess.run([sys.executable, "-m", "pipeline.ingesta", "--nombre", nombre, "--stream", stream,
                        "--prefijo-tablas", pref, "--lote", str(LOTE), "--pausa", "0", *extra],
                       cwd=RAIZ, capture_output=True, text=True)
    print(p.stdout, p.stderr)
    return p.returncode, p.stdout.strip()


def pendientes(r, stream):
    s = r.xpending(stream, "ingesta")
    det = r.xpending_range(stream, "ingesta", min="-", max="+", count=3)
    return s, det


def main():
    r, conn = conectar_redis(), conectar_hbase()
    esperado = CONTROL["vivo"]["claves"]

    # ------------------------------ caso correcto: put -> XACK ------------------------------
    stream, pref = "sensores:t2:d1", "d1_"
    print("== D.1: put y despues XACK ==")
    preparar(r, conn, stream, pref)
    rc1, out1 = ingesta(stream, pref, "ingesta-1", "--morir-en-lote", str(MUERE_EN))
    resumen, detalle = pendientes(r, stream)
    filas_muerto = contar_filas(conn, f"{pref}lecturas")
    procesadas_1 = int(r.get(f"stats:{stream}:ingesta:procesadas") or 0)

    time.sleep(2.5)   # que las pendientes lleven > min-idle sin confirmar (ingesta-1 esta muerto)
    rc2, out2 = ingesta(stream, pref, "ingesta-2", "--min-idle-ms", "2000")
    resumen_fin = r.xpending(stream, "ingesta")
    filas_fin = {t: contar_filas(conn, f"{pref}{t}") for t in ("lecturas", "lecturas_tipo")}

    # versiones: un evento del lote reprocesado vs uno de un lote normal (que no se repita en la hora)
    entradas = r.xrange(stream, "-", "+")
    cuenta = Counter((c["estacion_id"], c["timestamp"]) for _, c in entradas)
    ini_lote = (MUERE_EN - 1) * LOTE

    def unico(desde, hasta):
        for _, c in entradas[desde:hasta]:
            if cuenta[(c["estacion_id"], c["timestamp"])] == 1:
                return clave_lecturas(c["estacion_id"], c["timestamp"])

    t = conn.table(f"{pref}lecturas")
    filas_v = []
    for desc, k in (("lote 5 (reprocesado)", unico(ini_lote, ini_lote + LOTE)), ("lote 1 (normal)", unico(0, LOTE))):
        celdas = t.cells(k, b"d:valor", versions=3, include_timestamp=True)
        filas_v.append([desc, k.decode(), len(celdas),
                        "; ".join(f"valor={v.decode()} @ {ts}" for v, ts in celdas)])

    # ------------------------------ contraprueba: XACK -> put ------------------------------
    stream_x, pref_x = "sensores:t2:d1x", "d1x_"
    print("== D.1 contraprueba: XACK antes del put ==")
    preparar(r, conn, stream_x, pref_x)
    ingesta(stream_x, pref_x, "ingesta-1", "--morir-en-lote", str(MUERE_EN), "--ack-antes")
    resumen_x, _ = pendientes(r, stream_x)
    time.sleep(2.5)
    ingesta(stream_x, pref_x, "ingesta-2", "--min-idle-ms", "2000")
    filas_x = contar_filas(conn, f"{pref_x}lecturas")
    perdidas = {(c["estacion_id"], c["timestamp"]) for _, c in entradas[ini_lote:ini_lote + LOTE]} - \
               {(c["estacion_id"], c["timestamp"]) for _, c in entradas[:ini_lote] + entradas[ini_lote + LOTE:]}

    det_txt = tabla_md(["id", "consumidor", "ms sin confirmar", "veces entregada"],
                       [[d["message_id"], d["consumer"], d["time_since_delivered"], d["times_delivered"]]
                        for d in detalle])
    rescate = [l for l in out2.splitlines() if "XAUTOCLAIM" in l]
    texto = f"""# D.1 El consumidor muere a mitad de lote

Stream propio `{stream}` con los 7.200 eventos de la hora en vivo, tablas `{pref}lecturas` y `{pref}lecturas_tipo`
(recien creadas, VERSIONS => 3). `ingesta-1` lee de a {LOTE} y en el lote {MUERE_EN} hace `os._exit(1)` **despues del
put y antes del XACK** (codigo de salida: {rc1}).

## 1. Que quedo en XPENDING

- Entradas confirmadas antes de morir: {procesadas_1:,} (lotes 1 a {MUERE_EN - 1})
- `XPENDING {stream} ingesta` -> **{resumen['pending']} pendientes**, de `{resumen['min']}` a `{resumen['max']}`,
  consumidores: {[(c['name'], c['pending']) for c in resumen['consumers']]}
- Filas en {pref}lecturas en ese momento: {filas_muerto:,} (el put del lote {MUERE_EN} ya estaba hecho)

Primeras pendientes (`XPENDING ... - + 3`):

{det_txt}

## 2. Otro consumidor las recupera

`ingesta-2` arranca con `XAUTOCLAIM {stream} ingesta ingesta-2 2000 0-0 COUNT {LOTE}` (toma lo que lleva > 2 s sin
confirmar) y despues sigue leyendo con `XREADGROUP ... >` hasta el final:

```
{chr(10).join(rescate) or out2[-500:]}
```

## 3. HBase al final

- `XPENDING` al final: **{resumen_fin['pending']}**
- Filas: {", ".join(f"{t} = **{n:,}**" for t, n in filas_fin.items())} (control hora en vivo: {esperado:,}) ->
  {"calza exacto, ni faltan ni sobran" if all(n == esperado for n in filas_fin.values()) else "NO CALZA"}

## 4. Por que no duplica, y que quedo en la celda

La clave es `estacion#ts_medicion`: el reproceso escribe exactamente las mismas filas, entonces no aparecen filas
nuevas, solo una version mas de cada celda. `cells(fila, 'd:valor', versions=3, include_timestamp=True)`:

{tabla_md(["evento", "fila", "versiones", "contenido (valor @ timestamp de escritura en ms)"], filas_v)}

La fila del lote reprocesado tiene 2 versiones con el **mismo valor** (el put de ingesta-1 y el de ingesta-2, con
distinto timestamp de escritura); una lectura normal devuelve solo la mas nueva, asi que para quien consulta es lo
mismo. Con VERSIONS => 3 las versiones extra no crecen sin limite, y en la compactacion se descartan las que sobran.

## Contraprueba: XACK antes del put

Igual, pero `ingesta-1` hace el XACK primero y muere antes del put (stream `{stream_x}`, tablas `{pref_x}*`).
- XPENDING despues de morir: **{resumen_x['pending']}** -> no hay nada que rescatar, Redis cree que ya se proceso.
- Filas al final en {pref_x}lecturas: **{filas_x:,}** de {esperado:,} -> **se perdieron {esperado - filas_x}** claves
  (las {len(perdidas)} claves del lote {MUERE_EN} que no se repiten en otro lote).
Por eso el XACK va despues del put.
"""
    print(texto)
    escribir("d1_consumidor_muere.md", texto)


if __name__ == "__main__":
    main()
