"""Constantes y utilidades compartidas por todo el pipeline (parametros de la tarea, conexiones, claves)."""
import itertools
import os
import subprocess
import sys
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import happybase
import redis

from pipeline.generador import ESTACIONES, eventos

RAIZ = Path(__file__).resolve().parent.parent
RESULTADOS = RAIZ / "resultados"
RESULTADOS.mkdir(exist_ok=True)

# ---- parametros de la tarea --------------------------------------------------------------
CHILE = timezone(timedelta(hours=-3))          # todo en hora de Chile (-03:00), nunca UTC
T0 = datetime(2026, 11, 2, 0, 0, tzinfo=CHILE)
SEMILLA = 2026                                 # OJO: la del generador por defecto es 2509
N_HIST = 864_000                               # 5 dias x 172.800
N_VIVO = 7_200                                 # la hora siguiente
PASO_VIVO = 0.025                              # 20x: un evento cada 25 ms

CONTROL = {
    "hist": {"eventos": 864_000, "repetidos": 17_090, "claves": 846_906},
    "vivo": {"eventos": 7_200, "repetidos": 153, "claves": 7_047},
    "total": {"eventos": 871_200, "repetidos": 17_243, "claves": 853_953},
}

TIPO_DE = dict(ESTACIONES)
TIPOS = sorted(set(TIPO_DE.values()))
COMUNA = {"CC": "Concepcion", "TAL": "Talcahuano", "HUA": "Hualpen", "SPP": "San Pedro de la Paz",
          "CHI": "Chiguayante", "PEN": "Penco", "COR": "Coronel"}

# ---- Redis ---------------------------------------------------------------------------------
STREAM = "sensores:t2"
GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"
MAXLEN = 10_000          # ver README: ~4 min de atraso a 20x, ~83 min a ritmo real (2 ev/s)
VENTANA_MS = 10 * 60 * 1000

# ---- HBase ---------------------------------------------------------------------------------
FAM = "d"
SPLITS = {
    # estacion#ts : 16 estaciones con el mismo volumen -> 4 regiones de 4 estaciones cada una
    "lecturas": ["CC-005", "HUA-001", "SPP-002"],
    # tipo#ts#estacion : flujo tiene el doble de estaciones (8 de 16), se parte en 2 por tiempo
    "lecturas_tipo": ["flujo_vehicular", "flujo_vehicular#2026-11-04T12", "ocupacion_estacionamiento"],
}

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))


def historico():
    """Los primeros 864.000 eventos (van directo a HBase)."""
    return itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST)


def vivo():
    """Los 7.200 eventos siguientes (la hora en vivo)."""
    return itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST, N_HIST + N_VIVO)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def ts_ms(iso):
    """'2026-11-07T00:00:00.500-03:00' -> epoch en ms (entero exacto, sin floats)."""
    return (datetime.fromisoformat(iso) - _EPOCH) // timedelta(milliseconds=1)


def ms_a_iso(ms):
    return (_EPOCH + timedelta(milliseconds=int(ms))).astimezone(CHILE).isoformat(timespec="milliseconds")


# ---- row keys (salen de la estacion y de la MEDICION, nunca de la llegada) ----------------
def clave_lecturas(est, ts):
    return f"{est}#{ts}".encode()


def clave_tipo(tipo, ts, est):
    return f"{tipo}#{ts}#{est}".encode()


def salt(est):
    return zlib.crc32(est.encode()) % 4


# ---- conexiones ------------------------------------------------------------------------------
def conectar_redis(decode=True):
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=decode,
                       socket_connect_timeout=5, health_check_interval=10)


def conectar_hbase(espera=180):
    """happybase contra el Thrift. Reintenta: HBase puede estar 'sano' pero aun asignando hbase:meta."""
    fin = time.time() + espera
    while True:
        try:
            c = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120_000)
            c.tables()
            return c
        except Exception as e:  # noqa: BLE001
            if time.time() > fin:
                raise RuntimeError(f"HBase no responde en {HBASE_HOST}:{HBASE_PORT}: {e}") from e
            time.sleep(3)


def contar_filas(conn, tabla):
    """Cuenta filas trayendo solo la primera clave de cada una (Lab 07, 2.2).
    Si la conexion estuvo > 60 s sin uso, el Thrift la cierra (hbase.thrift.server.socket.read.timeout): se reconecta."""
    for intento in range(2):
        try:
            t = conn.table(tabla)
            return sum(1 for _ in t.scan(filter=b"FirstKeyOnlyFilter() AND KeyOnlyFilter()", batch_size=5000))
        except Exception:  # noqa: BLE001
            if intento:
                raise
            conn = conectar_hbase()


def hbase_shell(script, check=True):
    """Corre comandos en el hbase shell dentro del contenedor (para SPLITS, flush, etc.)."""
    r = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "hbase", "shell", "-n"],
                       input=script, text=True, capture_output=True, cwd=RAIZ)
    if check and r.returncode != 0:
        print(r.stdout[-3000:], r.stderr[-3000:], file=sys.stderr)
        raise RuntimeError("fallo hbase shell")
    return r.stdout


def recrear_tablas(conn, nombres_splits, versiones=3):
    """Borra (si existen) y crea tablas pre-divididas. happybase no sabe de SPLITS, por eso el create va por el
    hbase shell, todas en una sola llamada (cada llamada tarda ~10 s en partir la JVM)."""
    existentes = conn.tables()
    for nombre, _ in nombres_splits:
        if nombre.encode() in existentes:
            conn.delete_table(nombre, disable=True)
    lineas = []
    for nombre, splits in nombres_splits:
        lista = ", ".join(f"'{s}'" for s in splits)
        split_txt = f", SPLITS => [{lista}]" if splits else ""
        lineas.append(f"create '{nombre}', {{NAME => '{FAM}', VERSIONS => {versiones}}}{split_txt}")
    hbase_shell("\n".join(lineas) + "\n")


def escribir(nombre_archivo, texto):
    (RESULTADOS / nombre_archivo).write_text(texto, encoding="utf-8")
    print(f"  -> resultados/{nombre_archivo}")


def tabla_md(cabecera, filas):
    out = ["| " + " | ".join(str(c) for c in cabecera) + " |", "|" + "---|" * len(cabecera)]
    out += ["| " + " | ".join(str(c) for c in f) + " |" for f in filas]
    return "\n".join(out)
