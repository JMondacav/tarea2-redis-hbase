"""Medicion comun: 6 corridas, la primera aparte (calienta caches), se informa la mediana de las otras 5."""
import json
import statistics
import time
import urllib.request

from pipeline.comun import RESULTADOS


def medir(f, corridas=6):
    tiempos, res = [], None
    for _ in range(corridas):
        t = time.perf_counter()
        res = f()
        tiempos.append(time.perf_counter() - t)
    return res, {"primera_ms": tiempos[0] * 1000, "mediana_ms": statistics.median(tiempos[1:]) * 1000,
                 "corridas_ms": [round(x * 1000, 2) for x in tiempos]}


def fmt_t(m):
    return (f"**mediana {m['mediana_ms']:.2f} ms** (5 corridas; la primera, aparte: {m['primera_ms']:.2f} ms) - "
            f"todas: {m['corridas_ms']}")


def guardar_tiempo(clave, m):
    """Junta los tiempos de todas las preguntas en resultados/tiempos.json (para la tabla del informe)."""
    ruta = RESULTADOS / "tiempos.json"
    datos = json.loads(ruta.read_text(encoding="utf-8")) if ruta.exists() else {}
    datos[clave] = {"mediana_ms": round(m["mediana_ms"], 3), "primera_ms": round(m["primera_ms"], 3)}
    ruta.write_text(json.dumps(datos, indent=2, ensure_ascii=False), encoding="utf-8")


def jmx_regiones():
    """Metricas por region del RegionServer (Lab 07, 3.2). En standalone todo vive en la JVM del master (16010)."""
    url = "http://localhost:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)["beans"][0]
