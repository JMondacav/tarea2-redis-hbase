"""D.2 Redis se reinicia a mitad de camino, con dos configuraciones:
  aof  -> la elegida (docker-compose.yml): AOF everysec en volumen con nombre
  sin  -> la contraria (docker-compose.sin-persistencia.yml encima): sin AOF, sin RDB, sin volumen

En cada una: stream propio sensores:t2:d2, tablero + ingesta corriendo, productor a 20x. A los 2.000 eventos se
mata la ingesta (simula que HBase esta lento / la ingesta cayo: se acumula atraso). A los ~3.000 eventos:
`docker compose restart redis`. Se fotografia que habia antes y que sobrevivio despues; despues se vuelve a
levantar la ingesta, se termina la hora y se cuenta que llego a HBase. Al final se deja Redis como estaba (AOF).

    python -m experimentos.d2_reinicio_redis
"""
import subprocess
import sys
import threading
import time

import redis

from pipeline.comun import (CONTROL, PASO_VIVO, RAIZ, SPLITS, conectar_hbase, conectar_redis, contar_filas, escribir,
                            recrear_tablas, tabla_md)
from pipeline.productor import producir
from pipeline.verificar import tablero_esperado

STREAM = "sensores:t2:d2"
ARCHIVOS = {"aof": ["-f", "docker-compose.yml"],
            "sin": ["-f", "docker-compose.yml", "-f", "docker-compose.sin-persistencia.yml"]}
DESC = {"aof": "AOF everysec + volumen (la elegida)", "sin": "sin persistencia (la contraria)"}
MATAR_INGESTA, REINICIAR = 2_000, 3_000


def compose(cfg, *cmd):
    subprocess.run(["docker", "compose", *ARCHIVOS[cfg], *cmd], cwd=RAIZ, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def esperar_redis():
    while True:
        try:
            r = conectar_redis()
            r.ping()
            r.dbsize()
            return r
        except (redis.ConnectionError, redis.BusyLoadingError, redis.TimeoutError):
            time.sleep(0.2)


def lanzar(modulo, *args):
    return subprocess.Popen([sys.executable, "-m", modulo, "--stream", STREAM, *args], cwd=RAIZ,
                            stdout=subprocess.DEVNULL)


def foto(r):
    f = {"existe el stream": bool(r.exists(STREAM))}
    f["XLEN stream"] = r.xlen(STREAM) if f["existe el stream"] else 0
    grupos = {g["name"]: g for g in r.xinfo_groups(STREAM)} if f["existe el stream"] else {}
    for g in ("ingesta", "tablero"):
        gi = grupos.get(g)
        f[f"grupo {g}"] = (f"existe - entries-read {gi['entries-read']}, pendientes {gi['pending']}, "
                           f"sin leer (lag) {gi['lag']}" if gi else "NO existe")
    f["claves ultimo:*"] = len(list(r.scan_iter("ultimo:*")))
    f["lecturas en ventana:*"] = sum(r.zcard(k) for k in r.scan_iter("ventana:*"))
    f["catalogo tipo:* (estaciones)"] = sum(r.scard(k) for k in r.scan_iter("tipo:*"))
    f["contador producidos"] = int(r.get(f"{STREAM}:producidos") or 0)
    return f


def experimento(cfg):
    print(f"== D.2 config {cfg}: {DESC[cfg]} ==", flush=True)
    compose(cfg, "up", "-d", "--wait", "redis")
    r = esperar_redis()
    r.flushall()
    persistencia = {k: v for k, v in r.config_get("appendonly").items()}
    persistencia.update(r.config_get("appendfsync"))
    persistencia.update(r.config_get("save"))
    conn = conectar_hbase()
    pref = f"d2{cfg}_"
    recrear_tablas(conn, [(f"{pref}lecturas", SPLITS["lecturas"]), (f"{pref}lecturas_tipo", SPLITS["lecturas_tipo"])])
    for g in ("ingesta", "tablero"):
        r.xgroup_create(STREAM, g, id="0", mkstream=True)

    tablero = lanzar("pipeline.tablero", "--nombre", "tablero-1")
    ingesta = lanzar("pipeline.ingesta", "--nombre", "ingesta-1", "--prefijo-tablas", pref)
    n = {"v": 0}
    seguir = threading.Event()
    seguir.set()
    hilo = threading.Thread(target=lambda: producir(STREAM, PASO_VIVO, lambda k: n.__setitem__("v", k),
                                                    seguir=seguir))
    hilo.start()

    while n["v"] < MATAR_INGESTA:
        time.sleep(0.05)
    ingesta.kill()
    print(f"  {n['v']} producidos: ingesta detenida (se acumula atraso)", flush=True)
    while n["v"] < REINICIAR:
        time.sleep(0.01)
    seguir.clear()          # el productor se detiene un momento para que la foto antes/despues sea comparable
    time.sleep(0.5)         # el tablero alcanza a confirmar lo ultimo
    antes = foto(r)
    filas_antes = contar_filas(conn, f"{pref}lecturas")
    producidos_al_reiniciar = n["v"]
    print(f"  {producidos_al_reiniciar} producidos: docker compose restart redis", flush=True)
    t0 = time.perf_counter()
    compose(cfg, "restart", "redis")
    r = esperar_redis()
    caida = time.perf_counter() - t0
    despues = foto(r)
    seguir.set()            # sigue la hora en vivo (si Redis perdio todo, sobre un stream nuevo)

    ingesta = lanzar("pipeline.ingesta", "--nombre", "ingesta-1", "--prefijo-tablas", pref)
    hilo.join()
    for p in (ingesta, tablero):
        try:
            p.wait(timeout=300)
        except subprocess.TimeoutExpired:
            p.kill()
    final = foto(r)
    filas_fin = contar_filas(conn, f"{pref}lecturas")

    ultimo, ventanas, _ = tablero_esperado()
    ok_ult = sum(1 for e, (v, ts, _) in ultimo.items()
                 if (h := r.hgetall(f"ultimo:{e}")) and float(h["valor"]) == v and int(h["ts"]) == ts)
    ok_ven = sum(1 for e, s in ventanas.items() if set(r.zrange(f"ventana:{e}", 0, -1)) == s)
    return {"cfg": cfg, "persistencia": persistencia, "antes": antes, "despues": despues, "final": final,
            "filas_antes": filas_antes, "filas_fin": filas_fin, "producidos": producidos_al_reiniciar,
            "caida_s": caida, "ok_ult": ok_ult, "ok_ven": ok_ven}


def main():
    res = [experimento("aof"), experimento("sin")]
    print("Dejando Redis con la configuracion elegida (AOF) ...", flush=True)
    compose("aof", "up", "-d", "--wait", "redis")

    esperado = CONTROL["vivo"]["claves"]
    claves = list(res[0]["antes"].keys())
    filas = [[k, res[0]["antes"][k], res[0]["despues"][k], res[1]["antes"][k], res[1]["despues"][k]] for k in claves]
    filas.append(["filas en HBase (<pref>lecturas)", f"{res[0]['filas_antes']:,}", "(HBase no se reinicia)",
                  f"{res[1]['filas_antes']:,}", "(HBase no se reinicia)"])
    fin = [[f"{x['cfg']}: {DESC[x['cfg']]}", f"{x['filas_fin']:,} / {esperado:,}", esperado - x["filas_fin"],
            f"{x['ok_ult']}/16", f"{x['ok_ven']}/8", f"{x['caida_s']:.1f} s"] for x in res]
    texto = f"""# D.2 Redis se reinicia

Stream `{STREAM}`, productor a 20x, tablero e ingesta corriendo. A los {MATAR_INGESTA:,} eventos se detiene la ingesta
(como si HBase estuviera lento: el atraso crece) y a los {REINICIAR:,} se hace `docker compose restart redis`
(el productor se pausa durante el reinicio para que "antes" y "despues" se puedan comparar, y despues sigue).

Configuraciones (leidas con CONFIG GET en cada corrida):
- **aof**: {res[0]['persistencia']}
- **sin**: {res[1]['persistencia']}

## Que habia antes y que sobrevivio

{tabla_md(["", "aof: antes", "aof: despues", "sin: antes", "sin: despues"], filas)}

## Al final de la hora (se relanza la ingesta, el productor termina)

{tabla_md(["config", "filas en HBase / control", "claves perdidas", "ultimo:* correctos", "ventanas correctas", "reinicio tardo"], fin)}

**Lectura**: con AOF sobrevive todo: el stream, los dos grupos con su posicion y sus pendientes, y el tablero; la
ingesta retoma donde quedo y HBase termina con las {esperado:,} filas. Sin persistencia Redis parte vacio: se pierde el
stream, los grupos (el consumidor los recrea desde 0 sobre un stream nuevo) y todo el tablero. El tablero se rearma
solo con los eventos que siguen llegando (al final de la hora vuelve a estar correcto porque "ahora" son los ultimos
10 min), pero lo que estaba en el stream y todavia no llegaba a HBase (el atraso de la ingesta al momento del
reinicio) **se pierde para siempre de la historia**: {esperado - res[1]['filas_fin']} claves.
"""
    print(texto)
    escribir("d2_reinicio_redis.md", texto)


if __name__ == "__main__":
    main()
