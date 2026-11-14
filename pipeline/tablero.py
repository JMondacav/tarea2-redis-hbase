"""Consumidor del grupo `tablero`: mantiene en Redis el estado "de ahora".

  ultimo:<est>    hash  valor, ts (medicion, ms), tipo     -> ultimo POR MEDICION
  ranking:flujo   zset  estacion -> ultimo valor (solo flujo vehicular)
  ventana:<est>   zset  score = ts medicion (ms), miembro "ts:valor", solo flujo, ultimos 10 min de medicion
  tipo:<tipo>     set   estaciones de cada tipo (catalogo)
  tablero:ts_max  la medicion mas reciente vista en el stream (ancla de las ventanas)

Como una lectura tardia no hace retroceder el tablero: todo se hace en un script Lua (atomico) que
compara el ts de medicion que llega con el ts guardado en ultimo:<est> y solo escribe si es >=.
Asi da lo mismo el orden de llegada (y reprocesar un lote no cambia nada: es idempotente).

    python -m pipeline.tablero --nombre tablero-1
"""
import argparse

from pipeline.comun import GRUPO_TABLERO, STREAM, TIPO_DE, VENTANA_MS, conectar_redis, ts_ms
from pipeline.consumidor import Consumidor

LUA = """
-- KEYS: 1 ultimo:<est>  2 ranking:flujo  3 ventana:<est>  4 tipo:<tipo>  5 tablero:ts_max
-- ARGV: 1 est  2 tipo  3 valor  4 ts_ms  5 es_flujo  6 ventana_ms
local ts = tonumber(ARGV[4])
redis.call('SADD', KEYS[4], ARGV[1])
local tmax = tonumber(redis.call('GET', KEYS[5]) or '0')
if ts > tmax then redis.call('SET', KEYS[5], ARGV[4]); tmax = ts end
-- ultimo por medicion: solo avanza. Con el mismo ms gana el ultimo escrito (igual que HBase)
local cur = redis.call('HGET', KEYS[1], 'ts')
local avanza = (not cur) or ts >= tonumber(cur)
if avanza then
  redis.call('HSET', KEYS[1], 'valor', ARGV[3], 'ts', ARGV[4], 'tipo', ARGV[2])
  if ARGV[5] == '1' then redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1]) end
end
if ARGV[5] == '1' then
  local corte = string.format('%d', tmax - tonumber(ARGV[6]))
  if ts > tonumber(corte) then
    redis.call('ZREMRANGEBYSCORE', KEYS[3], ARGV[4], ARGV[4])          -- mismo ms: queda el ultimo escrito
    redis.call('ZADD', KEYS[3], ARGV[4], ARGV[4] .. ':' .. ARGV[3])
  end
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', corte)               -- recorte: (ts_max - 10 min, ts_max]
end
if avanza then return 1 else return 0 end
"""

# al final de cada lote se recortan TODAS las ventanas contra el ancla global, no solo las de las
# estaciones que trajeron eventos (si no, una estacion que no reporta guardaria lecturas viejas)
LUA_RECORTE = """
local tmax = tonumber(redis.call('GET', KEYS[1]) or '0')
local corte = string.format('%d', tmax - tonumber(ARGV[1]))
for i = 2, #KEYS do redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', corte) end
return corte
"""

FLUJO = [e for e, t in TIPO_DE.items() if t == "flujo_vehicular"]


def crear_procesador(r, prefijo=""):
    script = r.register_script(LUA)
    recorte = r.register_script(LUA_RECORTE)
    claves_recorte = [f"{prefijo}tablero:ts_max"] + [f"{prefijo}ventana:{e}" for e in FLUJO]

    def procesar(eventos):
        p = r.pipeline(transaction=False)
        for ev in eventos:
            est, tipo = ev["estacion_id"], ev["tipo_sensor"]
            script(keys=[f"{prefijo}ultimo:{est}", f"{prefijo}ranking:flujo", f"{prefijo}ventana:{est}",
                         f"{prefijo}tipo:{tipo}", f"{prefijo}tablero:ts_max"],
                   args=[est, tipo, ev["valor"], ts_ms(ev["timestamp"]),
                         1 if tipo == "flujo_vehicular" else 0, VENTANA_MS],
                   client=p)
        recorte(keys=claves_recorte, args=[VENTANA_MS], client=p)
        p.execute()

    return procesar


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nombre", default="tablero-1")
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--grupo", default=GRUPO_TABLERO)
    ap.add_argument("--lote", type=int, default=200)
    a = ap.parse_args()
    r = conectar_redis()
    Consumidor(a.grupo, a.nombre, crear_procesador(r), stream=a.stream, lote=a.lote).correr()


if __name__ == "__main__":
    main()
