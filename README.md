# Tarea 2 · Redis + HBase, del stream a la historia

Procesamiento Masivo de Datos (ELE051-B / EIN102B) · USM 2026-2 · Paralelo 701
Integrantes: Andres Perez, Vicente Hermosilla, Joaquin Mondaca

Los eventos de la hora en vivo entran a un stream de Redis (`sensores:t2`). Un grupo de consumidores (`ingesta`) los baja a
HBase sin perder ni duplicar nada, otro grupo (`tablero`) mantiene el estado "de ahora" en Redis, y las preguntas se
responden desde el motor que corresponde, midiendo cuánto cuesta ponerlas en el equivocado. La historia (5 días) va
directo a HBase.

## 1. Reproducir desde cero

Necesita Docker con compose v2 (~4 GB de RAM para Docker, ~3 GB de disco), Python >= 3.10 y bash (en Windows sirve Git Bash).

```bash
docker compose up -d --wait   # Redis 7.4 + HBase 2.5 (la 1a vez construye la imagen de HBase, ~340 MB de descarga)
./run.sh                      # todo lo demás: venv, carga, experimentos D, hora en vivo, verificación, preguntas
```

`./run.sh` también hace el `docker compose up -d --wait` al principio, así que basta con ese comando. No hay rutas que
editar. Crea `.venv` e instala `requirements.txt` solo.

| paso (`./run.sh`) | qué hace | tiempo medido |
|---|---|---|
| build de la imagen HBase (1 sola vez por máquina) | descarga HBase 2.5.x de Apache | ~5 min (depende de la red) |
| `docker compose up -d --wait` | levanta y espera los healthchecks | ~20-40 s |
| B.1 `pipeline.carga_historica` | crea `lecturas` y `lecturas_tipo` con SPLITS y carga 864.000 eventos (~24 s de carga, ~36 mil eventos/s) | 39 s |
| D.1 `experimentos.d1_consumidor_muere` | mata a la ingesta entre put y XACK, recupera, contraprueba | 34 s |
| D.2 `experimentos.d2_reinicio_redis` | reinicia Redis con AOF y sin persistencia (2 horas en vivo a 20x) | 387 s |
| B.2 `pipeline.vivo` | productor a 20x + ingesta + tablero (la hora en ~3 min) | 182 s |
| B.3 `pipeline.verificar` | compara todo con los números de control | 17 s |
| C `preguntas.correr_todas` | P1..P6 con mediana de 5 corridas | 50-60 s |
| **total** `./run.sh` desde cero (volúmenes y venv borrados, imagen ya construida) | | **756 s ≈ 12,6 min** |

Por partes: `./run.sh pipeline` (B.1 + B.2 + B.3), `./run.sh fallas` (D), `./run.sh preguntas`, `./run.sh verificar`.
Todo deja su salida en [`resultados/`](resultados/) (un `.md` por paso, con los tiempos).

Para revisar a mano: `docker compose exec redis redis-cli HGETALL ultimo:CC-001`, `ZREVRANGE ranking:flujo 0 -1 WITHSCORES`,
`ZRANGE ventana:CC-001 0 -1 WITHSCORES`, `SMEMBERS tipo:calidad_aire`; y `docker compose exec hbase hbase shell`.
Web de HBase: http://localhost:16010.

### Los datos

No se versionan: salen de `pipeline/generador.py` (el del Lab 06, `eventos()` sin tocar) con `eventos(T0, semilla=2026)`,
`T0 = 2026-11-02T00:00:00-03:00`. Histórico = primeros 864.000 eventos, en vivo = los 7.200 siguientes, a 20x (25 ms).
`verificar.py` recalcula los números de control desde el generador y dan exacto: 864.000 / 17.090 / 846.906,
7.200 / 153 / 7.047 y 871.200 / 17.243 / 853.953.

## 2. Estructura

```
docker-compose.yml                    redis (AOF) + hbase (Dockerfile del Lab 07), healthchecks, volúmenes con nombre
docker-compose.sin-persistencia.yml   la configuración contraria de Redis (solo para D.2)
hbase/Dockerfile                      HBase 2.5 standalone + Thrift
run.sh                                todo de punta a punta
pipeline/
  generador.py      generador del Lab 06
  comun.py          parámetros de la tarea, conexiones, row keys, SPLITS
  carga_historica.py  B.1
  productor.py      B.2 XADD a 20x con MAXLEN ~
  consumidor.py     base de los consumer groups (XREADGROUP -> procesar -> XACK, XAUTOCLAIM, reconexión)
  ingesta.py        grupo ingesta -> HBase (las dos tablas)
  tablero.py        grupo tablero -> ultimo:*, ranking:flujo, ventana:*, tipo:* (script Lua)
  vivo.py           orquesta la hora en vivo
  verificar.py      B.3
preguntas/p1..p6    Parte C (y correr_todas.py)
experimentos/       Parte D (d1_consumidor_muere.py, d2_reinicio_redis.py)
resultados/         salidas de cada paso (se regeneran)
```

## 3. Decisiones

### 3.1 Row keys (HBase)

Familia única `d`, `VERSIONS => 3`. Los timestamps van en ISO con milisegundos y siempre `-03:00`
(`2026-11-04T08:00:00.000-03:00`): con el mismo formato y el mismo offset, el orden de bytes es el orden de tiempo. Nada
en UTC.

**`lecturas`** — clave `estacion_id#timestamp_medicion`, ej. `CC-001#2026-11-04T08:00:00.000-03:00`. Columnas `d:valor`, `d:tipo`.
- Rápida: "lecturas de una estación en un rango de tiempo" (un scan por rango, P3: 6-22 ms), "lo último de una estación"
  (scan inverso con `limit=1`, P1) e "ir hacia atrás en la historia de una estación" (P6).
- Lenta: "todas las estaciones en un intervalo" (16 rangos, P5), "por tipo" (hay que pedir N estaciones, P4a) y
  cualquier cosa por valor (no hay índice: recorrer la tabla entera, P3b 0,5-2,3 s, ~100 veces más lento).
- Idempotente: la clave sale de la estación y del momento de **medición**, nunca de la llegada. Un evento repetido cae
  en la misma fila (no suma filas, solo una versión más con el mismo valor) y uno tardío cae en su lugar en el tiempo.
  Los 4 pares de choque (misma estación y mismo ms, distinto valor) quedan con el último valor escrito.
  Los ids de estación tienen largo distinto (`CC-001`, `TAL-001`) pero el `#` y que ningún id sea prefijo de otro hacen
  que los rangos no se mezclen.

**`lecturas_tipo`** — clave `tipo_sensor#timestamp_medicion#estacion_id`, ej. `calidad_aire#2026-11-05T07:00:00.000-03:00#CC-004`.
Columnas `d:valor`, `d:estacion`.
- Rápida: "un tipo de sensor en un rango de tiempo" en un solo scan (P4b).
- Lenta: "una estación" (hay que recorrer el tipo entero y filtrar).
- Idempotente por lo mismo; la estación va al final para que dos estaciones del mismo tipo que miden en el mismo ms no choquen.

### 3.2 Regiones (SPLITS)

- `lecturas`: `SPLITS => ['CC-005', 'HUA-001', 'SPP-002']` → 4 regiones de 4 estaciones cada una. El generador elige la
  estación al azar con la misma probabilidad, así que cada estación tiene ~1/16 de los datos y cada región ~1/4
  (~212 mil filas). Los cortes van justo en el inicio de una estación, así el rango de una estación nunca cruza
  regiones. Medido con JMX después de la carga: 214.925 / 215.201 / 215.500 / 215.545 escrituras por región. En vivo los
  eventos llegan de estaciones al azar, entonces las 4 regiones reciben escrituras parejas (P5: 25 % cada una).
  Son 4 y no 16 porque es un solo RegionServer: más regiones solo agregan MemStores y archivos sin paralelismo real.
- `lecturas_tipo`: `SPLITS => ['flujo_vehicular', 'flujo_vehicular#2026-11-04T12', 'ocupacion_estacionamiento']` →
  calidad del aire (4 estaciones, 25 %), flujo primera mitad (25 %), flujo segunda mitad (25 %), estacionamiento (25 %).
  Flujo tiene 8 de las 16 estaciones, por eso se corta en dos por la mitad de los 5 días (04-nov 12:00). Ojo: como
  dentro de cada tipo la clave sigue el tiempo, lo que llega en vivo siempre va a la última región de cada tipo.

### 3.3 MAXLEN del stream: `MAXLEN ~ 10000`

- A 20x (40 ev/s) son ~250 s de atraso tolerable; a ritmo real (2 ev/s), ~83 min. Cabe la hora en vivo entera (7.200).
- En la corrida el atraso máximo observado de `ingesta` fue de ~100 entradas y el de `tablero` de 1-3
  ([`resultados/b2_vivo.md`](resultados/b2_vivo.md)), o sea con harto margen. El atraso grande aparece si la ingesta se
  cae o HBase se pone lento (en D.2 lo forzamos: ~1.000 entradas en 25 s).
- Memoria: el stream con las 7.200 entradas ocupa 642 KB (`MEMORY USAGE sensores:t2 SAMPLES 0`, ~90 bytes por entrada),
  así que 10.000 son ~0,9 MB: barato.
- `~` (aproximado) deja que Redis recorte nodos enteros (es más barato), el largo puede pasarse un poco de 10.000.
- **Una entrada recortada que nadie leyó se pierde para ese grupo sin ningún error**: `XREADGROUP >` nunca la entrega
  y nunca llega a HBase. Si ya estaba entregada pero pendiente (en el PEL) y la recortan, `XPENDING` la sigue
  mostrando, pero sus datos ya no existen: `XAUTOCLAIM` la devuelve en la lista de borradas y la saca del PEL
  (nuestro consumidor lo avisa en el log). Por eso MAXLEN tiene que ser bastante más grande que el peor atraso esperado.

### 3.4 Persistencia de Redis: AOF con `appendfsync everysec`, sin RDB, en volumen con nombre

En [`docker-compose.yml`](docker-compose.yml): `redis-server --appendonly yes --appendfsync everysec --save ""`, volumen `redis-data:/data`.

- Lo que hay en Redis y todavía no en HBase (el stream + los pendientes de `ingesta`) es la **única copia** de esos
  eventos. Si Redis parte vacío se pierde historia para siempre (D.2: sin persistencia se perdieron 991 claves).
- `everysec`: en una caída dura (kill -9, corte de luz) se pierde a lo más ~1 s de escrituras (2 eventos a ritmo real,
  40 a 20x). En un `restart` normal no se pierde nada porque Redis hace fsync al apagarse (D.2 lo muestra).
- `always` haría fsync en cada XADD: más lento y no hace falta, porque un productor real puede reenviar lo último que
  no le confirmaron. `no` deja el fsync al sistema operativo (~30 s de pérdida posible).
- RDB solo: se pierde todo lo que pasó desde el último snapshot (minutos), malo para un stream. Lo dejamos apagado para
  que la decisión sea una sola y explícita.
- El tablero (`ultimo:*`, ventanas) no necesita persistencia: se rearma solo con los eventos que siguen llegando.

### 3.5 "Último" por medición y ventanas (tablero)

- Cada evento se aplica con un script Lua (atómico): compara el `ts` de medición que llega con el `ts` guardado en
  `ultimo:<est>` y solo escribe si es `>=`. Una lectura tardía no hace retroceder el tablero, y reprocesar un lote no
  cambia nada. `ranking:flujo` solo se actualiza cuando `ultimo` avanza.
- `ventana:<est>`: sorted set con score = ts de medición (ms) y miembro `"<ts>:<valor>"` (un repetido es el mismo
  miembro). `tablero:ts_max` guarda la medición más reciente vista en el stream (el ancla). La ventana es
  (ancla − 10 min, ancla]: una tardía más vieja que eso no entra, y al final de cada lote se recortan las 8 ventanas
  con `ZREMRANGEBYSCORE -inf <ancla − 10 min>`.

### 3.6 Consumidores

- `XREADGROUP` → put en HBase (las 2 tablas, en batch) → `XACK`. Si algo falla antes del XACK, el lote queda pendiente.
- Al arrancar (y al reconectar) cada consumidor relee sus propios pendientes con `XREADGROUP ... 0`, y cada 2 s hace
  `XAUTOCLAIM` de lo que otro consumidor dejó más de 10 s sin confirmar (el consumidor muerto de D.1).
- Si Redis no está o el grupo desapareció (`NOGROUP`), reintenta y recrea el grupo desde `0`.
- `ingesta` lee de a 200 como máximo; si el lote vino chico espera 250 ms para que el siguiente junte más (menos RPCs a
  HBase a cambio de ≤ 250 ms de latencia, que a la historia no le importa). `tablero` no espera.

## 4. Lo que no funcionó (y cómo quedó)

1. **Healthcheck de HBase**: la primera versión buscaba `"numRegionServers" : 1` y el JSON real no tiene espacio antes
   de los dos puntos, el contenedor quedaba `unhealthy` y `--wait` fallaba. Ahora es una regex.
2. **happybase no sabe crear tablas con SPLITS** (el `createTable` de Thrift 1 no recibe cortes). Las tablas se crean con
   `hbase shell` vía `docker compose exec`, todas en una sola llamada porque cada una tarda ~10 s en partir la JVM.
3. **Timestamp de celda = medición**: queríamos que el reproceso ni siquiera agregara versiones, pero en happybase el
   timestamp es uno por batch, no por put. Quedó con el timestamp de escritura y `VERSIONS => 3` (D.1 muestra las 2 versiones).
4. **El Thrift cierra conexiones ociosas a los 60 s** (`hbase.thrift.server.socket.read.timeout`): D.2 se caía al contar
   filas con una conexión vieja. Ahora el conteo y la ingesta reconectan.
5. **Lotes de 1**: la ingesta era más rápida que el productor y procesaba ~1 evento por lote (7.105 lotes para 7.200
   eventos). Con la espera de 250 ms quedan ~10 por lote (~690 lotes).
6. **Ventanas**: al principio solo se recortaba la ventana de la estación que mandaba el evento, y una estación que deja
   de reportar se quedaba con lecturas viejas. Ahora se recortan las 8 en cada lote.
7. **P6**: la primera idea (último valor contra la historia) daba respuestas triviales ("hace 10 segundos") porque una
   lectura sola es muy ruidosa. Se cambió al promedio de la ventana de 10 min.
8. **HBase 2.5.15 ya no está en el CDN de Apache** (404, solo queda 2.5.16). El Dockerfile prueba 2.5.16, después la
   última 2.5.x que liste el CDN y al final archive.apache.org.
9. En Windows, `python3` puede ser el atajo de la Microsoft Store que no hace nada; `run.sh` prueba `python3` y `python`.

## 5. Resultados

- B: [carga histórica](resultados/b1_carga_historica.md) · [hora en vivo](resultados/b2_vivo.md) · [verificación](resultados/b3_verificacion.md)
- C: [P1](resultados/p1_ahora.md) · [P2](resultados/p2_ventana.md) · [P3](resultados/p3_rango.md) · [P4](resultados/p4_dos_patrones.md) · [P5](resultados/p5_hotspotting.md) · [P6](resultados/p6_propia.md) · [tiempos](resultados/tiempos.md)
- D: [D.1](resultados/d1_consumidor_muere.md) · [D.2](resultados/d2_reinicio_redis.md)
- Informe: se entrega aparte por Aula (PDF).

Medido en: Intel Core 7 240H (10 núcleos / 16 hilos), 16 GB RAM, SSD NVMe, Windows 11 + Docker Desktop (WSL2, 8 GB para Docker).
