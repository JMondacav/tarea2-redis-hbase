# D.2 Redis se reinicia

Stream `sensores:t2:d2`, productor a 20x, tablero e ingesta corriendo. A los 2,000 eventos se detiene la ingesta
(como si HBase estuviera lento: el atraso crece) y a los 3,000 se hace `docker compose restart redis`
(el productor se pausa durante el reinicio para que "antes" y "despues" se puedan comparar, y despues sigue).

Configuraciones (leidas con CONFIG GET en cada corrida):
- **aof**: {'appendonly': 'yes', 'appendfsync': 'everysec', 'save': ''}
- **sin**: {'appendonly': 'no', 'appendfsync': 'everysec', 'save': ''}

## Que habia antes y que sobrevivio

|  | aof: antes | aof: despues | sin: antes | sin: despues |
|---|---|---|---|---|
| existe el stream | True | True | True | False |
| XLEN stream | 3001 | 3001 | 3001 | 0 |
| grupo ingesta | existe - entries-read 1996, pendientes 0, sin leer (lag) 1005 | existe - entries-read 1996, pendientes 0, sin leer (lag) 1005 | existe - entries-read 1994, pendientes 0, sin leer (lag) 1007 | NO existe |
| grupo tablero | existe - entries-read 3001, pendientes 0, sin leer (lag) 0 | existe - entries-read 3001, pendientes 0, sin leer (lag) 0 | existe - entries-read 3001, pendientes 0, sin leer (lag) 0 | NO existe |
| claves ultimo:* | 16 | 16 | 16 | 0 |
| lecturas en ventana:* | 570 | 570 | 570 | 0 |
| catalogo tipo:* (estaciones) | 16 | 16 | 16 | 0 |
| contador producidos | 3001 | 3001 | 3001 | 0 |
| filas en HBase (<pref>lecturas) | 1,954 | (HBase no se reinicia) | 1,952 | (HBase no se reinicia) |

## Al final de la hora (se relanza la ingesta, el productor termina)

| config | filas en HBase / control | claves perdidas | ultimo:* correctos | ventanas correctas | reinicio tardo |
|---|---|---|---|---|---|
| aof: AOF everysec + volumen (la elegida) | 7,047 / 7,047 | 0 | 16/16 | 8/8 | 1.0 s |
| sin: sin persistencia (la contraria) | 6,057 / 7,047 | 990 | 16/16 | 8/8 | 0.6 s |

**Lectura**: con AOF sobrevive todo: el stream, los dos grupos con su posicion y sus pendientes, y el tablero; la
ingesta retoma donde quedo y HBase termina con las 7,047 filas. Sin persistencia Redis parte vacio: se pierde el
stream, los grupos (el consumidor los recrea desde 0 sobre un stream nuevo) y todo el tablero. El tablero se rearma
solo con los eventos que siguen llegando (al final de la hora vuelve a estar correcto porque "ahora" son los ultimos
10 min), pero lo que estaba en el stream y todavia no llegaba a HBase (el atraso de la ingesta al momento del
reinicio) **se pierde para siempre de la historia**: 990 claves.
