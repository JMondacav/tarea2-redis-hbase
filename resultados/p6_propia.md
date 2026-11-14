# P6 La propia: "hace cuanto que no habia este flujo?"

Para cada estacion de flujo: el promedio de sus ultimos 10 min (ahora) y la ultima vez en la historia que tuvo una
ventana de 10 min con promedio igual o mayor. Ancla (ahora) = 2026-11-07T00:59:59.500-03:00.

- **Redis (el que)**: `SMEMBERS tipo:flujo_vehicular`, `GET tablero:ts_max` y `ZRANGEBYSCORE ventana:<est>` -> promedio de
  ahora de cada estacion, que define el orden y el umbral de cada una.
- **HBase (el cuando)**: `scan(row_start=b'<est>#<ancla - 10 min>', row_stop=b'<est>#', reverse=True)` en `lecturas`,
  con un promedio movil de 10 min (deque + suma) que se detiene en la primera ventana con promedio >= umbral.
  Solo ventanas que terminan antes de que empiece la de ahora (no se traslapan).

| rank | estacion | prom. 10 min ahora (Redis) | ultima ventana >= (fin, HBase) | su promedio | hace | filas recorridas |
|---|---|---|---|---|---|---|
| 1 | SPP-002 | 93.13 | 2026-11-07 00:49:56 | 95.99 | 10 min | 83 |
| 2 | HUA-001 | 92.08 | 2026-11-07 00:16:31 | 92.31 | 43 min | 322 |
| 3 | PEN-001 | 87.34 | 2026-11-06 21:52:30 | 87.44 | 3.1 h | 1,428 |
| 4 | HUA-002 | 76.50 | 2026-11-07 00:49:56 | 78.88 | 10 min | 74 |
| 5 | CC-002 | 58.80 | 2026-11-07 00:49:59 | 65.52 | 10 min | 79 |
| 6 | TAL-001 | 34.45 | 2026-11-07 00:42:26 | 35.05 | 18 min | 126 |
| 7 | CC-001 | 20.89 | 2026-11-07 00:49:58 | 25.12 | 10 min | 76 |
| 8 | CC-005 | 18.67 | 2026-11-07 00:32:25 | 18.98 | 28 min | 169 |

Tiempo (todo junto, 8 estaciones): **mediana 160.09 ms** (5 corridas; la primera, aparte: 186.06 ms) - todas: [186.06, 160.09, 175.8, 153.83, 155.99, 166.86] - 2,357 filas recorridas en HBase.

Como se lee: "10 min" quiere decir que la ventana justo anterior ya era igual o mas alta (la estacion viene
bajando o esta estable); las que salen con horas estan en un pico que no se veia hace rato, que es justo lo que
le interesa a quien mira el tablero.

**Por que necesita a los dos**: el promedio de ahora cambia cada pocos segundos y sale de la ventana que mantiene
el tablero con el stream (ultimo por medicion, tardias en su lugar): eso es Redis, y leerlo cuesta ~2 ms (P2). La
pregunta "desde cuando" necesita dias de historia por estacion, que en Redis no estan (P2: costaria GB de RAM al
año) y que en HBase estan ordenados por estacion y tiempo: el scan inverso desde "ahora" los recorre en el orden
justo y se detiene apenas encuentra la respuesta. Por eso el costo no es fijo: depende de que tan lejos este la
respuesta; si la estacion esta en su record hay que leer toda su historia (~54 mil filas en 5 dias, y creciendo).
