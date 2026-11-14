# P1 "Ahora" - Redis contra HBase

**Redis**: `SUNION tipo:*` (catalogo) + un pipeline con 16 `HGETALL ultimo:<est>` y `ZREVRANGE ranking:flujo 0 4 WITHSCORES`.
**HBase**: por cada estacion `scan(row_start=b'<est>#\xff', row_stop=b'<est>#', reverse=True, limit=1)` sobre `lecturas`
(la ultima fila del rango de la estacion = la medicion mas nueva). 16 scans; el top 5 se ordena en Python.

| estacion | tipo | valor Redis | ts Redis | valor HBase | ts HBase | igual |
|---|---|---|---|---|---|---|
| CC-001 | flujo_vehicular | 33.2 | 00:59:58.000 | 33.2 | 00:59:58.000 | si |
| CC-002 | flujo_vehicular | 62.7 | 00:59:54.000 | 62.7 | 00:59:54.000 | si |
| CC-003 | ocupacion_estacionamiento | 42.6 | 00:59:57.500 | 42.6 | 00:59:57.500 | si |
| CC-004 | calidad_aire | 73.4 | 00:59:53.000 | 73.4 | 00:59:53.000 | si |
| CC-005 | flujo_vehicular | 19.1 | 00:59:52.500 | 19.1 | 00:59:52.500 | si |
| CHI-001 | calidad_aire | 67.8 | 00:59:56.500 | 67.8 | 00:59:56.500 | si |
| COR-001 | calidad_aire | 19.4 | 00:59:43.500 | 19.4 | 00:59:43.500 | si |
| COR-002 | ocupacion_estacionamiento | 62.8 | 00:59:55.500 | 62.8 | 00:59:55.500 | si |
| HUA-001 | flujo_vehicular | 90.1 | 00:59:51.000 | 90.1 | 00:59:51.000 | si |
| HUA-002 | flujo_vehicular | 97.1 | 00:59:59.000 | 97.1 | 00:59:59.000 | si |
| PEN-001 | flujo_vehicular | 68.6 | 00:59:47.000 | 68.6 | 00:59:47.000 | si |
| SPP-001 | ocupacion_estacionamiento | 66.0 | 00:59:56.000 | 66.0 | 00:59:56.000 | si |
| SPP-002 | flujo_vehicular | 98.0 | 00:59:59.500 | 98.0 | 00:59:59.500 | si |
| TAL-001 | flujo_vehicular | 41.0 | 00:59:40.500 | 41.0 | 00:59:40.500 | si |
| TAL-002 | ocupacion_estacionamiento | 60.0 | 00:59:50.000 | 60.0 | 00:59:50.000 | si |
| TAL-003 | calidad_aire | 45.0 | 00:59:58.500 | 45.0 | 00:59:58.500 | si |

Top 5 flujo vehicular (Redis): SPP-002 = 98.0, HUA-002 = 97.1, HUA-001 = 90.1, PEN-001 = 68.6, CC-002 = 62.7
Top 5 flujo vehicular (HBase): SPP-002 = 98.0, HUA-002 = 97.1, HUA-001 = 90.1, PEN-001 = 68.6, CC-002 = 62.7 -> mismo resultado

## Tiempos

- Redis: **mediana 0.81 ms** (5 corridas; la primera, aparte: 9.08 ms) - todas: [9.08, 0.88, 0.82, 0.81, 0.74, 0.73]
- HBase: **mediana 36.00 ms** (5 corridas; la primera, aparte: 37.13 ms) - todas: [37.13, 41.66, 36.0, 38.38, 33.67, 35.08]
- HBase tarda **45 veces** lo que tarda Redis.

## Ultimo por medicion vs ultimo por llegada

| estacion | criterio | valor | timestamp de medicion | entrada del stream |
|---|---|---|---|---|
| TAL-001 | por medicion (tablero, HBase) | 41.0 | 2026-11-07T00:59:40.500-03:00 | - |
| TAL-001 | por llegada (ultima entrada del stream) | 48.9 | 2026-11-07T00:58:48.335-03:00 | 1794626739535-0 (evento n=7167) |

**Interpretacion**: un dashboard deberia leer de Redis: responde en 0.8 ms con 2 idas y vueltas
contra 36.0 ms y 16 scans en HBase, y su costo no depende de cuanta historia haya. En
TAL-001 la ultima lectura que llego es una lectura tardia (medida antes que otra que ya
habia llegado): si el tablero usara "lo ultimo que llego" mostraria un valor viejo. Por eso el tablero compara el ts
de medicion antes de escribir. HBase da lo mismo que Redis porque su clave ordena por medicion, pero no sabe nada
del orden de llegada (eso solo queda en el stream).
