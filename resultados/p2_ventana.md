# P2 Ventana de 10 minutos - Redis

Ancla = medicion mas reciente del stream (`tablero:ts_max`) = **2026-11-07T00:59:59.500-03:00**.
Ventana = (ancla - 10 min, ancla]. Por estacion: `ZRANGEBYSCORE ventana:<est> (1794023399500 1794023999500`
(los 8 en un pipeline); el miembro es `"<ts>:<valor>"` y el promedio se saca de esos valores.

| estacion | lecturas en la ventana | promedio flujo (10 min) |
|---|---|---|
| CC-001 | 75 | 20.89 |
| CC-002 | 67 | 58.80 |
| CC-005 | 70 | 18.67 |
| HUA-001 | 62 | 92.08 |
| HUA-002 | 67 | 76.50 |
| PEN-001 | 86 | 87.34 |
| SPP-002 | 75 | 93.13 |
| TAL-001 | 74 | 34.45 |

Tiempo: **mediana 1.49 ms** (5 corridas; la primera, aparte: 4.86 ms) - todas: [4.86, 1.44, 1.6, 1.49, 1.47, 1.57]

## Por que un sorted set con el ts de medicion como score

- `ZRANGEBYSCORE` saca un rango de tiempo en O(log n + k), sin recorrer todo.
- Recortar es un solo `ZREMRANGEBYSCORE -inf <ancla-10min>`, tambien por score.
- Ordena por **medicion** y no por llegada: una lectura tardia cae en su lugar y una que llega con mas de 10 min
  de atraso respecto al ancla ni entra.
- El miembro lleva el ts, asi un evento repetido tal cual es el mismo miembro (ZADD no lo duplica).

## Memoria (MEMORY USAGE ... SAMPLES 0)

| estacion | entradas recortada | bytes recortada | encoding | entradas sin recortar (1 h) | bytes sin recortar | encoding |
|---|---|---|---|---|---|---|
| CC-001 | 75 | 2616 | listpack | 413 | 43656 | skiplist |
| CC-002 | 67 | 2104 | listpack | 441 | 46208 | skiplist |
| CC-005 | 70 | 2616 | listpack | 409 | 42808 | skiplist |
| HUA-001 | 62 | 2112 | listpack | 427 | 44912 | skiplist |
| HUA-002 | 67 | 2112 | listpack | 417 | 43656 | skiplist |
| PEN-001 | 86 | 3136 | listpack | 471 | 49496 | skiplist |
| SPP-002 | 75 | 2624 | listpack | 472 | 49344 | skiplist |
| TAL-001 | 74 | 2624 | listpack | 453 | 47360 | skiplist |

Total: recortadas **19,944 bytes** (576 entradas) vs sin recortar **367,440 bytes** (3503 entradas), o sea
18.4 veces mas con solo 1 hora. Sin recortar ademas pasa de `listpack` (compacto, hasta 128 entradas)
a `skiplist` (~105 bytes por entrada). A ritmo real (0,125 lecturas/s por estacion) seria ~9.1 MB
por dia para las 8 estaciones y ~3.3 GB al año, creciendo para siempre en RAM: la historia larga no va en Redis,
va en HBase.

**Interpretacion**: la ventana recortada es chica y de tamano fijo (~75 lecturas por estacion), por eso responde en
1.5 ms y cabe en memoria siempre; sin recorte crece lineal con el tiempo.
