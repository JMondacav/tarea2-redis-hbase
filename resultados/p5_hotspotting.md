# P5 Hotspotting y salting - HBase

La hora en vivo (7.200 eventos -> 7.047 claves) escrita en orden de llegada, en lotes de 50, en tres tablas.
Escrituras por region = `writeRequestCount` del JMX (`/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions`),
leido despues de esperar que se publique (~10 s). Suma un poco mas que 7.047 y no 7.200: happybase junta en una sola
mutacion los puts a la misma fila dentro de un lote, y un repetido llega pegado al original (casi siempre en el mismo
lote); solo cuentan doble los pocos que quedaron en el borde entre dos lotes.

**p5_est_ts** (4 regiones, 7,047 filas)

| region (inicio) | region (fin) | escrituras | % del total |
|---|---|---|---|
| (inicio) | CC-005 | 1,761 | 25.0% |
| CC-005 | HUA-001 | 1,708 | 24.2% |
| HUA-001 | SPP-002 | 1,732 | 24.6% |
| SPP-002 | (fin) | 1,848 | 26.2% |

**p5_ts_est** (7 regiones, 7,047 filas)

| region (inicio) | region (fin) | escrituras | % del total |
|---|---|---|---|
| (inicio) | 2026-11-03 | 0 | 0.0% |
| 2026-11-03 | 2026-11-04 | 0 | 0.0% |
| 2026-11-04 | 2026-11-05 | 0 | 0.0% |
| 2026-11-05 | 2026-11-06 | 0 | 0.0% |
| 2026-11-06 | 2026-11-07 | 5 | 0.1% |
| 2026-11-07 | 2026-11-08 | 7,044 | 99.9% |
| 2026-11-08 | (fin) | 0 | 0.0% |

**p5_salt** (4 regiones, 7,047 filas)

| region (inicio) | region (fin) | escrituras | % del total |
|---|---|---|---|
| (inicio) | 1 | 1,818 | 25.8% |
| 1 | 2 | 2,220 | 31.5% |
| 2 | 3 | 1,731 | 24.6% |
| 3 | (fin) | 1,280 | 18.2% |

## Tabla 3 x regiones (resumen)

| tabla | regiones | regiones que recibieron escrituras | la region mas cargada se llevo |
|---|---|---|---|
| p5_est_ts | 4 | 4 | 26% |
| p5_ts_est | 7 | 2 | 100% |
| p5_salt | 4 | 4 | 31% |

Reparto de estaciones por salt (crc32 mod 4): {0: 4, 1: 5, 2: 4, 3: 3}

## "Todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov"

| tabla | rangos | lecturas | mediana (ms) | primera (ms) |
|---|---|---|---|---|
| p5_est_ts | 16 | 1180 | 55.72 | 70.35 |
| p5_ts_est | 1 | 1180 | 9.63 | 11.03 |
| p5_salt | 4 | 1180 | 19.15 | 17.84 |

- p5_est_ts: **mediana 55.72 ms** (5 corridas; la primera, aparte: 70.35 ms) - todas: [70.35, 57.63, 55.72, 55.5, 48.35, 56.48]
- p5_ts_est: **mediana 9.63 ms** (5 corridas; la primera, aparte: 11.03 ms) - todas: [11.03, 9.6, 8.56, 9.63, 9.84, 9.73]
- p5_salt: **mediana 19.15 ms** (5 corridas; la primera, aparte: 17.84 ms) - todas: [17.84, 19.15, 18.32, 25.8, 19.93, 18.24]

**Interpretacion**: con `ts#estacion` todas las escrituras de la hora caen en la region del 07-nov (mas unas pocas
tardias en la del 06): una sola region, y en un cluster un solo servidor, recibe todo = hotspot. Con
`estacion#ts` y con salt la carga se reparte. Para leer "todo un intervalo" pasa al reves: `ts#estacion` lo
resuelve en 1 rango (10 ms), salt en 4 (19 ms)
y `estacion#ts` en 16 (56 ms): cada rango es un RPC mas, y con
~1.200 filas en una sola maquina lo que pesa es la cantidad de idas y vueltas, no el volumen. Elegiriamos `estacion#ts` (que ya es `lecturas`): reparte bien las
escrituras y la pregunta tipica es por estacion. Cambiariamos a salt si hubiera muchas consultas "todas las
estaciones en un intervalo" con un cluster de verdad, y `ts#estacion` solo serviria si la tasa de escritura fuera
baja y casi todas las consultas fueran por intervalo de tiempo.
